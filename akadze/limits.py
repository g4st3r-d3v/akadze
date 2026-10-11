"""Cluster concurrency and start-rate limits. Commands change state and return nothing."""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from akadze.exc import AkadzeError


async def set_limit(
    session: AsyncConnection | AsyncSession,
    key: str,
    max_running: int | None = None,
    *,
    max_starts: int | None = None,
    window: timedelta | None = None,
) -> None:
    """Upsert limit policies for a limit_key. Omitted fields keep previous values."""

    if not key:
        raise AkadzeError("limit key must not be empty")

    set_concurrency = max_running is not None
    set_rate = max_starts is not None or window is not None
    if not set_concurrency and not set_rate:
        raise AkadzeError("set_limit requires max_running and/or max_starts with window")
    if max_running is not None and max_running < 1:
        raise AkadzeError("max_running must be at least 1")
    if set_rate:
        if max_starts is None or window is None:
            raise AkadzeError("max_starts and window must be set together")
        if max_starts < 1:
            raise AkadzeError("max_starts must be at least 1")
        if window <= timedelta(0):
            raise AkadzeError("window must be positive")

    connection = await _connection(session)
    await connection.execute(
        text(
            """
            INSERT INTO akadze.limits (key, max_running, max_starts, "window")
            VALUES (:key, :max_running, :max_starts, :window)
            ON CONFLICT (key) DO UPDATE
            SET max_running = CASE
                    WHEN :set_concurrency THEN EXCLUDED.max_running
                    ELSE akadze.limits.max_running
                END,
                max_starts = CASE
                    WHEN :set_rate THEN EXCLUDED.max_starts
                    ELSE akadze.limits.max_starts
                END,
                "window" = CASE
                    WHEN :set_rate THEN EXCLUDED."window"
                    ELSE akadze.limits."window"
                END
            """
        ),
        {
            "key": key,
            "max_running": max_running,
            "max_starts": max_starts,
            "window": window,
            "set_concurrency": set_concurrency,
            "set_rate": set_rate,
        },
    )


async def _connection(session: AsyncConnection | AsyncSession) -> AsyncConnection:
    if isinstance(session, AsyncConnection):
        connection = session
    elif isinstance(session, AsyncSession):
        connection = await session.connection()
    else:
        raise AkadzeError("session must be an AsyncConnection or AsyncSession")
    if not connection.in_transaction():
        raise AkadzeError("set_limit must run inside the caller's transaction")
    return connection
