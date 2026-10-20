"""Cluster concurrency and start-rate limits.

Commands change state and return nothing. Queries return state and change nothing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from akadze.exc import AkadzeError


@dataclass(frozen=True)
class LimitSummary:
    key: str
    max_running: int | None
    max_starts: int | None
    window: timedelta | None


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


async def clear_limit(session: AsyncConnection | AsyncSession, key: str) -> None:
    """Delete the limits row and the start marks for key. Idempotent if the key has no row.

    Removes the policy so the key is unlimited: jobs that still carry this
    limit_key are claimed like jobs with no limit. A later set_limit on the
    key starts its rate window from zero.
    """

    if not key:
        raise AkadzeError("limit key must not be empty")

    connection = await _connection(session)
    await connection.execute(
        text("DELETE FROM akadze.limits WHERE key = :key"),
        {"key": key},
    )
    await connection.execute(
        text("DELETE FROM akadze.limit_starts WHERE key = :key"),
        {"key": key},
    )


async def list_limits(engine: AsyncEngine) -> tuple[LimitSummary, ...]:
    """Return every limits row. Does not mutate state."""

    async with engine.connect() as connection:
        rows = await connection.execute(
            text(
                """
                SELECT key, max_running, max_starts, "window"
                FROM akadze.limits
                ORDER BY key
                """
            )
        )
        return tuple(
            LimitSummary(
                key=str(row.key),
                max_running=int(row.max_running) if row.max_running is not None else None,
                max_starts=int(row.max_starts) if row.max_starts is not None else None,
                window=_as_timedelta(row.window),
            )
            for row in rows
        )


def _as_timedelta(value: object) -> timedelta | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    raise TypeError("limit window must be a timedelta")


async def _connection(session: AsyncConnection | AsyncSession) -> AsyncConnection:
    if isinstance(session, AsyncConnection):
        connection = session
    elif isinstance(session, AsyncSession):
        connection = await session.connection()
    else:
        raise AkadzeError("session must be an AsyncConnection or AsyncSession")
    if not connection.in_transaction():
        raise AkadzeError("limit commands must run inside the caller's transaction")
    return connection
