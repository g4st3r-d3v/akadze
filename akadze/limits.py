"""Cluster concurrency limits. Commands change state and return nothing."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from akadze.exc import AkadzeError


async def set_limit(
    session: AsyncConnection | AsyncSession, key: str, max_running: int
) -> None:
    """Upsert max concurrent running jobs for a limit_key. Replaces previous N."""

    if not key:
        raise AkadzeError("limit key must not be empty")
    if max_running < 1:
        raise AkadzeError("max_running must be at least 1")
    connection = await _connection(session)
    await connection.execute(
        text(
            """
            INSERT INTO akadze.limits (key, max_running)
            VALUES (:key, :max_running)
            ON CONFLICT (key) DO UPDATE
            SET max_running = EXCLUDED.max_running
            """
        ),
        {"key": key, "max_running": max_running},
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
