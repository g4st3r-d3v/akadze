"""Pause and resume queues. Commands change state and return nothing."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from akadze.exc import AkadzeError


async def pause_queue(session: AsyncConnection | AsyncSession, queue: str) -> None:
    """Stop new claims from this queue. Running jobs finish. Idempotent."""

    connection = await _connection(session)
    await connection.execute(
        text(
            """
            INSERT INTO akadze.queue_pauses (queue)
            VALUES (:queue)
            ON CONFLICT (queue) DO NOTHING
            """
        ),
        {"queue": queue},
    )


async def resume_queue(session: AsyncConnection | AsyncSession, queue: str) -> None:
    """Allow claims from this queue again. Idempotent if the queue was not paused."""

    connection = await _connection(session)
    await connection.execute(
        text("DELETE FROM akadze.queue_pauses WHERE queue = :queue"),
        {"queue": queue},
    )


async def _connection(session: AsyncConnection | AsyncSession) -> AsyncConnection:
    if isinstance(session, AsyncConnection):
        connection = session
    elif isinstance(session, AsyncSession):
        connection = await session.connection()
    else:
        raise AkadzeError("session must be an AsyncConnection or AsyncSession")
    if not connection.in_transaction():
        raise AkadzeError("pause and resume must run inside the caller's transaction")
    return connection
