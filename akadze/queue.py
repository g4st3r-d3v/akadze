"""Read the queue. This does not claim or finish jobs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


@dataclass(frozen=True)
class StateCount:
    queue: str
    state: str
    jobs: int


@dataclass(frozen=True)
class QueueSnapshot:
    counts: tuple[StateCount, ...]
    lag: timedelta | None
    busy: int


async def queue_snapshot(engine: AsyncEngine) -> QueueSnapshot:
    """Return depth, lag of the oldest ready job, and how many jobs are running."""

    async with engine.connect() as connection:
        rows = await connection.execute(
            text(
                """
                SELECT queue, state, count(*) AS jobs
                FROM akadze.jobs
                GROUP BY queue, state
                ORDER BY queue, state
                """
            )
        )
        lag = await connection.scalar(
            text(
                """
                SELECT now() - min(run_at)
                FROM akadze.jobs
                WHERE state = 'queued'
                  AND run_at <= now()
                  AND cancel_requested_at IS NULL
                  AND (expires_at IS NULL OR expires_at > now())
                """
            )
        )
        busy = await connection.scalar(
            text("SELECT count(*) FROM akadze.jobs WHERE state = 'running'")
        )
    counts = tuple(
        StateCount(queue=str(row.queue), state=str(row.state), jobs=int(row.jobs))
        for row in rows
    )
    if lag is not None and not isinstance(lag, timedelta):
        raise TypeError("queue lag must be a timedelta")
    return QueueSnapshot(counts=counts, lag=lag, busy=int(busy or 0))
