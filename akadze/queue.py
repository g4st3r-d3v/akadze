"""Read the queue. This does not claim or finish jobs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from akadze.exc import AkadzeError

_JOB_STATES = frozenset({"queued", "running", "succeeded", "failed", "cancelled"})
_LIMIT_MIN = 1
_LIMIT_MAX = 1000
_LIMIT_DEFAULT = 50


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


@dataclass(frozen=True)
class JobSummary:
    id: int
    task: str
    queue: str
    state: str
    priority: int
    attempt: int
    max_attempts: int
    run_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


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
        StateCount(queue=str(row.queue), state=str(row.state), jobs=int(row.jobs)) for row in rows
    )
    if lag is not None and not isinstance(lag, timedelta):
        raise TypeError("queue lag must be a timedelta")
    return QueueSnapshot(counts=counts, lag=lag, busy=int(busy or 0))


async def list_jobs(
    engine: AsyncEngine,
    *,
    queue: str | None = None,
    state: str | None = None,
    limit: int = _LIMIT_DEFAULT,
) -> tuple[JobSummary, ...]:
    """Return job summaries. Optional filters; does not mutate rows."""

    if not _LIMIT_MIN <= limit <= _LIMIT_MAX:
        raise AkadzeError(f"limit must be between {_LIMIT_MIN} and {_LIMIT_MAX}")
    if state is not None and state not in _JOB_STATES:
        raise AkadzeError(f"unknown state {state!r}")

    clauses = ["TRUE"]
    params: dict[str, object] = {"limit": limit}
    if queue is not None:
        clauses.append("queue = :queue")
        params["queue"] = queue
    if state is not None:
        clauses.append("state = :state")
        params["state"] = state
    where = " AND ".join(clauses)

    async with engine.connect() as connection:
        rows = await connection.execute(
            text(
                f"""
                SELECT
                    id, task, queue, state, priority, attempt, max_attempts,
                    run_at, started_at, finished_at
                FROM akadze.jobs
                WHERE {where}
                ORDER BY priority DESC, run_at, id
                LIMIT :limit
                """
            ),
            params,
        )
        return tuple(
            JobSummary(
                id=int(row.id),
                task=str(row.task),
                queue=str(row.queue),
                state=str(row.state),
                priority=int(row.priority),
                attempt=int(row.attempt),
                max_attempts=int(row.max_attempts),
                run_at=row.run_at,
                started_at=row.started_at,
                finished_at=row.finished_at,
            )
            for row in rows
        )
