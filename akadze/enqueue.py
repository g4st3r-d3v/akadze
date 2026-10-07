"""Insert a job into the caller's transaction."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from akadze.exc import EnqueueError
from akadze.hooks import Hooks
from akadze.job import _COLUMNS, job_from_mapping

_PRIORITY_MIN = -32768
_PRIORITY_MAX = 32767


async def insert_job(
    session: AsyncConnection | AsyncSession,
    *,
    task: str,
    queue: str,
    priority: int,
    max_attempts: int,
    args: dict[str, Any],
    unique_key: str | None,
    delay: timedelta | None,
    run_at: datetime | None,
    hooks: Hooks,
) -> None:
    if delay is not None and run_at is not None:
        raise EnqueueError("pass delay or run_at, not both")
    if delay is not None and delay < timedelta(0):
        raise EnqueueError("delay must not be negative")
    if run_at is not None and run_at.tzinfo is None:
        raise EnqueueError("run_at must be timezone-aware")
    if not _PRIORITY_MIN <= priority <= _PRIORITY_MAX:
        raise EnqueueError("priority must fit in smallint")
    connection = await _connection(session)
    if delay is not None:
        run_at_sql = "now() + CAST(:delay AS interval)"
    elif run_at is not None:
        run_at_sql = ":run_at"
    else:
        run_at_sql = "now()"
    result = await connection.execute(
        text(
            f"""
            INSERT INTO akadze.jobs (
                task, queue, priority, state, args, max_attempts, run_at, unique_key
            )
            VALUES (
                :task, :queue, :priority, 'queued', CAST(:args AS jsonb), :max_attempts,
                {run_at_sql}, :unique_key
            )
            RETURNING {_COLUMNS}, false AS expired
            """
        ),
        {
            "task": task,
            "queue": queue,
            "priority": priority,
            "args": json.dumps(args),
            "max_attempts": max_attempts,
            "delay": delay,
            "run_at": run_at,
            "unique_key": unique_key,
        },
    )
    row = result.mappings().one()
    job = job_from_mapping(row)
    await hooks.ran_enqueue(connection, job)


async def request_cancel(session: AsyncConnection | AsyncSession, job_id: int) -> None:
    """Ask a queued or running job to stop. The worker applies the cancellation."""

    connection = await _connection(session)
    await connection.execute(
        text(
            """
            UPDATE akadze.jobs
            SET cancel_requested_at = now()
            WHERE id = :id AND state IN ('queued', 'running')
            """
        ),
        {"id": job_id},
    )


async def _connection(session: AsyncConnection | AsyncSession) -> AsyncConnection:
    if isinstance(session, AsyncConnection):
        connection = session
    elif isinstance(session, AsyncSession):
        connection = await session.connection()
    else:
        raise EnqueueError("session must be an AsyncConnection or AsyncSession")
    if not connection.in_transaction():
        raise EnqueueError("enqueue must run inside the caller's transaction")
    return connection
