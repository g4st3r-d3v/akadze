"""Helpers for tests that run against real Postgres."""

from __future__ import annotations

import json
from typing import Any

from pydantic_core import to_jsonable_python
from sqlalchemy import text

from akadze.app import Akadze
from akadze.exc import AkadzeError
from akadze.worker import Worker

_DRAIN_LIMIT = 1000

_READY = """
    SELECT
        task,
        bool_or(
            cancel_requested_at IS NULL
            AND (expires_at IS NULL OR expires_at > now())
            AND NOT EXISTS (
                SELECT 1 FROM akadze.queue_pauses WHERE queue_pauses.queue = jobs.queue
            )
        ) AS claimable
    FROM akadze.jobs
    WHERE state = 'queued'
      AND queue = ANY(CAST(:queues AS text[]))
      AND run_at <= now()
    GROUP BY task
"""


async def drain(app: Akadze, *, limit: int = _DRAIN_LIMIT) -> None:
    """Run every job that is ready now. Jobs scheduled in the future stay queued.

    Raises AkadzeError if a ready job's task is not registered on ``app`` (a
    worker would leave it queued), or if jobs are still ready after `limit` runs.
    """

    if limit < 1:
        raise AkadzeError("drain limit must be at least 1")
    worker = Worker(app, queues=await _queues(app))
    for _ in range(limit):
        worker.queues = await _queues(app)
        if not await _ready(app, worker.queues):
            return
        await worker.step()
    worker.queues = await _queues(app)
    if await _ready(app, worker.queues):
        raise AkadzeError("drain stopped: jobs are still ready")


async def _ready(app: Akadze, queues: list[str]) -> bool:
    """Whether any job is ready. Raises on a claimable job of an unregistered task.

    A cancelled, expired or paused job is ready (a step cancels the first two)
    but not claimable, so its task need not be registered.
    """

    async with app.engine.connect() as connection:
        rows = (await connection.execute(text(_READY), {"queues": queues})).all()
    unknown = sorted(str(row[0]) for row in rows if row[1] and row[0] not in app.tasks)
    if unknown:
        raise AkadzeError(f"drain stopped: tasks are not registered: {', '.join(unknown)}")
    return bool(rows)


async def assert_enqueued(app: Akadze, task: str, **arguments: Any) -> None:
    """Raise AssertionError unless a queued or running job has this task and these arguments.

    Arguments are compared in their stored JSON form, so models, ``UUID`` and
    ``datetime`` can be passed as the task receives them.
    """

    async with app.engine.connect() as connection:
        rows = await connection.execute(
            text(
                """
                SELECT args
                FROM akadze.jobs
                WHERE task = :task AND state IN ('queued', 'running')
                """
            ),
            {"task": task},
        )
        expected = to_jsonable_python(arguments)
        for row in rows:
            if _contains(_decode(row[0]), expected):
                return
    raise AssertionError(f"{task} is not enqueued")


def _decode(value: object) -> object:
    if isinstance(value, str) and value[:1] == "{":
        return json.loads(value)
    return value


def _contains(stored: object, arguments: dict[str, Any]) -> bool:
    if not isinstance(stored, dict):
        return False
    return all(stored.get(key) == value for key, value in arguments.items())


async def _queues(app: Akadze) -> list[str]:
    names = {task.queue for task in app.tasks.values()}
    async with app.engine.connect() as connection:
        rows = await connection.execute(
            text("SELECT DISTINCT queue FROM akadze.jobs WHERE state = 'queued'")
        )
        names.update(str(row[0]) for row in rows)
    return sorted(names) or ["default"]
