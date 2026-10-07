"""Helpers for tests that run against real Postgres."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text

from akadze.app import Akadze
from akadze.exc import AkadzeError
from akadze.worker import Worker

_DRAIN_LIMIT = 1000

_READY = """
    SELECT count(*)
    FROM akadze.jobs
    WHERE state = 'queued'
      AND queue = :queue
      AND run_at <= now()
"""


async def drain(app: Akadze, *, limit: int = _DRAIN_LIMIT) -> None:
    """Run every job that is ready now. Jobs scheduled in the future stay queued.

    Raises AkadzeError if jobs are still ready after `limit` runs.
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


async def assert_enqueued(app: Akadze, task: str, **arguments: Any) -> None:
    """Raise AssertionError unless a queued or running job has this task and these arguments."""

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
        for row in rows:
            if _contains(_decode(row[0]), arguments):
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


async def _ready(app: Akadze, queues: list[str]) -> bool:
    async with app.engine.connect() as connection:
        for queue in queues:
            count = await connection.scalar(text(_READY), {"queue": queue})
            if int(count or 0) > 0:
                return True
    return False
