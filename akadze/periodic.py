"""Insert at most one job per schedule fire. Every worker may try."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from croniter import croniter  # type: ignore[import-untyped]
from sqlalchemy import text

from akadze.args import dump_arguments
from akadze.enqueue import insert_job
from akadze.exc import DuplicateJob


@dataclass(frozen=True)
class Schedule:
    name: str
    cron: str | None
    every: timedelta | None
    overlap: bool
    queue: str


def previous_fire(schedule: Schedule, now: datetime) -> datetime | None:
    if schedule.cron is not None:
        previous = croniter(schedule.cron, now + timedelta(seconds=1)).get_prev(datetime)
        if not isinstance(previous, datetime):
            return None
        if previous.tzinfo is None and now.tzinfo is not None:
            return previous.replace(tzinfo=now.tzinfo)
        return previous
    if schedule.every is None:
        return None
    seconds = schedule.every.total_seconds()
    slot = now.timestamp() - (now.timestamp() % seconds)
    return datetime.fromtimestamp(slot, tz=now.tzinfo)


async def schedule_due(app: Any) -> None:
    """Create due periodic jobs. Returns nothing."""

    for schedule in list(app.schedules):
        try:
            await _schedule_one(app, schedule)
        except DuplicateJob:
            continue


async def _schedule_one(app: Any, schedule: Schedule) -> None:
    async with app.engine.begin() as connection:
        now = await connection.scalar(text("SELECT now()"))
        if not isinstance(now, datetime):
            return
        fire_at = previous_fire(schedule, now)
        if fire_at is None or now - fire_at > app.grace:
            return
        inserted = await connection.execute(
            text(
                """
                INSERT INTO akadze.periodic_runs (name, fire_at)
                VALUES (:name, :fire_at)
                ON CONFLICT DO NOTHING
                RETURNING name
                """
            ),
            {"name": schedule.name, "fire_at": fire_at},
        )
        if inserted.first() is None:
            return
        task = app.tasks[schedule.name]
        await insert_job(
            connection,
            task=task.name,
            queue=schedule.queue,
            priority=task.priority,
            max_attempts=task.max_attempts,
            args=dump_arguments(task.arguments, {}, task=task.name),
            unique_key=None if schedule.overlap else f"periodic:{schedule.name}",
            delay=None,
            run_at=None,
            expires_at=None,
            hooks=app.hooks,
        )
