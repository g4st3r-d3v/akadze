"""Insert at most one job per schedule fire. Every worker may try."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter  # type: ignore[import-untyped]
from sqlalchemy import text

from akadze.args import dump_arguments
from akadze.enqueue import insert_job
from akadze.exc import AkadzeError, DuplicateJob


@dataclass(frozen=True)
class Schedule:
    name: str
    cron: str | None
    every: timedelta | None
    overlap: bool
    queue: str
    timezone: str | None = None


@dataclass(frozen=True)
class ScheduleSpec:
    """Already-parsed schedule input for ``Akadze.add_schedule`` / ``add_schedules``.

    The app owns YAML/TOML/JSON and duration strings; core takes cron text or a
    ``timedelta``, not a config file.
    """

    name: str
    cron: str | None = None
    every: timedelta | None = None
    overlap: bool = True
    queue: str | None = None
    timezone: str | None = None


def check_timezone(timezone: str | None, *, every: timedelta | None) -> None:
    """Reject an unknown IANA zone, or a zone on an interval schedule."""

    if timezone is None:
        return
    if every is not None:
        raise AkadzeError("timezone applies to cron schedules only")
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError):
        raise AkadzeError(f"unknown timezone {timezone!r}") from None


def previous_fire(schedule: Schedule, now: datetime) -> datetime | None:
    if schedule.cron is not None and schedule.timezone is not None:
        return _previous_local_fire(schedule.cron, ZoneInfo(schedule.timezone), now)
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


def _previous_local_fire(cron: str, zone: ZoneInfo, now: datetime) -> datetime | None:
    """Latest fire at or before now, with cron read on the zone's wall clock.

    A wall time repeated when clocks go back fires once (the first one). A wall
    time skipped when clocks go forward fires shifted by the gap.
    """

    wall = now.astimezone(zone).replace(tzinfo=None)
    fires = croniter(cron, wall + timedelta(seconds=1))
    for _ in range(3):
        candidate = fires.get_prev(datetime)
        if not isinstance(candidate, datetime):
            return None
        fire = candidate.replace(tzinfo=zone, fold=0).astimezone(UTC)
        if fire <= now:
            return fire
    return None


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
            limit_key=None,
            delay=None,
            run_at=None,
            expires_at=None,
            hooks=app.hooks,
        )
