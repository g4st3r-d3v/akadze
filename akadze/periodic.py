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


def check_schedule(cron: str | None, every: timedelta | None, timezone: object) -> None:
    """Validate schedule options shared by ``@periodic`` and ``add_schedule``."""

    if (cron is None) == (every is None):
        raise AkadzeError("schedule needs cron or every")
    if every is not None and every <= timedelta(0):
        raise AkadzeError("every must be positive")
    if cron is not None:
        _check_cron(cron)
    if timezone is None:
        return
    if not isinstance(timezone, str):
        raise AkadzeError("timezone must be an IANA zone name (str)")
    if every is not None:
        raise AkadzeError("timezone applies to cron schedules only")
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError):
        raise AkadzeError(f"unknown timezone {timezone!r}") from None


def _check_cron(cron: str) -> None:
    """Reject what the scheduler tick could not read, before a worker runs it.

    Five fields, as in classic cron; croniter would read a sixth as seconds.
    """

    fields = cron.split()
    if not cron.startswith("@") and len(fields) != 5:
        raise AkadzeError(f"cron {cron!r} must have five fields")
    if not croniter.is_valid(cron):
        raise AkadzeError(f"cron {cron!r} is not a valid expression")


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


_DST_PROBE = timedelta(hours=12)
_MAX_CANDIDATES = 10_000


def _previous_local_fire(cron: str, zone: ZoneInfo, now: datetime) -> datetime | None:
    """Latest fire at or before now, with cron read on the zone's wall clock.

    DST follows classic cron. When clocks go back, a fixed-hour entry fires once
    (first occurrence) and an entry with ``*`` in the hour field fires in both
    passes. When clocks go forward, a fixed-hour wall time inside the gap fires
    shifted by the gap; an hourly-wildcard one just has no such minute.
    """

    wall = now.astimezone(zone).replace(tzinfo=None)
    offsets = {
        _seconds((now + step).astimezone(zone).utcoffset())
        for step in (-_DST_PROBE, timedelta(0), _DST_PROBE)
    }
    if len(offsets) == 1:
        fires = croniter(cron, wall + timedelta(seconds=1))
        candidate = fires.get_prev(datetime)
        if not isinstance(candidate, datetime):
            return None
        return candidate.replace(tzinfo=zone).astimezone(UTC)
    margin = timedelta(seconds=max(offsets) - min(offsets), minutes=1)
    fields = cron.split()
    hourly = len(fields) > 1 and "*" in fields[1]
    fires = croniter(cron, wall + margin)
    best: datetime | None = None
    for _ in range(_MAX_CANDIDATES):
        candidate = fires.get_prev(datetime)
        if not isinstance(candidate, datetime):
            break
        if best is not None and candidate < wall - margin:
            break
        for fire in _instants(candidate, zone, hourly=hourly):
            if fire <= now and (best is None or fire > best):
                best = fire
    return best


def _instants(local: datetime, zone: ZoneInfo, *, hourly: bool) -> list[datetime]:
    first = local.replace(tzinfo=zone, fold=0)
    second = local.replace(tzinfo=zone, fold=1)
    real = first.astimezone(UTC).astimezone(zone).replace(tzinfo=None) == local
    if not real:
        return [] if hourly else [first.astimezone(UTC)]
    if first.utcoffset() != second.utcoffset():
        repeated = [first.astimezone(UTC), second.astimezone(UTC)]
        return repeated if hourly else repeated[:1]
    return [first.astimezone(UTC)]


def _seconds(offset: timedelta | None) -> float:
    return 0.0 if offset is None else offset.total_seconds()


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
