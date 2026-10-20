from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from akadze import Akadze, AkadzeError
from akadze.periodic import Schedule, previous_fire, schedule_due
from tests.pg import connection, job_rows


def _schedule(cron: str, timezone: str) -> Schedule:
    return Schedule(
        name="tick",
        cron=cron,
        every=None,
        overlap=True,
        queue="default",
        timezone=timezone,
    )


def test_cron_reads_the_wall_clock_of_the_zone() -> None:
    # Arrange
    schedule = _schedule("0 9 * * *", "Europe/Moscow")
    now = datetime(2026, 10, 20, 7, 0, tzinfo=UTC)

    # Act
    fire = previous_fire(schedule, now)

    # Assert
    assert fire == datetime(2026, 10, 20, 6, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (datetime(2026, 3, 29, 1, 15, tzinfo=UTC), datetime(2026, 3, 28, 1, 30, tzinfo=UTC)),
        (datetime(2026, 3, 29, 1, 30, tzinfo=UTC), datetime(2026, 3, 29, 1, 30, tzinfo=UTC)),
    ],
    ids=["inside-the-gap", "gap-shifted-fire"],
)
def test_skipped_wall_time_fires_shifted_by_the_gap(now: datetime, expected: datetime) -> None:
    # Arrange: 02:30 does not exist in Berlin on 2026-03-29 (02:00 -> 03:00)
    schedule = _schedule("30 2 * * *", "Europe/Berlin")

    # Act
    fire = previous_fire(schedule, now)

    # Assert
    assert fire == expected


@pytest.mark.parametrize(
    "now",
    [
        datetime(2026, 10, 25, 0, 40, tzinfo=UTC),
        datetime(2026, 10, 25, 1, 40, tzinfo=UTC),
    ],
    ids=["first-pass", "repeated-hour"],
)
def test_repeated_wall_time_fires_once(now: datetime) -> None:
    # Arrange: 02:30 happens twice in Berlin on 2026-10-25 (03:00 -> 02:00)
    schedule = _schedule("30 2 * * *", "Europe/Berlin")

    # Act
    fire = previous_fire(schedule, now)

    # Assert
    assert fire == datetime(2026, 10, 25, 0, 30, tzinfo=UTC)


def _fires(schedule: Schedule, start: datetime, end: datetime, step: timedelta) -> list[datetime]:
    seen: list[datetime] = []
    now = start
    while now <= end:
        fire = previous_fire(schedule, now)
        if fire is not None and fire not in seen:
            seen.append(fire)
        now += step
    return seen


def test_hourly_wildcard_keeps_firing_through_the_repeated_hour() -> None:
    # Arrange
    schedule = _schedule("*/15 * * * *", "Europe/Berlin")
    start = datetime(2026, 10, 25, 0, 0, tzinfo=UTC)

    # Act
    fires = _fires(schedule, start, start + timedelta(hours=2), timedelta(minutes=5))

    # Assert
    assert fires == [start + timedelta(minutes=15 * n) for n in range(9)]


def test_every_minute_of_a_skipped_hour_fires_shifted() -> None:
    # Arrange
    schedule = _schedule("* 2 * * *", "Europe/Berlin")
    start = datetime(2026, 3, 29, 1, 0, tzinfo=UTC)

    # Act
    fires = _fires(schedule, start, start + timedelta(minutes=59), timedelta(minutes=1))

    # Assert
    assert fires == [start + timedelta(minutes=n) for n in range(60)]


def test_nickname_cron_in_a_dst_zone() -> None:
    # Arrange
    schedule = _schedule("@daily", "Europe/Berlin")
    now = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)

    # Act
    fire = previous_fire(schedule, now)

    # Assert
    assert fire == datetime(2026, 10, 24, 22, 0, tzinfo=UTC)


def test_zone_without_transition_uses_the_plain_offset() -> None:
    # Arrange
    schedule = _schedule("30 9 * * 1", "Asia/Kolkata")
    now = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)

    # Act
    fire = previous_fire(schedule, now)

    # Assert
    assert fire == datetime(2026, 10, 19, 4, 0, tzinfo=UTC)


def test_unknown_timezone_is_rejected(app: Akadze) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="unknown timezone"):
        app.add_schedule("tick", cron="0 9 * * *", timezone="Mars/Olympus")


def test_timezone_on_an_interval_is_rejected(app: Akadze) -> None:
    # Arrange / Act / Assert
    with pytest.raises(AkadzeError, match="cron schedules only"):

        @app.periodic("tick", every=timedelta(minutes=5), timezone="Europe/Moscow")
        async def tick() -> None:
            return None


def test_zoneinfo_object_is_rejected_with_a_hint(app: Akadze) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="IANA zone name"):
        app.add_schedule("tick", cron="0 9 * * *", timezone=ZoneInfo("Europe/Berlin"))  # type: ignore[arg-type]


def test_schedule_mapping_rejects_a_non_str_timezone(app: Akadze) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="timezone must be a str"):
        app.add_schedules([{"name": "tick", "cron": "0 9 * * *", "timezone": 3}])


async def test_zoned_schedule_fires_at_the_local_hour(app: Akadze, database_url: str) -> None:
    # Arrange: a cron for the current Kolkata minute, which never matches UTC (+05:30)
    async with connection(database_url) as opened:
        now = await opened.fetchval("SELECT now()")
        if now.second >= 50:
            await asyncio.sleep(61 - now.second)
            now = await opened.fetchval("SELECT now()")
    local = now.astimezone(ZoneInfo("Asia/Kolkata"))

    @app.task("tick")
    async def tick() -> None:
        return None

    cron = f"{local.minute} {local.hour} * * *"
    app.add_schedules([{"name": "tick", "cron": cron, "timezone": "Asia/Kolkata"}])

    # Act
    await schedule_due(app)

    # Assert
    rows = await job_rows(database_url)
    assert [row["task"] for row in rows] == ["tick"]
    async with connection(database_url) as opened:
        fire_at = await opened.fetchval("SELECT fire_at FROM akadze.periodic_runs")
    assert fire_at == local.replace(second=0, microsecond=0).astimezone(UTC)
