from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from akadze import Akadze, AkadzeError
from akadze.periodic import Schedule, previous_fire, schedule_due
from tests.pg import job_rows


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


def test_schedule_mapping_rejects_a_non_str_timezone(app: Akadze) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="timezone must be a str"):
        app.add_schedules([{"name": "tick", "cron": "0 9 * * *", "timezone": 3}])


async def test_zoned_schedule_from_mapping_feeds_schedule_due(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    app.add_schedules([{"name": "tick", "cron": "* * * * *", "timezone": "Asia/Kolkata"}])

    # Act
    await schedule_due(app)

    # Assert
    rows = await job_rows(database_url)
    assert [row["task"] for row in rows] == ["tick"]
    assert app.schedules[0].timezone == "Asia/Kolkata"
