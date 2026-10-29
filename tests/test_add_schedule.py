from __future__ import annotations

from datetime import timedelta

import pytest

from akadze import Akadze, AkadzeError
from akadze.periodic import ScheduleSpec, schedule_due
from tests.pg import job_rows


async def test_add_schedule_feeds_schedule_due(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    app.add_schedule("tick", cron="* * * * *")

    # Act
    await schedule_due(app)

    # Assert
    rows = await job_rows(database_url)
    assert len(rows) == 1
    assert rows[0]["task"] == "tick"
    assert rows[0]["queue"] == "default"
    assert rows[0]["state"] == "queued"


async def test_add_schedule_uses_task_queue_when_queue_omitted(app: Akadze) -> None:
    # Arrange
    @app.task("ops.prune", queue="ops")
    async def prune() -> None:
        return None

    # Act
    app.add_schedule("ops.prune", every=timedelta(minutes=1))

    # Assert
    assert len(app.schedules) == 1
    assert app.schedules[0].name == "ops.prune"
    assert app.schedules[0].queue == "ops"
    assert app.schedules[0].every == timedelta(minutes=1)
    assert app.schedules[0].cron is None


async def test_add_schedule_queue_override(app: Akadze) -> None:
    # Arrange
    @app.task("tick", queue="default")
    async def tick() -> None:
        return None

    # Act
    app.add_schedule("tick", cron="0 * * * *", queue="ops", overlap=False)

    # Assert
    schedule = app.schedules[0]
    assert schedule.queue == "ops"
    assert schedule.overlap is False
    assert schedule.cron == "0 * * * *"


async def test_add_schedule_rejects_unknown_task(app: Akadze) -> None:
    # Arrange — no task named missing

    # Act / Assert
    with pytest.raises(AkadzeError, match="task missing is not registered"):
        app.add_schedule("missing", cron="* * * * *")


async def test_add_schedule_rejects_both_cron_and_every(app: Akadze) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="schedule needs cron or every"):
        app.add_schedule(
            "tick",
            cron="* * * * *",
            every=timedelta(seconds=1),
        )


@pytest.mark.parametrize(
    ("cron", "match"),
    [
        ("not a cron", "is not a valid expression"),
        ("61 * * * *", "is not a valid expression"),
        ("0 0 31 2 *", "never fires"),
        (5, "cron must be a str"),
    ],
)
async def test_add_schedule_rejects_a_cron_the_scheduler_cannot_read(
    app: Akadze, cron: str, match: str
) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match=match):
        app.add_schedule("tick", cron=cron)
    assert app.schedules == []


@pytest.mark.parametrize(
    ("cron", "fields"),
    [("@hourly", "0 * * * *"), (" @Daily ", "0 0 * * *"), ("0  9 * * *", "0 9 * * *")],
)
async def test_add_schedule_keeps_the_cron_as_five_fields(
    app: Akadze, cron: str, fields: str
) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    # Act
    app.add_schedule("tick", cron=cron)

    # Assert
    assert app.schedules[0].cron == fields


async def test_add_schedule_rejects_a_task_that_needs_arguments(app: Akadze) -> None:
    # Arrange — a fire enqueues the task with no arguments
    @app.task("report")
    async def report(day: str) -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="schedule report: the task needs arguments"):
        app.add_schedule("report", every=timedelta(minutes=1))
    assert app.schedules == []


async def test_periodic_rejects_a_task_that_needs_arguments(app: Akadze) -> None:
    # Arrange — nothing registered yet

    # Act / Assert
    with pytest.raises(AkadzeError, match="the task needs arguments"):

        @app.periodic("report", every=timedelta(minutes=1))
        async def report(day: str) -> None:
            return None

    assert "report" not in app.tasks


@pytest.mark.parametrize("cron", ["0 0 * * * *", "0 0 1 1 * 0 2020"])
async def test_periodic_warns_about_a_cron_with_extra_fields(
    app: Akadze, caplog: pytest.LogCaptureFixture, cron: str
) -> None:
    # Arrange — croniter reads a sixth field as seconds and a seventh as a year

    # Act
    @app.periodic("tick", cron=cron)
    async def tick() -> None:
        return None

    # Assert
    assert "more than five fields" in caplog.text
    assert app.schedules[0].cron == cron


async def test_add_schedules_from_dicts_and_specs(app: Akadze) -> None:
    # Arrange
    @app.task("a")
    async def a() -> None:
        return None

    @app.task("b", queue="ops")
    async def b() -> None:
        return None

    # Act
    app.add_schedules(
        [
            {"name": "a", "cron": "0 * * * *"},
            ScheduleSpec(name="b", every=timedelta(seconds=30), overlap=False),
        ]
    )

    # Assert
    assert [s.name for s in app.schedules] == ["a", "b"]
    assert app.schedules[0].cron == "0 * * * *"
    assert app.schedules[0].queue == "default"
    assert app.schedules[1].every == timedelta(seconds=30)
    assert app.schedules[1].overlap is False
    assert app.schedules[1].queue == "ops"


async def test_add_schedules_rejects_string_every(app: Akadze) -> None:
    # Arrange
    @app.task("tick")
    async def tick() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="schedule every must be a timedelta"):
        app.add_schedules([{"name": "tick", "every": "30s"}])
