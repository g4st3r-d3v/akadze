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
