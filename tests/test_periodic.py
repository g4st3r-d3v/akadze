from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from akadze import Akadze
from akadze.periodic import Schedule, schedule_due
from tests.pg import connection, job_rows


async def test_three_workers_create_one_job_per_fire(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.periodic("tick", cron="* * * * *")
    async def tick() -> None:
        return None

    # Act
    await asyncio.gather(schedule_due(app), schedule_due(app), schedule_due(app))

    # Assert
    rows = await job_rows(database_url)
    assert len(rows) == 1
    assert rows[0]["task"] == "tick"
    assert rows[0]["state"] == "queued"
    async with connection(database_url) as opened:
        fires = await opened.fetchval("SELECT count(*) FROM akadze.periodic_runs")
    assert fires == 1


async def test_overlap_false_skips_while_the_previous_job_is_queued(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.periodic("tick", every=timedelta(seconds=1), overlap=False)
    async def tick() -> None:
        return None

    app.grace = timedelta(seconds=5)
    await schedule_due(app)
    await asyncio.sleep(1.1)

    # Act
    await schedule_due(app)

    # Assert
    rows = await job_rows(database_url)
    assert len(rows) == 1
    assert rows[0]["unique_key"] == "periodic:tick"
    assert rows[0]["state"] == "queued"


async def test_overlap_false_enqueues_the_held_fire_once_the_previous_job_finished(
    app: Akadze, database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange — the clock is pinned to fires, so the held fire is the one that runs
    @app.periodic("tick", every=timedelta(hours=1), overlap=False)
    async def tick() -> None:
        return None

    first = datetime.now(UTC) - timedelta(seconds=2)
    held = first + timedelta(seconds=1)
    fire = [first]
    monkeypatch.setattr("akadze.periodic.previous_fire", lambda schedule, now: fire[0])
    await schedule_due(app)
    fire[0] = held
    await schedule_due(app)
    assert len(await job_rows(database_url)) == 1
    async with connection(database_url) as opened:
        await opened.execute(
            "UPDATE akadze.jobs SET state = 'succeeded', finished_at = now() WHERE task = 'tick'"
        )

    # Act
    await schedule_due(app)

    # Assert
    rows = await job_rows(database_url)
    assert [row["state"] for row in rows] == ["succeeded", "queued"]
    async with connection(database_url) as opened:
        fires = await opened.fetch("SELECT fire_at FROM akadze.periodic_runs ORDER BY fire_at")
    assert [row["fire_at"] for row in fires] == [first, held]


async def test_an_unreadable_schedule_does_not_stop_the_others(
    app: Akadze, database_url: str
) -> None:
    # Arrange — a schedule that got past validation (built directly) and never fires
    @app.task("broken")
    async def broken() -> None:
        return None

    @app.periodic("tick", cron="* * * * *")
    async def tick() -> None:
        return None

    app.schedules.insert(
        0,
        Schedule(name="broken", cron="0 0 31 2 *", every=None, overlap=True, queue="default"),
    )

    # Act
    await schedule_due(app)

    # Assert
    rows = await job_rows(database_url)
    assert [row["task"] for row in rows] == ["tick"]
