from __future__ import annotations

import asyncio
from datetime import timedelta

from akadze import Akadze
from akadze.periodic import schedule_due
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
    app: Akadze, database_url: str
) -> None:
    # Arrange — the next fire came while the first job was queued and was held back
    @app.periodic("tick", every=timedelta(seconds=1), overlap=False)
    async def tick() -> None:
        return None

    app.grace = timedelta(seconds=5)
    await schedule_due(app)
    await asyncio.sleep(1.1)
    await schedule_due(app)
    async with connection(database_url) as opened:
        await opened.execute(
            "UPDATE akadze.jobs SET state = 'succeeded', finished_at = now() WHERE task = 'tick'"
        )

    # Act
    await schedule_due(app)

    # Assert
    rows = await job_rows(database_url)
    assert [row["state"] for row in rows] == ["succeeded", "queued"]
