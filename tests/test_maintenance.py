from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from akadze import Akadze
from akadze.maintenance import prune, rescue
from akadze.worker import Worker
from tests.factories import age_worker_heartbeat, apply_limit, insert_job, insert_periodic_run
from tests.pg import connection, job_row, job_rows


@pytest.mark.parametrize(
    ("seed_attempt", "expected_state", "expected_attempt"),
    [
        (0, "queued", 1),
        (2, "failed", 3),
    ],
    ids=["requeue", "exhausted"],
)
async def test_stale_heartbeat_rescue(
    app: Akadze,
    database_url: str,
    seed_attempt: int,
    expected_state: str,
    expected_attempt: int,
) -> None:
    # Arrange
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)
    await worker.register()
    await worker.claim_available()
    if seed_attempt:
        async with connection(database_url) as opened:
            await opened.execute(
                "UPDATE akadze.jobs SET attempt = $1 WHERE task = 'demo'",
                seed_attempt,
            )
    await age_worker_heartbeat(app.engine, worker.id, older_than=timedelta(minutes=2))

    # Act
    await rescue(app.engine, app.hooks, ttl=timedelta(seconds=30))

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == expected_state
    assert row["attempt"] == expected_attempt
    assert row["worker_id"] is None
    if expected_state == "failed":
        assert row["finished_at"] is not None
    async with connection(database_url) as opened:
        workers = await opened.fetchval("SELECT count(*) FROM akadze.workers")
    assert workers == 0


async def test_rescue_spreads_requeued_jobs_with_jittered_backoff(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("demo", max_attempts=10)
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        for _ in range(20):
            await demo.using(session=session).enqueue()
    worker = Worker(app, slots=40)
    await worker.register()
    await worker.claim_available()
    async with connection(database_url) as opened:
        await opened.execute("UPDATE akadze.jobs SET attempt = 4")
    await age_worker_heartbeat(app.engine, worker.id, older_than=timedelta(minutes=2))

    # Act
    await rescue(app.engine, app.hooks, ttl=timedelta(seconds=30))

    # Assert
    async with connection(database_url) as opened:
        waits = await opened.fetch(
            "SELECT extract(epoch FROM run_at - now()) AS wait FROM akadze.jobs"
        )
    seconds = [float(row["wait"]) for row in waits]
    assert all(7.0 <= wait <= 16.0 for wait in seconds)
    assert len({round(wait, 3) for wait in seconds}) > 1


async def test_rescue_leaves_a_finished_job_alone(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)
    await worker.register()
    await worker.step()
    await age_worker_heartbeat(app.engine, worker.id, older_than=timedelta(minutes=2))

    # Act
    await rescue(app.engine, app.hooks, ttl=timedelta(seconds=30))

    # Assert
    rows = await job_rows(database_url)
    assert len(rows) == 1
    assert rows[0]["state"] == "succeeded"
    assert rows[0]["attempt"] == 0


async def test_pruner_deletes_old_finished_jobs_in_batches(app: Akadze, database_url: str) -> None:
    # Arrange
    now = datetime.now(UTC)
    await insert_job(
        app.engine,
        task="old-a",
        state="succeeded",
        finished_at=now - timedelta(days=8),
    )
    await insert_job(
        app.engine,
        task="old-b",
        state="succeeded",
        finished_at=now - timedelta(days=9),
    )
    await insert_job(app.engine, task="fresh", state="succeeded", finished_at=now)
    await insert_periodic_run(app.engine, name="old", fire_at=now - timedelta(days=8))
    await insert_periodic_run(app.engine, name="fresh", fire_at=now)

    # Act
    await prune(app.engine, retention=timedelta(days=7), batch=1)

    # Assert
    rows = await job_rows(database_url)
    assert {row["task"] for row in rows} >= {"fresh"}
    assert len([row for row in rows if row["task"].startswith("old")]) == 1
    async with connection(database_url) as opened:
        names = await opened.fetch("SELECT name FROM akadze.periodic_runs ORDER BY name")
    assert [row["name"] for row in names] == ["fresh"]


async def test_pruner_deletes_start_marks_outside_the_window(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    await apply_limit(
        app.engine, key="idle", max_running=None, max_starts=1, window=timedelta(minutes=1)
    )
    async with connection(database_url) as opened:
        await opened.execute(
            """
            INSERT INTO akadze.limit_starts (key, started_at)
            VALUES ('idle', now() - interval '2 minutes'), ('idle', now())
            """
        )

    # Act
    await prune(app.engine, retention=timedelta(days=7))

    # Assert
    async with connection(database_url) as opened:
        fresh = await opened.fetch(
            "SELECT started_at > now() - interval '1 minute' AS fresh FROM akadze.limit_starts"
        )
    assert [row["fresh"] for row in fresh] == [True]
