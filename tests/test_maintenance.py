from __future__ import annotations

from datetime import timedelta

from akadze import Akadze
from akadze.maintenance import prune, rescue
from akadze.worker import Worker
from tests.pg import connection, job_row, job_rows


async def test_stale_heartbeat_requeues_the_job(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)
    await worker.register()
    await worker.claim_available()
    async with connection(database_url) as opened:
        await opened.execute(
            "UPDATE akadze.workers SET heartbeat_at = now() - interval '2 minutes' WHERE id = $1",
            worker.id,
        )

    # Act
    await rescue(app.engine, app.hooks, ttl=timedelta(seconds=30))

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 1
    assert row["worker_id"] is None
    async with connection(database_url) as opened:
        workers = await opened.fetchval("SELECT count(*) FROM akadze.workers")
    assert workers == 0


async def test_stale_heartbeat_fails_when_attempts_are_exhausted(
    app: Akadze, database_url: str
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
    async with connection(database_url) as opened:
        await opened.execute("UPDATE akadze.jobs SET attempt = 2 WHERE task = 'demo'")
        await opened.execute(
            """
            UPDATE akadze.workers
            SET heartbeat_at = now() - interval '2 minutes'
            WHERE id = $1
            """,
            worker.id,
        )

    # Act
    await rescue(app.engine, app.hooks, ttl=timedelta(seconds=30))

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "failed"
    assert row["attempt"] == 3
    assert row["finished_at"] is not None


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
    async with connection(database_url) as opened:
        await opened.execute(
            "UPDATE akadze.workers SET heartbeat_at = now() - interval '2 minutes' WHERE id = $1",
            worker.id,
        )

    # Act
    await rescue(app.engine, app.hooks, ttl=timedelta(seconds=30))

    # Assert
    rows = await job_rows(database_url)
    assert len(rows) == 1
    assert rows[0]["state"] == "succeeded"
    assert rows[0]["attempt"] == 0


async def test_pruner_deletes_old_finished_jobs_in_batches(app: Akadze, database_url: str) -> None:
    # Arrange
    async with connection(database_url) as opened:
        await opened.execute(
            """
            INSERT INTO akadze.jobs (task, state, finished_at)
            VALUES
                ('old-a', 'succeeded', now() - interval '8 days'),
                ('old-b', 'succeeded', now() - interval '9 days'),
                ('fresh', 'succeeded', now())
            """
        )
        await opened.execute(
            """
            INSERT INTO akadze.periodic_runs (name, fire_at)
            VALUES ('old', now() - interval '8 days'), ('fresh', now())
            """
        )

    # Act
    await prune(app.engine, retention=timedelta(days=7), batch=1)

    # Assert
    rows = await job_rows(database_url)
    assert {row["task"] for row in rows} >= {"fresh"}
    assert len([row for row in rows if row["task"].startswith("old")]) == 1
    async with connection(database_url) as opened:
        names = await opened.fetch("SELECT name FROM akadze.periodic_runs ORDER BY name")
    assert [row["name"] for row in names] == ["fresh"]
