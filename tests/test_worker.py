from __future__ import annotations

import asyncio

from akadze import Akadze
from akadze.worker import Worker
from tests.pg import as_json, connection, job_row, job_rows


async def test_sync_task_succeeds(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    def demo(name: str) -> str:
        return name

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue(name="ada")

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "succeeded"
    assert as_json(row["result"]) == "ada"


async def test_claim_marks_running_and_increments_run_count(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)

    # Act
    await worker.claim_available()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "running"
    assert row["run_count"] == 1
    assert str(row["worker_id"]) == str(worker.id)
    assert row["started_at"] is not None


async def test_higher_priority_is_claimed_first(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo(label: str) -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session, priority=1).enqueue(label="low")
        await demo.using(session=session, priority=10).enqueue(label="high")
    worker = Worker(app, slots=1)

    # Act
    await worker.claim_available()

    # Assert
    rows = await job_rows(database_url)
    running = [row for row in rows if row["state"] == "running"]
    queued = [row for row in rows if row["state"] == "queued"]
    assert len(running) == 1
    assert as_json(running[0]["args"]) == {"label": "high"}
    assert len(queued) == 1


async def test_two_workers_do_not_claim_the_same_job(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    first = Worker(app)
    second = Worker(app)

    # Act
    await asyncio.gather(first.claim_available(), second.claim_available())

    # Assert
    rows = await job_rows(database_url)
    assert len(rows) == 1
    assert rows[0]["state"] == "running"


async def test_run_starts_after_claim_commits(app: Akadze, database_url: str) -> None:
    # Arrange
    seen: dict[str, str] = {}

    @app.task("demo")
    async def demo() -> None:
        async with connection(database_url) as opened:
            seen["state"] = await opened.fetchval(
                "SELECT state FROM akadze.jobs WHERE task = 'demo'"
            )

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)

    # Act
    await worker.step()

    # Assert
    assert seen["state"] == "running"
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "succeeded"


async def test_stale_run_count_discards_the_finish(app: Akadze, database_url: str) -> None:
    # Arrange
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task("demo")
    async def demo() -> str:
        started.set()
        await release.wait()
        return "done"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)
    await worker._spawn()
    await started.wait()
    async with connection(database_url) as opened:
        await opened.execute("UPDATE akadze.jobs SET run_count = run_count + 1 WHERE task = 'demo'")

    # Act
    release.set()
    await asyncio.gather(*worker.running)

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "running"
    assert row["run_count"] == 2
    assert row["result"] is None


async def test_heartbeat_is_written(app: Akadze, database_url: str) -> None:
    # Arrange
    worker = Worker(app)
    await worker.register()
    async with connection(database_url) as opened:
        await opened.execute(
            "UPDATE akadze.workers SET heartbeat_at = now() - interval '1 hour' WHERE id = $1",
            worker.id,
        )
        before = await opened.fetchval(
            "SELECT heartbeat_at FROM akadze.workers WHERE id = $1",
            worker.id,
        )

    # Act
    await worker.heartbeat()

    # Assert
    async with connection(database_url) as opened:
        after = await opened.fetchval(
            "SELECT heartbeat_at FROM akadze.workers WHERE id = $1",
            worker.id,
        )
    assert before is not None
    assert after is not None
    assert after > before
