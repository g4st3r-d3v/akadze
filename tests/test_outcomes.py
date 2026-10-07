from __future__ import annotations

import asyncio
from datetime import timedelta

from akadze import Akadze, Cancel, Fail, Retry, Snooze, request_cancel
from akadze.worker import Worker
from tests.pg import as_json, connection, job_row


async def test_retry_requeues_and_increments_attempt(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        raise Retry(timedelta(seconds=30))

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 1
    assert row["worker_id"] is None
    async with connection(database_url) as opened:
        later = await opened.fetchval("SELECT run_at > now() FROM akadze.jobs WHERE task = 'demo'")
    assert later is True


async def test_error_requeues_and_increments_attempt(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        raise RuntimeError("boom")

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 1
    assert as_json(row["errors"])[0]["error"] == "RuntimeError"


async def test_last_attempt_fails(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", max_attempts=1)
    async def demo() -> None:
        raise RuntimeError("boom")

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "failed"
    assert row["attempt"] == 1
    assert row["finished_at"] is not None


async def test_fail_stops_without_another_try(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        raise Fail("stop")

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "failed"
    assert row["attempt"] == 1


async def test_snooze_does_not_spend_an_attempt(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", max_attempts=1)
    async def demo() -> None:
        raise Snooze(timedelta(seconds=15))

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 0
    assert row["snoozes"] == 1
    async with connection(database_url) as opened:
        later = await opened.fetchval("SELECT run_at > now() FROM akadze.jobs WHERE task = 'demo'")
    assert later is True


async def test_timeout_counts_as_a_failure(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", timeout=timedelta(milliseconds=50), max_attempts=3)
    async def demo() -> None:
        await asyncio.sleep(5)

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 1
    assert as_json(row["errors"])[0]["error"] == "TimeoutError"


async def test_cancel_from_the_task_marks_cancelled(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        raise Cancel("stop")

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "cancelled"
    assert row["attempt"] == 0


async def test_cancel_requested_job_is_cancelled(app: Akadze, database_url: str) -> None:
    # Arrange
    called = False

    @app.task("demo")
    async def demo() -> None:
        nonlocal called
        called = True

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    row = await job_row(database_url, "demo")
    assert row is not None
    async with app.engine.begin() as session:
        await request_cancel(session, int(row["id"]))

    # Act
    await Worker(app).step()

    # Assert
    assert called is False
    finished = await job_row(database_url, "demo")
    assert finished is not None
    assert finished["state"] == "cancelled"


async def test_expired_job_is_cancelled(app: Akadze, database_url: str) -> None:
    # Arrange
    called = False

    @app.task("demo")
    async def demo() -> None:
        nonlocal called
        called = True

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    async with connection(database_url) as opened:
        await opened.execute(
            "UPDATE akadze.jobs SET expires_at = now() - interval '1 minute' WHERE task = 'demo'"
        )

    # Act
    await Worker(app).step()

    # Assert
    assert called is False
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "cancelled"


async def test_shutdown_requeues_without_spending_an_attempt(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)
    await worker.register()
    await worker.claim_available()

    # Act
    await worker.shutdown()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 0
    assert row["worker_id"] is None
    async with connection(database_url) as opened:
        workers = await opened.fetchval("SELECT count(*) FROM akadze.workers")
    assert workers == 0
