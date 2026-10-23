from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from akadze import Akadze, Cancel, Fail, Retry, Snooze, request_cancel
from akadze.worker import Worker
from tests.factories import insert_job
from tests.pg import as_json, connection, job_row


@pytest.mark.parametrize(
    ("kind", "max_attempts", "expected_state", "expected_attempt"),
    [
        ("retry", 3, "queued", 1),
        ("error", 3, "queued", 1),
        ("error", 1, "failed", 1),
        ("fail", 3, "failed", 1),
        ("cancel", 3, "cancelled", 0),
        ("snooze", 1, "queued", 0),
    ],
    ids=["retry", "error-retry", "last-attempt", "fail", "cancel", "snooze"],
)
async def test_task_outcome(
    app: Akadze,
    database_url: str,
    kind: str,
    max_attempts: int,
    expected_state: str,
    expected_attempt: int,
) -> None:
    # Arrange
    def outcome() -> BaseException:
        if kind == "retry":
            return Retry(timedelta(seconds=30))
        if kind == "error":
            return RuntimeError("boom")
        if kind == "fail":
            return Fail("stop")
        if kind == "cancel":
            return Cancel("stop")
        return Snooze(timedelta(seconds=15))

    @app.task("demo", max_attempts=max_attempts)
    async def demo() -> None:
        raise outcome()

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == expected_state
    assert row["attempt"] == expected_attempt
    if expected_state == "queued":
        assert row["worker_id"] is None
    if expected_state == "failed":
        assert row["finished_at"] is not None
    if kind == "snooze":
        assert row["snoozes"] == 1
    if kind in {"retry", "snooze"}:
        async with connection(database_url) as opened:
            later = await opened.fetchval(
                "SELECT run_at > now() FROM akadze.jobs WHERE task = 'demo'"
            )
        assert later is True
    if kind == "error" and expected_state == "queued":
        assert as_json(row["errors"])[0]["error"] == "RuntimeError"


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


async def test_cancel_during_a_run_keeps_the_returned_result(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task("demo", max_attempts=3)
    async def demo() -> str:
        started.set()
        await release.wait()
        return "done"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)
    await worker._spawn()
    await started.wait()
    row = await job_row(database_url, "demo")
    assert row is not None
    async with app.engine.begin() as session:
        await request_cancel(session, int(row["id"]))

    # Act
    release.set()
    await asyncio.gather(*worker.running)

    # Assert
    finished = await job_row(database_url, "demo")
    assert finished is not None
    assert finished["state"] == "succeeded"
    assert as_json(finished["result"]) == "done"


async def _run_while(
    app: Akadze, database_url: str, change: str, *, raises: BaseException | None
) -> asyncpg.Record:
    """Run task "demo"; while its body waits, apply the SQL change to its row."""

    started = asyncio.Event()
    release = asyncio.Event()

    @app.task("demo", max_attempts=3)
    async def demo() -> str:
        started.set()
        await release.wait()
        if raises is not None:
            raise raises
        return "done"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)
    await worker._spawn()
    await started.wait()
    async with connection(database_url) as opened:
        await opened.execute(f"UPDATE akadze.jobs SET {change} WHERE task = 'demo'")
    release.set()
    await asyncio.gather(*worker.running)
    finished = await job_row(database_url, "demo")
    assert finished is not None
    return finished


@pytest.mark.parametrize(
    "raises",
    [Retry(timedelta(seconds=1)), Snooze(timedelta(seconds=1)), RuntimeError("boom")],
    ids=["retry", "snooze", "error"],
)
async def test_cancel_during_a_run_that_raises_cancels_without_an_attempt(
    app: Akadze, database_url: str, raises: BaseException
) -> None:
    # Act
    finished = await _run_while(app, database_url, "cancel_requested_at = now()", raises=raises)

    # Assert
    assert finished["state"] == "cancelled"
    assert finished["attempt"] == 0


async def test_expiry_during_a_run_keeps_the_returned_result(
    app: Akadze, database_url: str
) -> None:
    # Act
    finished = await _run_while(
        app, database_url, "expires_at = now() - interval '1 second'", raises=None
    )

    # Assert
    assert finished["state"] == "succeeded"
    assert as_json(finished["result"]) == "done"


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

    await insert_job(
        app.engine,
        task="demo",
        expires_at=datetime.now(UTC) - timedelta(minutes=1),
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


def test_shutdown_timeout_must_not_be_negative() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="shutdown_timeout"):
        Worker(object(), shutdown_timeout=timedelta(seconds=-1))


async def test_shutdown_waits_for_a_running_job(app: Akadze, database_url: str) -> None:
    # Arrange
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task("demo")
    async def demo() -> None:
        started.set()
        await release.wait()

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(
        app,
        shutdown_timeout=timedelta(seconds=2),
        poll_interval=timedelta(milliseconds=20),
    )
    stop = asyncio.Event()
    running = asyncio.create_task(worker.serve(stop))

    # Act
    await asyncio.wait_for(started.wait(), timeout=2)
    stop.set()
    for _ in range(200):
        if worker._draining:
            break
        await asyncio.sleep(0.01)
    assert worker._draining
    release.set()
    await asyncio.wait_for(running, timeout=2)

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "succeeded"
    assert row["attempt"] == 0


async def test_shutdown_requeues_a_job_that_outlasts_the_timeout(
    app: Akadze,
    database_url: str,
) -> None:
    # Arrange
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task("demo")
    async def demo() -> None:
        started.set()
        await release.wait()

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(
        app,
        shutdown_timeout=timedelta(milliseconds=50),
        poll_interval=timedelta(milliseconds=20),
        heartbeat_interval=timedelta(seconds=30),
    )
    stop = asyncio.Event()
    running = asyncio.create_task(worker.serve(stop))

    # Act
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        stop.set()
        await asyncio.wait_for(running, timeout=2)
    finally:
        release.set()
        stop.set()
        if not running.done():
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 0
    assert row["worker_id"] is None
