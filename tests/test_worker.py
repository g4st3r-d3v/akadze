from __future__ import annotations

import asyncio
import time
from datetime import timedelta

import pytest

from akadze import Akadze
from akadze.worker import Worker, poll_delay
from tests.factories import age_worker_heartbeat, bump_run_count
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
    await bump_run_count(app.engine, task="demo")

    # Act
    release.set()
    await asyncio.gather(*worker.running)

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "running"
    assert row["run_count"] == 2
    assert row["result"] is None


async def test_heartbeat_reregisters_a_missing_worker(app: Akadze, database_url: str) -> None:
    # Arrange
    worker = Worker(app)
    await worker.register()
    async with connection(database_url) as opened:
        await opened.execute("DELETE FROM akadze.workers WHERE id = $1", worker.id)

    # Act
    await worker.heartbeat()

    # Assert
    async with connection(database_url) as opened:
        count = await opened.fetchval(
            "SELECT count(*) FROM akadze.workers WHERE id = $1",
            worker.id,
        )
    assert count == 1


async def test_heartbeat_is_written(app: Akadze, database_url: str) -> None:
    # Arrange
    worker = Worker(app)
    await worker.register()
    await age_worker_heartbeat(app.engine, worker.id, older_than=timedelta(hours=1))
    async with connection(database_url) as opened:
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


async def test_stale_heartbeat_returns_the_job_to_the_queue(
    app: Akadze,
    database_url: str,
) -> None:
    # Arrange
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task("block")
    async def block() -> None:
        started.set()
        await release.wait()

    async with app.engine.begin() as session:
        await block.using(session=session).enqueue()
    worker = Worker(
        app,
        heartbeat_interval=timedelta(hours=1),
        heartbeat_ttl=timedelta(seconds=30),
        poll_interval=timedelta(milliseconds=10),
    )
    stop = asyncio.Event()
    serve_task = asyncio.create_task(worker.serve(stop))

    # Act
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        worker._last_heartbeat_ok = time.monotonic() - 31
        await asyncio.wait_for(serve_task, timeout=2)
    finally:
        release.set()
        stop.set()
        if not serve_task.done():
            serve_task.cancel()
            await asyncio.gather(serve_task, return_exceptions=True)

    # Assert
    row = await job_row(database_url, "block")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 0
    assert row["worker_id"] is None


def test_poll_delay_stays_within_half_of_the_interval() -> None:
    # Act
    low = poll_delay(2, 0)
    high = poll_delay(2, 1)
    idle = poll_delay(0, 0.9)

    # Assert
    assert low == 1
    assert high == 2
    assert idle == 0


@pytest.mark.parametrize(
    ("slots", "job_count", "expect_full"),
    [
        (1, 2, True),
        (2, 1, False),
    ],
    ids=["full", "slot-left"],
)
async def test_claim_full_flag(app: Akadze, slots: int, job_count: int, expect_full: bool) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        for _ in range(job_count):
            await demo.using(session=session).enqueue()
    worker = Worker(app, slots=slots)

    # Act
    await worker.claim_available()

    # Assert
    assert worker._last_claim_full is expect_full


async def _claims_until_pause(worker: Worker) -> int:
    claims = 0
    claim = worker.claim_available

    async def count() -> None:
        nonlocal claims
        claims += 1
        await claim()

    async def stop_on_pause(stop: asyncio.Event) -> None:
        stop.set()

    worker.claim_available = count  # type: ignore[method-assign]
    worker._pause = stop_on_pause  # type: ignore[method-assign]
    await worker.serve(asyncio.Event())
    return claims


async def test_partial_claim_pauses_without_another_claim(
    app: Akadze,
    database_url: str,
) -> None:
    # Arrange
    hold = asyncio.Event()

    @app.task("demo")
    async def demo() -> None:
        await hold.wait()

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(
        app,
        slots=2,
        poll_interval=timedelta(hours=1),
        heartbeat_interval=timedelta(hours=1),
        shutdown_timeout=timedelta(0),
    )

    # Act
    try:
        claims = await _claims_until_pause(worker)
    finally:
        hold.set()

    # Assert
    assert claims == 1
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["run_count"] == 1


async def test_full_claim_polls_again_before_sleeping(app: Akadze, database_url: str) -> None:
    # Arrange
    hold = asyncio.Event()

    @app.task("demo")
    async def demo() -> None:
        await hold.wait()

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
        await demo.using(session=session).enqueue()
    worker = Worker(
        app,
        slots=2,
        poll_interval=timedelta(hours=1),
        heartbeat_interval=timedelta(hours=1),
        shutdown_timeout=timedelta(0),
    )

    # Act
    try:
        claims = await _claims_until_pause(worker)
    finally:
        hold.set()

    # Assert
    assert claims == 2
    rows = await job_rows(database_url)
    assert len(rows) == 2
    assert rows[0]["run_count"] == 1
    assert rows[1]["run_count"] == 1


async def test_a_freed_slot_is_refilled_without_waiting_for_the_poll(
    app: Akadze, database_url: str
) -> None:
    # Arrange — one slot held by the first job until the worker is in its pause
    release = asyncio.Event()
    paused = asyncio.Event()

    @app.task("demo")
    async def demo(first: bool) -> None:
        if first:
            await release.wait()

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue(first=True)
        await demo.using(session=session, delay=timedelta(milliseconds=1)).enqueue(first=False)
    worker = Worker(
        app,
        slots=1,
        poll_interval=timedelta(hours=1),
        heartbeat_interval=timedelta(hours=1),
    )
    pause = worker._pause

    async def pause_seen(stop: asyncio.Event) -> None:
        paused.set()
        await pause(stop)

    worker._pause = pause_seen  # type: ignore[method-assign]
    stop = asyncio.Event()
    serving = asyncio.create_task(worker.serve(stop))
    await asyncio.wait_for(paused.wait(), 10)

    # Act
    release.set()

    # Assert — the second job runs well before the hour-long poll would end
    try:
        async with asyncio.timeout(10):
            while [row["state"] for row in await job_rows(database_url)] != ["succeeded"] * 2:
                await asyncio.sleep(0.02)
    finally:
        stop.set()
        await serving


@pytest.mark.parametrize(
    ("slots", "warns"),
    [(13, False), (14, True)],
    ids=["fits-5+10", "needs-16"],
)
async def test_serve_warns_when_the_pool_is_smaller_than_the_slots(
    app: Akadze, caplog: pytest.LogCaptureFixture, slots: int, warns: bool
) -> None:
    # Arrange — the default pool holds 5 + 10; a worker needs slots + 2
    worker = Worker(app, slots=slots, heartbeat_interval=timedelta(hours=1))
    stop = asyncio.Event()
    stop.set()

    # Act
    await worker.serve(stop)

    # Assert
    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.levelname == "WARNING" and "engine pool" in record.getMessage()
    ]
    expected = (
        "engine pool holds 15 connections (pool_size 5 + max_overflow 10); "
        f"this worker needs at least {slots + 2} (slots x queues + 2), "
        "so runs will wait for one"
    )
    assert warnings == ([expected] if warns else [])
