from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import text

from akadze import Akadze, AkadzeError, set_limit
from akadze.worker import Worker
from tests.pg import connection, job_row, job_rows


async def test_saturated_limit_key_claims_one_and_skips_waiter(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "first"

    @app.task("second")
    async def second() -> str:
        return "second"

    async with app.engine.begin() as session:
        await set_limit(session, "vendor", 1)
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()

    # Act
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()

    # Assert
    rows = await job_rows(database_url)
    states = {row["task"]: row["state"] for row in rows}
    assert states == {"first": "running", "second": "queued"}
    waiter = next(row for row in rows if row["task"] == "second")
    assert waiter["run_count"] == 0
    assert waiter["attempt"] == 0
    await worker.shutdown()


async def test_unlimited_job_claimed_while_limit_key_saturated(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("limited-a")
    async def limited_a() -> str:
        return "a"

    @app.task("limited-b")
    async def limited_b() -> str:
        return "b"

    @app.task("free")
    async def free() -> str:
        return "free"

    async with app.engine.begin() as session:
        await set_limit(session, "vendor", 1)
        await limited_a.using(session=session, limit_key="vendor").enqueue()
        await limited_b.using(session=session, limit_key="vendor").enqueue()
        await free.using(session=session).enqueue()

    # Act
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()

    # Assert
    rows = await job_rows(database_url)
    states = {row["task"]: row["state"] for row in rows}
    assert states["limited-a"] == "running"
    assert states["limited-b"] == "queued"
    assert states["free"] == "running"
    await worker.shutdown()


async def test_claim_after_success_takes_waiting_limit_key(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "ok"

    @app.task("second")
    async def second() -> str:
        return "ok"

    async with app.engine.begin() as session:
        await set_limit(session, "vendor", 1)
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()
    await worker.step()
    first_row = await job_row(database_url, "first")
    assert first_row is not None
    assert first_row["state"] == "succeeded"

    # Act
    await worker.claim_available()

    # Assert
    second_row = await job_row(database_url, "second")
    assert second_row is not None
    assert second_row["state"] == "running"
    await worker.shutdown()


async def test_limit_key_without_limits_row_stays_queued(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("typo")
    async def typo() -> str:
        return "nope"

    async with app.engine.begin() as session:
        await typo.using(session=session, limit_key="missing").enqueue()

    # Act
    worker = Worker(app, slots=1)
    await worker.register()
    await worker.claim_available()

    # Assert
    row = await job_row(database_url, "typo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["run_count"] == 0
    await worker.shutdown()


async def test_two_workers_respect_max_running_one(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("one")
    async def one() -> str:
        return "one"

    @app.task("two")
    async def two() -> str:
        return "two"

    async with app.engine.begin() as session:
        await set_limit(session, "vendor", 1)
        await one.using(session=session, limit_key="vendor").enqueue()
        await two.using(session=session, limit_key="vendor").enqueue()
    first = Worker(app, slots=1)
    second = Worker(app, slots=1)
    await first.register()
    await second.register()

    # Act
    await asyncio.gather(first.claim_available(), second.claim_available())

    # Assert
    rows = await job_rows(database_url)
    running = [row for row in rows if row["state"] == "running"]
    queued = [row for row in rows if row["state"] == "queued"]
    assert len(running) == 1
    assert len(queued) == 1
    await first.shutdown()
    await second.shutdown()


async def test_set_limit_replaces_max_running(app: Akadze, database_url: str) -> None:
    # Arrange
    async with app.engine.begin() as session:
        await set_limit(session, "vendor", 1)

    # Act
    async with app.engine.begin() as session:
        await set_limit(session, "vendor", 3)

    # Assert
    async with connection(database_url) as opened:
        rows = await opened.fetch("SELECT key, max_running FROM akadze.limits")
    assert [dict(row) for row in rows] == [{"key": "vendor", "max_running": 3}]


async def test_set_limit_rejects_max_running_below_one(app: Akadze) -> None:
    # Arrange / Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="max_running"):
            await set_limit(session, "vendor", 0)
        count = await session.scalar(text("SELECT count(*) FROM akadze.limits"))
    assert int(count or 0) == 0


async def test_empty_limit_key_is_stored_as_null(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> str:
        return "ok"

    # Act
    async with app.engine.begin() as session:
        await demo.using(session=session, limit_key="").enqueue()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["limit_key"] is None


async def test_paused_queue_still_blocks_limited_jobs(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    from akadze import pause_queue

    @app.task("demo", queue="mail")
    async def demo() -> str:
        return "ok"

    async with app.engine.begin() as session:
        await set_limit(session, "vendor", 2)
        await demo.using(session=session, limit_key="vendor").enqueue()
        await pause_queue(session, "mail")

    # Act
    worker = Worker(app, queues=["mail"])
    await worker.register()
    await worker.claim_available()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    await worker.shutdown()


async def test_rate_limit_blocks_same_key_while_other_key_claims(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "first"

    @app.task("second")
    async def second() -> str:
        return "second"

    @app.task("other")
    async def other() -> str:
        return "other"

    async with app.engine.begin() as session:
        await set_limit(
            session, "vendor", max_starts=1, window=timedelta(minutes=1)
        )
        await set_limit(
            session, "other", max_starts=1, window=timedelta(minutes=1)
        )
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()
        await other.using(session=session, limit_key="other").enqueue()
    worker = Worker(app, slots=3)
    await worker.register()

    # Act
    await worker.claim_available()

    # Assert
    rows = await job_rows(database_url)
    states = {row["task"]: row["state"] for row in rows}
    assert states == {
        "first": "running",
        "second": "queued",
        "other": "running",
    }
    waiter = next(row for row in rows if row["task"] == "second")
    assert waiter["run_count"] == 0
    assert waiter["attempt"] == 0
    await worker.shutdown()


async def test_rate_limit_allows_claim_after_window(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "first"

    @app.task("second")
    async def second() -> str:
        return "second"

    async with app.engine.begin() as session:
        await set_limit(
            session, "vendor", max_starts=1, window=timedelta(minutes=1)
        )
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()
    first_row = await job_row(database_url, "first")
    second_row = await job_row(database_url, "second")
    assert first_row is not None and first_row["state"] == "running"
    assert second_row is not None and second_row["state"] == "queued"
    async with connection(database_url) as opened:
        await opened.execute(
            """
            UPDATE akadze.limit_starts
            SET started_at = now() - interval '2 minutes'
            """
        )

    # Act
    await worker.claim_available()

    # Assert
    second_row = await job_row(database_url, "second")
    assert second_row is not None
    assert second_row["state"] == "running"
    assert second_row["attempt"] == 0
    assert second_row["run_count"] == 1
    await worker.shutdown()


async def test_set_limit_sets_rate_without_touching_max_running(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    async with app.engine.begin() as session:
        await set_limit(session, "vendor", 2)

    # Act
    async with app.engine.begin() as session:
        await set_limit(
            session, "vendor", max_starts=1, window=timedelta(minutes=1)
        )

    # Assert
    async with connection(database_url) as opened:
        row = await opened.fetchrow(
            """
            SELECT max_running, max_starts, "window" AS rate_window
            FROM akadze.limits
            WHERE key = 'vendor'
            """
        )
    assert dict(row) == {
        "max_running": 2,
        "max_starts": 1,
        "rate_window": timedelta(minutes=1),
    }


async def test_set_limit_sets_max_running_without_touching_rate(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    async with app.engine.begin() as session:
        await set_limit(
            session, "vendor", max_starts=3, window=timedelta(seconds=30)
        )

    # Act
    async with app.engine.begin() as session:
        await set_limit(session, "vendor", 4)

    # Assert
    async with connection(database_url) as opened:
        row = await opened.fetchrow(
            """
            SELECT max_running, max_starts, "window" AS rate_window
            FROM akadze.limits
            WHERE key = 'vendor'
            """
        )
    assert dict(row) == {
        "max_running": 4,
        "max_starts": 3,
        "rate_window": timedelta(seconds=30),
    }


async def test_set_limit_rejects_max_starts_below_one(app: Akadze) -> None:
    # Arrange / Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="max_starts"):
            await set_limit(
                session, "vendor", max_starts=0, window=timedelta(minutes=1)
            )
        count = await session.scalar(text("SELECT count(*) FROM akadze.limits"))
    assert int(count or 0) == 0


async def test_set_limit_rejects_non_positive_window(app: Akadze) -> None:
    # Arrange / Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="window"):
            await set_limit(
                session, "vendor", max_starts=1, window=timedelta(0)
            )
        count = await session.scalar(text("SELECT count(*) FROM akadze.limits"))
    assert int(count or 0) == 0


async def test_set_limit_rejects_rate_fields_separately(app: Akadze) -> None:
    # Arrange / Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="together"):
            await set_limit(session, "vendor", max_starts=1)
        with pytest.raises(AkadzeError, match="together"):
            await set_limit(session, "vendor", window=timedelta(minutes=1))
        count = await session.scalar(text("SELECT count(*) FROM akadze.limits"))
    assert int(count or 0) == 0


async def test_start_journal_prunes_marks_outside_window(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "first"

    @app.task("second")
    async def second() -> str:
        return "second"

    async with app.engine.begin() as session:
        await set_limit(
            session, "vendor", max_starts=1, window=timedelta(minutes=1)
        )
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()
    async with connection(database_url) as opened:
        await opened.execute(
            """
            UPDATE akadze.limit_starts
            SET started_at = now() - interval '2 minutes'
            """
        )

    # Act
    await worker.claim_available()

    # Assert
    async with connection(database_url) as opened:
        marks = await opened.fetch(
            """
            SELECT started_at > now() - interval '1 minute' AS fresh
            FROM akadze.limit_starts
            WHERE key = 'vendor'
            ORDER BY id
            """
        )
    assert len(marks) == 1
    assert marks[0]["fresh"] is True
    await worker.shutdown()
