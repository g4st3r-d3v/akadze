from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from akadze import Akadze, EnqueueError
from akadze.worker import Worker
from tests.pg import as_json, connection, job_row, job_rows


async def test_commit_keeps_the_job(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo(item: int) -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await demo.using(session=session, priority=5, unique_key="item:1").enqueue(item=7)

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["priority"] == 5
    assert row["unique_key"] == "item:1"
    assert as_json(row["args"]) == {"item": 7}
    assert row["run_count"] == 0


async def test_session_rollback_removes_the_job(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo(item: int) -> None:
        return None

    session = AsyncSession(app.engine)
    await session.begin()
    await demo.using(session=session).enqueue(item=7)

    # Act
    await session.rollback()

    # Assert
    try:
        assert await job_rows(database_url) == []
    finally:
        await session.close()


async def test_enqueue_rejects_a_bad_argument(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo(item: int) -> None:
        return None

    # Act / Assert
    with pytest.raises(EnqueueError, match="invalid arguments"):
        async with app.engine.begin() as session:
            await demo.using(session=session).enqueue(item="nope")
    assert await job_rows(database_url) == []


async def test_enqueue_rejects_positional_arguments(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo(item: int) -> None:
        return None

    # Act / Assert
    with pytest.raises(EnqueueError, match="pass arguments to demo by name"):
        async with app.engine.begin() as session:
            await demo.using(session=session).enqueue(1)  # type: ignore[call-arg]
    assert await job_rows(database_url) == []


async def test_enqueue_options_are_not_task_arguments(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo(priority: str) -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await demo.using(session=session, priority=4).enqueue(priority="high-word")

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["priority"] == 4
    assert as_json(row["args"]) == {"priority": "high-word"}


@pytest.mark.parametrize(
    ("using_kwargs", "match"),
    [
        (
            {"delay": timedelta(seconds=1), "run_at": datetime(2026, 1, 1, tzinfo=UTC)},
            "delay or run_at",
        ),
        ({"expires_at": datetime(2026, 1, 1)}, "expires_at"),
    ],
    ids=["delay-and-run-at", "naive-expires-at"],
)
async def test_enqueue_rejects_invalid_schedule(
    app: Akadze,
    using_kwargs: dict[str, object],
    match: str,
) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    # Act / Assert
    with pytest.raises(EnqueueError, match=match):
        async with app.engine.begin() as session:
            await demo.using(session=session, **using_kwargs).enqueue()


async def test_delay_sets_run_at_in_the_future(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await demo.using(session=session, delay=timedelta(minutes=5)).enqueue()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    async with connection(database_url) as opened:
        later = await opened.fetchval("SELECT run_at > now() FROM akadze.jobs WHERE task = 'demo'")
    assert later is True
    assert row["state"] == "queued"


async def test_enqueue_requires_a_transaction(app: Akadze) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    # Act / Assert
    async with app.engine.connect() as session:
        with pytest.raises(EnqueueError, match="transaction"):
            await demo.using(session=session).enqueue()


async def test_duplicate_unique_key_keeps_the_first_job(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo(item: int) -> None:
        return None

    # Act / Assert
    async with app.engine.begin() as session:
        await demo.using(session=session, unique_key="item:1").enqueue(item=7)
        with pytest.raises(EnqueueError, match="unique_key"):
            await demo.using(session=session, unique_key="item:1").enqueue(item=8)

    rows = await job_rows(database_url)
    assert len(rows) == 1
    assert as_json(rows[0]["args"]) == {"item": 7}


async def test_expires_at_cancels_before_the_task_runs(app: Akadze, database_url: str) -> None:
    # Arrange
    called = False

    @app.task("demo")
    async def demo() -> None:
        nonlocal called
        called = True

    async with app.engine.begin() as session:
        await demo.using(
            session=session,
            expires_at=datetime.now(UTC) - timedelta(minutes=1),
        ).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    assert called is False
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "cancelled"


@pytest.mark.parametrize(
    ("override", "expected"),
    [(None, 3), (10, 10)],
    ids=["task-default", "per-job"],
)
async def test_using_max_attempts_sets_the_job_ceiling(
    app: Akadze,
    database_url: str,
    override: int | None,
    expected: int,
) -> None:
    # Arrange
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await demo.using(session=session, max_attempts=override).enqueue()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["max_attempts"] == expected


@pytest.mark.parametrize("max_attempts", [0, -1, 2**31], ids=["zero", "negative", "above-int4"])
async def test_using_rejects_max_attempts_out_of_range(
    app: Akadze, database_url: str, max_attempts: int
) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    # Act / Assert
    with pytest.raises(EnqueueError, match="max_attempts must be between 1 and 2147483647"):
        async with app.engine.begin() as session:
            await demo.using(session=session, max_attempts=max_attempts).enqueue()
    assert await job_rows(database_url) == []


async def test_per_job_max_attempts_keeps_retrying_past_the_task_default(
    app: Akadze,
    database_url: str,
) -> None:
    # Arrange
    @app.task("demo", max_attempts=1)
    async def demo() -> None:
        raise RuntimeError("boom")

    async with app.engine.begin() as session:
        await demo.using(session=session, max_attempts=2).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 1


async def test_per_job_max_attempts_below_the_task_default_fails_early(
    app: Akadze,
    database_url: str,
) -> None:
    # Arrange
    @app.task("demo", max_attempts=5)
    async def demo() -> None:
        raise RuntimeError("boom")

    async with app.engine.begin() as session:
        await demo.using(session=session, max_attempts=1).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "failed"
    assert row["attempt"] == 1
