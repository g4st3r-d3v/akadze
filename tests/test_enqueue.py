from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from akadze import Akadze, EnqueueError
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


async def test_delay_and_run_at_cannot_both_be_set(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    # Act / Assert
    with pytest.raises(EnqueueError, match="delay or run_at"):
        async with app.engine.begin() as session:
            await demo.using(
                session=session,
                delay=timedelta(seconds=1),
                run_at=datetime.now(UTC),
            ).enqueue()


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
