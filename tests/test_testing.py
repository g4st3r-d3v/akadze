from __future__ import annotations

from datetime import timedelta

import pytest

from akadze import Akadze, AkadzeError
from akadze.testing import assert_enqueued, drain
from tests.pg import connection, job_row


async def test_assert_enqueued_matches_arguments(app: Akadze) -> None:
    # Arrange
    @app.task("demo")
    async def demo(item: int) -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue(item=3)

    # Act
    await assert_enqueued(app, "demo", item=3)


async def test_assert_enqueued_rejects_a_different_argument(app: Akadze) -> None:
    # Arrange
    @app.task("demo")
    async def demo(item: int) -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue(item=3)

    # Act / Assert
    with pytest.raises(AssertionError, match="demo"):
        await assert_enqueued(app, "demo", item=9)


async def test_drain_runs_a_job_enqueued_by_another(app: Akadze) -> None:
    # Arrange
    seen: list[str] = []

    @app.task("second")
    async def second() -> None:
        seen.append("second")

    @app.task("first")
    async def first() -> None:
        seen.append("first")
        async with app.engine.begin() as session:
            await second.using(session=session).enqueue()

    async with app.engine.begin() as session:
        await first.using(session=session).enqueue()

    # Act
    await drain(app)

    # Assert
    assert seen == ["first", "second"]


async def test_drain_leaves_a_future_job(app: Akadze, database_url: str) -> None:
    # Arrange
    called = False

    @app.task("later")
    async def later() -> None:
        nonlocal called
        called = True

    async with app.engine.begin() as session:
        await later.using(session=session, delay=timedelta(hours=1)).enqueue()

    # Act
    await drain(app)

    # Assert
    assert called is False
    row = await job_row(database_url, "later")
    assert row is not None
    assert row["state"] == "queued"


async def test_drain_stops_when_jobs_stay_ready(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("again")
    async def again() -> None:
        async with app.engine.begin() as session:
            await again.using(session=session).enqueue()

    async with app.engine.begin() as session:
        await again.using(session=session).enqueue()

    # Act / Assert
    with pytest.raises(AkadzeError, match="still ready"):
        await drain(app, limit=2)

    async with connection(database_url) as opened:
        queued = await opened.fetchval(
            "SELECT count(*) FROM akadze.jobs WHERE task = 'again' AND state = 'queued'"
        )
    assert queued == 1
