from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from akadze import Akadze
from akadze.worker import Worker
from tests.pg import job_row, states_by_task


@pytest.fixture
async def newer(database_url: str) -> AsyncIterator[Akadze]:
    """Another deploy of the app on the same database, which knows one more task."""

    application = Akadze(database_url=database_url)
    try:
        yield application
    finally:
        await application.aclose()


@pytest.fixture
async def worker(app: Akadze) -> AsyncIterator[Worker]:
    """A worker of the older deploy: one slot, registered, shut down after the asserts."""

    running = Worker(app, slots=1)
    await running.register()
    yield running
    await running.shutdown()


async def test_claim_leaves_a_task_this_worker_does_not_know_queued(
    app: Akadze, newer: Akadze, worker: Worker, database_url: str
) -> None:
    # Arrange
    @newer.task("added")
    async def added() -> None:
        return None

    @app.task("known")
    async def known() -> None:
        return None

    async with newer.engine.begin() as session:
        await added.using(session=session).enqueue()

    # Act
    await worker.claim_available()

    # Assert
    row = await job_row(database_url, "added")
    assert row is not None
    assert (row["state"], row["attempt"], row["run_count"]) == ("queued", 0, 0)


async def test_claim_takes_a_known_task_behind_an_unknown_one(
    app: Akadze, newer: Akadze, worker: Worker, database_url: str
) -> None:
    # Arrange
    @newer.task("added")
    async def added() -> None:
        return None

    @app.task("known")
    async def known() -> None:
        return None

    async with app.engine.begin() as session:
        await added.using(session=session, priority=10).enqueue_many([{}] * 20)
        await known.using(session=session).enqueue()

    # Act
    await worker.claim_available()

    # Assert
    assert await states_by_task(database_url) == {"added": "queued", "known": "running"}
