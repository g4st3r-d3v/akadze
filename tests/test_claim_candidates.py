from __future__ import annotations

from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from akadze import Akadze
from akadze.worker import _KEY_WALK, Worker
from tests.factories import apply_limit
from tests.pg import connection

Claim = Callable[..., Awaitable[Worker]]


async def _enqueue(app: Akadze, key: str | None, count: int) -> None:
    task = app.tasks["work"]
    async with app.engine.begin() as session:
        await task.using(session=session, limit_key=key).enqueue_many([{}] * count)


async def _running_by_key(database_url: str) -> Counter[str | None]:
    async with connection(database_url) as opened:
        rows = await opened.fetch("SELECT limit_key FROM akadze.jobs WHERE state = 'running'")
    return Counter(row["limit_key"] for row in rows)


@pytest.fixture
def work(app: Akadze) -> None:
    @app.task("work")
    async def work() -> None:
        return None


@pytest.fixture
async def claim(app: Akadze) -> AsyncIterator[Claim]:
    workers: list[Worker] = []

    async def run(*, slots: int, fair: bool = False) -> Worker:
        worker = Worker(app, slots=slots, fair=fair)
        workers.append(worker)
        await worker.register()
        await worker.claim_available()
        return worker

    yield run
    for worker in workers:
        await worker.shutdown()


async def test_saturated_key_with_a_deep_head_does_not_block_others(
    app: Akadze, database_url: str, work: None, claim: Claim
) -> None:
    # Arrange: key "a" is at its limit and has 500 older jobs ahead of "b"
    await apply_limit(app.engine, key="a", max_running=1)
    await _enqueue(app, "a", 501)
    await claim(slots=1)
    await _enqueue(app, "b", 3)

    # Act
    await claim(slots=3)

    # Assert
    assert await _running_by_key(database_url) == Counter({"a": 1, "b": 3})


@pytest.mark.parametrize("fair", [False, True], ids=["default", "fair"])
async def test_more_keys_than_the_walk_still_fill_the_slots(
    app: Akadze, database_url: str, work: None, claim: Claim, fair: bool
) -> None:
    # Arrange: one job per key, more keys than the per-key walk covers
    task = app.tasks["work"]
    async with app.engine.begin() as session:
        for n in range(_KEY_WALK + 50):
            await task.using(session=session, limit_key=f"k{n:04}").enqueue()

    # Act
    await claim(slots=6, fair=fair)

    # Assert
    assert sum((await _running_by_key(database_url)).values()) == 6


async def test_key_beyond_the_walk_is_claimed_when_it_reaches_the_head(
    app: Akadze, database_url: str, work: None, claim: Claim
) -> None:
    # Arrange: the last key in key order is the oldest job of the queue
    task = app.tasks["work"]
    async with app.engine.begin() as session:
        await task.using(session=session, limit_key="zzz").enqueue()
        for n in range(_KEY_WALK + 50):
            await task.using(session=session, limit_key=f"k{n:04}").enqueue()

    # Act
    await claim(slots=1)

    # Assert
    assert await _running_by_key(database_url) == Counter({"zzz": 1})
