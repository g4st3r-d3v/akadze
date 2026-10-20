from __future__ import annotations

import os
import subprocess
import sys
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from akadze import Akadze
from akadze.worker import Worker
from tests.factories import apply_limit
from tests.pg import connection

Claim = Callable[..., Awaitable[Worker]]


async def _enqueue(app: Akadze, key: str | None, count: int, *, priority: int = 0) -> None:
    task = app.tasks["work"]
    async with app.engine.begin() as session:
        await task.using(session=session, limit_key=key, priority=priority).enqueue_many(
            [{}] * count
        )


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

    async def run(*, slots: int, fair: bool) -> Worker:
        worker = Worker(app, slots=slots, fair=fair)
        workers.append(worker)
        await worker.register()
        await worker.claim_available()
        return worker

    yield run
    for worker in workers:
        await worker.shutdown()


@pytest.mark.parametrize(
    ("fair", "expected"),
    [(True, {"a": 1, "b": 1}), (False, {"a": 2})],
    ids=["fair", "default-order"],
)
async def test_deep_backlog_of_one_key_does_not_take_every_slot(
    app: Akadze,
    database_url: str,
    work: None,
    claim: Claim,
    fair: bool,
    expected: dict[str, int],
) -> None:
    # Arrange
    await _enqueue(app, "a", 100)
    await _enqueue(app, "b", 2)

    # Act
    await claim(slots=2, fair=fair)

    # Assert
    assert await _running_by_key(database_url) == Counter(expected)


@pytest.mark.parametrize(
    ("fair", "expected"),
    [(True, {"a": 2, "b": 2}), (False, {"a": 4})],
    ids=["fair", "default-order"],
)
async def test_single_slot_workers_share_slots_across_keys(
    app: Akadze,
    database_url: str,
    work: None,
    claim: Claim,
    fair: bool,
    expected: dict[str, int],
) -> None:
    # Arrange: steady state, one free slot per claim
    await _enqueue(app, "a", 100)
    await _enqueue(app, "b", 2)

    # Act
    for _ in range(4):
        await claim(slots=1, fair=fair)

    # Assert
    assert await _running_by_key(database_url) == Counter(expected)


async def test_priority_still_wins_over_fairness(
    app: Akadze, database_url: str, work: None, claim: Claim
) -> None:
    # Arrange
    await _enqueue(app, "a", 10, priority=5)
    await _enqueue(app, "b", 10)

    # Act
    await claim(slots=3, fair=True)

    # Assert
    assert await _running_by_key(database_url) == Counter({"a": 3})


async def test_jobs_without_a_key_share_one_turn(
    app: Akadze, database_url: str, work: None, claim: Claim
) -> None:
    # Arrange
    await _enqueue(app, None, 10)
    await _enqueue(app, "b", 1)
    await _enqueue(app, "c", 1)

    # Act
    await claim(slots=3, fair=True)

    # Assert
    assert await _running_by_key(database_url) == Counter({None: 1, "b": 1, "c": 1})


@pytest.mark.parametrize(
    ("fair", "expected"),
    [(True, {"a": 1, "b": 1, "c": 1}), (False, {"a": 1, "b": 2})],
    ids=["fair", "default-order"],
)
async def test_fair_turns_skip_jobs_blocked_by_a_limit(
    app: Akadze,
    database_url: str,
    work: None,
    claim: Claim,
    fair: bool,
    expected: dict[str, int],
) -> None:
    # Arrange
    await apply_limit(app.engine, key="a", max_running=1)
    await _enqueue(app, "a", 5)
    await _enqueue(app, "b", 5)
    await _enqueue(app, "c", 5)

    # Act
    await claim(slots=3, fair=fair)

    # Assert
    assert await _running_by_key(database_url) == Counter(expected)


def test_cli_worker_offers_the_fair_flag() -> None:
    # Arrange / Act
    result = subprocess.run(
        [sys.executable, "-m", "akadze.cli", "worker", "--help"],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    # Assert
    assert result.returncode == 0, result.stderr
    assert "--fair" in result.stdout
