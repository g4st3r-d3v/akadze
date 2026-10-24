from __future__ import annotations

from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from akadze import Akadze
from akadze.job import Job
from akadze.worker import Worker
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


@pytest.mark.parametrize("fair", [False, True], ids=["default", "fair"])
async def test_head_blocked_by_a_saturated_key_is_read_past(
    app: Akadze, database_url: str, work: None, claim: Claim, fair: bool
) -> None:
    # Arrange: key "a" is at its limit and has 500 older jobs ahead of "b"
    await apply_limit(app.engine, key="a", max_running=1)
    await _enqueue(app, "a", 501)
    await claim(slots=1)
    await _enqueue(app, "b", 3)

    # Act
    await claim(slots=3, fair=fair)

    # Assert
    assert await _running_by_key(database_url) == Counter({"a": 1, "b": 3})


async def test_head_filled_by_a_nearly_full_key_still_fills_the_slots(
    app: Akadze, database_url: str, work: None, claim: Claim
) -> None:
    # Arrange: "b" may start one more job and has 1000 older jobs; 20 small keys follow
    await apply_limit(app.engine, key="b", max_running=5)
    await _enqueue(app, "b", 1004)
    await claim(slots=4)
    task = app.tasks["work"]
    async with app.engine.begin() as session:
        for n in range(20):
            await task.using(session=session, limit_key=f"small-{n:02}").enqueue()

    # Act
    await claim(slots=10)

    # Assert
    running = await _running_by_key(database_url)
    assert running["b"] == 5
    assert sum(running.values()) == 14


@pytest.mark.parametrize("fair", [False, True], ids=["default", "fair"])
async def test_head_rows_held_by_another_claimer_are_skipped(
    app: Akadze, database_url: str, work: None, claim: Claim, fair: bool
) -> None:
    # Arrange: another transaction holds the 50 oldest ready rows
    await _enqueue(app, None, 60)
    async with connection(database_url) as other:
        holder = other.transaction()
        await holder.start()
        await other.execute(
            """
            SELECT id FROM akadze.jobs
            WHERE state = 'queued'
            ORDER BY priority DESC, run_at, id
            LIMIT 50
            FOR UPDATE
            """
        )

        # Act
        await claim(slots=2, fair=fair)

        # Assert
        assert await _running_by_key(database_url) == Counter({None: 2})
        await holder.rollback()


@pytest.mark.parametrize("fair", [False, True], ids=["default", "fair"])
async def test_head_claim_keeps_priority_order_beyond_the_head_size(
    app: Akadze, database_url: str, work: None, claim: Claim, fair: bool
) -> None:
    # Arrange: 200 older low-priority jobs, then 5 newer urgent ones
    task = app.tasks["work"]
    async with app.engine.begin() as session:
        await task.using(session=session, limit_key="bulk").enqueue_many([{}] * 200)
        await task.using(session=session, limit_key="urgent", priority=10).enqueue_many([{}] * 5)

    # Act
    await claim(slots=5, fair=fair)

    # Assert
    assert await _running_by_key(database_url) == Counter({"urgent": 5})


async def test_transition_hooks_run_once_per_claimed_job_across_takes(
    app: Akadze, database_url: str, work: None, claim: Claim
) -> None:
    # Arrange: another transaction holds the 20 oldest rows, more than the first plan
    await _enqueue(app, None, 30)
    seen: list[int] = []

    async def record(connection: object, job: Job, before: str, after: str) -> None:
        if after == "running":
            seen.append(job.id)

    app.hooks.on_transition.append(record)
    async with connection(database_url) as other:
        holder = other.transaction()
        await holder.start()
        await other.execute(
            """
            SELECT id FROM akadze.jobs
            WHERE state = 'queued'
            ORDER BY priority DESC, run_at, id
            LIMIT 20
            FOR UPDATE
            """
        )

        # Act
        await claim(slots=3)

        # Assert
        assert len(seen) == 3
        assert len(set(seen)) == 3
        await holder.rollback()


async def test_fair_claim_reaches_idle_groups_behind_a_busy_backlog(
    app: Akadze, database_url: str, work: None, claim: Claim
) -> None:
    # Arrange: "a" already runs one job and has 500 older jobs ahead of "b" and "c"
    await _enqueue(app, "a", 501)
    await claim(slots=1, fair=True)
    await _enqueue(app, "b", 1)
    await _enqueue(app, "c", 1)

    # Act
    await claim(slots=3, fair=True)

    # Assert
    assert await _running_by_key(database_url) == Counter({"a": 2, "b": 1, "c": 1})


async def test_fair_claim_goes_below_a_short_priority(
    app: Akadze, database_url: str, work: None, claim: Claim
) -> None:
    # Arrange: two urgent jobs, then a deep backlog of "a" ahead of "b"
    task = app.tasks["work"]
    async with app.engine.begin() as session:
        await task.using(session=session, limit_key="urgent", priority=5).enqueue_many([{}] * 2)
        await task.using(session=session, limit_key="a").enqueue_many([{}] * 300)
        await task.using(session=session, limit_key="b").enqueue_many([{}] * 2)

    # Act
    await claim(slots=4, fair=True)

    # Assert
    assert await _running_by_key(database_url) == Counter({"urgent": 2, "a": 1, "b": 1})
