"""Limits hold when a claim waits on another claimer's lock of the same key."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from akadze import Akadze
from akadze.claim import Policy, _policies, _take
from akadze.job import Job
from akadze.worker import Worker
from tests.factories import apply_limit
from tests.pg import connection, job_rows

_TIMEOUT = 10

_POLICIES = {
    "max_running": {"max_running": 1},
    "max_starts": {"max_running": None, "max_starts": 1, "window": timedelta(minutes=1)},
}


async def _wait_for_limits_lock_waiter(database_url: str) -> None:
    async with connection(database_url) as opened:
        for _ in range(500):
            waiting = await opened.fetchval(
                """
                SELECT count(*)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND wait_event_type = 'Lock'
                  AND query LIKE '%FROM akadze.limits AS l%ORDER BY l.key%FOR UPDATE%'
                """
            )
            if waiting:
                return
            await asyncio.sleep(0.01)
    raise AssertionError("second claim never waited on the limits lock")


@pytest.mark.parametrize("fair", [False, True], ids=["default", "fair"])
@pytest.mark.parametrize("policy", sorted(_POLICIES))
async def test_claim_waiting_on_a_limit_lock_sees_the_committed_claim(
    app: Akadze, database_url: str, policy: str, fair: bool
) -> None:
    # Arrange
    @app.task("job")
    async def job() -> None:
        return None

    await apply_limit(app.engine, key="vendor", **_POLICIES[policy])
    async with app.engine.begin() as session:
        await job.using(session=session, limit_key="vendor").enqueue()
        await job.using(session=session, limit_key="vendor").enqueue()

    held = asyncio.Event()
    release = asyncio.Event()

    async def hold_first_claim(_: AsyncConnection, __: Job, old: str, new: str) -> None:
        if (old, new) == ("queued", "running") and not held.is_set():
            held.set()
            await release.wait()

    app.hooks.on_transition.append(hold_first_claim)
    first = Worker(app, slots=1, fair=fair)
    second = Worker(app, slots=1, fair=fair)
    await first.register()
    await second.register()
    first_claim = asyncio.create_task(first.claim_available())
    second_claim: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(held.wait(), _TIMEOUT)
        async with app.engine.begin() as session:
            await job.using(session=session, limit_key="vendor", priority=10).enqueue()
        second_claim = asyncio.create_task(second.claim_available())
        await _wait_for_limits_lock_waiter(database_url)

        # Act
        release.set()
        await asyncio.wait_for(asyncio.gather(first_claim, second_claim), _TIMEOUT)
    finally:
        release.set()
        await asyncio.gather(
            first_claim, *([second_claim] if second_claim else []), return_exceptions=True
        )

    # Assert
    try:
        running = [row for row in await job_rows(database_url) if row["state"] == "running"]
        assert len(running) == 1
    finally:
        await first.shutdown()
        await second.shutdown()


async def test_claim_leaves_a_limited_key_it_did_not_lock_for_the_next_claim(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("limited")
    async def limited() -> None:
        return None

    @app.task("free")
    async def free() -> None:
        return None

    await apply_limit(app.engine, key="vendor", max_running=5)
    async with app.engine.begin() as session:
        await limited.using(session=session, limit_key="vendor").enqueue()
        await free.using(session=session).enqueue()
    ids = [row["id"] for row in await job_rows(database_url)]

    # Act
    async with app.engine.begin() as session:
        jobs = await _take(session, worker_id=uuid4(), ids=ids, limit=2, locked=set())

    # Assert
    assert [job.task for job in jobs] == ["free"]


async def test_a_limited_key_this_claim_did_not_lock_has_no_starts_left(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    await apply_limit(app.engine, key="vendor", max_running=5)

    # Act
    async with app.engine.begin() as session:
        policies = await _policies(session, keys=["vendor", "free"], locked=set())

    # Assert
    assert policies == {"vendor": Policy(left=0), "free": None}
