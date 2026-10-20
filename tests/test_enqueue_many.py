from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from akadze import Akadze, EnqueueError
from akadze.job import Job
from tests.pg import as_json, connection, job_rows


async def test_enqueue_many_inserts_every_item_in_order(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    items = [{"user_id": n} for n in range(2500)]

    # Act
    async with app.engine.begin() as session:
        await notify.using(session=session).enqueue_many(items)

    # Assert
    async with connection(database_url) as opened:
        rows = await opened.fetch("SELECT args FROM akadze.jobs ORDER BY id")
    assert [as_json(row["args"])["user_id"] for row in rows] == list(range(2500))


async def test_enqueue_many_applies_the_shared_options(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await notify.using(
            session=session,
            priority=7,
            max_attempts=5,
            limit_key="mailer",
            delay=timedelta(hours=1),
        ).enqueue_many([{"user_id": 1}, {"user_id": 2}])

    # Assert
    async with connection(database_url) as opened:
        rows = await opened.fetch(
            """
            SELECT priority, max_attempts, limit_key, state, run_at > now() + interval '59 minutes'
                AS delayed
            FROM akadze.jobs
            """
        )
    assert [tuple(row) for row in rows] == [(7, 5, "mailer", "queued", True)] * 2


async def test_enqueue_many_rejects_the_batch_when_one_item_is_invalid(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    # Act / Assert
    with pytest.raises(EnqueueError, match="invalid arguments"):
        async with app.engine.begin() as session:
            await notify.using(session=session).enqueue_many(
                [{"user_id": 1}, {"user_id": "not-a-number"}]
            )
    assert await job_rows(database_url) == []


async def test_enqueue_many_rejects_unique_key(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    # Act / Assert
    with pytest.raises(EnqueueError, match="unique_key applies to one job"):
        async with app.engine.begin() as session:
            await notify.using(session=session, unique_key="k").enqueue_many([{"user_id": 1}])
    assert await job_rows(database_url) == []


async def test_enqueue_many_with_no_items_inserts_nothing(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await notify.using(session=session).enqueue_many([])

    # Assert
    assert await job_rows(database_url) == []


async def test_enqueue_many_rolls_back_with_the_caller(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    # Act
    with pytest.raises(RuntimeError, match="abort"):
        async with app.engine.begin() as session:
            await notify.using(session=session).enqueue_many([{"user_id": 1}, {"user_id": 2}])
            raise RuntimeError("abort")

    # Assert
    assert await job_rows(database_url) == []


async def test_enqueue_many_calls_on_enqueue_for_each_job(app: Akadze) -> None:
    # Arrange
    seen: list[int] = []

    async def record(connection: AsyncConnection, job: Job) -> None:
        seen.append(job.args["user_id"])

    app.hooks.on_enqueue.append(record)

    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await notify.using(session=session).enqueue_many([{"user_id": n} for n in range(3)])

    # Assert
    assert seen == [0, 1, 2]


async def test_enqueue_many_leaves_nothing_when_a_later_chunk_fails(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    async def explode_late(connection: AsyncConnection, job: Job) -> None:
        if job.args.get("user_id") == 1500:
            raise RuntimeError("hook failed")

    app.hooks.on_enqueue.append(explode_late)

    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    @app.task("audit")
    async def audit() -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        with pytest.raises(RuntimeError, match="hook failed"):
            await notify.using(session=session).enqueue_many([{"user_id": n} for n in range(2500)])
        await audit.using(session=session).enqueue()

    # Assert
    rows = await job_rows(database_url)
    assert [row["task"] for row in rows] == ["audit"]


@pytest.mark.parametrize(
    ("items", "message"),
    [
        ({"user_id": 1}, "not one mapping"),
        ([{"user_id": 1}, 7], "items must be mappings, got int"),
    ],
    ids=["one-mapping", "non-mapping-item"],
)
async def test_enqueue_many_rejects_malformed_items(
    app: Akadze, database_url: str, items: object, message: str
) -> None:
    # Arrange
    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    # Act / Assert
    with pytest.raises(EnqueueError, match=message):
        async with app.engine.begin() as session:
            await notify.using(session=session).enqueue_many(items)  # type: ignore[arg-type]
    assert await job_rows(database_url) == []


async def test_enqueue_many_checks_options_before_reading_items(app: Akadze) -> None:
    # Arrange
    @app.task("notify")
    async def notify(user_id: int) -> None:
        return None

    def items() -> Iterator[dict[str, int]]:
        raise AssertionError("items were read")
        yield {"user_id": 1}

    # Act / Assert
    with pytest.raises(EnqueueError, match="must not be negative"):
        async with app.engine.begin() as session:
            await notify.using(session=session, delay=timedelta(seconds=-1)).enqueue_many(items())
