from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import text

from akadze import Akadze, request_cancel
from akadze.maintenance import rescue
from akadze.testing import drain
from akadze.worker import Worker
from tests.factories import age_worker_heartbeat
from tests.pg import as_json, connection, job_row, job_rows


async def test_enqueue_hook_error_rolls_back_the_insert(
    app: Akadze, database_url: str, mocker: MockerFixture
) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    reject = mocker.AsyncMock(side_effect=RuntimeError("enqueue hook"))
    app.hooks.on_enqueue.append(reject)

    # Act / Assert
    with pytest.raises(RuntimeError, match="enqueue hook"):
        async with app.engine.begin() as session:
            await demo.using(session=session).enqueue()
    assert await job_rows(database_url) == []
    reject.assert_awaited_once()


async def test_transition_hook_runs_in_the_transaction_that_changes_the_state(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    seen: dict[str, object] = {}

    @app.task("demo")
    async def demo() -> None:
        return None

    async def look(connection: object, job: object, from_state: str, to_state: str) -> None:
        if to_state != "succeeded":
            return
        state = await connection.scalar(  # type: ignore[attr-defined]
            text("SELECT state FROM akadze.jobs WHERE task = 'demo'")
        )
        seen["state"] = state
        seen["from"] = from_state
        seen["backend"] = await connection.scalar(text("SELECT pg_backend_pid()"))  # type: ignore[attr-defined]

    app.hooks.on_transition.append(look)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    assert seen["from"] == "running"
    assert seen["state"] == "succeeded"
    async with connection(database_url) as opened:
        other = await opened.fetchval("SELECT pg_backend_pid()")
    assert seen["backend"] != other


async def test_transition_hook_error_rolls_back_only_its_own_writes(
    app: Akadze, database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange — the hook audits every change and raises on the outcome
    async with connection(database_url) as opened:
        await opened.execute("CREATE TABLE akadze.test_audit (to_state text)")

    @app.task("demo", max_attempts=3)
    async def demo() -> str:
        return "done"

    async def audit(connection: object, job: object, from_state: str, to_state: str) -> None:
        await connection.execute(  # type: ignore[attr-defined]
            text("INSERT INTO akadze.test_audit VALUES (:to)"), {"to": to_state}
        )
        if to_state == "succeeded":
            raise RuntimeError("audit says no: secret-detail")

    app.hooks.on_transition.append(audit)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert (row["state"], as_json(row["result"])) == ("succeeded", "done")
    async with connection(database_url) as opened:
        audited = [
            r["to_state"] for r in await opened.fetch("SELECT to_state FROM akadze.test_audit")
        ]
    assert audited == ["running"]
    assert "on_transition hook" in caplog.text and "RuntimeError" in caplog.text
    assert "secret-detail" not in caplog.text


async def test_transition_hook_database_error_does_not_abort_the_transition(
    app: Akadze, database_url: str
) -> None:
    # Arrange — a failed statement would abort the whole transaction without a savepoint
    @app.task("demo")
    async def demo() -> None:
        return None

    async def broken(connection: object, job: object, from_state: str, to_state: str) -> None:
        await connection.execute(text("SELECT 1 / 0"))  # type: ignore[attr-defined]

    app.hooks.on_transition.append(broken)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await drain(app)

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "succeeded"


async def test_transition_hook_that_always_raises_does_not_stop_the_worker(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    calls = 0

    @app.task("demo")
    async def demo() -> None:
        nonlocal calls
        calls += 1

    def broken(connection: object, job: object, from_state: str, to_state: str) -> None:
        raise RuntimeError("broken hook")

    app.hooks.on_transition.append(broken)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(
        app, heartbeat_interval=timedelta(hours=1), poll_interval=timedelta(milliseconds=50)
    )
    stop = asyncio.Event()

    async def stop_when_done() -> None:
        while (row := await job_row(database_url, "demo")) is None or row["state"] != "succeeded":
            await asyncio.sleep(0.02)
        stop.set()

    # Act
    await asyncio.wait_for(asyncio.gather(worker.serve(stop), stop_when_done()), 10)

    # Assert
    assert calls == 1


async def test_transition_hook_error_keeps_the_retry(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        raise ValueError("bad input")

    def reject_retry(connection: object, job: object, from_state: str, to_state: str) -> None:
        if (from_state, to_state) == ("running", "queued"):
            raise RuntimeError("no retries")

    app.hooks.on_transition.append(reject_retry)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert (row["state"], row["attempt"]) == ("queued", 1)
    assert as_json(row["errors"])[-1]["message"] == "bad input"


async def test_transition_hook_error_keeps_a_queued_cancel(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    def reject_cancel(connection: object, job: object, from_state: str, to_state: str) -> None:
        if to_state == "cancelled":
            raise RuntimeError("no cancels")

    app.hooks.on_transition.append(reject_cancel)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    row = await job_row(database_url, "demo")
    assert row is not None
    async with app.engine.begin() as session:
        await request_cancel(session, row["id"])

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "cancelled"


async def test_logs_omit_arguments_and_results(
    app: Akadze, database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange
    @app.task("demo")
    async def demo(secret: str) -> str:
        return "secret-result-value"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue(secret="secret-arg-value")

    # Act
    with caplog.at_level(logging.INFO, logger="akadze"):
        await Worker(app).step()

    # Assert
    assert "secret-arg-value" not in caplog.text
    assert "secret-result-value" not in caplog.text
    assert "succeeded" in caplog.text


@pytest.mark.parametrize(
    "error",
    [TimeoutError("secret-msg"), ConnectionRefusedError("secret-msg")],
    ids=["timeout", "network"],
)
async def test_transition_hook_network_error_is_contained(
    app: Akadze, database_url: str, caplog: pytest.LogCaptureFixture, error: Exception
) -> None:
    # Arrange — an OSError from the hook is not a lost database connection
    @app.task("demo")
    async def demo() -> None:
        return None

    def webhook(connection: object, job: object, from_state: str, to_state: str) -> None:
        raise error

    app.hooks.on_transition.append(webhook)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "succeeded"
    assert f"{type(error).__name__} at " in caplog.text
    assert "secret-msg" not in caplog.text


async def test_transition_hook_error_does_not_block_rescue(app: Akadze, database_url: str) -> None:
    # Arrange — a stale worker holds the job; the hook rejects every change
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)
    await worker.register()
    await worker.claim_available()

    def broken(connection: object, job: object, from_state: str, to_state: str) -> None:
        raise RuntimeError("broken hook")

    app.hooks.on_transition.append(broken)
    await age_worker_heartbeat(app.engine, worker.id, older_than=timedelta(minutes=2))

    # Act
    await rescue(app.engine, app.hooks, ttl=timedelta(seconds=30))

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert (row["state"], row["attempt"], row["worker_id"]) == ("queued", 1, None)
