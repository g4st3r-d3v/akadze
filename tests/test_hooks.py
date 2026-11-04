from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import text

from akadze import Akadze, request_cancel
from akadze.testing import drain
from akadze.worker import Worker
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


async def test_transition_hook_error_rolls_its_change_back(app: Akadze, database_url: str) -> None:
    # Arrange
    seen: dict[str, object] = {}

    @app.task("demo")
    async def demo() -> None:
        return None

    async def reject(connection: object, job: object, from_state: str, to_state: str) -> None:
        if to_state != "succeeded":
            return
        state = await connection.scalar(  # type: ignore[attr-defined]
            text("SELECT state FROM akadze.jobs WHERE task = 'demo'")
        )
        seen["state"] = state
        seen["from"] = from_state
        seen["to"] = to_state
        backend = await connection.scalar(text("SELECT pg_backend_pid()"))  # type: ignore[attr-defined]
        seen["backend"] = backend
        raise RuntimeError("transition hook")

    app.hooks.on_transition.append(reject)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    assert seen["from"] == "running"
    assert seen["to"] == "succeeded"
    assert seen["state"] == "succeeded"
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "failed"
    assert row["result"] is None
    async with connection(database_url) as opened:
        other = await opened.fetchval("SELECT pg_backend_pid()")
    assert seen["backend"] != other


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


async def test_transition_hook_error_fails_the_job_without_running_it_again(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    calls = 0
    seen: list[tuple[str, str]] = []

    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        nonlocal calls
        calls += 1

    def audit(connection: object, job: object, from_state: str, to_state: str) -> None:
        seen.append((from_state, to_state))
        if to_state == "succeeded":
            raise RuntimeError("audit table is gone")

    app.hooks.on_transition.append(audit)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await drain(app)

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "failed"
    assert calls == 1
    assert seen == [("queued", "running"), ("running", "succeeded"), ("running", "failed")]
    errors = as_json(row["errors"])
    assert errors[-1]["message"] == (
        "outcome could not be recorded: RuntimeError: audit table is gone"
    )


async def test_transition_hook_that_always_raises_stops_the_worker(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    calls = 0

    @app.task("demo")
    async def demo() -> None:
        nonlocal calls
        calls += 1

    def broken(connection: object, job: object, from_state: str, to_state: str) -> None:
        if to_state in ("succeeded", "failed"):
            raise RuntimeError("broken hook")

    app.hooks.on_transition.append(broken)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app, heartbeat_interval=timedelta(hours=1))

    # Act
    with pytest.raises(RuntimeError, match="broken hook"):
        await worker.serve(asyncio.Event())

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert (row["state"], row["attempt"], row["worker_id"]) == ("running", 0, worker.id)
    assert calls == 1
    async with connection(database_url) as opened:
        kept = await opened.fetchval("SELECT count(*) FROM akadze.workers WHERE id = $1", worker.id)
    assert kept == 1


async def test_transition_hook_error_on_a_cancel_fails_the_job_with_the_reason(
    app: Akadze, database_url: str
) -> None:
    # Arrange — cancel arrives mid-run, the body raises, the hook rejects "cancelled"
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        row = await job_row(database_url, "demo")
        assert row is not None
        async with app.engine.begin() as session:
            await request_cancel(session, row["id"])
        raise RuntimeError("body")

    def reject_cancel(connection: object, job: object, from_state: str, to_state: str) -> None:
        if to_state == "cancelled":
            raise RuntimeError("no cancels")

    app.hooks.on_transition.append(reject_cancel)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "failed"
    assert as_json(row["errors"])[-1]["message"] == (
        "outcome could not be recorded: RuntimeError: no cancels; the run raised RuntimeError"
    )


async def test_transition_hook_error_keeps_the_run_error_in_the_reason(
    app: Akadze, database_url: str
) -> None:
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
    assert row["state"] == "failed"
    assert as_json(row["errors"])[-1]["message"] == (
        "outcome could not be recorded: RuntimeError: no retries; the run raised ValueError"
    )
