from __future__ import annotations

import logging

import pytest
from sqlalchemy import text

from akadze import Akadze
from akadze.worker import Worker
from tests.pg import connection, job_row, job_rows


async def test_enqueue_hook_error_rolls_back_the_insert(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    async def reject(connection: object, job: object) -> None:
        raise RuntimeError("enqueue hook")

    app.hooks.on_enqueue.append(reject)

    # Act / Assert
    with pytest.raises(RuntimeError, match="enqueue hook"):
        async with app.engine.begin() as session:
            await demo.using(session=session).enqueue()
    assert await job_rows(database_url) == []


async def test_transition_hook_sees_the_change_and_rolls_it_back(
    app: Akadze, database_url: str
) -> None:
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

    # Act / Assert
    with pytest.raises(RuntimeError, match="transition hook"):
        await Worker(app).step()
    assert seen["from"] == "running"
    assert seen["to"] == "succeeded"
    assert seen["state"] == "succeeded"
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "running"
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
