from __future__ import annotations

import os
import subprocess
import sys

import pytest
from sqlalchemy import text

from akadze import Akadze, AkadzeError, Cancel, DuplicateJob, Fail, requeue
from akadze.worker import Worker
from tests.pg import as_json, connection, job_row


async def test_requeue_failed_job_is_claimed_again(app: Akadze, database_url: str) -> None:
    # Arrange
    runs = 0

    @app.task("demo", max_attempts=1)
    async def demo() -> str:
        nonlocal runs
        runs += 1
        if runs == 1:
            raise RuntimeError("boom")
        return "ok"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    await Worker(app).step()
    failed = await job_row(database_url, "demo")
    assert failed is not None
    assert failed["state"] == "failed"
    assert failed["attempt"] == 1
    assert failed["max_attempts"] == 1
    errors_before = as_json(failed["errors"])

    # Act
    async with app.engine.begin() as session:
        await requeue(session, int(failed["id"]))

    # Assert
    after = await job_row(database_url, "demo")
    assert after is not None
    assert after["state"] == "queued"
    assert after["attempt"] == 1
    assert after["run_count"] == failed["run_count"]
    assert after["max_attempts"] == 2
    assert after["worker_id"] is None
    assert after["started_at"] is None
    assert after["finished_at"] is None
    assert after["cancel_requested_at"] is None
    assert as_json(after["errors"]) == errors_before
    assert as_json(after["args"]) == as_json(failed["args"])
    await Worker(app).step()
    done = await job_row(database_url, "demo")
    assert done is not None
    assert done["state"] == "succeeded"
    assert runs == 2


async def test_requeue_leaves_cancelled_unchanged(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        raise Cancel("stop")

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    await Worker(app).step()
    cancelled = await job_row(database_url, "demo")
    assert cancelled is not None
    assert cancelled["state"] == "cancelled"

    # Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="job is not failed"):
            await requeue(session, int(cancelled["id"]))
    still = await job_row(database_url, "demo")
    assert still is not None
    assert still["state"] == "cancelled"


async def test_requeue_rejects_queued(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> str:
        return "ok"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    queued = await job_row(database_url, "demo")
    assert queued is not None

    # Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="job is not failed"):
            await requeue(session, int(queued["id"]))


async def test_requeue_rejects_running(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> str:
        return "ok"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app)
    await worker.register()
    await worker.claim_available()
    running = await job_row(database_url, "demo")
    assert running is not None
    assert running["state"] == "running"

    # Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="job is not failed"):
            await requeue(session, int(running["id"]))
    await worker.shutdown()


async def test_requeue_rejects_succeeded(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> str:
        return "ok"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    await Worker(app).step()
    succeeded = await job_row(database_url, "demo")
    assert succeeded is not None
    assert succeeded["state"] == "succeeded"

    # Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="job is not failed"):
            await requeue(session, int(succeeded["id"]))


async def test_requeue_missing_job_raises(app: Akadze) -> None:
    # Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="job is not failed"):
            await requeue(session, 9_999_999)


async def test_requeue_duplicate_unique_key_keeps_failed_and_caller_txn(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("demo", max_attempts=1)
    async def demo() -> None:
        raise Fail("stop")

    async with app.engine.begin() as session:
        await demo.using(session=session, unique_key="once").enqueue()
    await Worker(app).step()
    failed = await job_row(database_url, "demo")
    assert failed is not None
    assert failed["state"] == "failed"
    async with app.engine.begin() as session:
        await demo.using(session=session, unique_key="once").enqueue()

    # Act
    async with app.engine.begin() as session:
        with pytest.raises(DuplicateJob):
            await requeue(session, int(failed["id"]))
        marker = await session.scalar(text("SELECT 1"))

    # Assert
    assert marker == 1
    async with connection(database_url) as opened:
        failed_row = await opened.fetchrow(
            "SELECT state FROM akadze.jobs WHERE id = $1", int(failed["id"])
        )
        queued_count = await opened.fetchval(
            "SELECT count(*) FROM akadze.jobs WHERE unique_key = 'once' AND state = 'queued'"
        )
    assert failed_row is not None
    assert failed_row["state"] == "failed"
    assert queued_count == 1


async def test_requeue_keeps_max_attempts_when_already_above_attempt(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("demo", max_attempts=5)
    async def demo() -> None:
        raise Fail("stop")

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    await Worker(app).step()
    failed = await job_row(database_url, "demo")
    assert failed is not None
    assert failed["attempt"] == 1
    assert failed["max_attempts"] == 5

    # Act
    async with app.engine.begin() as session:
        await requeue(session, int(failed["id"]))

    # Assert
    after = await job_row(database_url, "demo")
    assert after is not None
    assert after["max_attempts"] == 5
    assert after["state"] == "queued"


def _cli(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "akadze.cli", *argv],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


async def test_cli_jobs_requeue_returns_failed_to_queued(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("demo", max_attempts=1)
    async def demo() -> None:
        raise RuntimeError("boom")

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    await Worker(app).step()
    failed = await job_row(database_url, "demo")
    assert failed is not None
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "requeue", str(int(failed["id"])))

    # Assert
    assert result.returncode == 0, result.stderr
    after = await job_row(database_url, "demo")
    assert after is not None
    assert after["state"] == "queued"
    assert database_url not in result.stdout + result.stderr


def test_cli_jobs_requeue_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env, "jobs", "requeue", "1")

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr


def test_cli_jobs_requeue_hides_password_on_failure() -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = "postgresql://akadze:super-secret-pw@127.0.0.1:1/akadze"

    # Act
    result = _cli(env, "jobs", "requeue", "1")

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "jobs requeue failed:" in result.stderr
    assert "super-secret-pw" not in output
    assert "Traceback" not in output
