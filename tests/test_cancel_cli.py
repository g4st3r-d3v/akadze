from __future__ import annotations

import os
import subprocess
import sys

import pytest

from akadze import Akadze, request_cancel
from tests.factories import insert_job
from tests.pg import job_row


async def test_request_cancel_marks_queued_job(app: Akadze, database_url: str) -> None:
    # Arrange
    job_id = await insert_job(app.engine, task="demo", state="queued")

    # Act
    async with app.engine.begin() as session:
        await request_cancel(session, job_id)

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["cancel_requested_at"] is not None


@pytest.mark.parametrize("state", ["succeeded", "failed", "cancelled"])
async def test_request_cancel_ignores_terminal_state(
    app: Akadze, database_url: str, state: str
) -> None:
    # Arrange
    job_id = await insert_job(app.engine, task="demo", state=state)

    # Act
    async with app.engine.begin() as session:
        await request_cancel(session, job_id)

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["cancel_requested_at"] is None
    assert row["state"] == state


async def test_request_cancel_missing_job_is_noop(app: Akadze) -> None:
    # Act / Assert — same silent semantics as the UPDATE for wrong state
    async with app.engine.begin() as session:
        await request_cancel(session, 9_999_999)


def _cli(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "akadze.cli", *argv],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


async def test_cli_jobs_cancel_marks_queued_job(app: Akadze, database_url: str) -> None:
    # Arrange
    job_id = await insert_job(app.engine, task="demo", state="queued")
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "cancel", str(job_id))

    # Assert
    assert result.returncode == 0, result.stderr
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["cancel_requested_at"] is not None
    assert database_url not in result.stdout + result.stderr


@pytest.mark.parametrize("state", ["succeeded", "failed", "cancelled"])
async def test_cli_jobs_cancel_terminal_state_exits_one(
    app: Akadze, database_url: str, state: str
) -> None:
    # Arrange — request_cancel stays a no-op; the CLI reports it
    job_id = await insert_job(app.engine, task="demo", state=state)
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "cancel", str(job_id))

    # Assert
    assert result.returncode == 1
    assert "job is not active" in result.stderr
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["cancel_requested_at"] is None


async def test_cli_jobs_cancel_missing_id_exits_one(app: Akadze, database_url: str) -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "cancel", "9999999")

    # Assert
    assert result.returncode == 1
    assert "job is not active" in result.stderr


def test_cli_jobs_cancel_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env, "jobs", "cancel", "1")

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr


def test_cli_jobs_cancel_hides_password_on_failure() -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = "postgresql://akadze:super-secret-pw@127.0.0.1:1/akadze"

    # Act
    result = _cli(env, "jobs", "cancel", "1")

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "jobs cancel failed:" in result.stderr
    assert "super-secret-pw" not in output
    assert "Traceback" not in output
