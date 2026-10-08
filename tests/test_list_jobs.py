from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from akadze import Akadze, AkadzeError, JobSummary, list_jobs, migrate
from akadze.schema import async_database_url


async def _insert(
    app: Akadze,
    *,
    task: str,
    queue: str = "default",
    state: str = "queued",
    priority: int = 0,
    run_at: datetime | None = None,
    attempt: int = 0,
    max_attempts: int = 3,
) -> int:
    async with app.engine.begin() as connection:
        job_id = await connection.scalar(
            text(
                """
                INSERT INTO akadze.jobs (
                    task, queue, state, priority, run_at, attempt, max_attempts, args
                )
                VALUES (
                    :task, :queue, :state, :priority,
                    COALESCE(:run_at, now()), :attempt, :max_attempts, '{}'::jsonb
                )
                RETURNING id
                """
            ),
            {
                "task": task,
                "queue": queue,
                "state": state,
                "priority": priority,
                "run_at": run_at,
                "attempt": attempt,
                "max_attempts": max_attempts,
            },
        )
    assert job_id is not None
    return int(job_id)


async def test_list_jobs_orders_by_priority_run_at_and_id(app: Akadze) -> None:
    # Arrange
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    low = await _insert(app, task="low", priority=1, run_at=t0)
    mid_late = await _insert(app, task="mid-late", priority=5, run_at=t0 + timedelta(minutes=2))
    mid_early = await _insert(app, task="mid-early", priority=5, run_at=t0 + timedelta(minutes=1))
    high = await _insert(app, task="high", priority=10, run_at=t0 + timedelta(hours=1))

    # Act
    jobs = await list_jobs(app.engine)

    # Assert
    assert [job.id for job in jobs] == [high, mid_early, mid_late, low]
    assert [job.task for job in jobs] == ["high", "mid-early", "mid-late", "low"]


async def test_list_jobs_filters_by_queue(app: Akadze) -> None:
    # Arrange
    keep = await _insert(app, task="keep", queue="mail")
    await _insert(app, task="other", queue="default")

    # Act
    jobs = await list_jobs(app.engine, queue="mail")

    # Assert
    assert len(jobs) == 1
    assert jobs[0].id == keep
    assert jobs[0].queue == "mail"


async def test_list_jobs_filters_by_state(app: Akadze) -> None:
    # Arrange
    failed = await _insert(app, task="boom", state="failed")
    await _insert(app, task="ready", state="queued")

    # Act
    jobs = await list_jobs(app.engine, state="failed")

    # Assert
    assert len(jobs) == 1
    assert jobs[0].id == failed
    assert jobs[0].state == "failed"


async def test_list_jobs_respects_limit(app: Akadze) -> None:
    # Arrange
    first = await _insert(app, task="a", priority=3)
    second = await _insert(app, task="b", priority=2)
    await _insert(app, task="c", priority=1)

    # Act
    jobs = await list_jobs(app.engine, limit=2)

    # Assert
    assert [job.id for job in jobs] == [first, second]


async def test_list_jobs_summary_has_no_args(app: Akadze) -> None:
    # Arrange
    await _insert(app, task="demo")

    # Act
    jobs = await list_jobs(app.engine)

    # Assert
    assert len(jobs) == 1
    assert isinstance(jobs[0], JobSummary)
    names = {field.name for field in dataclasses.fields(jobs[0])}
    assert "args" not in names
    assert "result" not in names
    assert "errors" not in names
    assert "meta" not in names
    assert names == {
        "id",
        "task",
        "queue",
        "state",
        "priority",
        "attempt",
        "max_attempts",
        "run_at",
        "started_at",
        "finished_at",
    }


async def test_list_jobs_rejects_limit_below_one(app: Akadze) -> None:
    # Act / Assert
    with pytest.raises(AkadzeError, match="limit"):
        await list_jobs(app.engine, limit=0)


async def test_list_jobs_rejects_limit_above_thousand(app: Akadze) -> None:
    # Act / Assert
    with pytest.raises(AkadzeError, match="limit"):
        await list_jobs(app.engine, limit=1001)


async def test_list_jobs_rejects_unknown_state(app: Akadze) -> None:
    # Act / Assert
    with pytest.raises(AkadzeError, match="unknown state"):
        await list_jobs(app.engine, state="nope")


async def test_list_jobs_does_not_mutate_rows(app: Akadze, database_url: str) -> None:
    # Arrange
    job_id = await _insert(app, task="demo", priority=4)
    before = await _job_fingerprint(database_url, job_id)

    # Act
    await list_jobs(app.engine, queue="default", state="queued", limit=10)

    # Assert
    assert await _job_fingerprint(database_url, job_id) == before


async def _job_fingerprint(database_url: str, job_id: int) -> tuple[object, ...]:
    engine = create_async_engine(
        async_database_url(database_url),
        poolclass=NullPool,
        connect_args={"statement_cache_size": 0},
    )
    try:
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        """
                        SELECT state, priority, attempt, args, result, errors, meta, run_at
                        FROM akadze.jobs
                        WHERE id = :id
                        """
                    ),
                    {"id": job_id},
                )
            ).one()
        return tuple(row)
    finally:
        await engine.dispose()


def _cli(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "akadze.cli", *argv],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


async def test_cli_jobs_list_prints_one_line_per_job(app: Akadze, database_url: str) -> None:
    # Arrange
    first = await _insert(app, task="alpha", priority=2)
    second = await _insert(app, task="beta", priority=1)
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "list")

    # Assert
    assert result.returncode == 0, result.stderr
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 2
    assert f"id={first}" in lines[0]
    assert "task=alpha" in lines[0]
    assert f"id={second}" in lines[1]
    assert "task=beta" in lines[1]
    assert "args=" not in result.stdout


async def test_cli_jobs_list_applies_filters(app: Akadze, database_url: str) -> None:
    # Arrange
    keep = await _insert(app, task="mail-job", queue="mail", state="failed")
    await _insert(app, task="other", queue="mail", state="queued")
    await _insert(app, task="elsewhere", queue="default", state="failed")
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "list", "--queue", "mail", "--state", "failed", "--limit", "10")

    # Assert
    assert result.returncode == 0, result.stderr
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    assert f"id={keep}" in lines[0]
    assert "queue=mail" in lines[0]
    assert "state=failed" in lines[0]


def test_cli_jobs_list_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env, "jobs", "list")

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr


def test_cli_jobs_list_hides_password_on_connection_error() -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = "postgresql://akadze:super-secret-pw@127.0.0.1:1/akadze"

    # Act
    result = _cli(env, "jobs", "list")

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "jobs list failed:" in result.stderr
    assert "Traceback" not in output
    assert "super-secret-pw" not in output


async def test_cli_jobs_list_rejects_unknown_state(database_url: str) -> None:
    # Arrange
    await migrate(database_url)
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "list", "--state", "nope")

    # Assert
    assert result.returncode == 1
    assert "unknown state" in result.stderr
