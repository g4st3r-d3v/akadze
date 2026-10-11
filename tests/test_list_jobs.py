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
from tests.factories import insert_job


async def test_list_jobs_orders_by_priority_run_at_and_id(app: Akadze) -> None:
    # Arrange
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    low = await insert_job(app.engine, task="low", priority=1, run_at=t0)
    mid_late = await insert_job(
        app.engine, task="mid-late", priority=5, run_at=t0 + timedelta(minutes=2)
    )
    mid_early = await insert_job(
        app.engine, task="mid-early", priority=5, run_at=t0 + timedelta(minutes=1)
    )
    high = await insert_job(app.engine, task="high", priority=10, run_at=t0 + timedelta(hours=1))

    # Act
    jobs = await list_jobs(app.engine)

    # Assert
    assert [job.id for job in jobs] == [high, mid_early, mid_late, low]
    assert [job.task for job in jobs] == ["high", "mid-early", "mid-late", "low"]


@pytest.mark.parametrize(
    ("filter_kwargs", "keep_task", "other_kwargs"),
    [
        ({"queue": "mail"}, "keep", {"task": "other", "queue": "default"}),
        ({"state": "failed"}, "boom", {"task": "ready", "state": "queued"}),
    ],
    ids=["queue", "state"],
)
async def test_list_jobs_filters(
    app: Akadze,
    filter_kwargs: dict[str, str],
    keep_task: str,
    other_kwargs: dict[str, str],
) -> None:
    # Arrange
    keep = await insert_job(app.engine, task=keep_task, **filter_kwargs)
    await insert_job(app.engine, **other_kwargs)

    # Act
    jobs = await list_jobs(app.engine, **filter_kwargs)

    # Assert
    assert len(jobs) == 1
    assert jobs[0].id == keep
    for key, value in filter_kwargs.items():
        assert getattr(jobs[0], key) == value


@pytest.mark.parametrize("limit", [1, 2])
async def test_list_jobs_respects_limit(app: Akadze, limit: int) -> None:
    # Arrange
    first = await insert_job(app.engine, task="a", priority=3)
    second = await insert_job(app.engine, task="b", priority=2)
    await insert_job(app.engine, task="c", priority=1)
    expected = [first, second][:limit]

    # Act
    jobs = await list_jobs(app.engine, limit=limit)

    # Assert
    assert [job.id for job in jobs] == expected


async def test_list_jobs_summary_has_no_args(app: Akadze, fake) -> None:
    # Arrange
    await insert_job(app.engine, task=fake.slug(), args={"token": fake.uuid4()})

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


@pytest.mark.parametrize(
    ("limit", "match"),
    [
        (0, "limit"),
        (1001, "limit"),
    ],
    ids=["below-one", "above-thousand"],
)
async def test_list_jobs_rejects_invalid_limit(app: Akadze, limit: int, match: str) -> None:
    # Act / Assert
    with pytest.raises(AkadzeError, match=match):
        await list_jobs(app.engine, limit=limit)


async def test_list_jobs_rejects_unknown_state(app: Akadze) -> None:
    # Act / Assert
    with pytest.raises(AkadzeError, match="unknown state"):
        await list_jobs(app.engine, state="nope")


async def test_list_jobs_does_not_mutate_rows(app: Akadze, database_url: str) -> None:
    # Arrange
    job_id = await insert_job(app.engine, task="demo", priority=4)
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
    first = await insert_job(app.engine, task="alpha", priority=2)
    second = await insert_job(app.engine, task="beta", priority=1)
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
    keep = await insert_job(app.engine, task="mail-job", queue="mail", state="failed")
    await insert_job(app.engine, task="other", queue="mail", state="queued")
    await insert_job(app.engine, task="elsewhere", queue="default", state="failed")
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
