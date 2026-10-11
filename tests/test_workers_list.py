from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from akadze import Akadze, WorkerSummary, list_workers
from tests.factories import insert_worker


async def test_list_workers_returns_fields_and_order(app: Akadze) -> None:
    # Arrange
    older_id = UUID("00000000-0000-4000-8000-000000000001")
    newer_id = UUID("00000000-0000-4000-8000-000000000002")
    tie_a = UUID("00000000-0000-4000-8000-00000000000a")
    tie_b = UUID("00000000-0000-4000-8000-00000000000b")
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    await insert_worker(
        app.engine,
        worker_id=older_id,
        hostname="old-host",
        pid=11,
        queues=["default"],
        started_at=t0,
        heartbeat_at=t0,
    )
    await insert_worker(
        app.engine,
        worker_id=newer_id,
        hostname="new-host",
        pid=22,
        queues=["mail", "default"],
        started_at=t0 + timedelta(minutes=1),
        heartbeat_at=t0 + timedelta(minutes=5),
    )
    await insert_worker(
        app.engine,
        worker_id=tie_b,
        hostname="tie-b",
        pid=33,
        queues=["batch"],
        started_at=t0,
        heartbeat_at=t0 + timedelta(minutes=2),
    )
    await insert_worker(
        app.engine,
        worker_id=tie_a,
        hostname="tie-a",
        pid=44,
        queues=["batch"],
        started_at=t0,
        heartbeat_at=t0 + timedelta(minutes=2),
    )

    # Act
    workers = await list_workers(app.engine)

    # Assert
    assert [worker.id for worker in workers] == [newer_id, tie_a, tie_b, older_id]
    assert workers[0] == WorkerSummary(
        id=newer_id,
        hostname="new-host",
        pid=22,
        queues=("mail", "default"),
        started_at=t0 + timedelta(minutes=1),
        heartbeat_at=t0 + timedelta(minutes=5),
    )


async def test_list_workers_summary_has_no_job_arguments(app: Akadze) -> None:
    # Arrange
    await insert_worker(app.engine, worker_id=uuid4(), queues=["default"])

    # Act
    workers = await list_workers(app.engine)

    # Assert
    assert len(workers) == 1
    names = {field.name for field in dataclasses.fields(workers[0])}
    assert names == {
        "id",
        "hostname",
        "pid",
        "queues",
        "started_at",
        "heartbeat_at",
    }
    assert "args" not in names


async def test_list_workers_empty(app: Akadze) -> None:
    # Act
    workers = await list_workers(app.engine)

    # Assert
    assert workers == ()


def _cli(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "akadze.cli", *argv],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


async def test_cli_workers_list_prints_one_line_per_worker(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    worker_id = UUID("11111111-1111-4111-8111-111111111111")
    started = datetime(2026, 2, 1, 8, 0, tzinfo=UTC)
    heartbeat = datetime(2026, 2, 1, 8, 5, tzinfo=UTC)
    await insert_worker(
        app.engine,
        worker_id=worker_id,
        hostname="box",
        pid=4242,
        queues=["mail", "default"],
        started_at=started,
        heartbeat_at=heartbeat,
    )
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "workers", "list")

    # Assert
    assert result.returncode == 0, result.stderr
    assert (
        f"id={worker_id} hostname=box pid=4242 queues=mail,default "
        f"started_at={started.isoformat()} heartbeat_at={heartbeat.isoformat()}"
    ) in result.stdout
    assert database_url not in result.stdout + result.stderr


def test_cli_workers_list_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env, "workers", "list")

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr


def test_cli_workers_list_hides_password_on_failure() -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = "postgresql://akadze:super-secret-pw@127.0.0.1:1/akadze"

    # Act
    result = _cli(env, "workers", "list")

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "workers list failed:" in result.stderr
    assert "super-secret-pw" not in output
    assert "Traceback" not in output
