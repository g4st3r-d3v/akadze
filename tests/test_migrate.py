from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
import pytest

from akadze import applied_versions, migrate
from akadze.schema import MigrateError


@asynccontextmanager
async def _connection(database_url: str) -> AsyncIterator[asyncpg.Connection]:
    connection = await asyncpg.connect(database_url)
    try:
        yield connection
    finally:
        await connection.close()


def _cli(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "akadze.cli", "migrate"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


async def test_applied_versions_does_not_create_schema(database_url: str) -> None:
    # Act
    versions = await applied_versions(database_url)

    # Assert
    assert versions == []
    async with _connection(database_url) as connection:
        exists = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata WHERE schema_name = 'akadze'
            )
            """
        )
    assert exists is False


async def test_migrate_creates_schema(database_url: str) -> None:
    # Act
    await migrate(database_url)

    # Assert
    assert await applied_versions(database_url) == [
        "001_initial",
        "002_queue_pauses",
        "003_limits",
        "004_start_rate",
    ]
    async with _connection(database_url) as connection:
        tables = await connection.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'akadze'
            ORDER BY table_name
            """
        )
        identity = await connection.fetchval(
            """
            SELECT is_identity
            FROM information_schema.columns
            WHERE table_schema = 'akadze' AND table_name = 'jobs' AND column_name = 'id'
            """
        )
        result_nullable = await connection.fetchval(
            """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'akadze' AND table_name = 'jobs' AND column_name = 'result'
            """
        )
        limit_key_nullable = await connection.fetchval(
            """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'akadze'
              AND table_name = 'jobs'
              AND column_name = 'limit_key'
            """
        )
        max_running_nullable = await connection.fetchval(
            """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'akadze'
              AND table_name = 'limits'
              AND column_name = 'max_running'
            """
        )
        rate_columns = await connection.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'akadze'
              AND table_name = 'limits'
              AND column_name IN ('max_starts', 'window')
            ORDER BY column_name
            """
        )
        claim_index = await connection.fetchval(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'akadze' AND indexname = 'jobs_claim_idx'
            """
        )
        running_limit_index = await connection.fetchval(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'akadze' AND indexname = 'jobs_running_limit_key_idx'
            """
        )
        start_journal_index = await connection.fetchval(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'akadze'
              AND indexname = 'limit_starts_key_started_at_idx'
            """
        )
    assert [row["table_name"] for row in tables] == [
        "jobs",
        "limit_starts",
        "limits",
        "periodic_runs",
        "queue_pauses",
        "schema_migrations",
        "workers",
    ]
    assert identity == "YES"
    assert result_nullable == "YES"
    assert limit_key_nullable == "YES"
    assert max_running_nullable == "YES"
    assert [dict(row) for row in rate_columns] == [
        {"column_name": "max_starts", "data_type": "integer"},
        {"column_name": "window", "data_type": "interval"},
    ]
    assert claim_index is not None
    assert "priority DESC" in claim_index
    assert running_limit_index is not None
    assert start_journal_index is not None


async def test_migrate_is_idempotent(database_url: str) -> None:
    # Arrange
    await migrate(database_url)

    # Act
    await migrate(database_url)

    # Assert
    assert await applied_versions(database_url) == [
        "001_initial",
        "002_queue_pauses",
        "003_limits",
        "004_start_rate",
    ]


async def test_job_row_uses_defaults(database_url: str) -> None:
    # Arrange
    await migrate(database_url)

    # Act
    async with _connection(database_url) as connection:
        inserted = await connection.fetchrow(
            """
            INSERT INTO akadze.jobs (task)
            VALUES ('demo')
            RETURNING queue, priority, state, result
            """
        )

    # Assert
    assert dict(inserted) == {
        "queue": "default",
        "priority": 0,
        "state": "queued",
        "result": None,
    }


async def test_invalid_state_is_rejected(database_url: str) -> None:
    # Arrange
    await migrate(database_url)

    # Act / Assert
    async with _connection(database_url) as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                "INSERT INTO akadze.jobs (task, state) VALUES ('demo', 'nope')"
            )


async def test_active_unique_key_rejects_a_duplicate(database_url: str) -> None:
    # Arrange
    await migrate(database_url)
    async with _connection(database_url) as connection:
        await connection.execute(
            "INSERT INTO akadze.jobs (task, unique_key) VALUES ('demo', 'vendor:1')"
        )

        # Act / Assert
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                "INSERT INTO akadze.jobs (task, unique_key) VALUES ('other', 'vendor:1')"
            )


async def test_unique_key_is_free_after_success(database_url: str) -> None:
    # Arrange
    await migrate(database_url)
    async with _connection(database_url) as connection:
        await connection.execute(
            "INSERT INTO akadze.jobs (task, unique_key) VALUES ('demo', 'vendor:1')"
        )
        await connection.execute(
            """
            UPDATE akadze.jobs
            SET state = 'succeeded', finished_at = now()
            WHERE unique_key = 'vendor:1'
            """
        )

        # Act
        await connection.execute(
            "INSERT INTO akadze.jobs (task, unique_key) VALUES ('again', 'vendor:1')"
        )

        # Assert
        count = await connection.fetchval(
            "SELECT count(*) FROM akadze.jobs WHERE unique_key = 'vendor:1'"
        )
    assert count == 2


async def test_concurrent_migrate_applies_once(database_url: str) -> None:
    # Act
    await asyncio.gather(migrate(database_url), migrate(database_url))

    # Assert
    assert await applied_versions(database_url) == [
        "001_initial",
        "002_queue_pauses",
        "003_limits",
        "004_start_rate",
    ]


async def test_cli_migrate_prints_applied_version(database_url: str) -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env)

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 0, result.stderr
    assert "applied 001_initial" in result.stdout
    assert "applied 002_queue_pauses" in result.stdout
    assert "applied 003_limits" in result.stdout
    assert "applied 004_start_rate" in result.stdout
    assert database_url not in output


async def test_cli_migrate_prints_up_to_date(database_url: str) -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url
    setup = _cli(env)
    if setup.returncode != 0:
        raise RuntimeError(setup.stderr)

    # Act
    result = _cli(env)

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "schema up to date"
    assert database_url not in output


def test_cli_migrate_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env)

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr


def test_cli_connection_refused_hides_password_and_traceback() -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = "postgresql://akadze:super-secret-pw@127.0.0.1:1/akadze"

    # Act
    result = _cli(env)

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "migrate failed:" in result.stderr
    assert "Traceback" not in output
    assert "super-secret-pw" not in output


async def test_dollar_quotes_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setattr(
        "akadze.schema._migration_scripts",
        lambda: [("001_bad", "DO $$ BEGIN NULL; END $$;")],
    )

    # Act / Assert
    with pytest.raises(MigrateError, match="dollar quotes") as caught:
        await migrate("postgresql://akadze:super-secret-pw@127.0.0.1:1/akadze")
    assert "super-secret-pw" not in str(caught.value)
