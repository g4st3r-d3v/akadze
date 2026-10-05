from __future__ import annotations

import asyncio
import os
import subprocess
import sys

import asyncpg
import pytest

from akadze import migrate


async def test_migrate_creates_schema_and_is_idempotent(database_url: str) -> None:
    applied = await migrate(database_url)
    assert applied == ["001_initial"]

    conn = await asyncpg.connect(database_url)
    try:
        tables = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'akadze'
            ORDER BY table_name
            """
        )
        assert [row["table_name"] for row in tables] == [
            "jobs",
            "periodic_runs",
            "schema_migrations",
            "workers",
        ]

        identity = await conn.fetchval(
            """
            SELECT is_identity
            FROM information_schema.columns
            WHERE table_schema = 'akadze' AND table_name = 'jobs' AND column_name = 'id'
            """
        )
        assert identity == "YES"

        result_nullable = await conn.fetchval(
            """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'akadze' AND table_name = 'jobs' AND column_name = 'result'
            """
        )
        assert result_nullable == "YES"

        claim_index = await conn.fetchval(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = 'akadze' AND indexname = 'jobs_claim_idx'
            """
        )
        assert "priority DESC" in claim_index

        inserted = await conn.fetchrow(
            """
            INSERT INTO akadze.jobs (task)
            VALUES ('demo')
            RETURNING queue, priority, state, result
            """
        )
        assert dict(inserted) == {
            "queue": "default",
            "priority": 0,
            "state": "queued",
            "result": None,
        }
        versions = await conn.fetchval("SELECT count(*) FROM akadze.schema_migrations")
        assert versions == 1
    finally:
        await conn.close()

    assert await migrate(database_url) == []


async def test_state_check_and_active_unique_key(database_url: str) -> None:
    await migrate(database_url)
    conn = await asyncpg.connect(database_url)
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO akadze.jobs (task, state) VALUES ('demo', 'nope')"
            )

        await conn.execute(
            "INSERT INTO akadze.jobs (task, unique_key) VALUES ('demo', 'vendor:1')"
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO akadze.jobs (task, unique_key) VALUES ('other', 'vendor:1')"
            )

        await conn.execute(
            """
            UPDATE akadze.jobs
            SET state = 'succeeded', finished_at = now()
            WHERE unique_key = 'vendor:1'
            """
        )
        await conn.execute(
            "INSERT INTO akadze.jobs (task, unique_key) VALUES ('again', 'vendor:1')"
        )
    finally:
        await conn.close()


async def test_concurrent_migrate_applies_once(database_url: str) -> None:
    first, second = await asyncio.gather(migrate(database_url), migrate(database_url))
    assert sorted([*first, *second]) == ["001_initial"]

    conn = await asyncpg.connect(database_url)
    try:
        count = await conn.fetchval("SELECT count(*) FROM akadze.schema_migrations")
    finally:
        await conn.close()
    assert count == 1


async def test_cli_migrate_and_missing_url(database_url: str) -> None:
    env = dict(os.environ.items())
    env["AKADZE_DATABASE_URL"] = database_url
    first = subprocess.run(
        [sys.executable, "-m", "akadze.cli", "migrate"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert first.returncode == 0, first.stderr
    assert "applied 001_initial" in first.stdout
    assert database_url not in first.stdout + first.stderr

    second = subprocess.run(
        [sys.executable, "-m", "akadze.cli", "migrate"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert second.returncode == 0, second.stderr
    assert "schema up to date" in second.stdout
    assert database_url not in second.stdout + second.stderr

    env.pop("AKADZE_DATABASE_URL")
    missing = subprocess.run(
        [sys.executable, "-m", "akadze.cli", "migrate"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert missing.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in missing.stderr
