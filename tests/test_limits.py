from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import text

from akadze import (
    Akadze,
    AkadzeError,
    LimitSummary,
    clear_limit,
    list_limits,
    set_limit,
)
from akadze.worker import Worker
from tests.factories import apply_limit, pause
from tests.pg import connection, job_row, job_rows, states_by_task


async def test_saturated_limit_key_claims_one_and_skips_waiter(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "first"

    @app.task("second")
    async def second() -> str:
        return "second"

    await apply_limit(app.engine, key="vendor", max_running=1)
    async with app.engine.begin() as session:
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()

    # Act
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()

    # Assert
    assert await states_by_task(database_url) == {"first": "running", "second": "queued"}
    waiter = next(row for row in await job_rows(database_url) if row["task"] == "second")
    assert waiter["run_count"] == 0
    assert waiter["attempt"] == 0
    await worker.shutdown()


async def test_unlimited_job_claimed_while_limit_key_saturated(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("limited-a")
    async def limited_a() -> str:
        return "a"

    @app.task("limited-b")
    async def limited_b() -> str:
        return "b"

    @app.task("free")
    async def free() -> str:
        return "free"

    await apply_limit(app.engine, key="vendor", max_running=1)
    async with app.engine.begin() as session:
        await limited_a.using(session=session, limit_key="vendor").enqueue()
        await limited_b.using(session=session, limit_key="vendor").enqueue()
        await free.using(session=session).enqueue()

    # Act
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()

    # Assert
    states = await states_by_task(database_url)
    assert states["limited-a"] == "running"
    assert states["limited-b"] == "queued"
    assert states["free"] == "running"
    await worker.shutdown()


async def test_claim_after_success_takes_waiting_limit_key(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "ok"

    @app.task("second")
    async def second() -> str:
        return "ok"

    await apply_limit(app.engine, key="vendor", max_running=1)
    async with app.engine.begin() as session:
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()
    await worker.step()
    first_row = await job_row(database_url, "first")
    assert first_row is not None
    assert first_row["state"] == "succeeded"

    # Act
    await worker.claim_available()

    # Assert
    second_row = await job_row(database_url, "second")
    assert second_row is not None
    assert second_row["state"] == "running"
    await worker.shutdown()


async def test_limit_key_without_limits_row_is_claimed(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("typo")
    async def typo() -> str:
        return "nope"

    async with app.engine.begin() as session:
        await typo.using(session=session, limit_key="missing").enqueue()

    # Act
    worker = Worker(app, slots=1)
    await worker.register()
    await worker.claim_available()

    # Assert
    row = await job_row(database_url, "typo")
    assert row is not None
    assert row["state"] == "running"
    assert row["run_count"] == 1
    await worker.shutdown()


async def test_two_workers_respect_max_running_one(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("one")
    async def one() -> str:
        return "one"

    @app.task("two")
    async def two() -> str:
        return "two"

    await apply_limit(app.engine, key="vendor", max_running=1)
    async with app.engine.begin() as session:
        await one.using(session=session, limit_key="vendor").enqueue()
        await two.using(session=session, limit_key="vendor").enqueue()
    first = Worker(app, slots=1)
    second = Worker(app, slots=1)
    await first.register()
    await second.register()

    # Act
    await asyncio.gather(first.claim_available(), second.claim_available())

    # Assert
    rows = await job_rows(database_url)
    running = [row for row in rows if row["state"] == "running"]
    queued = [row for row in rows if row["state"] == "queued"]
    assert len(running) == 1
    assert len(queued) == 1
    await first.shutdown()
    await second.shutdown()


async def test_set_limit_replaces_max_running(app: Akadze, database_url: str) -> None:
    # Arrange
    await apply_limit(app.engine, key="vendor", max_running=1)

    # Act
    await apply_limit(app.engine, key="vendor", max_running=3)

    # Assert
    async with connection(database_url) as opened:
        rows = await opened.fetch("SELECT key, max_running FROM akadze.limits")
    assert [dict(row) for row in rows] == [{"key": "vendor", "max_running": 3}]


@pytest.mark.parametrize(
    ("params", "match"),
    [
        ({"max_running": 0}, "max_running"),
        ({"max_starts": 0, "window": timedelta(minutes=1)}, "max_starts"),
        ({"max_starts": 1, "window": timedelta(0)}, "window"),
        ({"max_starts": 1}, "together"),
        ({"window": timedelta(minutes=1)}, "together"),
    ],
    ids=[
        "max-running-below-one",
        "max-starts-below-one",
        "non-positive-window",
        "rate-starts-alone",
        "rate-window-alone",
    ],
)
async def test_set_limit_rejects_invalid_policy(
    app: Akadze, params: dict[str, Any], match: str
) -> None:
    # Arrange / Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match=match):
            await set_limit(session, "vendor", **params)
        count = await session.scalar(text("SELECT count(*) FROM akadze.limits"))
    assert int(count or 0) == 0


async def test_empty_limit_key_is_stored_as_null(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> str:
        return "ok"

    # Act
    async with app.engine.begin() as session:
        await demo.using(session=session, limit_key="").enqueue()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["limit_key"] is None


async def test_paused_queue_still_blocks_limited_jobs(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", queue="mail")
    async def demo() -> str:
        return "ok"

    await apply_limit(app.engine, key="vendor", max_running=2)
    async with app.engine.begin() as session:
        await demo.using(session=session, limit_key="vendor").enqueue()
    await pause(app.engine, "mail")

    # Act
    worker = Worker(app, queues=["mail"])
    await worker.register()
    await worker.claim_available()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    await worker.shutdown()


async def test_rate_limit_blocks_same_key_while_other_key_claims(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "first"

    @app.task("second")
    async def second() -> str:
        return "second"

    @app.task("other")
    async def other() -> str:
        return "other"

    window = timedelta(minutes=1)
    await apply_limit(app.engine, key="vendor", max_running=None, max_starts=1, window=window)
    await apply_limit(app.engine, key="other", max_running=None, max_starts=1, window=window)
    async with app.engine.begin() as session:
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()
        await other.using(session=session, limit_key="other").enqueue()
    worker = Worker(app, slots=3)
    await worker.register()

    # Act
    await worker.claim_available()

    # Assert
    assert await states_by_task(database_url) == {
        "first": "running",
        "second": "queued",
        "other": "running",
    }
    waiter = next(row for row in await job_rows(database_url) if row["task"] == "second")
    assert waiter["run_count"] == 0
    assert waiter["attempt"] == 0
    await worker.shutdown()


async def test_rate_limit_allows_claim_after_window(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "first"

    @app.task("second")
    async def second() -> str:
        return "second"

    await apply_limit(
        app.engine,
        key="vendor",
        max_running=None,
        max_starts=1,
        window=timedelta(minutes=1),
    )
    async with app.engine.begin() as session:
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()
    first_row = await job_row(database_url, "first")
    second_row = await job_row(database_url, "second")
    assert first_row is not None and first_row["state"] == "running"
    assert second_row is not None and second_row["state"] == "queued"
    async with connection(database_url) as opened:
        await opened.execute(
            """
            UPDATE akadze.limit_starts
            SET started_at = now() - interval '2 minutes'
            """
        )

    # Act
    await worker.claim_available()

    # Assert
    second_row = await job_row(database_url, "second")
    assert second_row is not None
    assert second_row["state"] == "running"
    assert second_row["attempt"] == 0
    assert second_row["run_count"] == 1
    await worker.shutdown()


async def test_set_limit_sets_rate_without_touching_max_running(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    await apply_limit(app.engine, key="vendor", max_running=2)

    # Act
    await apply_limit(
        app.engine,
        key="vendor",
        max_running=None,
        max_starts=1,
        window=timedelta(minutes=1),
    )

    # Assert
    async with connection(database_url) as opened:
        row = await opened.fetchrow(
            """
            SELECT max_running, max_starts, "window" AS rate_window
            FROM akadze.limits
            WHERE key = 'vendor'
            """
        )
    assert dict(row) == {
        "max_running": 2,
        "max_starts": 1,
        "rate_window": timedelta(minutes=1),
    }


async def test_set_limit_sets_max_running_without_touching_rate(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    await apply_limit(
        app.engine,
        key="vendor",
        max_running=None,
        max_starts=3,
        window=timedelta(seconds=30),
    )

    # Act
    await apply_limit(app.engine, key="vendor", max_running=4)

    # Assert
    async with connection(database_url) as opened:
        row = await opened.fetchrow(
            """
            SELECT max_running, max_starts, "window" AS rate_window
            FROM akadze.limits
            WHERE key = 'vendor'
            """
        )
    assert dict(row) == {
        "max_running": 4,
        "max_starts": 3,
        "rate_window": timedelta(seconds=30),
    }


async def test_start_journal_prunes_marks_outside_window(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("first")
    async def first() -> str:
        return "first"

    @app.task("second")
    async def second() -> str:
        return "second"

    await apply_limit(
        app.engine,
        key="vendor",
        max_running=None,
        max_starts=1,
        window=timedelta(minutes=1),
    )
    async with app.engine.begin() as session:
        await first.using(session=session, limit_key="vendor").enqueue()
        await second.using(session=session, limit_key="vendor").enqueue()
    worker = Worker(app, slots=2)
    await worker.register()
    await worker.claim_available()
    async with connection(database_url) as opened:
        await opened.execute(
            """
            UPDATE akadze.limit_starts
            SET started_at = now() - interval '2 minutes'
            """
        )

    # Act
    await worker.claim_available()

    # Assert
    async with connection(database_url) as opened:
        marks = await opened.fetch(
            """
            SELECT started_at > now() - interval '1 minute' AS fresh
            FROM akadze.limit_starts
            WHERE key = 'vendor'
            ORDER BY id
            """
        )
    assert len(marks) == 1
    assert marks[0]["fresh"] is True
    await worker.shutdown()


async def test_list_limits_returns_key_and_policy_fields(app: Akadze) -> None:
    # Arrange
    await apply_limit(app.engine, key="alpha", max_running=2)
    await apply_limit(
        app.engine,
        key="beta",
        max_running=None,
        max_starts=3,
        window=timedelta(seconds=30),
    )

    # Act
    summaries = await list_limits(app.engine)

    # Assert
    assert summaries == (
        LimitSummary(key="alpha", max_running=2, max_starts=None, window=None),
        LimitSummary(
            key="beta",
            max_running=None,
            max_starts=3,
            window=timedelta(seconds=30),
        ),
    )


async def test_clear_limit_deletes_row(app: Akadze, database_url: str) -> None:
    # Arrange
    await apply_limit(app.engine, key="vendor", max_running=1)

    # Act
    async with app.engine.begin() as session:
        await clear_limit(session, "vendor")

    # Assert
    assert await list_limits(app.engine) == ()
    async with connection(database_url) as opened:
        count = await opened.fetchval("SELECT count(*) FROM akadze.limits")
    assert count == 0


async def test_clear_limit_deletes_start_marks_for_the_key(app: Akadze, database_url: str) -> None:
    # Arrange
    await apply_limit(app.engine, key="vendor", max_starts=1, window=timedelta(hours=1))
    await apply_limit(app.engine, key="other", max_starts=1, window=timedelta(hours=1))
    async with connection(database_url) as opened:
        await opened.execute("INSERT INTO akadze.limit_starts (key) VALUES ('vendor'), ('other')")

    # Act
    async with app.engine.begin() as session:
        await clear_limit(session, "vendor")

    # Assert
    async with connection(database_url) as opened:
        keys = await opened.fetch("SELECT key FROM akadze.limit_starts ORDER BY key")
    assert [row["key"] for row in keys] == ["other"]


async def test_set_limit_after_clear_counts_starts_from_zero(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> str:
        return "ok"

    await apply_limit(app.engine, key="vendor", max_starts=1, window=timedelta(hours=1))
    async with connection(database_url) as opened:
        await opened.execute("INSERT INTO akadze.limit_starts (key) VALUES ('vendor')")
    async with app.engine.begin() as session:
        await clear_limit(session, "vendor")
        await set_limit(session, "vendor", max_starts=1, window=timedelta(hours=1))
        await demo.using(session=session, limit_key="vendor").enqueue()

    # Act
    worker = Worker(app)
    await worker.register()
    await worker.claim_available()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "running"
    await worker.shutdown()


async def test_clear_limit_is_idempotent(app: Akadze) -> None:
    # Arrange / Act
    async with app.engine.begin() as session:
        await clear_limit(session, "missing")

    # Assert
    assert await list_limits(app.engine) == ()


async def test_clear_limit_rejects_empty_key(app: Akadze) -> None:
    # Arrange / Act / Assert
    async with app.engine.begin() as session:
        with pytest.raises(AkadzeError, match="empty"):
            await clear_limit(session, "")


async def test_clear_limit_allows_claim_for_jobs_with_that_key(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> str:
        return "ok"

    await apply_limit(app.engine, key="vendor", max_running=1)
    async with app.engine.begin() as session:
        await demo.using(session=session, limit_key="vendor").enqueue()
        await clear_limit(session, "vendor")

    # Act
    worker = Worker(app)
    await worker.register()
    await worker.claim_available()

    # Assert — clearing removes the policy; missing row means no limit
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "running"
    await worker.shutdown()


def _cli(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "akadze.cli", *argv],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


async def test_cli_limits_list_prints_one_line_per_key(app: Akadze, database_url: str) -> None:
    # Arrange
    await apply_limit(app.engine, key="vendor", max_running=2)
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "limits", "list")

    # Assert
    assert result.returncode == 0, result.stderr
    assert "key=vendor max_running=2 max_starts=- window=-" in result.stdout
    assert database_url not in result.stdout + result.stderr


async def test_cli_limits_clear_removes_key(app: Akadze, database_url: str) -> None:
    # Arrange
    await apply_limit(app.engine, key="vendor", max_running=1)
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "limits", "clear", "vendor")

    # Assert
    assert result.returncode == 0, result.stderr
    assert await list_limits(app.engine) == ()


def test_cli_limits_list_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env, "limits", "list")

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr


def test_cli_limits_clear_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env, "limits", "clear", "vendor")

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr


def test_cli_limits_list_hides_password_on_failure() -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = "postgresql://akadze:super-secret-pw@127.0.0.1:1/akadze"

    # Act
    result = _cli(env, "limits", "list")

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "limits list failed:" in result.stderr
    assert "super-secret-pw" not in output
    assert "Traceback" not in output


def test_cli_limits_clear_hides_password_on_failure() -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = "postgresql://akadze:super-secret-pw@127.0.0.1:1/akadze"

    # Act
    result = _cli(env, "limits", "clear", "vendor")

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "limits clear failed:" in result.stderr
    assert "super-secret-pw" not in output
    assert "Traceback" not in output
