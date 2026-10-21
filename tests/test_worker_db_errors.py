"""The worker rides out database errors and stops only on a stale heartbeat."""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import asyncpg
import pytest
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import create_async_engine

from akadze import Akadze
from akadze.cli import main
from akadze.schema import is_transient_db_error
from akadze.testing import drain
from akadze.worker import Worker
from tests.pg import as_json, connection, job_row

_UNREACHABLE = "postgresql+asyncpg://akadze:secret-pw@127.0.0.1:1/akadze"


async def _until(predicate_sql: str, database_url: str, seconds: float = 5) -> None:
    for _ in range(int(seconds / 0.02)):
        async with connection(database_url) as opened:
            if await opened.fetchval(predicate_sql):
                return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for: {predicate_sql}")


async def test_worker_keeps_serving_after_the_database_is_gone_for_a_while(
    app: Akadze, database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> str:
        return "done"

    worker = Worker(
        app,
        heartbeat_interval=timedelta(milliseconds=50),
        heartbeat_ttl=timedelta(seconds=30),
        poll_interval=timedelta(milliseconds=10),
    )
    stop = asyncio.Event()
    serve_task = asyncio.create_task(worker.serve(stop))
    caplog.set_level(logging.WARNING, logger="akadze")
    reachable = app.engine
    unreachable = create_async_engine(_UNREACHABLE)
    try:
        await _until("SELECT count(*) FROM akadze.workers", database_url)

        # Act
        app.engine = unreachable
        await asyncio.sleep(0.3)
        app.engine = reachable
        async with connection(database_url) as opened:
            await opened.execute("INSERT INTO akadze.jobs (task) VALUES ('demo')")
        await _until(
            "SELECT count(*) FROM akadze.jobs WHERE task = 'demo' AND state = 'succeeded'",
            database_url,
        )

        # Assert
        assert not serve_task.done()
        assert "heartbeat failed, retrying" in caplog.text
        assert "failed, retrying: ConnectionRefusedError" in caplog.text
    finally:
        app.engine = reachable
        stop.set()
        await asyncio.wait_for(serve_task, timeout=5)
        await unreachable.dispose()


async def test_a_persistent_database_error_stops_the_worker(app: Akadze, database_url: str) -> None:
    # Arrange
    async with connection(database_url) as opened:
        await opened.execute("ALTER TABLE akadze.queue_pauses RENAME TO queue_pauses_gone")
    worker = Worker(
        app,
        heartbeat_interval=timedelta(milliseconds=50),
        poll_interval=timedelta(milliseconds=10),
    )

    # Act
    with pytest.raises(ProgrammingError):
        await asyncio.wait_for(worker.serve(asyncio.Event()), timeout=5)

    # Assert
    async with connection(database_url) as opened:
        workers = await opened.fetchval("SELECT count(*) FROM akadze.workers")
    assert workers == 0


@pytest.mark.parametrize(
    ("error", "transient"),
    [
        (ConnectionRefusedError(111, "refused"), True),
        (asyncpg.exceptions.CannotConnectNowError("the database system is starting up"), True),
        (asyncpg.exceptions.DeadlockDetectedError("deadlock detected"), True),
        (asyncpg.exceptions.QueryCanceledError("canceling statement due to timeout"), True),
        (PoolTimeout("QueuePool limit of size 5 overflow 10 reached"), True),
        (asyncpg.InterfaceError("connection is closed"), True),
        (asyncpg.InterfaceError("the server expects 2 arguments, 1 was passed"), False),
        (asyncpg.exceptions.UndefinedTableError("relation does not exist"), False),
        (asyncpg.exceptions.UntranslatableCharacterError("unsupported Unicode escape"), False),
    ],
    ids=[
        "refused",
        "starting-up",
        "deadlock",
        "statement-timeout",
        "pool-timeout",
        "closed",
        "misuse",
        "undefined-table",
        "untranslatable",
    ],
)
def test_transient_database_errors_are_told_apart(error: BaseException, transient: bool) -> None:
    # Act
    result = is_transient_db_error(error)

    # Assert
    assert result is transient


async def test_an_outcome_that_cannot_be_stored_fails_the_job_without_running_it_again(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    calls = 0

    @app.task("nul", max_attempts=3)
    async def nul() -> str:
        nonlocal calls
        calls += 1
        return "a\x00b"

    async with app.engine.begin() as session:
        await nul.using(session=session).enqueue()

    # Act
    await drain(app)

    # Assert
    row = await job_row(database_url, "nul")
    assert row is not None
    assert row["state"] == "failed"
    assert calls == 1
    errors = as_json(row["errors"])
    assert errors[0]["message"] == "outcome could not be stored: DBAPIError (SQLSTATE 22P05)"


async def test_worker_stops_when_the_database_is_gone_longer_than_the_ttl(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task("block")
    async def block() -> None:
        started.set()
        await release.wait()

    async with app.engine.begin() as session:
        await block.using(session=session).enqueue()
    worker = Worker(
        app,
        heartbeat_interval=timedelta(milliseconds=50),
        heartbeat_ttl=timedelta(milliseconds=500),
        poll_interval=timedelta(milliseconds=10),
    )
    serve_task = asyncio.create_task(worker.serve(asyncio.Event()))
    reachable = app.engine
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        app.engine = create_async_engine(_UNREACHABLE)

        # Act
        await asyncio.wait_for(serve_task, timeout=5)
    finally:
        release.set()
        if not serve_task.done():
            serve_task.cancel()
            await asyncio.gather(serve_task, return_exceptions=True)
        await app.engine.dispose()
        app.engine = reachable

    # Assert
    assert worker._abandon is True
    row = await job_row(database_url, "block")
    assert row is not None
    assert row["state"] == "running"
    assert row["worker_id"] == worker.id


def test_cli_worker_reports_a_rejected_login_without_the_password(
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Arrange
    parsed = urlparse(database_url)
    wrong = parsed._replace(netloc=f"{parsed.username}:secret-pw@{parsed.hostname}:{parsed.port}")
    (tmp_path / "unreachable_demo.py").write_text(
        f"from akadze import Akadze\napp = Akadze(database_url={urlunparse(wrong)!r})\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    # Act
    try:
        code = main(["worker", "unreachable_demo:app"])
    finally:
        sys.modules.pop("unreachable_demo", None)
        while str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))

    # Assert
    err = capsys.readouterr().err
    assert code == 1
    assert err.startswith("worker failed: ")
    assert "secret-pw" not in err
    assert "Traceback" not in err


def test_error_backoff_stays_at_the_heartbeat_interval_after_many_failures(app: Akadze) -> None:
    # Arrange
    worker = Worker(app, heartbeat_interval=timedelta(seconds=5))

    # Act
    delay = worker._error_delay(5000)

    # Assert
    assert 2.5 <= delay <= 5


async def test_outcome_is_recorded_once_after_the_database_comes_back(
    app: Akadze, database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange
    calls = 0
    reachable = app.engine
    unreachable = create_async_engine(_UNREACHABLE)

    @app.task("demo")
    async def demo() -> str:
        nonlocal calls
        calls += 1
        return "done"

    async def restore_later() -> None:
        await asyncio.sleep(0.3)
        app.engine = reachable

    restorer: list[asyncio.Task[None]] = []

    def lose_database(_: object) -> None:
        app.engine = unreachable
        restorer.append(asyncio.create_task(restore_later()))

    app.hooks.after_run.append(lose_database)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(
        app,
        heartbeat_interval=timedelta(milliseconds=50),
        poll_interval=timedelta(milliseconds=10),
    )
    stop = asyncio.Event()
    serve_task = asyncio.create_task(worker.serve(stop))
    caplog.set_level(logging.WARNING, logger="akadze")

    # Act
    try:
        await _until(
            "SELECT count(*) FROM akadze.jobs WHERE task = 'demo' AND state = 'succeeded'",
            database_url,
        )
        await asyncio.sleep(0.2)
    finally:
        stop.set()
        await asyncio.wait_for(serve_task, timeout=5)
        await asyncio.gather(*restorer)
        app.engine = reachable
        await unreachable.dispose()

    # Assert
    assert calls == 1
    assert "outcome not recorded, retrying" in caplog.text


async def test_a_hanging_heartbeat_stops_the_worker_within_the_ttl(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task("block")
    async def block() -> None:
        started.set()
        await release.wait()

    async with app.engine.begin() as session:
        await block.using(session=session).enqueue()
    worker = Worker(
        app,
        heartbeat_interval=timedelta(milliseconds=50),
        heartbeat_ttl=timedelta(milliseconds=500),
        poll_interval=timedelta(milliseconds=10),
    )
    serve_task = asyncio.create_task(worker.serve(asyncio.Event()))
    async with connection(database_url) as holder:
        try:
            await asyncio.wait_for(started.wait(), timeout=5)
            transaction = holder.transaction()
            await transaction.start()
            await holder.execute("SELECT 1 FROM akadze.workers FOR UPDATE")

            # Act
            await asyncio.wait_for(serve_task, timeout=3)
        finally:
            release.set()
            if not serve_task.done():
                serve_task.cancel()
                await asyncio.gather(serve_task, return_exceptions=True)

    # Assert
    assert worker._abandon is True
    assert serve_task.exception() is None
