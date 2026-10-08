from __future__ import annotations

import asyncio
import os
import subprocess
import sys

from akadze import Akadze, pause_queue, resume_queue
from akadze.worker import Worker
from tests.pg import connection, job_row


async def test_paused_queue_is_not_claimed_other_queue_is(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("mail-job", queue="mail")
    async def mail_job() -> str:
        return "mail"

    @app.task("default-job", queue="default")
    async def default_job() -> str:
        return "default"

    async with app.engine.begin() as session:
        await mail_job.using(session=session).enqueue()
        await default_job.using(session=session).enqueue()
        await pause_queue(session, "mail")

    # Act
    worker = Worker(app, queues=["mail", "default"])
    await worker.register()
    await worker.claim_available()

    # Assert
    mail = await job_row(database_url, "mail-job")
    default = await job_row(database_url, "default-job")
    assert mail is not None
    assert default is not None
    assert mail["state"] == "queued"
    assert default["state"] == "running"
    await worker.shutdown()


async def test_resume_queue_restores_claim(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", queue="mail")
    async def demo() -> str:
        return "ok"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
        await pause_queue(session, "mail")
    worker = Worker(app, queues=["mail"])
    await worker.register()
    await worker.claim_available()
    paused = await job_row(database_url, "demo")
    assert paused is not None
    assert paused["state"] == "queued"
    async with app.engine.begin() as session:
        await resume_queue(session, "mail")

    # Act
    await worker.claim_available()

    # Assert
    claimed = await job_row(database_url, "demo")
    assert claimed is not None
    assert claimed["state"] == "running"
    await worker.shutdown()


async def test_running_job_finishes_while_queue_is_paused(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    started = asyncio.Event()
    release = asyncio.Event()

    @app.task("demo", queue="mail")
    async def demo() -> str:
        started.set()
        await release.wait()
        return "done"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app, queues=["mail"])
    await worker._spawn()
    await started.wait()
    async with app.engine.begin() as session:
        await pause_queue(session, "mail")

    # Act
    release.set()
    await asyncio.gather(*worker.running)

    # Assert
    finished = await job_row(database_url, "demo")
    assert finished is not None
    assert finished["state"] == "succeeded"


async def test_enqueue_into_paused_queue_stays_queued(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("demo", queue="mail")
    async def demo() -> str:
        return "ok"

    async with app.engine.begin() as session:
        await pause_queue(session, "mail")

    # Act
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    worker = Worker(app, queues=["mail"])
    await worker.register()
    await worker.claim_available()
    still = await job_row(database_url, "demo")
    assert still is not None
    assert still["state"] == "queued"
    await worker.shutdown()


async def test_pause_and_resume_are_idempotent(app: Akadze, database_url: str) -> None:
    # Arrange / Act
    async with app.engine.begin() as session:
        await pause_queue(session, "mail")
        await pause_queue(session, "mail")
        await resume_queue(session, "other")
        await resume_queue(session, "mail")
        await resume_queue(session, "mail")

    # Assert
    async with connection(database_url) as opened:
        count = await opened.fetchval("SELECT count(*) FROM akadze.queue_pauses")
    assert count == 0


def _cli(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "akadze.cli", *argv],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


async def test_cli_queues_pause_and_resume(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", queue="mail")
    async def demo() -> str:
        return "ok"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    paused = _cli(env, "queues", "pause", "mail")
    worker = Worker(app, queues=["mail"])
    await worker.register()
    await worker.claim_available()
    while_paused = await job_row(database_url, "demo")
    resumed = _cli(env, "queues", "resume", "mail")
    await worker.claim_available()
    after = await job_row(database_url, "demo")
    await worker.shutdown()

    # Assert
    assert paused.returncode == 0, paused.stderr
    assert resumed.returncode == 0, resumed.stderr
    assert while_paused is not None
    assert while_paused["state"] == "queued"
    assert after is not None
    assert after["state"] == "running"
    assert database_url not in paused.stdout + paused.stderr
    assert database_url not in resumed.stdout + resumed.stderr


def test_cli_queues_pause_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env, "queues", "pause", "mail")

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr


def test_cli_queues_resume_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env, "queues", "resume", "mail")

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr
