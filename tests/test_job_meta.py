from __future__ import annotations

import os
import subprocess
import sys
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from akadze import Akadze, AkadzeError, EnqueueError, list_jobs
from akadze.job import Job
from tests.pg import as_json, connection, job_rows


async def _enqueue(app: Akadze, **jobs: dict[str, Any]) -> None:
    @app.task("order.sync")
    async def sync(order_id: int) -> None:
        return None

    async with app.engine.begin() as session:
        for order_id, (_, meta) in enumerate(jobs.items()):
            await sync.using(session=session, meta=meta).enqueue(order_id=order_id)


def _cli(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "akadze.cli", *argv],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


async def test_using_meta_is_stored_on_the_job(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("order.sync")
    async def sync(order_id: int) -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await sync.using(session=session, meta={"tenant": "acme", "order": 42}).enqueue(order_id=42)

    # Assert
    async with connection(database_url) as opened:
        meta = await opened.fetchval("SELECT meta FROM akadze.jobs")
    assert as_json(meta) == {"tenant": "acme", "order": 42}


async def test_list_jobs_filters_by_meta_containment(app: Akadze) -> None:
    # Arrange
    await _enqueue(
        app,
        a={"tenant": "acme", "region": "eu"},
        b={"tenant": "acme", "region": "us"},
        c={"tenant": "globex", "region": "eu"},
    )

    # Act
    jobs = await list_jobs(app.engine, meta={"tenant": "acme", "region": "eu"})

    # Assert
    assert [job.meta for job in jobs] == [{"tenant": "acme", "region": "eu"}]


async def test_list_jobs_without_meta_filter_returns_meta(app: Akadze) -> None:
    # Arrange
    await _enqueue(app, a={"tenant": "acme"}, b={})

    # Act
    jobs = await list_jobs(app.engine)

    # Assert
    assert sorted(job.meta.get("tenant", "") for job in jobs) == ["", "acme"]


@pytest.mark.parametrize(
    ("meta", "message"),
    [
        (["not", "a", "mapping"], "mapping with str keys"),
        ({1: "int key"}, "mapping with str keys"),
        ({"when": object()}, "JSON-serializable"),
        ({"blob": "x" * 9000}, "at most 8192 bytes"),
        ({"score": float("nan")}, "JSON-serializable"),
        ({"name": "a\x00b"}, "NUL"),
        ({"items": {1: "a"}}, "mapping with str keys"),
    ],
    ids=["list", "int-key", "not-json", "too-big", "nan", "nul", "nested-int-key"],
)
async def test_using_rejects_bad_meta(
    app: Akadze, database_url: str, meta: object, message: str
) -> None:
    # Arrange
    @app.task("order.sync")
    async def sync(order_id: int) -> None:
        return None

    # Act / Assert
    with pytest.raises(EnqueueError, match=message):
        async with app.engine.begin() as session:
            await sync.using(session=session, meta=meta).enqueue(order_id=1)  # type: ignore[arg-type]
    assert await job_rows(database_url) == []


@pytest.mark.parametrize("meta", [{1: "x"}, [], ""], ids=["int-key", "empty-list", "empty-str"])
async def test_list_jobs_rejects_a_bad_meta_filter(app: Akadze, meta: object) -> None:
    # Arrange / Act / Assert
    with pytest.raises(AkadzeError, match="mapping with str keys"):
        await list_jobs(app.engine, meta=meta)  # type: ignore[arg-type]


async def test_non_ascii_meta_is_measured_in_utf8(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("order.sync")
    async def sync(order_id: int) -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await sync.using(session=session, meta={"customer": "д" * 1400}).enqueue(order_id=1)

    # Assert
    assert len(await job_rows(database_url)) == 1


async def test_summary_stays_hashable_and_meta_read_only(app: Akadze) -> None:
    # Arrange
    await _enqueue(app, a={"tenant": "acme"})

    # Act
    (job,) = await list_jobs(app.engine)

    # Assert
    assert {job} == {job}
    with pytest.raises(TypeError):
        job.meta["tenant"] = "globex"  # type: ignore[index]


async def test_hooks_see_the_job_meta(app: Akadze) -> None:
    # Arrange
    seen: list[dict[str, Any]] = []

    async def record(connection: AsyncConnection, job: Job) -> None:
        seen.append(job.meta)

    app.hooks.on_enqueue.append(record)

    # Act
    await _enqueue(app, a={"tenant": "acme"})

    # Assert
    assert seen == [{"tenant": "acme"}]


async def test_cli_jobs_list_filters_and_prints_meta(app: Akadze, database_url: str) -> None:
    # Arrange
    await _enqueue(app, a={"tenant": "acme", "order": 7}, b={"tenant": "globex", "order": 7})
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "list", "--meta", "tenant=acme", "--meta", "order=7")

    # Assert
    assert result.returncode == 0, result.stderr
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    assert 'meta={"order":7,"tenant":"acme"}' in lines[0]


async def test_cli_jobs_list_rejects_a_malformed_meta_flag(database_url: str) -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "list", "--meta", "tenant")

    # Assert
    assert result.returncode == 2
    assert "KEY=VALUE" in result.stderr


async def test_cli_jobs_list_rejects_a_repeated_meta_key(database_url: str) -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, "jobs", "list", "--meta", "tenant=acme", "--meta", "tenant=globex")

    # Assert
    assert result.returncode == 2
    assert "more than once" in result.stderr


async def test_enqueue_many_shares_meta_across_jobs(app: Akadze) -> None:
    # Arrange
    @app.task("order.sync")
    async def sync(order_id: int) -> None:
        return None

    # Act
    async with app.engine.begin() as session:
        await sync.using(session=session, meta={"batch": "b-1"}).enqueue_many(
            [{"order_id": n} for n in range(3)]
        )

    # Assert
    jobs = await list_jobs(app.engine, meta={"batch": "b-1"})
    assert len(jobs) == 3
