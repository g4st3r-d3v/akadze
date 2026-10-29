"""Throughput of akadze, PgQueuer and Procrastinate on the same Postgres.

Each library gets its own fresh database. Measured per library, in order:

* enqueue_batch: N no-op jobs through the library's batch call, one transaction.
* process: those N queued no-op async jobs, one worker with CONCURRENCY slots,
  from worker start until the last job is done.
* enqueue_single: then M more jobs, one call (and one transaction) each.

Each database is dropped and created again: point BENCH_DSN at a server you
can do that on. Usage:

    pip install -r bench/requirements.txt
    BENCH_DSN=postgresql://user:pass@host:5432 python bench/compare.py [runs]
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import sys
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from urllib.parse import urlsplit, urlunsplit

import asyncpg

DSN = os.environ.get("BENCH_DSN", "postgresql://akadze:akadze@localhost:5432")
N = int(os.environ.get("BENCH_N", "10000"))
M = int(os.environ.get("BENCH_M", "1000"))
CONCURRENCY = int(os.environ.get("BENCH_CONCURRENCY", "20"))
POLL = 0.05  # seconds between polls for PgQueuer and Procrastinate


def database_url(name: str) -> str:
    """BENCH_DSN with its database replaced by ``name``; query parameters kept."""

    return urlunsplit(urlsplit(DSN)._replace(path=f"/{name}"))


async def reset_database(name: str) -> None:
    admin = await asyncpg.connect(database_url("postgres"))
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()


async def wait_until(check: Callable[[], Awaitable[bool]]) -> None:
    while not await check():
        await asyncio.sleep(0.02)


# --- akadze -----------------------------------------------------------------


async def bench_akadze() -> dict[str, float]:
    from sqlalchemy.ext.asyncio import create_async_engine

    from akadze import Akadze, Worker, migrate
    from akadze.schema import async_database_url

    url = database_url("bench_akadze")
    await reset_database("bench_akadze")
    await migrate(url)
    # Sized as the docs advise: slots x queues + 2 connections, with headroom.
    engine = create_async_engine(
        async_database_url(url),
        pool_size=CONCURRENCY + 5,
        connect_args={"statement_cache_size": 0},
    )
    app = Akadze(engine=engine)

    @app.task("noop")
    async def noop() -> None:
        return None

    started = time.perf_counter()
    async with app.engine.begin() as connection:
        await noop.using(session=connection).enqueue_many({} for _ in range(N))
    enqueue_batch = time.perf_counter() - started

    probe = await asyncpg.connect(url)
    worker = Worker(app, slots=CONCURRENCY)
    stop = asyncio.Event()
    started = time.perf_counter()
    serving = asyncio.create_task(worker.serve(stop))

    async def done() -> bool:
        if serving.done():
            serving.result()  # raises what stopped the worker
            raise RuntimeError("worker stopped before the queue was empty")
        # Partial indexes cover queued and running, so this stays cheap.
        return not await probe.fetchval(
            "SELECT EXISTS (SELECT 1 FROM akadze.jobs WHERE state IN ('queued', 'running'))"
        )

    try:
        await wait_until(done)
        process = time.perf_counter() - started
    finally:
        stop.set()
        await serving
        await probe.close()

    started = time.perf_counter()
    for _ in range(M):
        async with app.engine.begin() as connection:
            await noop.using(session=connection).enqueue()
    enqueue_single = time.perf_counter() - started
    await engine.dispose()
    return {"enqueue_batch": enqueue_batch, "process": process, "enqueue_single": enqueue_single}


# --- PgQueuer ---------------------------------------------------------------


async def bench_pgqueuer() -> dict[str, float]:
    from pgqueuer import AsyncpgDriver, QueueManager
    from pgqueuer.queries import Queries
    from pgqueuer.types import QueueExecutionMode

    await reset_database("bench_pgqueuer")
    connection = await asyncpg.connect(database_url("bench_pgqueuer"))
    try:
        queries = Queries(AsyncpgDriver(connection))
        await queries.install()

        started = time.perf_counter()
        await queries.enqueue(["noop"] * N, [None] * N, [0] * N)
        enqueue_batch = time.perf_counter() - started

        manager = QueueManager(queries)

        @manager.entrypoint("noop")
        async def noop(job: object) -> None:
            return None

        started = time.perf_counter()
        await manager.run(
            mode=QueueExecutionMode.drain,
            batch_size=max(1, CONCURRENCY // 2),
            max_concurrent_tasks=max(2, CONCURRENCY),
            dequeue_timeout=timedelta(seconds=POLL),
        )
        process = time.perf_counter() - started

        started = time.perf_counter()
        for _ in range(M):
            await queries.enqueue("noop", None)
        enqueue_single = time.perf_counter() - started
    finally:
        await connection.close()
    return {"enqueue_batch": enqueue_batch, "process": process, "enqueue_single": enqueue_single}


# --- Procrastinate ----------------------------------------------------------


async def bench_procrastinate() -> dict[str, float]:
    import procrastinate

    await reset_database("bench_procrastinate")
    connector = procrastinate.PsycopgConnector(conninfo=database_url("bench_procrastinate"))
    app = procrastinate.App(connector=connector)

    @app.task(name="noop")
    async def noop() -> None:
        return None

    async with app.open_async():
        await app.schema_manager.apply_schema_async()

        started = time.perf_counter()
        await noop.batch_defer_async(*({} for _ in range(N)))
        enqueue_batch = time.perf_counter() - started

        started = time.perf_counter()
        await app.run_worker_async(
            concurrency=CONCURRENCY,
            wait=False,
            install_signal_handlers=False,
            fetch_job_polling_interval=POLL,
        )
        process = time.perf_counter() - started

        started = time.perf_counter()
        for _ in range(M):
            await noop.defer_async()
        enqueue_single = time.perf_counter() - started
    return {"enqueue_batch": enqueue_batch, "process": process, "enqueue_single": enqueue_single}


LIBRARIES = {
    "akadze": bench_akadze,
    "pgqueuer": bench_pgqueuer,
    "procrastinate": bench_procrastinate,
}


async def main(runs: int) -> None:
    results: dict[str, dict[str, list[float]]] = {}
    for _ in range(runs):
        for name, bench in LIBRARIES.items():
            for metric, value in (await bench()).items():
                results.setdefault(name, {}).setdefault(metric, []).append(value)
    summary = {
        name: {metric: statistics.median(values) for metric, values in metrics.items()}
        for name, metrics in results.items()
    }
    report = {"N": N, "M": M, "concurrency": CONCURRENCY, "runs": runs, "median_s": summary}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 1))
