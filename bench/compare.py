"""Throughput of akadze, PgQueuer and Procrastinate on the same Postgres.

Each library gets its own fresh database. Measured per library:

* enqueue_batch: N no-op jobs through the library's batch call, one transaction.
* enqueue_single: M jobs, one call (and one transaction) each.
* process: N queued no-op async jobs, one worker with CONCURRENCY slots,
  from worker start until the last job is done.

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

import asyncpg

DSN = os.environ.get("BENCH_DSN", "postgresql://akadze:akadze@localhost:5432")
N = int(os.environ.get("BENCH_N", "20000"))
M = int(os.environ.get("BENCH_M", "1000"))
CONCURRENCY = int(os.environ.get("BENCH_CONCURRENCY", "20"))


async def fresh_database(name: str) -> str:
    admin = await asyncpg.connect(f"{DSN}/postgres")
    await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    await admin.execute(f'CREATE DATABASE "{name}"')
    await admin.close()
    return f"{DSN}/{name}"


async def wait_until(check: Callable[[], Awaitable[bool]]) -> None:
    while not await check():
        await asyncio.sleep(0.02)


# --- akadze -----------------------------------------------------------------


async def bench_akadze() -> dict[str, float]:
    from sqlalchemy.ext.asyncio import create_async_engine

    from akadze import Akadze, Worker, migrate
    from akadze.schema import async_database_url

    url = await fresh_database("bench_akadze")
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

    started = time.perf_counter()
    for _ in range(M):
        async with app.engine.begin() as connection:
            await noop.using(session=connection).enqueue()
    enqueue_single = time.perf_counter() - started

    probe = await asyncpg.connect(url)

    async def done() -> bool:
        left = await probe.fetchval("SELECT count(*) FROM akadze.jobs WHERE state <> 'succeeded'")
        return left == 0

    worker = Worker(app, slots=CONCURRENCY)
    stop = asyncio.Event()
    started = time.perf_counter()
    serving = asyncio.create_task(worker.serve(stop))
    await wait_until(done)
    process = time.perf_counter() - started
    stop.set()
    await serving
    await probe.close()
    await engine.dispose()
    return {"enqueue_batch": enqueue_batch, "enqueue_single": enqueue_single, "process": process}


# --- PgQueuer ---------------------------------------------------------------


async def bench_pgqueuer() -> dict[str, float]:
    from pgqueuer import AsyncpgDriver, QueueManager
    from pgqueuer.queries import Queries
    from pgqueuer.types import QueueExecutionMode

    url = await fresh_database("bench_pgqueuer")
    connection = await asyncpg.connect(url)
    driver = AsyncpgDriver(connection)
    queries = Queries(driver)
    await queries.install()

    started = time.perf_counter()
    await queries.enqueue(["noop"] * N, [None] * N, [0] * N)
    enqueue_batch = time.perf_counter() - started

    started = time.perf_counter()
    for _ in range(M):
        await queries.enqueue("noop", None)
    enqueue_single = time.perf_counter() - started

    manager = QueueManager(queries)

    @manager.entrypoint("noop")
    async def noop(job: object) -> None:
        return None

    started = time.perf_counter()
    await manager.run(
        mode=QueueExecutionMode.drain,
        batch_size=CONCURRENCY // 2,
        max_concurrent_tasks=CONCURRENCY,
        dequeue_timeout=timedelta(seconds=0.05),
    )
    process = time.perf_counter() - started
    await connection.close()
    return {"enqueue_batch": enqueue_batch, "enqueue_single": enqueue_single, "process": process}


# --- Procrastinate ----------------------------------------------------------


async def bench_procrastinate() -> dict[str, float]:
    import procrastinate

    url = await fresh_database("bench_procrastinate")
    app = procrastinate.App(connector=procrastinate.PsycopgConnector(conninfo=url))

    @app.task(name="noop")
    async def noop() -> None:
        return None

    async with app.open_async():
        await app.schema_manager.apply_schema_async()

        started = time.perf_counter()
        await noop.batch_defer_async(*({} for _ in range(N)))
        enqueue_batch = time.perf_counter() - started

        started = time.perf_counter()
        for _ in range(M):
            await noop.defer_async()
        enqueue_single = time.perf_counter() - started

        started = time.perf_counter()
        await app.run_worker_async(
            concurrency=CONCURRENCY,
            wait=False,
            install_signal_handlers=False,
            fetch_job_polling_interval=0.05,
        )
        process = time.perf_counter() - started
    return {"enqueue_batch": enqueue_batch, "enqueue_single": enqueue_single, "process": process}


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
    print(
        json.dumps(
            {"N": N, "M": M, "concurrency": CONCURRENCY, "runs": runs, "median_s": summary},
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 1))
