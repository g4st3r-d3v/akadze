# Benchmark

akadze against two other Postgres task queues for Python, on the same machine and the same Postgres. The script is [`bench/compare.py`](https://github.com/g4st3r-d3v/akadze/blob/main/bench/compare.py); run it yourself before trusting numbers from someone's laptop.

## Setup

- PostgreSQL 16 in Docker, 12 CPU / 15 GB host, Python 3.13, client on the same host.
- akadze `main` after 0.6.0 (with the freed-slot wake-up), PgQueuer 1.6.0, Procrastinate 3.10.0.
- Each library gets a fresh database. Jobs are async no-ops: this measures the queue, not the work.
- One worker process with 20 concurrent jobs: akadze `Worker(slots=20)` with a pool of 25 connections ([why](guides/operations.md#connection-pool)) and the default `poll_interval`; PgQueuer `batch_size=10, max_concurrent_tasks=20`; Procrastinate `concurrency=20`. PgQueuer and Procrastinate poll every 50 ms here, faster than their defaults.
- Median of 3 runs.

## Results

| | akadze | PgQueuer | Procrastinate |
|---|---|---|---|
| Enqueue 10 000 jobs in one batch | **0.10 s** | 0.16 s | 0.37 s |
| Enqueue 1 000 jobs, one transaction each | 4.2 s | **3.1 s** | 3.5 s |
| Process 10 000 queued jobs | 19.6 s | **8.5 s** | 70 s |
| Jobs per second, processing | ~510 | ~1 170 | ~140 |

## Reading it

- **Batch enqueue** is fastest in akadze: `enqueue_many` writes a thousand rows per statement.
- **Processing** is about 2.3× slower than PgQueuer. Each akadze run re-reads its row before it starts (fencing against a rescue), records the outcome in its own transaction with hooks, and goes through SQLAlchemy; PgQueuer batches dequeues and completions over raw asyncpg. Throughput per worker is the price of those guarantees today, and the place to improve next.
- The numbers are for no-op jobs. A job that calls an API for 200 ms caps 20 slots at 100 jobs per second, and the queue's own overhead weighs much less there.
- What is not measured: limits, fair claim (see [How a claim works](concepts/claim.md)), several workers, network latency to the database.

Run it:

```bash
pip install -r bench/requirements.txt
BENCH_DSN=postgresql://user:pass@localhost:5432 python bench/compare.py 3
```

`BENCH_N`, `BENCH_M` and `BENCH_CONCURRENCY` change the job counts and the concurrency. The script drops and creates its databases.
