# akadze

[![ci](https://github.com/g4st3r-d3v/akadze/actions/workflows/ci.yml/badge.svg)](https://github.com/g4st3r-d3v/akadze/actions/workflows/ci.yml)

Postgres-backed task queue: enqueue work and run it in a worker. Periodic schedules run in that same process.

Shape is familiar if you know Celery. The broker is PostgreSQL (`FOR UPDATE SKIP LOCKED` + lease), not Redis or RabbitMQ.

**Status:** alpha. Enqueue, the worker, periodic schedules, retry, and queue maintenance run on Postgres.

## Goals

- Small library, no product domain
- Tasks, retries, lease recovery
- Periodic cron and interval schedules run inside the worker
- Clear Python call surface; CLI for migrate, worker, jobs, and queues

## Non-goals (for now)

- HTTP / REST / web UI (another library or app owns that)
- Replacing any product service queue
- Multi-broker adapters
- Distributed tracing / full observability suite

## First task

Install the library, point it at the application database, and create the `akadze` schema. The tables live in that schema; they are not part of the application's own migrations.

```bash
poetry install
export AKADZE_DATABASE_URL=postgresql://akadze:akadze@localhost:5432/akadze
akadze migrate
```

Save this as `demo.py`. `enqueue` runs in the caller's transaction, so a rollback removes the job.

```python
import asyncio

from akadze import Akadze

app = Akadze(database_url="postgresql://akadze:akadze@localhost:5432/akadze")


@app.task("hello")
async def hello(name: str) -> str:
    return f"hello {name}"


async def main() -> None:
    async with app.engine.begin() as connection:
        await hello.using(session=connection).enqueue(name="ada")


if __name__ == "__main__":
    asyncio.run(main())
```

Run the worker from the directory that contains `demo.py`. It loads `demo:app` from there, claims the job, runs `hello`, and sets the row to `succeeded`. Stop it with Ctrl-C: the worker takes no new jobs and waits up to 10 seconds (`shutdown_timeout`) for the ones already running. Whatever is still running goes back to the queue, and an attempt is not spent. Periodic tasks run in this same process; there is no separate beat.

If a heartbeat does not succeed within 30 seconds, the worker stops and puts unfinished jobs back on the queue. An attempt is not spent. A synchronous task runs in a thread. Stopping the worker does not stop that thread.

A claim that fills every free slot is followed by another claim immediately. Otherwise the worker waits up to `poll_interval` (1 second). The wait is at least half of that, so idle workers do not poll in step or spin. A job can run more than once if its worker disappears.

```bash
python demo.py
akadze worker demo:app
```

`akadze.jobs.state` is then `succeeded`. A higher `priority` is claimed first. `result` is optional. Pass enqueue options (`delay`, `run_at`, `priority`, `unique_key`, `expires_at`) through `using(...)`, not as task arguments. `expires_at` must be timezone-aware. A job that has already expired is cancelled before it runs. A second enqueue with the same active `unique_key` raises `DuplicateJob` and does not abort the rest of the caller's transaction.

Enqueue the follow-up job on the `complete_tx` connection so it commits only if this job succeeds:

```python
from akadze import current

async with current().complete_tx() as connection:
    await next_step.using(session=connection).enqueue()
```

To commit your own writes together with the transition to `succeeded`, take the connection from `current().complete_tx()`. If this run no longer owns the job, or the job was cancelled or has expired, the block raises and those writes roll back. An error inside the block rolls the writes back and spends an attempt.

```python
from akadze import current

async with current().complete_tx() as connection:
    await connection.execute(...)
```

`akadze.testing.drain(app)` runs every job that is ready now. `assert_enqueued(app, "hello", name="ada")` checks that a queued or running job has those arguments.

`queue_snapshot(engine)` reads committed rows: how many jobs are in each queue and state, how long the oldest ready job has waited, and how many are running. `list_jobs(engine, queue=..., state=..., limit=50)` returns `JobSummary` rows (no args/result/errors/meta), ordered by priority, then `run_at`, then id. `akadze jobs list` prints one summary per line. Count outcomes in an `on_transition` hook. Do not log task arguments or results.

`requeue(session, job_id)` returns a `failed` job to `queued` in the caller's transaction. It keeps `attempt`, `errors`, and `args`, and raises `max_attempts` when needed. `pause_queue` / `resume_queue` stop and restore claims for one queue; jobs already `running` finish. CLI: `akadze jobs requeue JOB_ID`, `akadze queues pause NAME`, `akadze queues resume NAME`.

Do not copy this package's SQL into the application's Alembic history. Deploy runs `akadze migrate`, or `await migrate(database_url)`, against the same database. The engine disables asyncpg's statement cache so a transaction-mode pool such as PgBouncer can sit in front.

Tests use `AKADZE_DATABASE_URL` and drop schema `akadze` in that database. The database name must be `akadze` or end with `_test`.

## Release

Create a tag `vX.Y.Z` that matches the version in `pyproject.toml` and push it. GitHub Actions runs the tests, publishes the wheel to PyPI, and opens a GitHub release.

PyPI trusted publishing, set once on the project `akadze`:

- Owner: `g4st3r-d3v`
- Repository: `akadze`
- Workflow: `release.yml`
- Environment: `pypi`

After that publish succeeds, an application installs the package from PyPI:

```bash
pip install akadze
```

## Layout

```text
akadze/   # library package
tests/
```

Design notes: [`g4st3r-d3v/akadze-harness`](https://github.com/g4st3r-d3v/akadze-harness) (`HUB.md`). This repo is code-only.

## License

MIT
