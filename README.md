# akadze

Postgres-backed task queue: enqueue work and run it in a worker. Periodic schedules run in that same process.

Shape is familiar if you know Celery. The broker is PostgreSQL (`FOR UPDATE SKIP LOCKED` + lease), not Redis or RabbitMQ.

**Status:** pre-alpha. Enqueue, the worker, periodic schedules, retry, and queue maintenance run on Postgres.

## Goals

- Small library, no product domain
- Tasks, retries, lease recovery
- Periodic cron and interval schedules run inside the worker
- Clear Python call surface; CLI for migrate and worker

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

Run the worker from the directory that contains `demo.py`. It loads `demo:app` from there, claims the job, runs `hello`, and sets the row to `succeeded`. Stop it with Ctrl-C. Periodic tasks run in this same process; there is no separate beat.

A synchronous task runs in a thread. Stopping the worker does not stop that thread.

```bash
python demo.py
akadze worker demo:app
```

`akadze.jobs.state` is then `succeeded`. A higher `priority` is claimed first. `result` is optional. Pass enqueue options (`delay`, `run_at`, `priority`, `unique_key`) through `using(...)`, not as task arguments.

To commit your own writes together with the transition to `succeeded`, take the connection from `current().complete_tx()`. If this run no longer owns the job, or the job was cancelled or has expired, the block raises and those writes roll back. An error inside the block rolls the writes back and spends an attempt.

```python
from akadze import current

async with current().complete_tx() as connection:
    await connection.execute(...)
```

`akadze.testing.drain(app)` runs every job that is ready now. `assert_enqueued(app, "hello", name="ada")` checks that a queued or running job has those arguments.

Tests use `AKADZE_DATABASE_URL` and drop schema `akadze` in that database. The database name must be `akadze` or end with `_test`.

## Layout

```text
akadze/   # library package
tests/
```

Design notes: [`g4st3r-d3v/akadze-harness`](https://github.com/g4st3r-d3v/akadze-harness) (`HUB.md`). This repo is code-only.

## License

MIT
