# akadze

Postgres-backed task queue: enqueue work, run workers, optionally schedule with beat.

Shape is familiar if you know Celery. The broker is PostgreSQL (`FOR UPDATE SKIP LOCKED` + lease), not Redis or RabbitMQ.

**Status:** pre-alpha scaffold. The Python call surface will move.

## Goals

- Small library, no product domain
- Tasks, retries, lease recovery
- Beat: cron / interval → enqueue
- Clear Python call surface; CLI for worker and beat

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

Run the worker. It claims the job, runs `hello`, and sets the row to `succeeded`. Stop it with Ctrl-C. Periodic tasks run in this same process; there is no separate beat.

```bash
python demo.py
akadze worker demo:app
```

`akadze.jobs.state` is then `succeeded`. A higher `priority` is claimed first. `result` is optional. Pass enqueue options (`delay`, `run_at`, `priority`, `unique_key`) through `using(...)`, not as task arguments.

Tests use `AKADZE_DATABASE_URL` and drop schema `akadze` in that database. The database name must be `akadze` or end with `_test`.

## Layout

```text
akadze/   # library package
tests/
```

Design notes: [`g4st3r-d3v/akadze-harness`](https://github.com/g4st3r-d3v/akadze-harness) (`HUB.md`). This repo is code-only.

## License

MIT
