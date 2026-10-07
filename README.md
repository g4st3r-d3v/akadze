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

## Quick start

```bash
poetry install
export AKADZE_DATABASE_URL=postgresql://akadze:akadze@localhost:5432/akadze
akadze migrate
```

`akadze worker` and `akadze beat` are still stubs. The migrate command creates schema `akadze` (jobs, workers, periodic runs). Higher `priority` is claimed first. `result` is optional.

Tests use `AKADZE_DATABASE_URL` and drop schema `akadze` in that database. The database name must be `akadze` or end with `_test`.

## Layout

```text
akadze/   # library package
tests/
```

Design notes: [`g4st3r-d3v/akadze-harness`](https://github.com/g4st3r-d3v/akadze-harness) (`HUB.md`). This repo is code-only.

## License

MIT
