# akadze

Postgres-backed task queue: enqueue work, run workers, optionally schedule with beat.

Shape is familiar if you know Celery. The broker is PostgreSQL (`FOR UPDATE SKIP LOCKED` + lease), not Redis or RabbitMQ.

**Status:** pre-alpha scaffold. Python API will move.

## Goals

- Small library, no product domain
- Tasks, retries, lease recovery
- Beat: cron / interval → enqueue
- Clear Python API; CLI for worker and beat

## Non-goals (for now)

- HTTP / REST / web UI (another library or app owns that)
- Replacing any product service queue
- Multi-broker adapters
- Distributed tracing / full observability suite

## Quick start (planned)

```bash
poetry install
# migrate / ensure schema
akadze worker
akadze beat
```

## Layout

```text
akadze/   # library package
tests/
```

Design notes live in Obsidian «База Знаний» (`Проекты/IT/akadze`), mirrored on GitHub as [`g4st3r-d3v/obsidian-kb`](https://github.com/g4st3r-d3v/obsidian-kb). This repo has no `kb/`.

## License

MIT
