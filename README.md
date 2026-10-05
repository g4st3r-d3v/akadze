# akadze

Postgres-backed task queue: enqueue work, run workers, optionally schedule with beat.

Shape is familiar if you know Celery. The broker is PostgreSQL (`FOR UPDATE SKIP LOCKED` + lease), not Redis or RabbitMQ.

**Status:** pre-alpha scaffold. API will move.

## Goals

- Small library, no product domain
- Tasks, retries, lease recovery
- Beat: cron / interval → enqueue
- Clear Python API; CLI for worker and beat
- Knowledge base in `kb/` (Obsidian-friendly)

## Non-goals (for now)

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
akadze/          # library package
kb/              # knowledge base (Diátaxis-ish notes)
tests/
```

## License

MIT
