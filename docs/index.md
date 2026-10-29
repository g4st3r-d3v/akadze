# akadze

A task queue for Python whose broker is the PostgreSQL database your application already has. You enqueue work in the same transaction as your own writes, and a worker claims it with `FOR UPDATE SKIP LOCKED` under a heartbeat lease. Periodic schedules run inside the same worker; there is no separate beat process.

```bash
pip install akadze
```

!!! warning "Status: alpha (0.x)"
    The public API may still change between minor versions until 1.0. Report bugs in [Issues](https://github.com/g4st3r-d3v/akadze/issues).

    These pages follow the `main` branch, which may be ahead of the latest release on PyPI. Behavior that changed in 0.6.0 is marked "since 0.6.0"; see the [release notes](https://github.com/g4st3r-d3v/akadze/releases) for upgrades.

!!! note "Terms"
    A **task** is a registered function. A **job** is a row in `akadze.jobs`: one call of a task with its arguments. A **claim** is a worker taking jobs from a queue.

## Why Postgres as the broker

- **Enqueue is transactional.** `enqueue` runs on your connection. If your transaction rolls back, the job is gone too. No `on_commit` hooks, no outbox table.
- **One system to run.** No Redis or RabbitMQ to deploy, back up and monitor next to the database you already operate.
- **State is queryable.** Jobs are rows in `akadze.jobs`. The CLI, `list_jobs` and Prometheus text read the same rows.

## What it has

| | |
|---|---|
| Tasks | `async def` or sync functions, arguments validated from the signature |
| Retries | jittered exponential backoff, custom per task, `Retry` / `Snooze` / `Fail` / `Cancel` |
| Delivery | at least once, heartbeat lease, lost jobs rescued by other workers |
| Schedules | cron (with IANA time zones) and intervals, inside the worker, one fire per slot cluster-wide |
| Limits | cluster-wide `max_running` and `max_starts` per window, by `limit_key` |
| Fairness | optional fair claim across tenants within a priority |
| Operations | CLI for jobs, queues, limits and workers; pause/resume; Prometheus text |
| Testing | `akadze.testing.drain` runs ready jobs in-process |

## What it does not have

- Brokers other than Postgres.
- Canvas (`chain`, `group`, `chord`) and a result backend. Chain steps with [`complete_tx`](guides/retries.md#commit-your-writes-with-the-outcome) instead.
- An HTTP API or web UI.

## Where to go next

- New here: [First task](first-task.md), then a runnable [FastAPI example](examples.md).
- Coming from Celery: [Moving from Celery](guides/celery.md).
- How it compares on throughput: [Benchmark](benchmark.md).
- Running it in production: [Deploy and operate](guides/operations.md) and [Delivery and the worker](concepts/delivery.md).
- Design notes and decisions live in [akadze-harness](https://github.com/g4st3r-d3v/akadze-harness).
