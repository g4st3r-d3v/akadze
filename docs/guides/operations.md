# Deploy and operate

## Deploy

1. Run `akadze migrate` (or `await migrate(database_url)`) against the application database before starting new workers. Migrations are tracked; `applied_versions(database_url)` lists them.
2. Start workers: `akadze worker app.module:app --slots 8 --queue default --queue mail`.

`--slots` is per queue: with three `--queue` flags a worker runs up to `3 × slots` jobs. Without `--queue` it claims from `default`.

!!! note "Large existing `akadze.jobs` table"
    Migration `005_jobs_meta` builds a GIN index on `akadze.jobs.meta` and blocks writes to the table while it runs. On a large table create it first with `CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_meta_idx ON akadze.jobs USING gin (meta jsonb_path_ops)`; the migration then skips it.

### Rolling deploys

A worker claims only jobs whose task is registered on its app. A job of a task that only the new code defines stays `queued`, without spending an attempt, until a new worker takes it. Workers on akadze 0.5.0 and older still claim it and spend attempts, so upgrade akadze before adding tasks.

A job whose task no worker registers (removed or renamed) stays queued with no error, keeps its `unique_key` taken and counts in queue lag. Cancel such jobs when you remove a task.

### PgBouncer

The engine `Akadze(database_url=...)` builds disables asyncpg's statement cache, so a transaction-mode pool can sit in front. If you pass your own `engine`, set `connect_args={"statement_cache_size": 0}` yourself.

## Look at the queue

| Python | CLI | Shows |
|---|---|---|
| `queue_snapshot(engine)` | — | counts per queue and state, wait of the oldest ready job, running count |
| `list_jobs(engine, queue=, state=, limit=50, meta=)` | `akadze jobs list` | job summaries, no args/result/errors |
| `list_workers(engine)` | `akadze workers list` | id, host, pid, queues, start and last heartbeat |
| `list_limits(engine)` | `akadze limits list` | limit policies |

## Control

| Python | CLI |
|---|---|
| `pause_queue(session, name)` / `resume_queue` | `akadze queues pause NAME` / `resume NAME` |
| `request_cancel(session, job_id)` | `akadze jobs cancel JOB_ID` |
| `requeue(session, job_id)` | `akadze jobs requeue JOB_ID` |
| `clear_limit(session, key)` | `akadze limits clear KEY` |

Pausing is cluster-wide: no worker claims from the queue, and running jobs finish. To stop one host, stop its worker (Ctrl-C or SIGTERM).

## Metrics

`akadze metrics` prints Prometheus text: jobs per queue and state, lag, running, and `akadze_queue_paused`. From Python, `akadze.metrics.render_metrics(engine)`. akadze serves no HTTP; expose the text from your app or a sidecar.

Count outcomes per task in an `on_transition` [hook](tasks.md#hooks). Do not put task arguments or results in logs or metric labels.

## Retention

The worker deletes finished jobs (`succeeded`, `failed`, `cancelled`) and periodic runs older than the app's `retention` (default 7 days), in batches, about once a minute. Set it with `Akadze(retention=timedelta(days=30))`.

## Worker and database errors

A transient database error (restart, failover, dropped connection, deadlock) is logged and retried with backoff; the worker keeps running and does not claim while its heartbeat fails. Other database errors, such as a schema that was not migrated, stop it. See [Delivery and the worker](../concepts/delivery.md).
