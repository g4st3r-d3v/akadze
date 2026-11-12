# Deploy and operate

## Deploy

1. Run `akadze migrate` (or `await migrate(database_url)`) against the application database before starting new workers. Migrations are tracked; `applied_versions(database_url)` lists them.
2. Start workers: `akadze worker app.module:app --slots 8 --queue default --queue mail`.

`--slots` is per queue: with three `--queue` flags a worker runs up to `3 × slots` jobs. Without `--queue` it claims from `default`.

!!! note "Large existing `akadze.jobs` table"
    Migration `005_jobs_meta` builds a GIN index on `akadze.jobs.meta` and blocks writes to the table while it runs. On a large table create it first with `CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_meta_idx ON akadze.jobs USING gin (meta jsonb_path_ops)`; the migration then skips it. Migration `006_jobs_cancel_idx` adds a small partial index for the cancel step the same way; on a large table create it first with `CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_cancel_idx ON akadze.jobs (queue) WHERE state = 'queued' AND (cancel_requested_at IS NOT NULL OR expires_at IS NOT NULL)`.

### Rolling deploys

Since 0.6.0, a worker claims only jobs whose task is registered on its app. A job of a task that only the new code defines stays `queued`, without spending an attempt, until a new worker takes it. Workers on akadze 0.5.0 and older still claim it and spend attempts, so upgrade akadze before adding tasks.

A job whose task no worker registers (removed or renamed) stays queued with no error, keeps its `unique_key` taken and counts in queue lag. Cancel such jobs when you remove a task.

### Connection pool

Each running job holds a connection to start and another to record its outcome, one at a time, and the worker loop and heartbeat need one each. So a worker needs at least `slots × queues + 2` connections, more if task bodies use `app.engine` themselves or a web app shares it. The engine `Akadze(database_url=...)` builds has SQLAlchemy's default pool, 5 plus 10 overflow: enough for 13 concurrent runs (`slots × queues`). With more, runs wait for a connection; the worker logs a warning at start. Set `pool_size` for larger workers (overflow stays 10):

```python
app = Akadze(database_url="postgresql://app@db/app", pool_size=25)
```

If you pass your own `engine`, size its pool when you create it; `pool_size` is not accepted together with `engine`.

On 2000 no-op jobs with 20 slots, `pool_size=25` takes the run from 7.8 s to 3.1 s.

### Batched outcomes

By default each finished run writes its outcome in its own transaction. `Worker(app, outcome_batch=timedelta(milliseconds=10))` (CLI: `akadze worker app:app --outcome-batch 10`) writes them together: a batch goes out as soon as every run in flight is waiting in it, when 200 wait, or after `outcome_batch` at the latest. On short jobs this raises throughput a lot (1.67x on 10 000 no-op jobs with 20 slots in CI). An outcome may land in the database up to `outcome_batch` later than without it, and a run keeps its slot until its outcome is written.

Delivery is at-least-once in both modes. If a batch fails, each of its runs writes its outcome alone, as without batching. `on_transition` hooks run per row in the batch transaction, each in its own savepoint, but outside the run's context (`current()` is not that run).

### Event loop

`pip install "akadze[uvloop]"` and `akadze worker` runs on [uvloop](https://github.com/MagicStack/uvloop), an event loop in C. The worker spends most of its CPU in the loop and in socket I/O, so it gets faster with no code change. `--loop auto` (the default) takes uvloop when it is installed, `--loop asyncio` keeps the standard loop, `--loop uvloop` fails without it. In your own process, run the worker with `uvloop.run(...)` instead of `asyncio.run(...)`. uvloop does not support Windows.

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

The worker deletes finished jobs (`succeeded`, `failed`, `cancelled`) and periodic runs older than the app's `retention` (default 7 days), about once a minute: each worker deletes at most 1 000 finished jobs per pass. A cluster that finishes more than about 1 000 jobs a minute per worker outgrows this; delete old rows yourself (`DELETE FROM akadze.jobs WHERE state IN ('succeeded','failed','cancelled') AND finished_at < …` in batches). Set it with `Akadze(retention=timedelta(days=30))`.

## Worker and database errors

Since 0.6.0, a transient database error (restart, failover, dropped connection, deadlock) is logged and retried with backoff; the worker keeps running and does not claim while its heartbeat fails. Other database errors, such as a schema that was not migrated, stop it. See [Delivery and the worker](../concepts/delivery.md).
