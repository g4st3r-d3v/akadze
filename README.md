# akadze

[![ci](https://github.com/g4st3r-d3v/akadze/actions/workflows/ci.yml/badge.svg)](https://github.com/g4st3r-d3v/akadze/actions/workflows/ci.yml)

Postgres-backed task queue: enqueue work and run it in a worker. Periodic schedules run in that same process.

Shape is familiar if you know Celery. The broker is PostgreSQL (`FOR UPDATE SKIP LOCKED` + lease), not Redis or RabbitMQ.

**Status:** alpha (0.x). Public API may still change between minors until 1.0; after Beta the surface freezes. Bugs → [Issues](https://github.com/g4st3r-d3v/akadze/issues).

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

Split task modules with a thin `include`. Each module exposes `register(app)` and attaches `@app.task` / `@app.periodic` / `app.register` there — no package walk, no import-for-side-effect:

```python
# tasks/hello.py
def register(app: Akadze) -> None:
    @app.task("hello")
    async def hello(name: str) -> str:
        return f"hello {name}"


# app.py
app = Akadze(database_url="postgresql://akadze:akadze@localhost:5432/akadze")
app.include("tasks.hello")
```

When dependencies exist before the live `Akadze` instance, declare a plain callable and bind it later. Canonical DI path: factory builds the app → `include` modules → `app.register` unbound callables (closures over deps) → optional `add_schedules`. No global app bind and no Celery `shared_task`.

```python
# tasks/hello.py — no app, no decorator
async def hello(name: str, *, runner: Runner) -> str:
    return await runner.greet(name)


# bootstrap after assemble_runtime / DI
def create_app(deps: Deps) -> Akadze:
    app = Akadze(database_url="postgresql://akadze:akadze@localhost:5432/akadze")
    app.include("tasks.ops")

    async def _hello(name: str) -> str:
        return await hello(name, runner=deps.runner)

    app.register(_hello, name="hello")  # same options as @app.task
    # app.add_schedules([...])  # optional, after tasks exist
    return app
```

`app.register(fn, *, name, queue=..., max_attempts=..., timeout=..., priority=...)` is equivalent to `@app.task(...)` without decorating on a live instance. It returns the registered `Task`.

Schedules can also come from app config without a `@periodic` registrar per service. Parse YAML/TOML/JSON in the app, turn rows into already-parsed specs (`cron` text or `timedelta` — not duration strings), then call `add_schedule` / `add_schedules`. The task must already be in `app.tasks`; the worker tick is unchanged.

```python
from datetime import timedelta

@app.task("ops.prune", queue="ops")
async def prune() -> None:
    ...

# after include / task registration — values already parsed by the app
app.add_schedule("ops.prune", every=timedelta(seconds=30), overlap=False)
# or many: app.add_schedules([{"name": "ops.prune", "cron": "0 * * * *"}, ...])
```

Cron runs in UTC unless the schedule names an IANA zone: `@app.periodic("digest", cron="0 9 * * *", timezone="Europe/Moscow")`, or `timezone=` in `add_schedule` / a spec. The cron is read on that zone's wall clock, and DST follows classic cron. When clocks go back, an entry with a fixed hour fires once (the first pass) and an entry with `*` in the hour field keeps firing through both passes. When clocks go forward, a fixed-hour wall time inside the gap fires shifted by the gap (02:30 becomes 03:30), so a daily job is not lost. If a shifted fire lands on another fire of the same schedule, they run once. `timezone` must be an IANA name (`str`) and is rejected for `every` schedules and unknown zones.

`akadze.jobs.state` is then `succeeded`. A higher `priority` is claimed first. `result` is optional. Pass enqueue options (`delay`, `run_at`, `priority`, `max_attempts`, `unique_key`, `limit_key`, `expires_at`) through `using(...)`, not as task arguments. `priority` and `max_attempts` override the task defaults for that one job; `max_attempts` must be between 1 and 2147483647, otherwise `EnqueueError` is raised and nothing is inserted. `expires_at` must be timezone-aware. A job that has already expired is cancelled before it runs. A second enqueue with the same active `unique_key` raises `DuplicateJob` and does not abort the rest of the caller's transaction.

An error spends an attempt and the job is retried after a backoff: exponential (`2^(attempt-1)` seconds, capped at 300) with jitter in `[50 %, 100 %]`, so a batch that failed together does not hit a vendor again in one wave. A task can set its own curve with `@app.task(..., backoff=lambda attempt: timedelta(minutes=attempt))` (same option on `app.register`). `raise Retry(delay)` still wins over both. The policy must be a cheap, plain (not async) function: it runs on the worker's event loop. A policy that raises or returns something other than a `timedelta` falls back to the default; delays are clamped to `[0, 365 days]`, so a negative one means retry now. Jobs requeued from a lost worker get the same jittered default curve instead of all starting at once. `akadze.BackoffPolicy` is the type to annotate a policy with.

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

`queue_snapshot(engine)` reads committed rows: how many jobs are in each queue and state, how long the oldest ready job has waited, and how many are running. `list_jobs(engine, queue=..., state=..., limit=50)` returns `JobSummary` rows (no args/result/errors/meta), ordered by priority, then `run_at`, then id. `akadze jobs list` prints one summary per line. `list_workers(engine)` returns `WorkerSummary` rows from `akadze.workers` (`id`, `hostname`, `pid`, `queues`, `started_at`, `heartbeat_at`), ordered by `heartbeat_at` desc, then `id`. `akadze workers list` prints one summary per line. `akadze.metrics.render_metrics(engine)` returns Prometheus text for those counts, lag, busy, and `akadze_queue_paused` (not exported from the package root; no HTTP). `akadze metrics` (or `akadze metrics render`) prints that text to stdout. Count outcomes in an `on_transition` hook. Do not log task arguments or results.

`requeue(session, job_id)` returns a `failed` job to `queued` in the caller's transaction. It keeps `attempt`, `errors`, and `args`, and raises `max_attempts` when needed. `request_cancel(session, job_id)` sets `cancel_requested_at` on a `queued` or `running` job; other states and missing ids are a no-op. The worker applies the cancellation. `pause_queue` / `resume_queue` stop and restore claims for one queue; jobs already `running` finish. CLI: `akadze jobs requeue JOB_ID`, `akadze jobs cancel JOB_ID`, `akadze queues pause NAME`, `akadze queues resume NAME`.

`set_limit(session, key, max_running=..., max_starts=..., window=...)` writes policies for a `limit_key` in `akadze.limits`. Pass the key through `using(..., limit_key=...)`; limits never live on the job row. `max_running` caps how many may be `running` cluster-wide. `max_starts` + `window` cap how many successful claims may start in that window (tracked in `akadze.limit_starts`, not by scanning `jobs`). One call may set concurrency, rate, or both; omitted fields keep their previous values. `list_limits(engine)` returns `LimitSummary` rows (`key`, `max_running`, `max_starts`, `window`). `clear_limit(session, key)` deletes that limits row and its start marks (idempotent) and makes the key unlimited; a later `set_limit` on the key counts starts from zero — a non-empty `limit_key` without a row is claimed like a job with no limit. Empty/`None` `limit_key` has no limit. When a limits row exists, concurrency and rate checks still apply. This is separate from process-local `Worker.slots`. CLI: `akadze limits list`, `akadze limits clear KEY`.

Do not copy this package's SQL into the application's Alembic history. Deploy runs `akadze migrate`, or `await migrate(database_url)`, against the same database. The engine disables asyncpg's statement cache so a transaction-mode pool such as PgBouncer can sit in front.

Tests use `AKADZE_DATABASE_URL` and drop schema `akadze` in that database. The database name must be `akadze` or end with `_test`.

## Celery migration

A how-to for moving a Celery app onto akadze. The broker is the application's Postgres, so moving means deploying `akadze migrate`, porting task definitions and call sites, and replacing `celery beat` with the worker's own schedules.

### 1. Define tasks

Celery tasks are sync functions bound to a global app. akadze tasks are usually `async def`; a sync function also works and runs in a thread. Arguments must be JSON-serializable: they are validated by a model built from the signature and stored in `akadze.jobs.args`.

```python
# Celery
@shared_task(bind=True, max_retries=5, queue="mail", time_limit=60)
def send_mail(self, user_id: int) -> None: ...

# akadze
@app.task("mail.send", queue="mail", max_attempts=6, timeout=timedelta(seconds=60))
async def send_mail(user_id: int) -> None: ...
```

`max_attempts` is the attempt budget including the first run, so Celery's `max_retries=5` is `max_attempts=6`. The default is 3: a Celery task without retries that must not run twice (a charge, an SMS) needs `max_attempts=1`. An error, `Retry`, a timeout and a worker lost by another worker's heartbeat check each spend an attempt; `Snooze` and a graceful shutdown do not.

`timeout` is not a hard `time_limit`. When it fires, the run counts as an error and is retried with backoff, not failed. A sync task keeps running in its thread after the timeout, so a retry can overlap it; give sync tasks their own deadline. There is no `shared_task`: modules expose `register(app)` and the app calls `include`, or a factory binds plain callables with `app.register` (see above).

### 2. Enqueue

| Celery | akadze |
|---|---|
| `send_mail.delay(7)` | `await send_mail.using(session=connection).enqueue(user_id=7)` |
| `apply_async(countdown=30)` | `using(..., delay=timedelta(seconds=30))` |
| `apply_async(eta=dt)` | `using(..., run_at=dt)` (timezone-aware) |
| `apply_async(expires=dt)` | `using(..., expires_at=dt)` |
| `apply_async(priority=…)` | `using(..., priority=…)` — higher is claimed first, smallint range; Celery on Redis treats 0 as highest, so invert those values |
| `apply_async(task_id=…)` for dedup | `using(..., unique_key=…)` → `DuplicateJob` while active |
| `transaction.on_commit(lambda: task.delay())` | not needed: `enqueue` runs in the caller's transaction |

Arguments are keyword-only in `enqueue(...)`. Options never go into task arguments.

### 3. Retries and control flow

| Celery | akadze |
|---|---|
| `raise self.retry(countdown=10)` | `raise Retry(timedelta(seconds=10))` — spends an attempt |
| `autoretry_for=(Exception,)`, `retry_backoff`, `retry_jitter` | default: any exception retries with jittered exponential backoff until `max_attempts`; custom curve via `@app.task(..., backoff=fn)` |
| retry without spending the budget (polling) | `raise Snooze(timedelta(...))` — does not spend an attempt |
| `raise Reject(requeue=False)` / give up | `raise Fail("reason")` |
| `revoke(task_id)` | `request_cancel(session, job_id)` or `akadze jobs cancel JOB_ID`; inside a task `raise Cancel()` |
| `acks_late` + visibility timeout | lease with heartbeat; another worker requeues a lost worker's jobs and spends an attempt (on the last one the job is `failed`) |

`request_cancel` does not interrupt a running task. The worker checks it before the run and when the run finishes: a job cancelled mid-run ends `cancelled`, its result is dropped and its `complete_tx` block raises, so writes made there roll back. Side effects outside the database stay.

Delivery is at least once: a job can run again if its worker disappears, so tasks must be idempotent, as with `acks_late`. Size `max_attempts` for crashes too.

### 4. Chains and results

There is no canvas (`chain`, `group`, `chord`) and no result backend or `.get()`. To run a follow-up only when a step succeeds, enqueue it on the `complete_tx` connection. Write the outcome into your own tables in the same transaction instead of reading it back from the queue.

```python
from akadze import current

@app.task("report.build")
async def build(report_id: int) -> None:
    ...
    async with current().complete_tx() as connection:
        await connection.execute(...)  # mark the report built
        await notify.using(session=connection).enqueue(report_id=report_id)
```

### 5. Limits and routing

| Celery | akadze |
|---|---|
| `rate_limit="10/m"` | `set_limit(session, "vendor", max_starts=10, window=timedelta(minutes=1))` + `using(..., limit_key="vendor")` — cluster-wide |
| a dedicated worker pool for a vendor | `set_limit(session, "vendor", max_running=N)` + `using(..., limit_key="vendor")` |
| `-Q mail` / routing | `@app.task(..., queue="mail")` and `akadze worker app:app --queue mail` |
| `--concurrency=8` | `akadze worker app:app --slots 8` — slots are per queue: with three `--queue` flags the worker runs up to 24 jobs |
| stop a queue everywhere | `akadze queues pause mail` / `resume` — cluster-wide, not one worker |
| `celery control cancel_consumer` on one host | stop that worker (Ctrl-C / SIGTERM): it takes no new jobs and lets running ones finish |

### 6. Beat

`celery beat` and its schedule move into the worker: `@app.periodic("name", cron="0 * * * *")` (with `timezone="Europe/Berlin"` for Celery's `beat_cron_timezone` / `CELERY_TIMEZONE`) or `every=timedelta(...)`, or `app.add_schedules([...])` from the application's config. Every worker runs the schedule; a due slot is enqueued once. There is no separate beat process (`akadze beat` is a stub).

### 7. Operating it

Flower's screens map to CLI and queries: `akadze jobs list`, `akadze workers list`, `akadze limits list`, `akadze jobs requeue JOB_ID`, and `akadze metrics` for a Prometheus sidecar. Signals: `task_prerun` → `app.hooks.before_run`; `task_success` → `app.hooks.after_run`, which runs only when the task returns and inside its timeout (an error there fails the run); `task_postrun`, `task_failure` and `task_retry` → `app.hooks.on_transition`, which sees every state change in the transaction that makes it.

### What is not there, and why

- **Redis/RabbitMQ brokers** — Postgres is the broker so enqueue can share the application's transaction.
- **Canvas and `.get()`** — chords hold worker slots while waiting; a follow-up on `complete_tx` covers chains without that.
- **Result backend** — results belong in application tables; `result` on the job row is optional.
- **HTTP / web UI** — another package can build one on the queries above.

Design reasoning: [akadze-harness](https://github.com/g4st3r-d3v/akadze-harness).

## Release

Create a tag `vX.Y.Z` that matches the version in `pyproject.toml` and push it. Do not create the GitHub release by hand. GitHub Actions runs the tests, publishes the wheel to PyPI, and creates the GitHub release with the built files. If a release for the tag already exists, the workflow uploads the files to it and publishes it if it is a draft. Re-running the workflow is safe: files already on PyPI are skipped.

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
