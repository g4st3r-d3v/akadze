# Moving from Celery

A how-to for moving a Celery app onto akadze. The broker is the application's Postgres, so moving means deploying `akadze migrate`, porting task definitions and call sites, and replacing `celery beat` with the worker's own schedules.

## 1. Define tasks

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

`timeout` is not a hard `time_limit`. When it fires, the run counts as an error and is retried with backoff, not failed. A sync task keeps running in its thread after the timeout, so a retry can overlap it, and the thread no longer counts against `slots` (repeated timeouts can fill the default thread pool); give sync tasks their own deadline. There is no `shared_task`: modules expose `register(app)` and the app calls `include`, or a factory binds plain callables with `app.register` (see [Tasks and the app](tasks.md#bind-dependencies)).

## 2. Enqueue

| Celery | akadze |
|---|---|
| `send_mail.delay(7)` | `await send_mail.using(session=connection).enqueue(user_id=7)` |
| `apply_async(countdown=30)` | `using(..., delay=timedelta(seconds=30))` |
| `apply_async(eta=dt)` | `using(..., run_at=dt)` (timezone-aware) |
| `apply_async(expires=dt)` | `using(..., expires_at=dt)` |
| `apply_async(priority=…)` | `using(..., priority=…)` — higher is claimed first, smallint range; Celery on Redis treats 0 as highest, so invert those values |
| `apply_async(task_id=…)` for dedup | `using(..., unique_key=…)` → `DuplicateJob` while active |
| `transaction.on_commit(lambda: task.delay())` | not needed: `enqueue` runs in the caller's transaction |
| `group(task.s(x) for x in xs).apply_async()` (fan-out, no result) | `await task.using(session=connection).enqueue_many({"x": x} for x in xs)` |

Arguments are keyword-only in `enqueue(...)`. Options never go into task arguments.

## 3. Retries and control flow

| Celery | akadze |
|---|---|
| `raise self.retry(countdown=10)` | `raise Retry(timedelta(seconds=10))` — spends an attempt |
| `autoretry_for=(Exception,)`, `retry_backoff`, `retry_jitter` | default: any exception retries with jittered exponential backoff until `max_attempts`; custom curve via `@app.task(..., backoff=fn)` |
| retry without spending the budget (polling) | `raise Snooze(timedelta(...))` — does not spend an attempt |
| `raise Reject(requeue=False)` / give up | `raise Fail("reason")` |
| `revoke(task_id)` | `request_cancel(session, job_id)` or `akadze jobs cancel JOB_ID`; inside a task `raise Cancel()` |
| `acks_late` + visibility timeout | lease with heartbeat; another worker requeues a lost worker's jobs and spends an attempt (on the last one the job is `failed`) |

`request_cancel` does not interrupt a running task. A job whose cancel was requested, or whose `expires_at` passed, before the run is cancelled without running. During the run, the body decides: if it returns, the job ends `succeeded` with its result (its side effects happened); if it raises anything, `Retry` and `Snooze` included, or returns a result that is not JSON, the job ends `cancelled` and no attempt is spent. Once a `complete_tx` block has committed, the job is `succeeded` whatever the body does next.

Delivery is at least once: a job can run again if its worker disappears, so tasks must be idempotent, as with `acks_late`. Size `max_attempts` for crashes too.

## 4. Chains and results

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

## 5. Limits and routing

| Celery | akadze |
|---|---|
| `rate_limit="10/m"` | `set_limit(session, "vendor", max_starts=10, window=timedelta(minutes=1))` + `using(..., limit_key="vendor")` — cluster-wide |
| a dedicated worker pool for a vendor | `set_limit(session, "vendor", max_running=N)` + `using(..., limit_key="vendor")` |
| `-Q mail` / routing | `@app.task(..., queue="mail")` and `akadze worker app:app --queue mail` |
| `--concurrency=8` | `akadze worker app:app --slots 8` — slots are per queue: with three `--queue` flags the worker runs up to 24 jobs |
| stop a queue everywhere | `akadze queues pause mail` / `resume` — cluster-wide, not one worker |
| `celery control cancel_consumer` on one host | stop that worker (Ctrl-C / SIGTERM): it takes no new jobs and lets running ones finish |

## 6. Beat

`celery beat` and its schedule move into the worker: `@app.periodic("name", cron="0 * * * *")` (with `timezone="Europe/Berlin"` for Celery's `beat_cron_timezone` / `CELERY_TIMEZONE`) or `every=timedelta(...)`, or `app.add_schedules([...])` from the application's config. Every worker runs the schedule; a due slot is enqueued once. There is no separate beat process (`akadze beat` is a stub).

## 7. Operating it

Flower's screens map to CLI and queries: `akadze jobs list`, `akadze workers list`, `akadze limits list`, `akadze jobs requeue JOB_ID`, and `akadze metrics` for a Prometheus sidecar. Signals: `task_prerun` → `app.hooks.before_run`; `task_success` → `app.hooks.after_run`, which runs only when the task returns and inside its timeout (an error there spends an attempt and the body runs again on retry); `task_postrun`, `task_failure` and `task_retry` → `app.hooks.on_transition`, which sees every state change in the transaction that makes it.

## What is not there, and why

- **Redis/RabbitMQ brokers** — Postgres is the broker so enqueue can share the application's transaction.
- **Canvas and `.get()`** — chords hold worker slots while waiting; a follow-up on `complete_tx` covers chains without that.
- **Result backend** — results belong in application tables; `result` on the job row is optional.
- **HTTP / web UI** — another package can build one on the queries above.
