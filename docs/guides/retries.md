# Retries, cancel and completion

## What spends an attempt

`max_attempts` is the budget including the first run (default 3). When it runs out, the job ends `failed`.

| Outcome of a run | Attempt spent | Next |
|---|---|---|
| returns a JSON-serializable value (or nothing) | — | `succeeded` |
| returns a value that is not JSON (`datetime`, a model) | yes | retried after backoff (`ResultNotJson`) |
| raises any exception | yes | retried after backoff |
| exceeds `timeout` | yes | retried after backoff |
| `raise Retry(delay)` | yes | retried after `delay` |
| `raise Snooze(delay)` | no | runs again after `delay` |
| `raise Fail("reason")` | yes | `failed` now |
| `raise Cancel()` | — | `cancelled` now |
| worker lost, rescued by another | yes | retried after backoff |
| graceful worker shutdown | no | back to `queued` |

Use `Snooze` for polling: "not ready yet, look again later".

```python
from datetime import timedelta
from akadze import Retry, Snooze, Fail

@app.task("export.poll")
async def poll(export_id: str) -> None:
    status = await vendor.status(export_id)
    if status == "pending":
        raise Snooze(timedelta(seconds=30))
    if status == "rejected":
        raise Fail("vendor rejected the export")
```

## Backoff

The default delay is `2^(attempt-1)` seconds, capped at 300, with jitter in `[50 %, 100 %]`, so a batch that failed together does not hit a vendor again in one wave. Jobs rescued from a lost worker always get this default curve, even when the task has its own policy.

Set your own per task:

```python
@app.task("sync", backoff=lambda attempt: timedelta(minutes=attempt))
async def sync() -> None: ...
```

The policy must be a cheap, plain (not async) function: it runs on the worker's event loop. A policy that raises or returns something other than a `timedelta` falls back to the default. Policy delays are clamped to `[0, 365 days]`. `Retry(delay)` wins over any policy and is not clamped. Annotate policies with `akadze.BackoffPolicy`.

## Cancel

```python
from akadze import request_cancel

await request_cancel(connection, job_id)
```

```bash
akadze jobs cancel JOB_ID
```

`request_cancel` marks a `queued` or `running` job; other states and missing ids are a no-op. The CLI exits 1 with `job is not active` in that case.

- A job cancelled, or past its `expires_at`, before it starts is `cancelled` without running.
- A running task is not interrupted. Since 0.6.0 (on PyPI: 0.5.0 still ends such a job `cancelled`): when the body returns, the job is `succeeded` with its result: its side effects happened. When it raises anything (`Retry` and `Snooze` included) or returns a non-JSON result, it ends `cancelled` and no attempt is spent.

## Requeue a failed job

```python
from akadze import requeue

await requeue(connection, job_id)
```

`requeue` returns a `failed` job to `queued` in your transaction. It keeps `attempt`, `errors` and `args`, and raises `max_attempts` when needed. CLI: `akadze jobs requeue JOB_ID`.

## Commit your writes with the outcome

`current().complete_tx()` gives a connection whose transaction also moves the job to `succeeded`. Your writes and the outcome commit together or not at all.

```python
from akadze import current

@app.task("report.build")
async def build(report_id: int) -> None:
    ...
    async with current().complete_tx() as connection:
        await connection.execute(...)  # mark the report built
        await notify.using(session=connection).enqueue(report_id=report_id)
```

This is how to chain steps: the follow-up job exists only if this step succeeded.

- If this run no longer owns the job (another worker rescued it), the block raises and your writes roll back.
- A cancel or expiry that arrives while the block runs does not stop it: a block that reaches its end commits its writes and `succeeded`.
- An error inside the block rolls the writes back and spends an attempt (none when a cancel was requested or the job expired meanwhile: it ends `cancelled`).
