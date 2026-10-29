# Enqueue

A task is enqueued on a connection or session you pass in, inside your transaction:

```python
async with app.engine.begin() as connection:
    await hello.using(session=connection).enqueue(name="ada")
```

Task arguments are keyword-only. Options never go into task arguments; they go through `using(...)`.

## Options

| `using(...)` option | Meaning |
|---|---|
| `session` | `AsyncConnection` or `AsyncSession` to insert on (required) |
| `delay` | run no earlier than now + this `timedelta` |
| `run_at` | run no earlier than this timezone-aware `datetime` |
| `priority` | overrides the task's priority for this job; higher is claimed first (smallint) |
| `max_attempts` | overrides the task's budget, between 1 and 2147483647 |
| `unique_key` | at most one active (`queued` or `running`) job per key |
| `limit_key` | the key [limits](limits.md) apply to |
| `expires_at` | timezone-aware; a job not started by then is cancelled |
| `meta` | a small JSON object to label the job for operations |

An invalid option raises `EnqueueError` and inserts nothing.

## Deduplicate

```python
from akadze import DuplicateJob

try:
    await sync_user.using(session=connection, unique_key=f"sync:{user_id}").enqueue(user_id=user_id)
except DuplicateJob:
    pass
```

A second enqueue with the same active `unique_key` raises `DuplicateJob`. It does not abort the rest of your transaction. Once the first job finishes, the key is free again.

## Fan out

```python
await notify.using(session=connection, priority=5).enqueue_many(
    {"user_id": user_id} for user_id in user_ids
)
```

`enqueue_many` inserts one job per item with the shared options, in order, a thousand rows per statement: 10 000 jobs take about 0.1 s against 16 s for a loop of `enqueue`. Every item is validated first; one bad item raises `EnqueueError` and nothing is inserted. `unique_key` names one job, so `enqueue_many` rejects it. `on_enqueue` hooks run for each job.

## Label jobs with meta

```python
await charge.using(session=connection, meta={"tenant": "acme", "order_id": 42}).enqueue(order_id=42)
```

`meta` is a JSON object with string keys, at most 8 KiB, indexed with GIN. It is a label, not a place for arguments or secrets: it shows up in `JobSummary.meta`, on the `Job` that hooks receive, and in the CLI. NaN, infinities and NUL characters are rejected.

Filter by it:

```python
from akadze import list_jobs

jobs = await list_jobs(app.engine, meta={"tenant": "acme"})
```

```bash
akadze jobs list --meta tenant=acme --meta order_id=42
```

A job's meta matches when it contains every given pair. CLI values are JSON when they parse, text otherwise.
