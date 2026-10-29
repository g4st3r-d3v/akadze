# Limits and fair claim

## Limits

A limit is a row in `akadze.limits` keyed by a string. Jobs opt in with `limit_key`; the policy never lives on the job row, so you can change it while jobs wait.

```python
from datetime import timedelta
from akadze import set_limit

async with app.engine.begin() as connection:
    # at most 4 running at once, cluster-wide
    await set_limit(connection, "vendor-x", max_running=4)
    # and at most 100 starts per minute
    await set_limit(connection, "vendor-x", max_starts=100, window=timedelta(minutes=1))

    await call_vendor.using(session=connection, limit_key="vendor-x").enqueue(item_id=7)
```

- `max_running` caps how many jobs with the key may be `running` across all workers.
- `max_starts` with `window` caps how many claims may start in a sliding window. Starts are tracked in `akadze.limit_starts`, not by scanning jobs.
- One call may set either or both; omitted fields keep their values.
- `clear_limit(connection, key)` deletes the row and its start marks; the key becomes unlimited. A later `set_limit` counts starts from zero.
- A `limit_key` without a limits row, and an empty key, have no limit.

Limits hold under concurrent claims: a claim locks the limits rows of the keys it considers and counts running jobs and starts only after it holds the lock.

`list_limits(engine)` returns the policies; on the CLI, `akadze limits list` and `akadze limits clear KEY`.

Limits are cluster-wide. `Worker(slots=...)` is a separate, per-process cap.

## Fair claim

By default a worker claims by priority, then `run_at`, then id. A tenant that enqueued a million jobs then takes every free slot while another tenant's two jobs wait.

Fair claim fixes that within a priority:

```bash
akadze worker app:app --fair
```

```python
Worker(app, fair=True)
```

Jobs are grouped by `limit_key` (jobs without a key form one group). A free slot goes to the group with the fewest jobs already running in that queue across the cluster; ties go to the oldest job. This holds when slots free up one at a time and across many workers. Priority still comes first, and limits still apply.

A claim on a 100 000-job backlog takes a few milliseconds in both orders; see [How a claim works](../concepts/claim.md).
