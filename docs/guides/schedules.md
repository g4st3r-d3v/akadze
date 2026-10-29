# Periodic schedules

Schedules run inside every worker. Each due fire is enqueued once across the cluster; there is no separate beat process (`akadze beat` is a stub).

```python
from datetime import timedelta

@app.periodic("digest", cron="0 9 * * *", timezone="Europe/Berlin")
async def digest() -> None: ...

@app.periodic("ops.heartbeat", every=timedelta(seconds=30), overlap=False)
async def heartbeat() -> None: ...
```

`@app.periodic` takes the task options (`queue`, `max_attempts`, `timeout`, `backoff`) plus exactly one of `cron` and `every`.

## From configuration

Schedules can come from your app's config instead of decorators. Parse YAML, TOML or JSON yourself, turn rows into parsed values (`cron` text or a `timedelta`, not duration strings), then:

```python
@app.task("ops.prune", queue="ops")
async def prune() -> None: ...

app.add_schedule("ops.prune", every=timedelta(seconds=30), overlap=False)
# or several at once
app.add_schedules([{"name": "ops.prune", "cron": "0 * * * *"}])
```

The task must already be registered.

## Time zones and DST

Cron runs in UTC unless the schedule names an IANA zone (`timezone="Europe/Moscow"`). The expression is read on that zone's wall clock and DST follows classic cron:

- **Clocks go back:** an entry with a fixed hour fires once (the first pass); an entry with `*` in the hour field fires through both passes.
- **Clocks go forward:** a fixed-hour time inside the gap fires shifted by the gap (02:30 becomes 03:30), so a daily job is not lost. If a shifted fire lands on another fire of the same schedule, it runs once.

`timezone` must be a string with an IANA name. It is rejected for `every` schedules and for unknown zones.

## Overlap

`overlap=False` holds a fire back while the schedule's previous job is still `queued` or `running`. Every scheduler tick retries the latest due fire; it is enqueued once the previous job finishes, as long as the fire is not older than the app's `grace` (default one minute). Fires do not pile up: a newer due fire replaces the held one, and a fire held past `grace` is skipped. A long run delays the next start instead of replaying missed fires.
