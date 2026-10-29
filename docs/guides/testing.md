# Testing

## Run jobs in-process

`akadze.testing` runs your tasks without a worker process.

```python
from akadze.testing import assert_enqueued, drain

async def test_signup_sends_welcome(app, client):
    await client.post("/signup", json={"email": "ada@example.com"})

    await assert_enqueued(app, "mail.welcome", email="ada@example.com")
    await drain(app)
```

- `drain(app)` runs every job that is ready now, the same way a worker would. Jobs a run enqueues and that are ready also run. It raises `AkadzeError` if a ready job's task is not registered on `app`. Paused queues are skipped. The claim still applies limits, so a job held back by `max_starts` keeps `drain` stepping until its window passes; `drain(app, limit=...)` caps the number of steps and raises `AkadzeError` if jobs are still ready after them.
- `assert_enqueued(app, name, **arguments)` checks that a `queued` or `running` job of that task has those arguments, compared in their JSON form.

## The test database

Tests need a real PostgreSQL. akadze's own suite reads `AKADZE_DATABASE_URL` and drops the `akadze` schema in that database, so it refuses any database not named `akadze` or ending in `_test`. Use the same rule for yours: a dedicated database, migrated once per session with `await migrate(url)`.
