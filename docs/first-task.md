# First task

This page takes you from an empty database to a job that ran. You need Python 3.13 and a PostgreSQL database you can create a schema in.

## 1. Install and migrate

```bash
pip install akadze
export AKADZE_DATABASE_URL=postgresql://akadze:akadze@localhost:5432/akadze
akadze migrate
```

`akadze migrate` creates the `akadze` schema and its tables. They are not part of your application's own migrations: do not copy them into Alembic. Deploy runs `akadze migrate` (or `await akadze.migrate(database_url)`) against the same database.

## 2. Define a task and enqueue it

Save this as `demo.py`:

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
    await app.aclose()


if __name__ == "__main__":
    asyncio.run(main())
```

`enqueue` runs in the caller's transaction, so a rollback removes the job. Arguments go by name and are validated against the function's signature.

```bash
python demo.py
```

## 3. Run the worker

Run it from the directory that contains `demo.py`:

```bash
akadze worker demo:app
```

The worker loads `demo:app`, claims the job, runs `hello` and sets the row to `succeeded`. Check it:

```bash
akadze jobs list --state succeeded
```

Stop the worker with Ctrl-C. It takes no new jobs and waits up to 10 seconds for running ones; whatever is still running goes back to the queue without spending an attempt.

## Next

- Split tasks into modules and bind dependencies: [Tasks and the app](guides/tasks.md).
- Delays, priorities, deduplication, fan-out: [Enqueue](guides/enqueue.md).
- What happens when a task fails: [Retries, cancel and completion](guides/retries.md).
