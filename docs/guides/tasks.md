# Tasks and the app

An `Akadze` app holds the engine, the registered tasks and the schedules. Workers load it by import path (`module:attribute`).

```python
from datetime import timedelta
from akadze import Akadze

app = Akadze(database_url="postgresql://akadze:akadze@localhost:5432/akadze")
```

| Option | Default | Meaning |
|---|---|---|
| `database_url` or `engine` | — | One is required. With `database_url` the app builds an async engine and owns it; pass your own `engine` to share a pool |
| `grace` | 1 minute | How late a periodic fire may still be enqueued |
| `retention` | 7 days | Finished jobs and periodic runs older than this are deleted by the worker |

The engine the app builds disables asyncpg's statement cache, so a transaction-mode pool such as PgBouncer can sit in front.

## Define a task

```python
@app.task("mail.send", queue="mail", max_attempts=6, timeout=timedelta(seconds=60), priority=0)
async def send_mail(user_id: int) -> None:
    ...
```

| Option | Default | Meaning |
|---|---|---|
| `queue` | `"default"` | Workers claim from the queues they are started with |
| `max_attempts` | 3 | Attempt budget including the first run |
| `timeout` | none | A run longer than this counts as an error and is retried |
| `priority` | 0 | Higher is claimed first; overridable per job |
| `backoff` | jittered exponential | A function `attempt -> timedelta`, see [Retries](retries.md#backoff) |

Arguments are validated by a model built from the signature and stored as JSON. Values come back with their types: nested pydantic models, dataclasses, `datetime`, `UUID` and `Decimal` arrive in the task as those types.

A synchronous function also works: it runs in a thread.

!!! warning "Sync tasks and `timeout`"
    A timeout does not stop a thread. The run counts as an error, a retry can start while the old thread still works, and the thread no longer counts against `slots`. Registering a sync task with `timeout` logs a warning on logger `akadze.app`. Give sync tasks their own deadline.

## Split tasks into modules

Each module exposes `register(app)` and attaches tasks there. The app includes modules by name; there is no package walk and no import for side effects.

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

## Bind dependencies

When dependencies exist before the app, declare a plain callable and register a closure later. `app.register(fn, *, name, ...)` takes the same options as `@app.task` and returns the `Task`.

```python
# tasks/hello.py: no app, no decorator
async def hello(name: str, *, runner: Runner) -> str:
    return await runner.greet(name)


def create_app(deps: Deps) -> Akadze:
    app = Akadze(database_url="postgresql://akadze:akadze@localhost:5432/akadze")
    app.include("tasks.ops")

    async def _hello(name: str) -> str:
        return await hello(name, runner=deps.runner)

    app.register(_hello, name="hello")
    return app
```

There is no global app and no `shared_task`.

## Hooks

`app.hooks` holds four lists of callables. Each may be sync or async.

| Hook | Called with | When |
|---|---|---|
| `on_enqueue` | `(connection, job)` | in the enqueue transaction, once per job |
| `before_run` | `(job)` | before the task body |
| `after_run` | `(job)` | after the body returns, inside its timeout; an error here fails the run |
| `on_transition` | `(connection, job, from_state, to_state)` | in the transaction that changes the state |

```python
async def count(connection, job, from_state, to_state):
    outcomes.labels(job.task, to_state).inc()

app.hooks.on_transition.append(count)
```

Do not log task arguments or results from hooks.
