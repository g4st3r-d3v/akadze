# Задачи и приложение

Приложение `Akadze` хранит engine, зарегистрированные задачи и расписания. Воркер загружает его по пути импорта (`модуль:атрибут`).

```python
from datetime import timedelta
from akadze import Akadze

app = Akadze(database_url="postgresql://akadze:akadze@localhost:5432/akadze")
```

| Параметр | По умолчанию | Смысл |
|---|---|---|
| `database_url` или `engine` | — | Нужен один из двух. С `database_url` приложение само создаёт async engine и владеет им; передайте свой `engine`, чтобы делить пул |
| `grace` | 1 минута | Насколько поздно периодический запуск ещё можно поставить в очередь |
| `retention` | 7 дней | Завершённые задания и запуски расписаний старше этого воркер удаляет |

Engine, который создаёт приложение, отключает кэш prepared statements в asyncpg, поэтому перед базой может стоять пул в transaction-режиме, например PgBouncer.

## Объявить задачу

```python
@app.task("mail.send", queue="mail", max_attempts=6, timeout=timedelta(seconds=60), priority=0)
async def send_mail(user_id: int) -> None:
    ...
```

| Параметр | По умолчанию | Смысл |
|---|---|---|
| `queue` | `"default"` | Воркер берёт задания из очередей, с которыми запущен |
| `max_attempts` | 3 | Бюджет попыток, включая первый запуск |
| `timeout` | нет | Запуск дольше этого считается ошибкой и уходит в ретрай |
| `priority` | 0 | Больший забирается раньше; можно переопределить для задания |
| `backoff` | экспонента с джиттером | Функция `attempt -> timedelta`, см. [Ретраи](retries.md#backoff) |

Аргументы проверяются моделью, построенной по сигнатуре, и хранятся как JSON. С 0.6.0 значения возвращаются со своими типами: вложенные pydantic-модели, dataclass, `datetime`, `UUID` и `Decimal` приходят в задачу именно этими типами.

Синхронная функция тоже подходит: она выполняется в потоке.

!!! warning "Синхронные задачи и `timeout`"
    Таймаут не останавливает поток. Запуск считается ошибкой, ретрай может начаться, пока старый поток ещё работает, и этот поток больше не учитывается в `slots`. При регистрации синхронной задачи с `timeout` в логгер `akadze.app` пишется предупреждение. Дайте синхронной задаче собственный дедлайн.

## Разнести задачи по модулям

Каждый модуль объявляет `register(app)` и регистрирует задачи там. Приложение подключает модули по имени: без обхода пакета и без импорта ради побочных эффектов.

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

## Передать зависимости { #bind-dependencies }

Если зависимости создаются раньше приложения, объявите обычную функцию и зарегистрируйте замыкание позже. `app.register(fn, *, name, ...)` принимает те же параметры, что `@app.task`, и возвращает `Task`.

```python
# tasks/hello.py: без app и без декоратора
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

Глобального приложения и `shared_task` нет.

## Хуки { #hooks }

`app.hooks` — четыре списка функций. Каждая может быть синхронной или async.

| Хук | Аргументы | Когда |
|---|---|---|
| `on_enqueue` | `(connection, job)` | в транзакции постановки, по разу на задание |
| `before_run` | `(job)` | перед телом задачи |
| `after_run` | `(job)` | после возврата из тела, внутри его таймаута; ошибка здесь считается неудачным запуском: попытка тратится, и при ретрае тело выполняется заново |
| `on_transition` | `(connection, job, from_state, to_state)` | в транзакции, меняющей состояние |

```python
async def count(connection, job, from_state, to_state):
    outcomes.labels(job.task, to_state).inc()

app.hooks.on_transition.append(count)
```

Не пишите аргументы и результаты задач в логи из хуков.
