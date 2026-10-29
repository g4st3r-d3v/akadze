# Ретраи, отмена и завершение

## Что тратит попытку

`max_attempts` — бюджет попыток, включая первый запуск (по умолчанию 3). Когда он кончается, задание завершается в `failed`.

| Исход запуска | Попытка тратится | Дальше |
|---|---|---|
| вернула значение, сериализуемое в JSON (или ничего) | — | `succeeded` |
| вернула значение, которое не JSON (`datetime`, модель) | да | ретрай после backoff (`ResultNotJson`) |
| бросила любое исключение | да | ретрай после backoff |
| превысила `timeout` | да | ретрай после backoff |
| `raise Retry(delay)` | да | ретрай через `delay` |
| `raise Snooze(delay)` | нет | снова запуск через `delay` |
| `raise Fail("reason")` | да | сразу `failed` |
| `raise Cancel()` | — | сразу `cancelled` |
| воркер пропал, задание подобрал другой | да | ретрай после backoff |
| штатная остановка воркера | нет | обратно в `queued` |

`Snooze` нужен для опроса: «ещё не готово, посмотри позже».

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

## Backoff { #backoff }

Задержка по умолчанию — `2^(attempt-1)` секунд, не больше 300, с джиттером в `[50 %, 100 %]`, чтобы пачка, упавшая разом, не ударила по вендору снова одной волной. Задания, подобранные у пропавшего воркера, всегда идут по этой кривой, даже если у задачи своя политика.

Своя кривая для задачи:

```python
@app.task("sync", backoff=lambda attempt: timedelta(minutes=attempt))
async def sync() -> None: ...
```

Политика должна быть дешёвой обычной (не async) функцией: она выполняется в event loop воркера. Если политика бросает исключение или возвращает не `timedelta`, используется кривая по умолчанию. Задержки политики ограничены `[0, 365 дней]`. `Retry(delay)` важнее любой политики и не ограничивается. Для аннотации политики есть `akadze.BackoffPolicy`.

## Отмена

```python
from akadze import request_cancel

await request_cancel(connection, job_id)
```

```bash
akadze jobs cancel JOB_ID
```

`request_cancel` помечает задание в `queued` или `running`; для остальных состояний и несуществующих id ничего не делает. CLI в этом случае завершается с кодом 1 и сообщением `job is not active`.

- Задание, отменённое или просроченное (`expires_at`) до старта, переходит в `cancelled` без запуска.
- Выполняющаяся задача не прерывается. С 0.6.0 (0.5.0 на PyPI в этом случае завершает задание в `cancelled`): если тело вернуло значение, задание `succeeded` с результатом — его побочные эффекты уже случились. Если тело бросило что угодно (включая `Retry` и `Snooze`) или вернуло не-JSON, задание `cancelled`, попытка не тратится.

## Вернуть упавшее задание

```python
from akadze import requeue

await requeue(connection, job_id)
```

`requeue` возвращает задание из `failed` в `queued` в вашей транзакции. `attempt`, `errors` и `args` сохраняются, `max_attempts` при необходимости увеличивается. CLI: `akadze jobs requeue JOB_ID`.

## Записать свои данные вместе с исходом { #commit-your-writes-with-the-outcome }

`current().complete_tx()` даёт соединение, транзакция которого заодно переводит задание в `succeeded`. Ваши записи и исход коммитятся вместе или не коммитятся вовсе.

```python
from akadze import current

@app.task("report.build")
async def build(report_id: int) -> None:
    ...
    async with current().complete_tx() as connection:
        await connection.execute(...)  # отметить отчёт готовым
        await notify.using(session=connection).enqueue(report_id=report_id)
```

Так строятся цепочки: следующее задание появится, только если этот шаг успешен.

- Если этот запуск больше не владеет заданием (его подобрал другой воркер), блок бросает исключение и ваши записи откатываются.
- Отмена или истечение срока во время блока его не останавливают: блок, дошедший до конца, коммитит ваши записи и `succeeded`.
- Ошибка внутри блока откатывает записи и тратит попытку (не тратит, если за это время запросили отмену или истёк срок: тогда задание `cancelled`).
