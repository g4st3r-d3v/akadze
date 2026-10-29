# Переезд с Celery

Как перенести приложение с Celery на akadze. Брокер — Postgres приложения, поэтому переезд — это `akadze migrate` при деплое, перенос объявлений задач и мест вызова и замена `celery beat` расписаниями в самом воркере.

## 1. Объявление задач

Задачи Celery — синхронные функции, привязанные к глобальному приложению. Задачи akadze обычно `async def`; синхронная функция тоже подходит и выполняется в потоке. Аргументы должны сериализоваться в JSON: они проверяются моделью по сигнатуре и хранятся в `akadze.jobs.args`.

```python
# Celery
@shared_task(bind=True, max_retries=5, queue="mail", time_limit=60)
def send_mail(self, user_id: int) -> None: ...

# akadze
@app.task("mail.send", queue="mail", max_attempts=6, timeout=timedelta(seconds=60))
async def send_mail(user_id: int) -> None: ...
```

`max_attempts` — бюджет попыток, включая первый запуск, поэтому `max_retries=5` в Celery — это `max_attempts=6`. По умолчанию 3: задаче Celery без ретраев, которую нельзя выполнить дважды (списание, SMS), нужен `max_attempts=1`. Ошибка, `Retry`, таймаут и потеря воркера (по проверке heartbeat другим воркером) тратят попытку; `Snooze` и штатная остановка — нет.

`timeout` — не жёсткий `time_limit`. Когда он срабатывает, запуск считается ошибкой и уходит в ретрай с backoff, а не в `failed`. Синхронная задача после таймаута продолжает работать в своём потоке, поэтому ретрай может пойти параллельно, а поток больше не учитывается в `slots` (повторные таймауты могут забить пул потоков по умолчанию); дайте синхронным задачам собственный дедлайн. `shared_task` нет: модули объявляют `register(app)`, приложение вызывает `include`, или фабрика регистрирует обычные функции через `app.register` (см. [Задачи и приложение](tasks.md#bind-dependencies)).

## 2. Постановка в очередь

| Celery | akadze |
|---|---|
| `send_mail.delay(7)` | `await send_mail.using(session=connection).enqueue(user_id=7)` |
| `apply_async(countdown=30)` | `using(..., delay=timedelta(seconds=30))` |
| `apply_async(eta=dt)` | `using(..., run_at=dt)` (с часовым поясом) |
| `apply_async(expires=dt)` | `using(..., expires_at=dt)` |
| `apply_async(priority=…)` | `using(..., priority=…)` — больший забирается раньше, диапазон smallint; Celery на Redis считает 0 наивысшим, так что значения нужно инвертировать |
| `apply_async(task_id=…)` для дедупликации | `using(..., unique_key=…)` → `DuplicateJob`, пока задание активно |
| `transaction.on_commit(lambda: task.delay())` | не нужно: `enqueue` выполняется в транзакции вызывающего кода |
| `group(task.s(x) for x in xs).apply_async()` (fan-out без результата) | `await task.using(session=connection).enqueue_many({"x": x} for x in xs)` |

Аргументы `enqueue(...)` передаются только по имени. Параметры постановки никогда не попадают в аргументы задачи.

## 3. Ретраи и управление выполнением

| Celery | akadze |
|---|---|
| `raise self.retry(countdown=10)` | `raise Retry(timedelta(seconds=10))` — тратит попытку |
| `autoretry_for=(Exception,)`, `retry_backoff`, `retry_jitter` | по умолчанию любое исключение уходит в ретрай с экспоненциальным backoff и джиттером до `max_attempts`; своя кривая — `@app.task(..., backoff=fn)` |
| ретрай без траты бюджета (опрос) | `raise Snooze(timedelta(...))` — попытку не тратит |
| `raise Reject(requeue=False)` / сдаться | `raise Fail("reason")` |
| `revoke(task_id)` | `request_cancel(session, job_id)` или `akadze jobs cancel JOB_ID`; внутри задачи `raise Cancel()` |
| `acks_late` + visibility timeout | аренда с heartbeat; другой воркер возвращает задания потерянного в очередь и тратит попытку (на последней задание `failed`) |

`request_cancel` не прерывает выполняющуюся задачу. Задание, для которого отмену запросили или `expires_at` прошёл до запуска, отменяется без запуска. Во время запуска решает тело: если оно вернуло значение, задание `succeeded` с результатом (побочные эффекты уже случились); если бросило что угодно, включая `Retry` и `Snooze`, или вернуло не-JSON, задание `cancelled`, попытка не тратится. После коммита блока `complete_tx` задание `succeeded`, что бы тело ни делало дальше.

Доставка at least once: задание может выполниться снова, если пропал его воркер, поэтому задачи должны быть идемпотентными, как и с `acks_late`. Закладывайте `max_attempts` и на падения.

## 4. Цепочки и результаты

Canvas (`chain`, `group`, `chord`), result backend и `.get()` нет. Чтобы следующий шаг выполнился только при успехе этого, поставьте его на соединении `complete_tx`. Исход пишите в свои таблицы в той же транзакции, а не читайте его потом из очереди.

```python
from akadze import current

@app.task("report.build")
async def build(report_id: int) -> None:
    ...
    async with current().complete_tx() as connection:
        await connection.execute(...)  # отметить отчёт готовым
        await notify.using(session=connection).enqueue(report_id=report_id)
```

## 5. Лимиты и маршрутизация

| Celery | akadze |
|---|---|
| `rate_limit="10/m"` | `set_limit(session, "vendor", max_starts=10, window=timedelta(minutes=1))` + `using(..., limit_key="vendor")` — на весь кластер |
| отдельный пул воркеров под вендора | `set_limit(session, "vendor", max_running=N)` + `using(..., limit_key="vendor")` |
| `-Q mail` / маршрутизация | `@app.task(..., queue="mail")` и `akadze worker app:app --queue mail` |
| `--concurrency=8` | `akadze worker app:app --slots 8` — слоты на каждую очередь: с тремя `--queue` воркер выполняет до 24 заданий |
| остановить очередь везде | `akadze queues pause mail` / `resume` — на весь кластер, а не на один воркер |
| `celery control cancel_consumer` на одном хосте | остановить этот воркер (Ctrl-C / SIGTERM): он не берёт новые задания и даёт выполняющимся завершиться |

## 6. Beat

`celery beat` и его расписание переезжают в воркер: `@app.periodic("name", cron="0 * * * *")` (с `timezone="Europe/Berlin"` вместо `beat_cron_timezone` / `CELERY_TIMEZONE`) или `every=timedelta(...)`, либо `app.add_schedules([...])` из конфига приложения. Расписание выполняет каждый воркер; наступивший слот ставится в очередь один раз. Отдельного процесса beat нет (`akadze beat` — заглушка).

## 7. Эксплуатация

Экраны Flower заменяют CLI и запросы: `akadze jobs list`, `akadze workers list`, `akadze limits list`, `akadze jobs requeue JOB_ID` и `akadze metrics` для sidecar Prometheus. Сигналы: `task_prerun` → `app.hooks.before_run`; `task_success` → `app.hooks.after_run`, который вызывается, только когда задача вернула значение, и внутри её таймаута (ошибка в нём тратит попытку, и при ретрае тело выполняется заново); `task_postrun`, `task_failure` и `task_retry` → `app.hooks.on_transition`, который видит каждую смену состояния в транзакции, где она происходит.

## Чего нет и почему

- **Брокеры Redis/RabbitMQ** — брокер Postgres, чтобы постановка шла в транзакции приложения.
- **Canvas и `.get()`** — chord держит слоты воркера, пока ждёт; следующий шаг через `complete_tx` закрывает цепочки без этого.
- **Result backend** — результатам место в таблицах приложения; `result` в строке задания необязателен.
- **HTTP / веб-интерфейс** — его может построить другой пакет на запросах выше.
