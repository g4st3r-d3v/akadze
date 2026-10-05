---
status: research
updated: 2026-10-05
tags:
  - akadze
  - research
  - celery
---

# Celery — что берём и что обходим

Модель Celery стоит сохранить: задача, очередь, воркер, Beat по расписанию. Почти все боли Celery растут из двух корней. Первый: семантика брокера протекает в поведение приложения. Второй: важное состояние живёт вне базы приложения.

Очередь, где задача лежит строкой в Postgres, убирает оба корня устройством. Взамен она приносит свои риски. См. [[04 — Postgres как брокер — риски и правила]].

## Где Celery сейчас (октябрь 2026)

- Стабильная ветка: 5.6 (5.6.3, март 2026). Нативного `async def` в задачах нет. Мейнтейнер обещал asyncio в 5.7 или 6.0. Работа упирается в финансирование: [issue #6552](https://github.com/celery/celery/issues/6552).
- В kombu 5.7 появился транспорт PGMQ: Postgres как брокер с семантикой SQS. `visibility_timeout` по умолчанию 1800 с ([docs](https://docs.celeryq.dev/en/latest/getting-started/backends-and-brokers/pgmq.html)). На сентябрь 2026 это alpha. Это всё ещё Celery: visibility timeout, ETA в памяти воркера, нет постановки задачи в транзакции приложения.

## Что берём

| Возможность Celery | Как это выглядит в akadze |
|---|---|
| `@app.task` и `.delay()` | `@app.task` и `await task.enqueue(...)` с проверкой типов аргументов |
| Retry: `autoretry_for`, `retry_backoff`, jitter, `max_retries` | Политика retry на задаче: какие исключения, экспонента, jitter, `max_attempts` |
| ETA и countdown | `run_at` и `delay` лежат колонкой в таблице, не сообщение в памяти воркера |
| Несколько очередей, приоритеты | `queue` и `priority` на задаче. Воркер слушает список очередей. У каждой свой лимит слотов |
| Soft и hard time limits | `timeout` на задачу (отмена coroutine) |
| Beat: crontab и интервалы | `@app.periodic(cron=...)` и `every=...` без единственного планировщика |
| Signals | Хуки жизненного цикла: постановка, старт, финиш, переход состояния |
| `expires` | `expires_at`: задача, не начатая вовремя, отменяется |
| Flower | Сначала SQL и CLI (`akadze jobs ...`). Web UI позже |
| Result backend | Необязательный маленький `result` (jsonb). Большие результаты отдаём ссылкой, как S3 в ml-core |

Canvas (`chain`, `group`, `chord`) в v0 не берём. Это самая хрупкая часть Celery: chord требует result backend и счётчиков. В ml-core цепочка шагов устроена проще: следующий шаг вставляется в той же транзакции, что и завершение текущего.

## Боли Celery: механизм и ответ akadze

| # | Боль | Механизм | Источник | Ответ akadze |
|---|---|---|---|---|
| 1 | Длинные или отложенные задачи выполняются повторно | Redis и SQS: сообщение без ack за `visibility_timeout` (по умолчанию 1 ч) уходит другому воркеру. ETA дольше таймаута выполняется снова и снова | [Redis caveats](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html) | Брокерского таймаута нет. Задачей владеет воркер, пока шлёт heartbeat. Длительность не ограничена |
| 2 | Длинные задачи с `acks_late` рвут канал RabbitMQ | `consumer_timeout` по умолчанию 30 мин → `PRECONDITION_FAILED` | [RabbitMQ](https://github.com/rabbitmq/rabbitmq-server/blob/main/consumer_timeouts.md) | Не применимо: брокера нет |
| 3 | Задача теряется при падении воркера | `task_acks_late` по умолчанию выключен: сообщение подтверждается до выполнения. Это at-most-once | [Configuration](https://docs.celeryq.dev/en/stable/userguide/configuration.html) | At-least-once по умолчанию: claim пишется в базу. Задачу умершего воркера вернёт rescuer |
| 4 | Короткие задачи стоят за длинными | Prefetch: воркер заранее резервирует `worker_prefetch_multiplier` × слоты | [Optimizing](https://docs.celeryq.dev/en/latest/userguide/optimizing.html) | Prefetch нет. Воркер берёт ровно столько задач, сколько у него свободных слотов |
| 5 | Отложенные задачи раздувают память воркера | ETA и countdown сразу забираются воркером и ждут в памяти | [Calling Tasks](https://docs.celeryq.dev/en/main/userguide/calling.html) | Отложенная задача лежит в таблице до `run_at` |
| 6 | Beat: единственная точка отказа и источник дублей | Нужен ровно один scheduler на расписание. Иначе дубли. Состояние лежит в локальном `shelve`-файле | [Periodic Tasks](https://docs.celeryq.dev/en/latest/userguide/periodic-tasks.html) | Расписание исполняет любой воркер. Дубль отсекает уникальный индекс `(schedule, fire_at)` |
| 7 | Rate limit не глобальный | Лимит на экземпляр воркера, не на кластер | [Tasks](https://docs.celeryq.dev/en/stable/userguide/tasks.html) | v1: лимиты по ключу (например, по вендору) на весь кластер через Postgres |
| 8 | Revoke забывается после рестарта | Revoke работает как broadcast. Список живёт в памяти воркеров и переживает рестарт только с `--statedb` | [Workers Guide](https://docs.celeryq.dev/en/stable/userguide/workers.html) | Отмена хранится состоянием строки. Работающая задача получает cooperative cancel |
| 9 | Задача стартует раньше коммита или теряется при откате | Dual-write: запись в базу и публикация в брокер идут в две системы | — | Постановка идёт через `INSERT` в транзакции вызывающего кода. Откат убирает и задачу |
| 10 | «Где моя задача?» | Состояние в сообщениях брокера. История только в result backend | — | Всё лежит строками. SQL, CLI и метрики из одной таблицы |
| 11 | Нет asyncio | Пулы prefork, threads, gevent. `async def` только обёртками и плагинами | [discussion #9049](https://github.com/celery/celery/discussions/9049) | Воркер на asyncio. Sync-задачи идут в пул потоков |
| 12 | Тесты проверяют не то | `task_always_eager` эмулирует воркер и расходится с реальностью | [Testing](https://docs.celeryq.dev/en/stable/userguide/testing.html) | Тестовые хелперы на настоящем Postgres: `drain()`, `assert_enqueued()` |
| 13 | Shutdown теряет задачи | Cold shutdown. Soft shutdown появился в 5.5 и выключен по умолчанию | [v5.5.0](https://github.com/celery/celery/releases/tag/v5.5.0) | Graceful drain с таймаутом. Брошенные задачи вернёт rescuer по heartbeat |

## Корни: почему это не лечится настройками

1. Брокер и база: две системы. Отсюда dual-write, задачи на незакоммиченных данных и потеря задач при откате.
2. Состояние вне SQL. Расписание лежит в `shelve`, revoke и ETA в памяти воркера, статус в сообщении.
3. Надёжность задаёт ack-модель брокера. По умолчанию это at-most-once. `acks_late` даёт дубли и упирается в таймауты брокера.
4. Prefetch и ETA в памяти воркера ломают справедливость и предсказуемость.
5. Синхронное ядро. Для async-сервисов вроде ml-core Celery остаётся чужим.

## Чего Postgres не исправит

- Доставка остаётся at-least-once. Идемпотентность внешних вызовов на приложении. В ml-core это `provider_calls`.
- Пропускная способность ниже, чем у Redis и RabbitMQ. Для ml-core (порядок 10 RPS) это не ограничение.
- Fan-out и pub/sub не работа очереди. События к BE остаются в Kafka.

Связано: [[03 — Ландшафт Postgres-очередей 2026]], [[06 — Дизайн akadze v0]].
