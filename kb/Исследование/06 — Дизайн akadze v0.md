---
status: draft
updated: 2026-10-05
tags:
  - akadze
  - design
---

# Дизайн akadze v0

Черновик дизайна с зафиксированными решениями владельца. Обоснования: [[02 — Celery — что берём и что обходим]], [[04 — Postgres как брокер — риски и правила]], [[05 — ml-core как первый потребитель]].

## Что такое akadze v0

Библиотека для asyncio-приложений на Postgres: задачи, воркер, periodic и обслуживание очереди. Без брокера, без отдельного планировщика, без UI. Один пакет и одна схема `akadze` в базе приложения.

## Модель

- **Задача (task)**. Функция с именем, зарегистрированная в `App`. Имя задаётся явно и не зависит от пути модуля. Переезд файла не ломает задачи, которые уже в очереди.
- **Job**. Строка в `akadze.jobs`: какую задачу запустить, с какими аргументами, когда и сколько раз уже пробовали.
- **Запуск (run)**. Одно выполнение job воркером. Номер запуска `run_count` растёт при каждом claim и служит fencing-токеном.
- **Попытка (attempt)**. Запуск, который закончился ошибкой, таймаутом или падением воркера. Snooze и штатный возврат при shutdown попыткой не считаются. С этим счётчиком сравнивается `max_attempts`.
- **Воркер**. Процесс с N слотами на каждую очередь. Регистрирует себя в `akadze.workers` и шлёт heartbeat.
- **Расписание (periodic)**. Задача с cron или интервалом. Каждый запуск: обычный job, созданный без дублей.

### Состояния job

```text
queued ──claim──▶ running ──ok──────────────────────────────▶ succeeded
  ▲  │              ├─ Retry или ошибка, попытки есть ──▶ queued  (run_at = now + backoff, attempt + 1)
  │  │              ├─ Snooze ──────────────────────────▶ queued  (attempt не меняется)
  │  │              ├─ Fail или попытки кончились ──────▶ failed
  │  │              └─ отмена ──────────────────────────▶ cancelled
  │  └─ отмена или истёк expires_at ──────────────────────▶ cancelled
  └──── rescuer: воркер умер ─── running ▶ queued (attempt + 1) или failed
```

Пять состояний: `queued`, `running`, `succeeded`, `failed`, `cancelled`. Отложенная задача: `queued` с `run_at` в будущем.

## Схема (набросок)

```sql
create schema akadze;

create table akadze.jobs (
	id                  bigint generated always as identity primary key,
	task                text not null,
	queue               text not null default 'default',
	priority            smallint not null default 0,
	state               text not null default 'queued',
	args                jsonb not null default '{}',
	meta                jsonb not null default '{}',  -- traceparent, теги
	run_count           int not null default 0,       -- fencing-токен
	attempt             int not null default 0,       -- неудачные запуски
	max_attempts        int not null default 3,
	snoozes             int not null default 0,
	run_at              timestamptz not null default now(),
	expires_at          timestamptz,
	unique_key          text,
	worker_id           uuid,
	cancel_requested_at timestamptz,
	errors              jsonb not null default '[]',  -- последние N ошибок, через redaction
	result              jsonb,                        -- маленький, необязательный
	created_at          timestamptz not null default now(),
	started_at          timestamptz,
	finished_at         timestamptz
);

-- claim: только queued, маленький частичный индекс
create index on akadze.jobs (queue, priority desc, run_at, id) where state = 'queued';
-- rescuer: что держит каждый воркер
create index on akadze.jobs (worker_id) where state = 'running';
-- дедупликация, пока job жив
create unique index on akadze.jobs (unique_key)
	where unique_key is not null and state in ('queued', 'running');
-- pruner
create index on akadze.jobs (finished_at) where state in ('succeeded', 'failed', 'cancelled');

create table akadze.workers (
	id           uuid primary key,
	hostname     text not null,
	pid          int not null,
	queues       text[] not null,
	started_at   timestamptz not null default now(),
	heartbeat_at timestamptz not null default now()
);

create table akadze.periodic_runs (
	name    text not null,
	fire_at timestamptz not null,
	job_id  bigint,
	primary key (name, fire_at)
);
```

## API (набросок)

```python
from datetime import timedelta

from akadze import Akadze, Snooze

app = Akadze(engine=engine)  # готовый SQLAlchemy AsyncEngine или database_url=...


@app.task("stt.poll", queue="vendors", max_attempts=5, timeout=timedelta(minutes=2))
async def poll_transcription(job_id: int, external_id: str) -> None:
	status = await vendor.status(external_id)
	if status.pending:
		raise Snooze(timedelta(seconds=15))  # попытка не тратится
	await save_transcript(job_id, status)


@app.periodic("purge-expired-assets", cron="*/10 * * * *")
async def purge_expired_assets() -> None: ...


# постановка в транзакции приложения: откат убирает и задачу
async with session.begin():
	session.add(job_row)
	await session.flush()
	await poll_transcription.using(session=session, delay=timedelta(seconds=10)).enqueue(
		job_id=job_row.id,
		external_id=external_id,
	)
```

Решения в API:

- `enqueue(**kwargs)` типизируется по сигнатуре функции через `ParamSpec`. mypy видит опечатку в имени аргумента.
- Опции постановки (`session`, `delay`, `run_at`, `priority`, `unique_key`) передаются через `using(...)`. Смешать их с типизированными kwargs нельзя. Так же устроены `configure()` в Procrastinate и `apply_async` в Celery.
- Аргументы проверяются и сериализуются в JSON при постановке и проверяются снова при запуске. Это ловит дрейф схемы аргументов между деплоями.
- Ход выполнения задача меняет исключениями: `Retry(delay)`, `Snooze(delay)`, `Fail(reason)`, `Cancel(reason)`. Исключение можно бросить из вложенного вызова, а не только из тела задачи.

## Воркер

1. При старте воркер пишет себя в `akadze.workers`.
2. Claim: если есть свободные слоты, один запрос `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT :free) RETURNING ...`. Он ставит `state = 'running'`, `run_count = run_count + 1`, `worker_id` и `started_at = now()`. Транзакция сразу закрывается.
3. Каждая задача идёт отдельной asyncio task с `timeout`. Sync-функции выполняются в пуле потоков. Поток убить нельзя. Это ограничение пишем в документацию.
4. Завершение идёт одним запросом `UPDATE ... WHERE id = :id AND run_count = :run AND state = 'running'`. Если обновилось ноль строк, этот запуск уже забрали: результат отбрасываем и пишем в лог.
5. Heartbeat процесса раз в `heartbeat_interval` (5 с). Если heartbeat не проходит дольше `heartbeat_ttl` (30 с), воркер сам отменяет свои задачи: rescuer вот-вот отдаст их другому.
6. Опрос: если пачка пришла полной, сразу следующий claim. Если пустой, пауза `poll_interval` (1 с) с jitter.
7. Shutdown: перестаём брать задачи, ждём `shutdown_timeout`, остальное отменяем и возвращаем в `queued`. Попытка не тратится.

### Транзакционное завершение

Приём из River (`JobCompleteTx`). Задача берёт `ctx` и пишет в базу в той же транзакции, что и завершение job: `async with ctx.complete_tx() as session: ...`. Если запуск уже забрали, fencing обновит ноль строк, и транзакция откатится вместе с изменениями задачи. Для изменений в базе это даёт exactly-once. Внешние вызовы остаются at-least-once.

## Periodic

- Работает в каждом воркере. Отдельного процесса нет.
- Раз в секунду для каждого расписания считаем последний `fire_at <= now()`. В одной транзакции делаем `INSERT INTO akadze.periodic_runs ... ON CONFLICT DO NOTHING`. Если строка вставилась, создаём job. Сколько бы ни было воркеров, на один `fire_at` получится один job. Это приём Solid Queue и GoodJob.
- Пропущенные запуски догоняются в окне `backfill`. По умолчанию окно 0: ставим только последний запуск, если он не старше grace-периода.
- `overlap=False` не даёт создать новый запуск, пока предыдущий в `queued` или `running`. Реализуется через `unique_key = "periodic:<name>"`.

## Обслуживание

Встроенные periodic-задачи:

- **Rescuer** возвращает в `queued` задачи `running` у воркеров с протухшим heartbeat. Если попытки кончились, переводит в `failed`. Удаляет записи мёртвых воркеров.
- **Pruner** удаляет завершённые jobs старше срока хранения пачками по 1 000 и чистит старые `periodic_runs`.

Обе задачи идемпотентны и берут строки через `SKIP LOCKED`. Лидер не нужен.

## Хуки

- `on_enqueue`, `before_run`, `after_run`, `on_transition(conn, job, from_state, to_state)`.
- `on_transition` получает соединение транзакции, в которой меняется состояние. Так ml-core пишет строку outbox для Kafka атомарно с переходом, а отдельный relay её публикует.

## Наблюдаемость

- Метрики (extra `akadze[prometheus]`): `akadze_jobs_total{task,queue,outcome}`, `akadze_job_duration_seconds`, `akadze_queue_depth{queue,state}`, `akadze_queue_lag_seconds`, `akadze_worker_busy_slots`. Главный SLO: `akadze_queue_lag_seconds`, сколько ждёт самый старый готовый job.
- Трейсинг (extra `akadze[otel]`): `traceparent` пишется в `meta` при постановке. Span выполнения связан со span постановки.
- Логи структурные. Аргументы и результаты задач в логи не пишем.

## Тестирование

- Только настоящий Postgres (docker в CI). In-memory фейк базы не делаем. Урок ml-core: `fake_session.py`: 800 строк, которые не моделируют `SKIP LOCKED`.
- Обязательные сценарии:
	- N воркеров конкурентно забирают задачи без дублей
	- kill -9 воркера → rescuer возвращает задачи
	- fencing отбрасывает завершение от зомби-воркера
	- snooze не тратит попытки
	- три воркера с одним расписанием создают ровно один job на `fire_at`
- Для пользователей есть `akadze.testing`: `drain(app)` выполняет всё готовое, `assert_enqueued(task, **args)` проверяет постановку.

## Объём версий

| Версия | Входит |
|---|---|
| v0 | Схема и миграции. Постановка в транзакции. Воркер со слотами, fencing, heartbeat, retry с backoff, snooze, timeout и отменой. Periodic. Rescuer и pruner. `unique_key`. Хуки. CLI `migrate`, `worker`, `jobs`. Метрики через хуки. Документация |
| v1 | Лимиты по ключу на весь кластер (конкурентность и rate, например по вендору). Сахар для цепочек. NOTIFY как опция. Read-only web UI. Быстрый путь для asyncpg |
| v2 | По необходимости: durable-шаги с чекпоинтами, как в DBOS, для долгих пайплайнов. Секционированное хранилище для высокой нагрузки |

Не делаем: совместимость с протоколом Celery, другие брокеры, multi-tenant control plane.

## Решения (2026-10-05)

1. Строим akadze. ml-core: первый потребитель. План Б: Oban-py.
2. Слой базы в v0: SQLAlchemy 2 async Core.
3. В v0 только задачи: постановка, воркер, retry, snooze, отмена, periodic, обслуживание очереди. Цепочки шагов и durable workflows позже.

## Открытые вопросы

1. Минимальная версия Python: предлагаю 3.12+. Сейчас в `pyproject.toml` стоит 3.13.
2. Приоритет: большее число раньше (как в ml-core) или меньшее (как в Oban и Graphile Worker).
3. Хранить ли `result` в v0 или ограничиться ссылками и хуками.
4. Откуда ml-core ставит первую задачу пайплайна: из API-транзакции создания job или из Kafka consumer. Должны работать оба пути.
