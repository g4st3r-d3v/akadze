---
status: research
updated: 2026-10-05
tags:
  - akadze
  - research
  - landscape
---

# Ландшафт Postgres-очередей 2026

Готовые Postgres-очереди для Python уже есть. Две близки к нужному: Oban-py по семантике и PgQueuer по инфраструктуре. Ни одна не закрывает разом всё, что нужно ml-core в OSS:

- постановку задачи из `AsyncSession` на asyncpg
- snooze, который не тратит попытку
- lease с защитой от зомби-воркера
- лимиты по вендору на весь кластер
- миграции, которые встают в Alembic

Эта щель и оправдывает akadze. План Б, если akadze застрянет: Oban-py. Для него ml-core переводит SQLAlchemy с asyncpg на psycopg3.

Данные по Python-библиотекам собраны 2026-10-05 из GitHub API, PyPI и документации в репозиториях. Звёзды приблизительные. «Tx-enqueue» значит: задачу можно поставить в транзакции самого приложения.

## Python

| Библиотека | Лицензия | Релиз | ★ | Драйвер | Tx-enqueue | Liveness | Cron без дублей | Snooze без траты попытки | Глобальные лимиты | Workflows | Статус |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Oban-py | Apache-2.0 (+Pro) | 0.6.6 · 2026-09 | 0.3k | psycopg3 | да, `conn=AsyncSession`, только psycopg3 | rescue по таймауту | да, через лидера | да | только Pro | только Pro | beta, с января 2026 |
| PgQueuer | MIT | 1.5.0 · 2026-10 | 1.5k | asyncpg, psycopg3 | да, через сырое соединение | heartbeat задачи + перехват | да | нет, retry тратит attempt | concurrency на entrypoint | нет | активна |
| Procrastinate | MIT | 3.10 · 2026-09 | 1.4k | psycopg3 (asyncpg нет) | да, с 3.8 | heartbeat воркера, возврат задач вручную | да | нет | нет (есть `lock`, `queueing_lock`) | нет | зрелая, ищет мейнтейнеров |
| DBOS | MIT (+Conductor) | 3.2 · 2026-09 | 1.6k | psycopg3, SQLAlchemy | да, SQL-функцией | по executor; кластер требует платный Conductor | да, с backfill | `DBOS.sleep` | да, с партициями | durable workflows | активна |
| awa | MIT / Apache-2.0 | 0.x · 2026 | 0.02k | Rust-ядро + PyO3 | да, `awa.bridge` | heartbeat процесса + fencing `run_lease` | да | да | ? | callbacks | очень молодая |
| Chancy | MIT | 0.25 · 2025-10 | 0.3k | psycopg3 | да, `push_ex(cursor)` | heartbeat + Recovery | да, через лидера | нет | rate и concurrency | DAG | заморожена |
| SAQ | MIT | 0.26 · 2026-05 | 0.9k | psycopg3 | нет | timeout + sweeper | ключ `cron:<fn>` | нет | на воркер | нет | активна |
| Absurd | Apache-2.0 | 0.5 · 2026-08 | 2.5k | psycopg3 | ? | lease + heartbeat | нет | `sleep_for` | нет | durable steps | alpha |
| TaskIQ + taskiq-postgres | MIT | 0.13 / 0.9 | 2.3k / 0.03k | asyncpg и др. | нет | нет | нет, «only one instance of the scheduler» | нет | нет | нет | слабый PG-брокер |
| Celery + PGMQ | BSD-3 | kombu 5.7 alpha | 28.9k | psycopg3 | нет | visibility timeout | нет | нет | нет | canvas | alpha |
| Hatchet | MIT (+Cloud) | 2026-10 | 8.1k | свой Go-engine, gRPC | нет | heartbeats | да | да | да, по CEL-ключам | DAG + durable | отдельный сервис |
| Django tasks + tasks-db | BSD-3 | Django 6.1 | — | Django ORM | `on_commit` | нет | нет | нет | нет | нет | только Django |
| pgmq | PostgreSQL / Apache-2.0 | 1.13 · 2026-09 | 5.3k | любой | да | visibility timeout | нет | `set_vt` | нет | нет | кирпич, не очередь задач |

### Нюансы

**Oban-py.** Порт Oban с Elixir от его авторов. Вышел 2026-01-21 ([анонс](https://oban.pro/articles/introducing-oban-python)). Есть `Snooze` без траты попытки, `Cancel`, хранение результата, cron через лидера и постановка из `AsyncSession`, но только на psycopg3. Глобальный rate limit и workflows в платном Pro. Unique jobs, по данным документации oban-py, тоже в Pro. Проверить перед решением. Lifeline в OSS возвращает задачи по таймауту и может задублировать задачу, которая ещё работает ([Oban.Lifeline](https://oban.hexdocs.pm/Oban.Lifeline.html)).

**PgQueuer.** Нативный asyncpg, heartbeat в строке задачи и автоматический перехват зависшей задачи, `dedupe_key`, глобальный concurrency на entrypoint, OTel и Prometheus. Retry с задержкой тратит attempt, поэтому честный snooze для поллинга вендоров не сделать. Payload: bytes. Результатов нет. `pgq sql upgrade` выдаёт SQL для Alembic.

**Procrastinate.** Самая зрелая из чисто Python-очередей. Транзакционный defer появился в 3.8.0 (апрель 2026), только на psycopg. Зависшие задачи нужно возвращать своей periodic-задачей. В README объявление о поиске мейнтейнеров.

**DBOS.** Другая модель: durable workflows, а не очередь задач. Пайплайн «submit → poll + sleep → save» ложится на неё напрямую. Работу упавшего узла в кластере восстанавливает Conductor. Dev-лицензия self-hosted Conductor ограничена одним executor на приложение ([docs](https://docs.dbos.dev/production/hosting-conductor-with-kubernetes)).

**awa.** Rust-ядро, Python-воркеры через PyO3: транзакционная постановка, snooze, cron, DLQ, UI. Дизайн интересен: heartbeat на процесс, fencing-счётчик `run_lease`, сегментированное хранилище, чтобы мёртвые строки не лежали на горячем пути ([architecture](https://github.com/hardbyte/awa/blob/main/docs/architecture.md)). Проект молодой (около 20★). Ядро не на Python, вкладываться сложнее.

**Celery + PGMQ.** Убирает Redis и RabbitMQ, но не корни болей Celery: visibility timeout, ETA в памяти, нет транзакционной постановки, нет asyncio.

## Другие экосистемы: что стоит украсть

**River (Go).** `InsertTx` ставит задачу в транзакции приложения. `JobCompleteTx` завершает задачу в той же транзакции, что и её работу: «a successful commit guarantees that the job will never rerun» ([docs](https://riverqueue.com/docs/transactional-job-completion)). Snooze не считается ошибкой. Periodic исполняет лидер. Расписание у лидера в памяти. River сам советует добавлять unique jobs, чтобы при смене лидера не было пропусков и дублей ([periodic](https://riverqueue.com/docs/periodic-jobs)).

**Oban (Elixir).** Плагины обслуживания: Pruner чистит завершённые задачи, Lifeline спасает зависшие, Cron ставит периодические. Лидер выбирается через таблицу `oban_peers` с проверкой раз в 30 с. Без лидера плагины не работают нигде. Классическая ловушка: лидером стала web-нода ([clustering](https://github.com/oban-bg/oban/blob/main/guides/learning/clustering.md)).

**Solid Queue (Rails 8 по умолчанию).** Отдельные таблицы ready, scheduled и claimed executions держат горячий путь маленьким. Heartbeat идёт от процесса. Задачи процесса с протухшим heartbeat (порог по умолчанию 5 мин) помечаются failed. Для recurring-задач в той же транзакции, что и задача, создаётся строка в `recurring_executions` с уникальным индексом `(task_key, run_at)`. Поэтому планировщиков может быть несколько ([README](https://github.com/rails/solid_queue/blob/main/README.md)).

**GoodJob (Ruby).** Тот же приём для cron: уникальный индекс `(cron_key, cron_at)`, «optimistic, database-enforced» ([разбор автора](https://island94.org/2023/01/how-goodjob-s-cron-does-distributed-locks)). Лимиты конкурентности по ключу считаются под `pg_advisory_xact_lock`. Сессионные advisory locks и LISTEN делают GoodJob несовместимым с PgBouncer в transaction mode.

**Graphile Worker (Node).** `job_key` с режимами: `replace` для debounce, `preserve_run_at` для throttle, `unsafe_dedupe` ([job key](https://worker.graphile.org/docs/job-key)). Cron догоняет пропущенные запуски (backfill), но только для расписаний из таблицы `known_crontabs` ([cron](https://worker.graphile.org/docs/cron)).

**Sidekiq (Redis), для контраста.** Надёжная выборка, unique jobs и periodic в платных редакциях. Тот же сюжет, что у Oban Pro: самое нужное для надёжности стоит денег.

## Про бенчмарки

Цифры зависят от стенда в десятки раз. Сравнивать их напрямую нельзя.

- River на своём стенде показывает около 46 000 no-op задач в секунду. В кросс-бенчмарке [hardbyte](https://github.com/hardbyte/postgresql-job-queue-benchmarking) (май 2026) у River 501/с, у Oban 284/с, у Procrastinate 269/с, у awa 14 158/с. Автор этого бенчмарка это автор awa. Учитывай возможную предвзятость.
- Голая таблица с `SKIP LOCKED` на ноутбуке с PG16 даёт пик около 16 000 claim/с при 16 воркерах. Дальше пропускная способность падает. Упираются в WAL и vacuum, а не в блокировки строк ([замер](https://cstsolution.com/blog/how-fast-is-a-postgres-queue/)).

Для ml-core (порядок 10 RPS) пропускная способность любой из этих систем избыточна. Важнее восстановление после падений и устойчивость к bloat.

## Строить или брать готовое

| Вариант | Плюсы | Минусы |
|---|---|---|
| Строить akadze | Семантика под ml-core: asyncpg, `AsyncSession`, snooze, fencing, лимиты по вендору. Маленькое понятное ядро. Контроль над развитием | Поддержка на нас. Несколько месяцев ml-core живёт со старой очередью |
| Oban-py | Десять лет опыта Oban, snooze, cron, UI, совместимость с Elixir | psycopg3 вместо asyncpg. Самое ценное в платном Pro. Beta |
| PgQueuer | asyncpg, хороший heartbeat, активная разработка | Нет честного snooze, payload bytes, нет результатов |
| DBOS | Пайплайны вендоров ложатся идеально | Кластерное восстановление платное. API не похож на Celery |

Решение (2026-10-05): строим akadze. Маленький v0, проверенные приёмы River, Oban, Solid Queue и GoodJob, ранняя проверка на ml-core. Код ml-core держать за тонким интерфейсом, чтобы план Б оставался реальным.

Связано: [[02 — Celery — что берём и что обходим]], [[06 — Дизайн akadze v0]].
