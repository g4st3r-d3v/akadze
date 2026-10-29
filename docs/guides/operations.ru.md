# Деплой и эксплуатация

## Деплой

1. Перед запуском новых воркеров выполните `akadze migrate` (или `await migrate(database_url)`) на базе приложения. Применённые миграции учитываются; `applied_versions(database_url)` возвращает их список.
2. Запустите воркеры: `akadze worker app.module:app --slots 8 --queue default --queue mail`.

`--slots` действует на каждую очередь: с тремя `--queue` воркер выполняет до `3 × slots` заданий. Без `--queue` он берёт задания из `default`.

!!! note "Большая таблица `akadze.jobs`"
    Миграция `005_jobs_meta` строит GIN-индекс по `akadze.jobs.meta` и на время построения блокирует запись в таблицу. На большой таблице сначала создайте его сами: `CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_meta_idx ON akadze.jobs USING gin (meta jsonb_path_ops)` — тогда миграция его пропустит.

### Rolling deploy

С 0.6.0 воркер забирает только задания тех задач, которые зарегистрированы в его приложении. Задание задачи, которая есть только в новом коде, остаётся в `queued`, не тратя попыток, пока его не возьмёт новый воркер. Воркеры на akadze 0.5.0 и старше всё ещё забирают такие задания и тратят попытки, поэтому обновите akadze до того, как добавлять задачи.

Задание задачи, которую не регистрирует ни один воркер (удалённой или переименованной), остаётся в очереди без ошибки, держит свой `unique_key` и учитывается в lag очереди. Отменяйте такие задания, когда удаляете задачу.

### PgBouncer

Engine, который создаёт `Akadze(database_url=...)`, отключает кэш prepared statements в asyncpg, поэтому перед базой может стоять пул в transaction-режиме. Если передаёте свой `engine`, задайте `connect_args={"statement_cache_size": 0}` сами.

## Посмотреть на очередь

| Python | CLI | Что показывает |
|---|---|---|
| `queue_snapshot(engine)` | — | число заданий по очередям и состояниям, ожидание самого старого готового, число выполняющихся |
| `list_jobs(engine, queue=, state=, limit=50, meta=)` | `akadze jobs list` | сводки заданий без args/result/errors |
| `list_workers(engine)` | `akadze workers list` | id, хост, pid, очереди, старт и последний heartbeat |
| `list_limits(engine)` | `akadze limits list` | политики лимитов |

## Управление

| Python | CLI |
|---|---|
| `pause_queue(session, name)` / `resume_queue` | `akadze queues pause NAME` / `resume NAME` |
| `request_cancel(session, job_id)` | `akadze jobs cancel JOB_ID` |
| `requeue(session, job_id)` | `akadze jobs requeue JOB_ID` |
| `clear_limit(session, key)` | `akadze limits clear KEY` |

Пауза действует на весь кластер: ни один воркер не берёт задания из очереди, выполняющиеся завершаются. Чтобы остановить один хост, остановите его воркер (Ctrl-C или SIGTERM).

## Метрики

`akadze metrics` печатает текст Prometheus: задания по очередям и состояниям, lag, выполняющиеся и `akadze_queue_paused`. Из Python — `akadze.metrics.render_metrics(engine)`. HTTP akadze не поднимает: отдавайте текст из своего приложения или sidecar.

Исходы по задачам считайте в [хуке](tasks.md#hooks) `on_transition`. Не кладите аргументы и результаты задач в логи и метки метрик.

## Хранение

Воркер удаляет завершённые задания (`succeeded`, `failed`, `cancelled`) и запуски расписаний старше `retention` приложения (по умолчанию 7 дней) примерно раз в минуту: каждый воркер удаляет не больше 1 000 заданий за проход. Кластер, который завершает больше ~1 000 заданий в минуту на воркер, это перерастает; удаляйте старые строки сами (`DELETE FROM akadze.jobs WHERE state IN ('succeeded','failed','cancelled') AND finished_at < …` пачками). Срок задаётся через `Akadze(retention=timedelta(days=30))`.

## Ошибки воркера и базы

С 0.6.0 транзиентная ошибка базы (рестарт, failover, обрыв соединения, deadlock) пишется в лог и повторяется с backoff; воркер продолжает работу и не делает claim, пока не проходит heartbeat. Другие ошибки базы, например непримененная схема, его останавливают. См. [Доставка и воркер](../concepts/delivery.md).
