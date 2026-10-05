---
status: research
updated: 2026-10-05
tags:
  - akadze
  - research
  - ml-core
---

# ml-core как первый потребитель

В ml-core уже живёт маленькая самописная Postgres-очередь: около 1 200 строк общей механики и около 790 строк её тестов. akadze может забрать её почти целиком. Главная выгода не в меньшем коде, а в закрытых дырах. Поллинг вендоров тратит попытки. Потеря lease не останавливает шаг. Отмены нет. События в Kafka могут теряться. Шаги идут строго по одному на процесс.

Источник: аудит кода ml-core от 2026-10-05, ветка `main`.

## Что есть сейчас

- Единица очереди: строка `job_steps`. Claim: `FOR UPDATE OF job_steps SKIP LOCKED`, порядок `priority DESC, available_at, id`, `LIMIT 1` (`services/jobs/claim_step_command.py`).
- Lease: `locked_until = now + lease_seconds` (60 с), heartbeat каждые 30 с (`tasks/runner.py`, `_keep_step_lease_alive`).
- Переходы `complete`, `retry` и `fail` проверяют `locked_by == worker_id`.
- Цепочка: executor возвращает `StepResult(next_step=...)`, и следующий шаг вставляется в той же транзакции, что и завершение текущего (`complete_step_command.py`). Это правильный приём. Его сохраняем.
- Retry: `RetryableStepError` и `TerminalStepError`, экспонента от `retry_base_seconds` до `retry_max_seconds`.
- Reaper переводит в `failed` шаги с протухшим lease и исчерпанными попытками.
- Housekeeping (reap и purge assets) идёт в каждом проходе цикла воркера, до claim.

| Зона | Файлы | Строк |
|---|---|---|
| Цикл воркера, heartbeat, shutdown | `tasks/runner.py` | 477 |
| Переходы состояний | `services/jobs/`: claim, complete, retry, fail, extend lease, add step | ~440 |
| Reaper и снимок очереди | `reap_orphaned_exhausted_steps_command.py`, `queue_snapshot_query.py` | ~170 |
| Модели, контракты, ошибки, dispatch | `db/models/job_step.py`, `tasks/contracts.py`, `tasks/errors.py`, `tasks/dispatch.py` и др. | ~300 |
| Тесты механики | `tests/unit/jobs/` | ~790 |
| In-memory фейк сессии | `tests/utils/fake_session.py` | 799 |

## Слабые места, которые закрывает akadze

| # | Что сейчас | Где | Чем закрываем |
|---|---|---|---|
| 1 | Один шаг на процесс. Параллелизм только числом подов | `runner.py` | Слоты: N задач на процесс |
| 2 | Каждый опрос вендора идёт как retry той же строки, и он тратит attempt. Поэтому `max_attempts` выводят из таймаутов: `max(3, ceil(read_timeout / poll_interval) + 5)` | `pipelines/stt/executor.py` | `Snooze`: перенос без траты попытки |
| 3 | Если продлить lease не удалось, heartbeat-цикл завершается, а шаг продолжает работать. Когда lease протухнет, шаг возьмёт другой воркер. Получается двойное выполнение | `runner.py` | Fencing по номеру запуска и самоотмена задач при потере heartbeat |
| 4 | Lease и `available_at` считаются по часам приложения | все команды переходов | Время из базы |
| 5 | Отмены нет: `CANCELLED` есть в enum и CHECK, но код его не ставит | `db/enums.py` | Отмена как состояние и cooperative cancel |
| 6 | События в Kafka уходят best-effort после коммита, без outbox | `runner.py`, `_emit_job_event_safely` | Хук перехода внутри транзакции → строка outbox → relay в Kafka |
| 7 | Housekeeping на каждом проходе цикла даёт лишние запросы под нагрузкой | `runner.py` | `@app.periodic` без дублей |
| 8 | `UNIQUE (job_id, idempotency_key)` с ключом `job_id:step_type` даёт один шаг каждого типа на job. Из-за этого опрос вынужден переиспользовать строку | `db/models/job_step.py` | `unique_key` задаёт приложение |
| 9 | Юнит-тесты идут на in-memory фейке, который не моделирует `SKIP LOCKED`. Гонку двух воркеров никто не проверяет | `tests/utils/fake_session.py` | Тесты akadze на настоящем Postgres, включая конкурентный claim и kill -9 |

## Что остаётся в ml-core

- `jobs`: бизнес-сущность (`public_id`, тип, статус, параметры, assets) и её API.
- Пайплайны и executors, около 4 200 строк.
- `provider_calls`: защита от двойного счёта за платные вызовы вендоров. Это не очередь. akadze только даёт id задачи и номер запуска для ключей идемпотентности.
- Kafka: схемы событий, `job_commands`, consumer.
- Assets и S3.

## Как ml-core ляжет на akadze

| ml-core | akadze |
|---|---|
| `job_steps` + claim, lease, retry, reap | `akadze.jobs` + воркер |
| `step_type` + `DispatchingExecutor` | имя задачи + `@app.task` |
| `NextStep` в `StepResult` | постановка следующей задачи в транзакции завершения текущей |
| `RetryableStepError(retry_after_seconds=...)` | `Retry(delay=...)` |
| опрос через retry | `Snooze(delay=...)` |
| `TerminalStepError` | `Fail(...)` |
| reap и purge в цикле воркера | rescuer akadze + `@app.periodic("purge-expired-assets")` |
| `GET /ops/queue` | статистика очереди из akadze (SQL-представление или метод) |

## Миграция по фазам

1. **P0. akadze отдельно.** Ядро v0, тесты на Postgres, CI. Критерий выхода: kill -9 воркера под нагрузкой не теряет и не дублирует завершённые задачи.
2. **P1. housekeeping.** В ml-core akadze подключается только для `purge_expired_assets` как periodic-задачи. Проверяем миграции через Alembic, метрики и работу за PgBouncer. Риск низкий.
3. **P2. Один пайплайн за флагом.** Например, STT: шаги идут через akadze, таблица `jobs` не меняется. Сравниваем метрики со старым путём.
4. **P3. Переключение.** Новые шаги создаются только в akadze. Старая очередь дорабатывает свои шаги и выключается. Удаляем `runner.py` и команды переходов.
5. **P4. Outbox для Kafka** через хук перехода состояния.

## Честно о цене

- На фазах P2 и P3 в ml-core две очереди. Это временно и ограничено флагом.
- Кода станет меньше примерно на 1 200 строк плюс тесты, но появится зависимость от akadze. Настоящий выигрыш в возможностях, которые иначе пришлось бы дописывать в ml-core: snooze, отмена, слоты, periodic, fencing.
- Ответственность за корректность очереди переезжает в akadze. Нужны тесты, которых сейчас в ml-core нет.

Связано: [[06 — Дизайн akadze v0]], [[04 — Postgres как брокер — риски и правила]].
