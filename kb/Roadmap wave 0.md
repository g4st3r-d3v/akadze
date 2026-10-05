---
status: active
updated: 2026-10-05
tags:
  - akadze
  - research
---

# Roadmap wave 0

## Сделано

- [x] Репо вне product trees
- [x] Poetry scaffold и CLI stubs
- [x] GitHub https://github.com/g4st3r-d3v/akadze
- [x] Заметки в «База Знаний» (`Проекты/IT/akadze`)

## Wave 1. Исследование (2026-10-05)

- [x] Celery: сильные стороны и корни болей
- [x] Обзор Postgres-очередей: Python и другие экосистемы
- [x] Риски Postgres как брокера и правила дизайна
- [x] Аудит очереди ml-core: что akadze может забрать
- [x] Черновик дизайна v0
- [x] Решения владельца: строим akadze. SQLAlchemy 2 async Core; v0 покрывает только задачи

## Wave 2. Ядро v0

- [ ] Схема `akadze` и миграции
- [ ] Постановка в транзакции, воркер со слотами, fencing, heartbeat
- [ ] Retry, snooze, timeout, отмена
- [ ] Beat и croniter
- [ ] Тесты на настоящем Postgres: конкурентный claim, kill -9, fencing
- [ ] How-to: первая задача за пять минут

## Парковка

- Cloud-hosted workers
- Адаптеры продуктовых сервисов
