---
status: active
updated: 2026-10-05
tags:
  - akadze
  - explanation
---

# Почему akadze

Модель Celery понятна: enqueue, worker, Beat по расписанию. Нужна та же модель с PostgreSQL как брокером (`SKIP LOCKED` и lease), без Redis и RabbitMQ.

akadze: маленькая библиотека без продуктового домена. Это не Temporal и не SaaS в v0.

## Успех v0

1. Зарегистрировать task в коде.
2. Поставить задачу сейчас или с `run_at`.
3. Worker забирает задачу с lease.
4. Beat ставит задачи по cron или interval.
5. Документация, которую читают один раз.
