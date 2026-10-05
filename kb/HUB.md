---
status: active
updated: 2026-10-05
tags:
  - akadze
  - hub
---

# akadze — knowledge hub

Зеркало заметок из Obsidian «База Знаний» (`Проекты/IT/akadze`). Правь здесь или в vault; держи в синхроне.

Очередь задач на PostgreSQL в стиле Celery. В роли брокера Postgres: `SKIP LOCKED` и lease.

Репозиторий: https://github.com/g4st3r-d3v/akadze
Харнесс в репо: `kb/` (зеркало этих заметок).  
Код: `/Users/g4st3r/Development/akadze` (вне Cempa).

## Заметки

- [[Почему akadze]]
- [[Скетч продукта]]
- [[Roadmap wave 0]]

## Исследование (2026-10-05)

Перед кодом: что берём у Celery, что уже есть на рынке, риски Postgres как брокера и как ml-core ляжет на akadze.

- [[02 — Celery — что берём и что обходим]]
- [[03 — Ландшафт Postgres-очередей 2026]]
- [[04 — Postgres как брокер — риски и правила]]
- [[05 — ml-core как первый потребитель]]
- [[06 — Дизайн akadze v0]]

## Решения (2026-10-05)

1. Строим akadze. Первый потребитель: ml-core. План Б: Oban-py.
2. Слой базы в v0: SQLAlchemy 2 async Core.
3. В v0 только задачи: постановка, воркер, retry, snooze, отмена, periodic, обслуживание очереди.
