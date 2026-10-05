---
status: active
updated: 2026-10-05
tags:
  - akadze
  - explanation
diataxis: explanation
---

# Why akadze

Celery's mental model is clear: enqueue a task, workers pull it, Beat puts work on a schedule. Many teams still want that model with **PostgreSQL as the broker**, not Redis or RabbitMQ — one less system, `SKIP LOCKED` claim, lease recovery after a crash.

akadze aims to be that small library: familiar shape, Postgres underneath, no product domain.

## What it is not

- Not a rewrite of a product queue overnight
- Not Temporal (long human-in-the-loop workflows)
- Not a SaaS in v0 — a library first

## Success for v0

1. Register a task in code
2. Enqueue it (now or `run_at`)
3. Worker claims with lease and runs it
4. Beat enqueues on cron/interval
5. Docs a tired engineer can follow once
