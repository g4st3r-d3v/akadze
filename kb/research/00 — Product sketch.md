---
status: research
updated: 2026-10-05
tags:
  - akadze
  - research
diataxis: research
---

# Product sketch — where akadze might go

Open questions for the same chat / owners. Not commitments.

## Near term (library)

- Schema: single `tasks` table vs jobs+steps
- Sync vs async executors (`asyncpg` first?)
- Payload: JSONB limits, no secrets in logs
- Exactly-once vs at-least-once (document the choice)
- Beat in-process vs separate process

## Medium

- GitHub public repo + CI
- Plugin hooks: metrics, tracing allow-list
- Multi-queue / priority
- Dead-letter / poison task UI (CLI first)

## Later / speculative

- Hosted runner / Cursor Cloud worker that pulls from a user's Postgres
- HTTP enqueue API as optional extra package
- Adoption by an existing product queue behind an adapter (only after API freeze)

## Explicit non-goals until API stabilizes

- Drop-in Celery protocol compatibility
- Replacing Kafka as event bus
- Multi-tenant cloud control plane
