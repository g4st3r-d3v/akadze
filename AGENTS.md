# Agent notes for akadze

## Purpose

Library: Postgres task queue and worker. Periodic schedules run inside the worker. No product domain. No HTTP layer.

## Always

- Reply to the owner in **Russian** unless they write in English and ask to switch
- Specs / intent: https://github.com/g4st3r-d3v/akadze-harness (local: `/Users/g4st3r/Development/akadze-harness`). Start at `HUB.md`
- Prefer small, testable increments
- Conventional commits
- Tests hit real Postgres through `AKADZE_DATABASE_URL`. Do not add an in-memory database
- Tests follow AAA: Arrange, Act, Assert, one act per test
- Commands change state and return nothing. Queries return state and change nothing. Pure functions do neither

## Ask first

- Public Python call surface breaks after 0.1
- Adding Redis/Rabbit or Celery compatibility
- Pushing secrets or real DSNs

## Never

- Add HTTP / REST / web UI in this package (another library owns that layer)
- Keep design notes in this repo (they live in akadze-harness)
- Import or depend on application packages
- Put credentials in logs or traces
- Force-push `main`
