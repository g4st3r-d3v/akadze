# Agent notes for akadze

## Purpose

Library: Postgres task queue + worker + optional beat. No product domain. No HTTP layer.

## Always

- Reply to the owner in **Russian** unless they write in English and ask to switch
- Specs / intent: Obsidian «База Знаний» → `Проекты/IT/akadze` (GitHub: `g4st3r-d3v/obsidian-kb`). Not in this repo
- Prefer small, testable increments
- Conventional commits

## Ask first

- Public Python call surface breaks after 0.1
- Adding Redis/Rabbit or Celery compatibility
- Pushing secrets or real DSNs

## Never

- Add HTTP / REST / web UI in this package (another library owns that layer)
- Keep a second copy of design notes in this repo
- Import or depend on Cempa / ml-core packages
- Put credentials in logs or traces
- Force-push `main`
