# Agent notes for akadze

## Purpose

Library: Postgres task queue + worker + optional beat. No product domain.

## Always

- Reply to the owner in **Russian** unless they write in English and ask to switch
- Specs / intent live in the owner's personal Obsidian vault «База Знаний» (`Проекты/IT/akadze`) — not in this repo
- Prefer small, testable increments
- Conventional commits

## Ask first

- Public API breaks after 0.1
- Adding Redis/Rabbit or Celery compatibility
- Pushing secrets or real DSNs

## Never

- Import or depend on Cempa / ml-core packages
- Put credentials in logs or traces
- Force-push `main`
