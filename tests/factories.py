"""Domain-shaped builders for arrange blocks. Inserts go to real Postgres."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import factory
from faker import Faker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from akadze import pause_queue, set_limit

fake = Faker()


@dataclass(frozen=True)
class JobSpec:
    task: str
    queue: str = "default"
    state: str = "queued"
    priority: int = 0
    attempt: int = 0
    max_attempts: int = 3
    run_at: datetime | None = None
    finished_at: datetime | None = None
    expires_at: datetime | None = None
    args: dict[str, Any] | None = None
    limit_key: str | None = None
    unique_key: str | None = None


class JobFactory(factory.Factory):
    class Meta:
        model = JobSpec

    task = factory.LazyFunction(lambda: f"task-{fake.unique.hexify(text='^^^^^^^^')}")
    queue = "default"
    state = "queued"
    priority = 0
    attempt = 0
    max_attempts = 3
    run_at = None
    finished_at = None
    expires_at = None
    args = factory.LazyFunction(dict)
    limit_key = None
    unique_key = None


@dataclass(frozen=True)
class LimitSpec:
    key: str
    max_running: int | None = 1
    max_starts: int | None = None
    window: timedelta | None = None


class LimitFactory(factory.Factory):
    class Meta:
        model = LimitSpec

    key = factory.LazyFunction(lambda: f"limit-{fake.unique.hexify(text='^^^^^^^^')}")
    max_running = 1
    max_starts = None
    window = None


async def insert_job(engine: AsyncEngine, **overrides: Any) -> int:
    """Insert one jobs row from JobFactory. Returns the new id."""

    spec = JobFactory.build(**overrides)
    payload = json.dumps(spec.args if spec.args is not None else {})
    async with engine.begin() as connection:
        job_id = await connection.scalar(
            text(
                """
                INSERT INTO akadze.jobs (
                    task, queue, state, priority, run_at, finished_at, expires_at,
                    attempt, max_attempts, args, limit_key, unique_key
                )
                VALUES (
                    :task, :queue, :state, :priority,
                    COALESCE(:run_at, now()), :finished_at, :expires_at,
                    :attempt, :max_attempts,
                    CAST(:args AS jsonb), :limit_key, :unique_key
                )
                RETURNING id
                """
            ),
            {
                "task": spec.task,
                "queue": spec.queue,
                "state": spec.state,
                "priority": spec.priority,
                "run_at": spec.run_at,
                "finished_at": spec.finished_at,
                "expires_at": spec.expires_at,
                "attempt": spec.attempt,
                "max_attempts": spec.max_attempts,
                "args": payload,
                "limit_key": spec.limit_key,
                "unique_key": spec.unique_key,
            },
        )
    assert job_id is not None
    return int(job_id)


async def apply_limit(engine: AsyncEngine, **overrides: Any) -> LimitSpec:
    """Upsert a limits row via set_limit (command). Returns the applied spec."""

    spec = LimitFactory.build(**overrides)
    async with engine.begin() as session:
        await set_limit(
            session,
            spec.key,
            spec.max_running,
            max_starts=spec.max_starts,
            window=spec.window,
        )
    return spec


async def pause(engine: AsyncEngine, queue: str) -> None:
    """Pause a queue via pause_queue (command)."""

    async with engine.begin() as session:
        await pause_queue(session, queue)


async def insert_periodic_run(
    engine: AsyncEngine,
    *,
    name: str,
    fire_at: datetime | None = None,
) -> None:
    """Insert one periodic_runs row (real Postgres)."""

    async with engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO akadze.periodic_runs (name, fire_at)
                VALUES (:name, COALESCE(:fire_at, now()))
                """
            ),
            {"name": name, "fire_at": fire_at},
        )


async def age_worker_heartbeat(
    engine: AsyncEngine,
    worker_id: UUID | str,
    *,
    older_than: timedelta,
) -> None:
    """Move a worker heartbeat into the past (arrange for rescue / stale checks)."""

    async with engine.begin() as connection:
        await connection.execute(
            text(
                """
                UPDATE akadze.workers
                SET heartbeat_at = now() - CAST(:seconds AS double precision) * interval '1 second'
                WHERE id = CAST(:id AS uuid)
                """
            ),
            {"seconds": older_than.total_seconds(), "id": str(worker_id)},
        )


async def bump_run_count(engine: AsyncEngine, *, task: str) -> None:
    """Increment jobs.run_count for a task (fencing arrange)."""

    async with engine.begin() as connection:
        await connection.execute(
            text("UPDATE akadze.jobs SET run_count = run_count + 1 WHERE task = :task"),
            {"task": task},
        )
