"""Claim jobs, run them, and finish them under the fencing token."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import random
import socket
import time
from datetime import timedelta
from functools import partial
from typing import Any, TypeGuard
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection

from akadze.args import load_arguments
from akadze.context import bind_run, reset_run
from akadze.exc import AkadzeError, Cancel
from akadze.hooks import Hooks
from akadze.job import (
    _COLUMNS,
    Job,
    columns,
    count_running,
    job_by_id,
    job_from_mapping,
    running_jobs,
)
from akadze.maintenance import prune, rescue
from akadze.outcome import decide, push_error
from akadze.periodic import schedule_due

logger = logging.getLogger("akadze")


def poll_delay(seconds: float, unit: float) -> float:
    """Wait in `[seconds / 2, seconds]` so empty polls do not line up or spin."""

    if seconds <= 0:
        return 0.0
    bounded = min(max(unit, 0.0), 1.0)
    return seconds * (0.5 + 0.5 * bounded)


class _Lost(Exception):
    """This run no longer owns the job."""


class Worker:
    def __init__(
        self,
        app: Any,
        *,
        queues: list[str] | None = None,
        slots: int = 1,
        heartbeat_interval: timedelta = timedelta(seconds=5),
        heartbeat_ttl: timedelta = timedelta(seconds=30),
        poll_interval: timedelta = timedelta(seconds=1),
        shutdown_timeout: timedelta = timedelta(seconds=10),
    ) -> None:
        if slots < 1:
            raise ValueError("slots must be at least 1")
        if shutdown_timeout < timedelta(0):
            raise ValueError("shutdown_timeout must not be negative")
        self.app = app
        self.queues = queues or ["default"]
        self.slots = slots
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_ttl = heartbeat_ttl
        self.poll_interval = poll_interval
        self.shutdown_timeout = shutdown_timeout
        self.id = uuid4()
        self._inflight: dict[int, asyncio.Task[None]] = {}
        self._last_heartbeat_ok = time.monotonic()
        self._abandon = False
        self._draining = False
        self._last_claim_full = False

    async def register(self) -> None:
        async with self.app.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO akadze.workers (id, hostname, pid, queues)
                    VALUES (:id, :hostname, :pid, CAST(:queues AS text[]))
                    """
                ),
                {
                    "id": self.id,
                    "hostname": socket.gethostname(),
                    "pid": os.getpid(),
                    "queues": self.queues,
                },
            )

    async def heartbeat(self) -> None:
        async with self.app.engine.begin() as connection:
            updated = await connection.execute(
                text(
                    """
                    UPDATE akadze.workers
                    SET heartbeat_at = now()
                    WHERE id = :id
                    RETURNING id
                    """
                ),
                {"id": self.id},
            )
            if updated.first() is None:
                await connection.execute(
                    text(
                        """
                        INSERT INTO akadze.workers (id, hostname, pid, queues)
                        VALUES (:id, :hostname, :pid, CAST(:queues AS text[]))
                        """
                    ),
                    {
                        "id": self.id,
                        "hostname": socket.gethostname(),
                        "pid": os.getpid(),
                        "queues": self.queues,
                    },
                )
        self._last_heartbeat_ok = time.monotonic()

    async def claim_available(self) -> None:
        """Claim up to the free slots. The transaction closes before a task runs."""

        full = False
        self._last_claim_full = False
        async with self.app.engine.begin() as connection:
            for queue in self.queues:
                await _cancel_due(connection, self.app.hooks, queue)
                busy = await count_running(connection, self.id, queue)
                limit = self.slots - busy
                claimed = await _claim(
                    connection,
                    hooks=self.app.hooks,
                    worker_id=self.id,
                    queue=queue,
                    limit=limit,
                )
                if limit > 0 and claimed == limit:
                    full = True
        self._last_claim_full = full

    @property
    def running(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(self._inflight.values())

    async def step(self) -> None:
        """Claim and wait until those runs finish."""

        started = await self._spawn()
        if started:
            await asyncio.gather(*started)

    async def _spawn(self) -> list[asyncio.Task[None]]:
        await self.claim_available()
        async with self.app.engine.connect() as connection:
            jobs = await running_jobs(connection, self.id)
        started: list[asyncio.Task[None]] = []
        for job in jobs:
            if job.id in self._inflight:
                continue
            task = asyncio.create_task(self._execute(job))
            self._inflight[job.id] = task
            started.append(task)
        return started

    async def serve(self, stop: asyncio.Event) -> None:
        await self.register()
        self._last_heartbeat_ok = time.monotonic()
        next_heartbeat = 0.0
        next_prune = 0.0
        try:
            while not stop.is_set():
                now = time.monotonic()
                if now - self._last_heartbeat_ok > self.heartbeat_ttl.total_seconds():
                    logger.info("worker %s heartbeat is stale; stopping", self.id)
                    self._abandon = True
                    return
                if now >= next_heartbeat:
                    await self.heartbeat()
                    await rescue(self.app.engine, self.app.hooks, ttl=self.heartbeat_ttl)
                    next_heartbeat = now + self.heartbeat_interval.total_seconds()
                if now >= next_prune:
                    await prune(self.app.engine, retention=self.app.retention)
                    next_prune = now + 60
                await schedule_due(self.app)
                if stop.is_set():
                    break
                await self._spawn()
                if stop.is_set():
                    break
                if self._last_claim_full:
                    continue
                await self._pause(stop)
        finally:
            await self.shutdown()

    async def _pause(self, stop: asyncio.Event) -> None:
        delay = poll_delay(self.poll_interval.total_seconds(), random.random())
        try:
            await asyncio.wait_for(stop.wait(), delay)
        except TimeoutError:
            return

    async def shutdown(self) -> None:
        """Return unfinished jobs to the queue without spending an attempt."""

        if not self._abandon:
            await self._drain_inflight()
        pending = [task for task in self._inflight.values() if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        async with self.app.engine.begin() as connection:
            await _release(connection, self.app.hooks, self.id)
            await connection.execute(
                text("DELETE FROM akadze.workers WHERE id = :id"),
                {"id": self.id},
            )

    async def _drain_inflight(self) -> None:
        self._draining = True
        deadline = time.monotonic() + self.shutdown_timeout.total_seconds()
        while self._inflight and time.monotonic() < deadline:
            try:
                await self.heartbeat()
            except (SQLAlchemyError, OSError):
                logger.info("worker %s heartbeat failed during shutdown", self.id)
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            interval = self.heartbeat_interval.total_seconds()
            timeout = remaining if interval <= 0 else min(interval, remaining)
            pending = list(self._inflight.values())
            if not pending:
                return
            await asyncio.wait(pending, timeout=timeout)

    async def _execute(self, job: Job) -> None:
        async def finish(connection: AsyncConnection) -> None:
            await _complete_open_run(connection, self.app.hooks, job)

        _run, token = bind_run(self.app.engine, finish)
        try:
            try:
                result = await self._invoke(job)
            except _Lost:
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if _run.done:
                    logger.info("job %s raised after complete_tx", job.id)
                    return
                await _finish(self.app.engine, self.app.hooks, job, exc, None)
            else:
                if _run.done:
                    return
                await _finish(self.app.engine, self.app.hooks, job, None, result)
        finally:
            reset_run(token)
            if self._inflight.get(job.id) is asyncio.current_task():
                self._inflight.pop(job.id, None)

    async def _invoke(self, job: Job) -> object:
        async with self.app.engine.connect() as connection:
            fresh = await job_by_id(connection, job.id)
        if (
            fresh is None
            or fresh.state != "running"
            or fresh.run_count != job.run_count
            or fresh.worker_id != self.id
        ):
            raise _Lost
        if fresh.cancel_requested_at is not None or fresh.expired:
            raise Cancel()
        registered = self.app.tasks.get(job.task)
        if registered is None:
            raise RuntimeError("task is not registered")
        arguments = load_arguments(registered.arguments, fresh.args)

        async def body() -> object:
            await self.app.hooks.run_before(fresh)
            if inspect.iscoroutinefunction(registered.fn):
                value = await registered.fn(**arguments)
            else:
                value = await asyncio.to_thread(partial(registered.fn, **arguments))
            await self.app.hooks.run_after(fresh)
            return value

        if registered.timeout is None:
            return await body()
        try:
            return await asyncio.wait_for(body(), registered.timeout.total_seconds())
        except TimeoutError:
            raise TimeoutError("timeout") from None


async def _cancel_due(connection: AsyncConnection, hooks: Hooks, queue: str) -> None:
    result = await connection.execute(
        text(
            f"""
            UPDATE akadze.jobs
            SET state = 'cancelled', finished_at = now()
            WHERE id IN (
                SELECT id
                FROM akadze.jobs
                WHERE state = 'queued'
                  AND queue = :queue
                  AND (
                        cancel_requested_at IS NOT NULL
                        OR (expires_at IS NOT NULL AND expires_at <= now())
                  )
                FOR UPDATE SKIP LOCKED
            )
            RETURNING {_COLUMNS}, false AS expired
            """
        ),
        {"queue": queue},
    )
    for row in result.mappings():
        job = job_from_mapping(row)
        await hooks.ran_transition(connection, job, "queued", "cancelled")
        logger.info("job %s queued -> cancelled", job.id)


async def _claim(
    connection: AsyncConnection,
    *,
    hooks: Hooks,
    worker_id: UUID,
    queue: str,
    limit: int,
) -> int:
    if limit <= 0:
        return 0
    result = await connection.execute(
        text(
            f"""
            WITH picked AS (
                SELECT id
                FROM akadze.jobs
                WHERE state = 'queued'
                  AND queue = :queue
                  AND run_at <= now()
                  AND cancel_requested_at IS NULL
                  AND (expires_at IS NULL OR expires_at > now())
                ORDER BY priority DESC, run_at, id
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            )
            UPDATE akadze.jobs AS job
            SET state = 'running',
                run_count = job.run_count + 1,
                worker_id = :worker_id,
                started_at = now()
            FROM picked
            WHERE job.id = picked.id
            RETURNING {columns("job")}, false AS expired
            """
        ),
        {"queue": queue, "limit": limit, "worker_id": worker_id},
    )
    claimed = 0
    for row in result.mappings():
        claimed += 1
        job = job_from_mapping(row)
        await hooks.ran_transition(connection, job, "queued", "running")
        logger.info("job %s queued -> running", job.id)
    return claimed


async def _release(connection: AsyncConnection, hooks: Hooks, worker_id: UUID) -> None:
    result = await connection.execute(
        text(
            f"""
            UPDATE akadze.jobs
            SET state = 'queued',
                worker_id = NULL,
                started_at = NULL,
                run_at = now()
            WHERE worker_id = :worker_id AND state = 'running'
            RETURNING {_COLUMNS}, false AS expired
            """
        ),
        {"worker_id": worker_id},
    )
    for row in result.mappings():
        job = job_from_mapping(row)
        await hooks.ran_transition(connection, job, "running", "queued")
        logger.info("job %s running -> queued", job.id)


async def _complete_open_run(connection: AsyncConnection, hooks: Hooks, job: Job) -> None:
    current = await job_by_id(connection, job.id)
    if not _owns(current, job):
        raise AkadzeError("this run no longer owns the job")
    if current.cancel_requested_at is not None or current.expired:
        raise Cancel()
    if not await _apply(connection, hooks, current, None, None):
        raise AkadzeError("this run no longer owns the job")


def _owns(current: Job | None, job: Job) -> TypeGuard[Job]:
    return (
        current is not None
        and current.state == "running"
        and current.run_count == job.run_count
        and current.worker_id == job.worker_id
    )


async def _finish(
    engine: Any,
    hooks: Hooks,
    job: Job,
    exc: BaseException | None,
    result: object,
) -> None:
    async with engine.begin() as connection:
        current = await job_by_id(connection, job.id)
        if not _owns(current, job):
            logger.info("job %s finish discarded", job.id)
            return
        if not await _apply(connection, hooks, current, exc, result):
            logger.info("job %s finish discarded", job.id)


async def _apply(
    connection: AsyncConnection,
    hooks: Hooks,
    job: Job,
    exc: BaseException | None,
    result: object,
) -> bool:
    if job.cancel_requested_at is not None or job.expired:
        exc = Cancel()
        result = None
    outcome = decide(job, exc, result)
    errors = push_error(list(job.errors), outcome)
    updated = await connection.execute(
        text(
            f"""
            UPDATE akadze.jobs
            SET state = :state,
                attempt = :attempt,
                snoozes = :snoozes,
                run_at = CASE
                    WHEN :has_delay THEN now() + CAST(:delay AS interval)
                    ELSE run_at
                END,
                worker_id = CASE WHEN :state = 'queued' THEN NULL ELSE worker_id END,
                started_at = CASE WHEN :state = 'queued' THEN NULL ELSE started_at END,
                finished_at = CASE WHEN :state = 'queued' THEN NULL ELSE now() END,
                errors = CAST(:errors AS jsonb),
                result = CAST(:result AS jsonb)
            WHERE id = :id AND run_count = :run_count AND state = 'running'
            RETURNING {_COLUMNS}, false AS expired
            """
        ),
        {
            "state": outcome.state,
            "attempt": outcome.attempt,
            "snoozes": outcome.snoozes,
            "has_delay": outcome.delay is not None,
            "delay": outcome.delay or timedelta(0),
            "errors": json.dumps(errors),
            "result": outcome.result_json,
            "id": job.id,
            "run_count": job.run_count,
        },
    )
    row = updated.mappings().first()
    if row is None:
        return False
    finished = job_from_mapping(row)
    await hooks.ran_transition(connection, finished, job.state, finished.state)
    logger.info("job %s %s -> %s", job.id, job.state, finished.state)
    return True


