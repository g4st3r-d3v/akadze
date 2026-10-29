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
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from typing import Any, TypeGuard
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from akadze.args import load_arguments
from akadze.claim import claim_queue
from akadze.context import bind_run, reset_run
from akadze.exc import AkadzeError, Cancel, Fail
from akadze.hooks import Hooks
from akadze.job import (
    _COLUMNS,
    Job,
    count_running,
    job_by_id,
    job_from_mapping,
    running_jobs,
)
from akadze.maintenance import prune, rescue
from akadze.outcome import BackoffPolicy, decide, push_error
from akadze.periodic import schedule_due
from akadze.schema import DB_ERRORS, is_transient_db_error, public_db_error

logger = logging.getLogger("akadze")


@dataclass(frozen=True)
class WorkerSummary:
    id: UUID
    hostname: str
    pid: int
    queues: tuple[str, ...]
    started_at: datetime
    heartbeat_at: datetime


async def list_workers(engine: AsyncEngine) -> tuple[WorkerSummary, ...]:
    """Return registered workers. Ordered by heartbeat_at desc, then id. No mutation."""

    async with engine.connect() as connection:
        rows = await connection.execute(
            text(
                """
                SELECT id, hostname, pid, queues, started_at, heartbeat_at
                FROM akadze.workers
                ORDER BY heartbeat_at DESC, id
                """
            )
        )
        return tuple(
            WorkerSummary(
                id=_as_uuid(row.id),
                hostname=str(row.hostname),
                pid=int(row.pid),
                queues=_as_queues(row.queues),
                started_at=_as_datetime(row.started_at),
                heartbeat_at=_as_datetime(row.heartbeat_at),
            )
            for row in rows
        )


def _as_uuid(value: object) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _as_queues(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raise TypeError("worker queues must be a sequence of strings")
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value)
    raise TypeError("worker queues must be a sequence of strings")


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    raise TypeError("worker timestamp must be a datetime")


def poll_delay(seconds: float, unit: float) -> float:
    """Wait in `[seconds / 2, seconds]` so empty polls do not line up or spin."""

    if seconds <= 0:
        return 0.0
    bounded = min(max(unit, 0.0), 1.0)
    return seconds * (0.5 + 0.5 * bounded)


_ERROR_DELAY_FLOOR = 0.1
_ERROR_DELAY_MAX_DOUBLINGS = 16
_STALE_CHECK_SECONDS = 1.0
_STALE_MARGIN = 0.1


class _Lost(Exception):
    """This run no longer owns the job."""


class Worker:
    """Claims jobs from ``queues`` and runs them; also runs schedules and maintenance.

    ``slots`` is how many jobs run at once per queue. Liveness is a heartbeat
    every ``heartbeat_interval``; a worker whose heartbeat is older than
    ``heartbeat_ttl`` is presumed dead: another worker requeues its jobs,
    spending an attempt with the default backoff (``failed`` on the last one).
    An idle worker polls about every ``poll_interval``; a worker whose slots
    are all taken claims again as soon as one of those runs ends. On stop it
    waits up to ``shutdown_timeout`` for running jobs, then returns the rest to
    the queue without spending an attempt. ``fair=True`` shares slots across
    ``limit_key`` groups within a priority instead of taking the oldest jobs.

    ``akadze worker module:app`` builds one and calls ``serve``.
    """

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
        fair: bool = False,
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
        self.fair = fair
        self.id = uuid4()
        self._inflight: dict[int, asyncio.Task[None]] = {}
        self._last_heartbeat_ok = time.monotonic()
        self._abandon = False
        self._heartbeat_failing = False
        self._fatal: BaseException | None = None
        self._draining = False
        self._last_claim_full = False
        # Set when a run of a queue with every slot taken writes its outcome. The
        # pause waits on it, not on the poll interval, so the slot is refilled at
        # once. Made per claim, so it belongs to the running loop.
        self._slot_freed: asyncio.Event | None = None
        self._taken: frozenset[str] = frozenset()

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
        started = time.monotonic()
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
        self._last_heartbeat_ok = started

    async def claim_available(self) -> None:
        """Claim up to the free slots, one transaction per queue.

        Each transaction closes before a task runs. A transaction per queue keeps
        the limits locks of one queue from being held while another queue's claim
        waits for its own, so workers with different queue orders cannot deadlock.
        """

        full = False
        taken: set[str] = set()
        self._last_claim_full = False
        self._slot_freed = asyncio.Event()
        tasks = list(self.app.tasks)
        for queue in self.queues:
            async with self.app.engine.begin() as connection:
                await _cancel_due(connection, self.app.hooks, queue)
                busy = await count_running(connection, self.id, queue)
                limit = self.slots - busy
                claimed = await claim_queue(
                    connection,
                    hooks=self.app.hooks,
                    worker_id=self.id,
                    queue=queue,
                    tasks=tasks,
                    limit=limit,
                    fair=self.fair,
                )
                if limit > 0 and claimed == limit:
                    full = True
                if limit <= 0:
                    taken.add(queue)
        self._last_claim_full = full
        self._taken = frozenset(taken)

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
        """Run until stop is set or no heartbeat succeeds within heartbeat_ttl.

        The heartbeat runs as its own task, so a slow or hung step cannot delay
        it. When it goes stale, the loop is cancelled and running jobs are
        stopped before other workers rescue them. In the loop, a transient
        database error (connection lost, restart, deadlock) is logged and
        retried with backoff; any other error stops the worker.
        """

        await self.register()
        self._last_heartbeat_ok = time.monotonic()
        loop = asyncio.create_task(self._loop(stop))
        beat = asyncio.create_task(self._beat(stop))
        grace = self.heartbeat_ttl.total_seconds()
        try:
            done, _ = await asyncio.wait({loop, beat}, return_when=asyncio.FIRST_COMPLETED)
            if beat in done and not self._abandon and beat.exception() is None:
                await asyncio.wait({loop}, timeout=grace)
            for task in (loop, beat):
                if task.done():
                    task.result()
        finally:
            pending = {task for task in (loop, beat) if not task.done()}
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=grace)
            await self.shutdown()

    async def _beat(self, stop: asyncio.Event) -> None:
        failures = 0
        while not stop.is_set():
            if self._stale():
                logger.info("worker %s heartbeat is stale; stopping", self.id)
                self._abandon = True
                for task in self._inflight.values():
                    task.cancel()
                return
            try:
                async with asyncio.timeout(max(self._heartbeat_left(), 0.0)):
                    await self.heartbeat()
            except DB_ERRORS as exc:
                failures += 1
                self._heartbeat_failing = True
                logger.warning(
                    "worker %s heartbeat failed, retrying: %s", self.id, public_db_error(exc)
                )
                delay = self._error_delay(failures)
            else:
                failures = 0
                self._heartbeat_failing = False
                delay = self.heartbeat_interval.total_seconds()
            deadline = time.monotonic() + delay
            while not stop.is_set() and not self._stale() and time.monotonic() < deadline:
                left = min(deadline - time.monotonic(), _STALE_CHECK_SECONDS)
                await self._wait(stop, max(min(left, self._heartbeat_left()), 0.0))

    async def _loop(self, stop: asyncio.Event) -> None:
        next_rescue = 0.0
        next_prune = 0.0
        failures = 0
        while not stop.is_set():
            if self._fatal is not None:
                raise self._fatal
            now = time.monotonic()
            ok = True
            if now >= next_rescue:
                rescued = partial(rescue, self.app.engine, self.app.hooks, ttl=self.heartbeat_ttl)
                if await self._step("rescue", rescued):
                    next_rescue = now + self.heartbeat_interval.total_seconds()
                else:
                    ok = False
            if now >= next_prune:
                pruned = partial(prune, self.app.engine, retention=self.app.retention)
                if await self._step("prune", pruned):
                    next_prune = now + 60
                else:
                    ok = False
            ok &= await self._step("schedule", partial(schedule_due, self.app))
            if stop.is_set():
                break
            if self._heartbeat_failing:
                ok = False
            else:
                ok &= await self._step("claim", self._spawn)
            if not ok:
                failures += 1
                await self._wait(stop, self._error_delay(failures))
                continue
            failures = 0
            if stop.is_set():
                break
            if self._last_claim_full:
                continue
            await self._pause(stop)

    async def _pause(self, stop: asyncio.Event) -> None:
        delay = poll_delay(self.poll_interval.total_seconds(), random.random())
        if not self._taken or self._slot_freed is None:
            await self._wait(stop, delay)
            return
        # Every slot of some queue was taken, so it may hold more: claim again
        # once one of its runs ends instead of sleeping the whole interval. If
        # that claim finds the queue empty, the next pause polls as usual.
        freed = asyncio.ensure_future(self._slot_freed.wait())
        stopped = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({freed, stopped}, timeout=delay, return_when=asyncio.FIRST_COMPLETED)
        finally:
            freed.cancel()
            stopped.cancel()

    def _stale(self) -> bool:
        return self._heartbeat_left() <= 0

    def _heartbeat_left(self) -> float:
        """Seconds until this worker counts itself stale.

        That is a margin before heartbeat_ttl, so its jobs are stopped before
        other workers, which go by heartbeat_at in the database, rescue them.
        """

        ttl = self.heartbeat_ttl.total_seconds()
        margin = min(self.heartbeat_interval.total_seconds(), ttl * _STALE_MARGIN)
        return ttl - margin - (time.monotonic() - self._last_heartbeat_ok)

    async def _step(self, name: str, run: Callable[[], Awaitable[object]]) -> bool:
        """Run one loop step. False after a transient database error; others raise."""

        try:
            await run()
        except DB_ERRORS as exc:
            if not is_transient_db_error(exc):
                raise
            logger.warning("worker %s %s failed, retrying: %s", self.id, name, public_db_error(exc))
            return False
        return True

    def _error_delay(self, failures: int) -> float:
        """Back off after consecutive database errors, up to the heartbeat interval."""

        ceiling = max(self.heartbeat_interval.total_seconds(), _ERROR_DELAY_FLOOR)
        doublings = min(failures - 1, _ERROR_DELAY_MAX_DOUBLINGS)
        seconds = min(_ERROR_DELAY_FLOOR * 2**doublings, ceiling)
        return poll_delay(seconds, random.random())

    async def _wait(self, stop: asyncio.Event, delay: float) -> None:
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
        try:
            async with asyncio.timeout(self.heartbeat_ttl.total_seconds()):
                async with self.app.engine.begin() as connection:
                    await _release(connection, self.app.hooks, self.id)
                    await connection.execute(
                        text("DELETE FROM akadze.workers WHERE id = :id"),
                        {"id": self.id},
                    )
        except DB_ERRORS as exc:
            logger.warning(
                "worker %s could not release its jobs; another worker will rescue them: %s",
                self.id,
                public_db_error(exc),
            )

    async def _drain_inflight(self) -> None:
        self._draining = True
        deadline = time.monotonic() + self.shutdown_timeout.total_seconds()
        while self._inflight and time.monotonic() < deadline:
            try:
                async with asyncio.timeout(max(self._heartbeat_left(), 0.0)):
                    await self.heartbeat()
            except DB_ERRORS:
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
        registered = self.app.tasks.get(job.task)
        policy = None if registered is None else registered.backoff
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
                    self._freed(job)
                    return
                await self._record(job, exc, None, policy)
            else:
                if _run.done:
                    self._freed(job)
                    return
                await self._record(job, None, result, policy)
        finally:
            reset_run(token)
            if self._inflight.get(job.id) is asyncio.current_task():
                self._inflight.pop(job.id, None)

    def _freed(self, job: Job) -> None:
        """Wake the pause: this run's outcome is written, so its slot is free.

        Only then: a run that left its row running (lost, stale) is not a free
        slot, and waking for it would restart it at once.
        """

        if self._slot_freed is not None and job.queue in self._taken:
            self._slot_freed.set()

    async def _record(
        self,
        job: Job,
        exc: BaseException | None,
        result: object,
        policy: BackoffPolicy | None,
    ) -> None:
        """Write the outcome; the job stays in flight until it is written.

        A transient database error is retried while the heartbeat is fresh, so
        the claim loop does not run the job again. Once the heartbeat is stale
        the job is left for rescue. Any other error (the outcome cannot be
        stored) fails the job without another attempt, since the body may have
        succeeded; if even that fails, the worker stops.
        """

        failures = 0
        while True:
            try:
                await _finish(self.app.engine, self.app.hooks, job, exc, result, policy)
                self._freed(job)
                return
            except DB_ERRORS as error:
                if not is_transient_db_error(error):
                    await self._record_unstorable(job, error, policy, returned=exc is None)
                    return
                failures += 1
                if self._stale():
                    logger.warning(
                        "job %s outcome not recorded; leaving it for rescue: %s",
                        job.id,
                        public_db_error(error),
                    )
                    return
                logger.warning(
                    "job %s outcome not recorded, retrying: %s", job.id, public_db_error(error)
                )
                await asyncio.sleep(
                    max(min(self._error_delay(failures), self._heartbeat_left()), 0.0)
                )

    async def _record_unstorable(
        self, job: Job, error: BaseException, policy: BackoffPolicy | None, *, returned: bool
    ) -> None:
        """Fail the job. A body that returned is not turned into a cancel: it ran."""

        reason = Fail(f"outcome could not be stored: {public_db_error(error)}")
        try:
            await _finish(
                self.app.engine,
                self.app.hooks,
                job,
                reason,
                None,
                policy,
                honor_cancel=not returned,
            )
        except DB_ERRORS as again:
            self._fatal = again
            logger.error(
                "job %s outcome could not be stored; stopping the worker: %s",
                job.id,
                public_db_error(again),
            )
        else:
            self._freed(job)
            logger.warning("job %s outcome could not be stored: %s", job.id, public_db_error(error))

    async def _invoke(self, job: Job) -> object:
        try:
            async with self.app.engine.connect() as connection:
                fresh = await job_by_id(connection, job.id)
        except DB_ERRORS as exc:
            if not is_transient_db_error(exc):
                self._fatal = exc
            logger.warning("job %s not started: %s", job.id, public_db_error(exc))
            raise _Lost from None
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
            if registered.is_async:
                value = await registered.fn(**arguments)
            else:
                value = await asyncio.to_thread(partial(registered.fn, **arguments))
                if inspect.isawaitable(value):
                    # A wrapper that hides a coroutine function returns its coroutine.
                    value = await value
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
    """Finish the job as succeeded in the complete_tx transaction.

    A cancel requested, or an expiry reached, while the block ran does not stop
    it: the block reached its end, so its writes and ``succeeded`` commit.
    """

    current = await job_by_id(connection, job.id)
    if not _owns(current, job):
        raise AkadzeError("this run no longer owns the job")
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
    policy: BackoffPolicy | None = None,
    *,
    honor_cancel: bool = True,
) -> None:
    async with engine.begin() as connection:
        current = await job_by_id(connection, job.id)
        if not _owns(current, job):
            logger.info("job %s finish discarded", job.id)
            return
        if not await _apply(
            connection, hooks, current, exc, result, policy, honor_cancel=honor_cancel
        ):
            logger.info("job %s finish discarded", job.id)


async def _apply(
    connection: AsyncConnection,
    hooks: Hooks,
    job: Job,
    exc: BaseException | None,
    result: object,
    policy: BackoffPolicy | None = None,
    *,
    honor_cancel: bool = True,
) -> bool:
    outcome = decide(job, exc, result, policy, honor_cancel=honor_cancel)
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
