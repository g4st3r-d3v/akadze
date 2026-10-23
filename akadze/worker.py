"""Claim jobs, run them, and finish them under the fencing token."""

from __future__ import annotations

import asyncio
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
from akadze.context import bind_run, reset_run
from akadze.exc import AkadzeError, Cancel, Fail
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
        self._last_claim_full = False
        tasks = list(self.app.tasks)
        for queue in self.queues:
            async with self.app.engine.begin() as connection:
                await _cancel_due(connection, self.app.hooks, queue)
                busy = await count_running(connection, self.id, queue)
                limit = self.slots - busy
                claimed = await _claim_queue(
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
        await self._wait(stop, poll_delay(self.poll_interval.total_seconds(), random.random()))

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
                    return
                await self._record(job, exc, None, policy)
            else:
                if _run.done:
                    return
                await self._record(job, None, result, policy)
        finally:
            reset_run(token)
            if self._inflight.get(job.id) is asyncio.current_task():
                self._inflight.pop(job.id, None)

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
                return
            except DB_ERRORS as error:
                if not is_transient_db_error(error):
                    await self._record_unstorable(job, error, policy)
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
        self, job: Job, error: BaseException, policy: BackoffPolicy | None
    ) -> None:
        reason = Fail(f"outcome could not be stored: {public_db_error(error)}")
        try:
            await _finish(self.app.engine, self.app.hooks, job, reason, None, policy)
        except DB_ERRORS as again:
            self._fatal = again
            logger.error(
                "job %s outcome could not be stored; stopping the worker: %s",
                job.id,
                public_db_error(again),
            )
        else:
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


_HEAD_FACTOR = 4


async def _claim_queue(
    connection: AsyncConnection,
    *,
    hooks: Hooks,
    worker_id: UUID,
    queue: str,
    tasks: list[str],
    limit: int,
    fair: bool,
) -> int:
    """Claim up to limit jobs, reading only the head of the queue when that is enough.

    In the default order, the eligible jobs ahead of any picked job lie in the
    head, so a head claim that fills ``limit`` picks what a full scan would; so
    does one whose head held every ready row. Otherwise (limits block the head,
    other claimers hold its rows) the head claim is rolled back to its
    savepoint, releasing its locks, and a full scan claims instead. Fair turns
    are not a prefix of the head, so fair claims always scan. Each claim first
    locks the limits rows of its candidates (see ``_lock_limits``). Transition
    hooks run once the claim is final.
    """

    if limit <= 0:
        return 0
    jobs: list[Job] = []
    if not fair:
        head = limit * _HEAD_FACTOR
        savepoint = await connection.begin_nested()
        locked = await _lock_limits(connection, queue=queue, tasks=tasks, head=head)
        jobs, ready_rows = await _claim(
            connection,
            worker_id=worker_id,
            queue=queue,
            tasks=tasks,
            limit=limit,
            locked=locked,
            head=head,
        )
        if len(jobs) == limit or 0 < ready_rows < limit * _HEAD_FACTOR:
            await savepoint.commit()
        else:
            await savepoint.rollback()
            jobs = []
    if not jobs:
        locked = await _lock_limits(connection, queue=queue, tasks=tasks, head=None)
        jobs, _ = await _claim(
            connection,
            worker_id=worker_id,
            queue=queue,
            tasks=tasks,
            limit=limit,
            locked=locked,
            fair=fair,
        )
    for job in jobs:
        await hooks.ran_transition(connection, job, "queued", "running")
        logger.info("job %s queued -> running", job.id)
    return len(jobs)


def _ready_sql(head: int | None) -> str:
    """Ready rows of :queue whose task is in :tasks, or only the top :head of them.

    A job whose task this worker does not know stays queued for a worker that
    does, so a rolling deploy does not spend its attempts.
    """

    head_sql = (
        """
        ORDER BY j.priority DESC, j.run_at, j.id
        LIMIT :head"""
        if head is not None
        else ""
    )
    return f"""
        SELECT j.id, j.limit_key, j.priority, j.run_at
        FROM akadze.jobs AS j
        WHERE j.state = 'queued'
          AND j.queue = :queue
          AND j.task = ANY(CAST(:tasks AS text[]))
          AND j.run_at <= now()
          AND j.cancel_requested_at IS NULL
          AND (j.expires_at IS NULL OR j.expires_at > now())
          AND NOT EXISTS (
              SELECT 1
              FROM akadze.queue_pauses
              WHERE queue_pauses.queue = :queue
          ){head_sql}"""


async def _lock_limits(
    connection: AsyncConnection, *, queue: str, tasks: list[str], head: int | None
) -> list[str]:
    """Lock the limits rows of the ready rows' keys and return those keys.

    This is its own statement because under READ COMMITTED a statement reads
    one snapshot, taken before it waits for a lock. The claim that follows
    starts after these locks are held, so it counts running jobs and start
    marks that other claimers of the same keys have committed.
    """

    rows = await connection.execute(
        text(
            f"""
            WITH ready AS ({_ready_sql(head)}
            )
            SELECT l.key
            FROM akadze.limits AS l
            WHERE l.key IN (
                SELECT DISTINCT ready.limit_key
                FROM ready
                WHERE ready.limit_key IS NOT NULL
            )
            ORDER BY l.key
            FOR UPDATE
            """
        ),
        {"queue": queue, "tasks": tasks, "head": head},
    )
    return [str(row[0]) for row in rows]


async def _claim(
    connection: AsyncConnection,
    *,
    worker_id: UUID,
    queue: str,
    tasks: list[str],
    limit: int,
    locked: list[str],
    fair: bool = False,
    head: int | None = None,
) -> tuple[list[Job], int]:
    """Claim up to limit ready jobs from queue in one statement.

    Order is priority, then run_at, then id. With ``fair``, jobs of the same
    priority go to the limit_key group (jobs without a key form one group)
    with the fewest jobs already running in this queue across the cluster,
    then the next: a job's turn is its group's running count plus its place
    in the group. One key with a deep backlog cannot hold every slot, across
    claims and across workers. With ``head``, only the top ``head`` ready rows
    are candidates. ``locked`` are the limit keys whose rows this transaction
    already holds; a candidate whose key has a limits row outside it waits for
    the next claim, so every limit is counted in a snapshot taken after its lock.

    Returns the claimed jobs and how many ready rows were candidates (0 when
    nothing was claimed). Transition hooks are the caller's.
    """

    if fair:
        fair_rn = """,
                    row_number() OVER (
                        PARTITION BY ranked.priority, ranked.limit_key
                        ORDER BY ranked.run_at, ranked.id
                    ) AS fair_rn"""
        fair_ctes = """
            group_load AS (
                SELECT jobs.limit_key, count(*)::int AS busy
                FROM akadze.jobs AS jobs
                WHERE jobs.state = 'running'
                  AND jobs.queue = :queue
                GROUP BY jobs.limit_key
            ),
            turns AS (
                SELECT eligible.id, eligible.fair_rn + coalesce(group_load.busy, 0) AS turn
                FROM eligible
                LEFT JOIN group_load
                    ON group_load.limit_key IS NOT DISTINCT FROM eligible.limit_key
                WHERE eligible.fair_rn <= :limit
            ),"""
        source = "turns"
        order = "job.priority DESC, turns.turn, job.run_at, job.id"
    else:
        fair_rn = fair_ctes = ""
        source = "eligible"
        order = "job.priority DESC, job.run_at, job.id"
    result = await connection.execute(
        text(
            f"""
            WITH ready AS ({_ready_sql(head)}
            ),
            locked_limits AS (
                SELECT l.key, l.max_running, l.max_starts, l."window" AS rate_window
                FROM akadze.limits AS l
                WHERE l.key = ANY(CAST(:locked AS text[]))
            ),
            running AS (
                SELECT jobs.limit_key, count(*)::int AS running_count
                FROM akadze.jobs AS jobs
                WHERE jobs.state = 'running'
                  AND jobs.limit_key IS NOT NULL
                  AND jobs.limit_key IN (SELECT locked_limits.key FROM locked_limits)
                GROUP BY jobs.limit_key
            ),
            starts AS (
                SELECT
                    marks.key AS limit_key,
                    count(*)::int AS start_count
                FROM akadze.limit_starts AS marks
                INNER JOIN locked_limits
                    ON locked_limits.key = marks.key
                WHERE locked_limits.max_starts IS NOT NULL
                  AND marks.started_at > now() - locked_limits.rate_window
                GROUP BY marks.key
            ),
            candidates AS (
                SELECT
                    ready.id,
                    ready.limit_key,
                    ready.priority,
                    ready.run_at,
                    locked_limits.max_running,
                    locked_limits.max_starts,
                    coalesce(running.running_count, 0) AS running_count,
                    coalesce(starts.start_count, 0) AS start_count
                FROM ready
                LEFT JOIN locked_limits
                    ON locked_limits.key = ready.limit_key
                LEFT JOIN running
                    ON running.limit_key = ready.limit_key
                LEFT JOIN starts
                    ON starts.limit_key = ready.limit_key
                WHERE ready.limit_key IS NULL
                   OR (
                        locked_limits.key IS NULL
                        AND NOT EXISTS (
                            SELECT 1
                            FROM akadze.limits AS policy
                            WHERE policy.key = ready.limit_key
                        )
                   )
                   OR (
                        locked_limits.key IS NOT NULL
                        AND (
                            locked_limits.max_running IS NULL
                            OR coalesce(running.running_count, 0)
                               < locked_limits.max_running
                        )
                        AND (
                            locked_limits.max_starts IS NULL
                            OR coalesce(starts.start_count, 0)
                               < locked_limits.max_starts
                        )
                   )
            ),
            ranked AS (
                SELECT
                    candidates.id,
                    candidates.limit_key,
                    candidates.max_running,
                    candidates.max_starts,
                    candidates.running_count,
                    candidates.start_count,
                    candidates.priority,
                    candidates.run_at,
                    CASE
                        WHEN candidates.limit_key IS NULL THEN 0
                        ELSE row_number() OVER (
                            PARTITION BY candidates.limit_key
                            ORDER BY candidates.priority DESC, candidates.run_at,
                                     candidates.id
                        )
                    END AS key_rn
                FROM candidates
            ),
            eligible AS (
                SELECT
                    ranked.id,
                    ranked.priority,
                    ranked.run_at,
                    ranked.limit_key{fair_rn}
                FROM ranked
                WHERE ranked.limit_key IS NULL
                   OR (
                        (
                            ranked.max_running IS NULL
                            OR ranked.running_count + ranked.key_rn
                               <= ranked.max_running
                        )
                        AND (
                            ranked.max_starts IS NULL
                            OR ranked.start_count + ranked.key_rn
                               <= ranked.max_starts
                        )
                   )
            ),{fair_ctes}
            picked AS (
                SELECT job.id
                FROM akadze.jobs AS job
                INNER JOIN {source} ON {source}.id = job.id
                WHERE job.state = 'queued'
                ORDER BY {order}
                LIMIT :limit
                FOR UPDATE OF job SKIP LOCKED
            ),
            claimed AS (
                UPDATE akadze.jobs AS job
                SET state = 'running',
                    run_count = job.run_count + 1,
                    worker_id = :worker_id,
                    started_at = now()
                FROM picked
                WHERE job.id = picked.id
                RETURNING {columns("job")}, false AS expired
            ),
            recorded AS (
                INSERT INTO akadze.limit_starts (key, started_at)
                SELECT claimed.limit_key, now()
                FROM claimed
                INNER JOIN locked_limits
                    ON locked_limits.key = claimed.limit_key
                WHERE claimed.limit_key IS NOT NULL
                  AND locked_limits.max_starts IS NOT NULL
                RETURNING key
            ),
            expired_marks AS (
                SELECT marks.id
                FROM akadze.limit_starts AS marks
                INNER JOIN locked_limits
                    ON locked_limits.key = marks.key
                WHERE locked_limits.rate_window IS NOT NULL
                  AND marks.started_at < now() - locked_limits.rate_window
                FOR UPDATE OF marks SKIP LOCKED
            ),
            pruned AS (
                DELETE FROM akadze.limit_starts AS marks
                USING expired_marks
                WHERE marks.id = expired_marks.id
                RETURNING marks.id
            )
            SELECT claimed.*, (SELECT count(*) FROM ready) AS ready_rows FROM claimed
            """
        ),
        {
            "queue": queue,
            "tasks": tasks,
            "limit": limit,
            "head": head,
            "worker_id": worker_id,
            "locked": locked,
        },
    )
    rows = result.mappings().all()
    ready_rows = int(rows[0]["ready_rows"]) if rows else 0
    return [job_from_mapping(row) for row in rows], ready_rows


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
    policy: BackoffPolicy | None = None,
) -> None:
    async with engine.begin() as connection:
        current = await job_by_id(connection, job.id)
        if not _owns(current, job):
            logger.info("job %s finish discarded", job.id)
            return
        if not await _apply(connection, hooks, current, exc, result, policy):
            logger.info("job %s finish discarded", job.id)


async def _apply(
    connection: AsyncConnection,
    hooks: Hooks,
    job: Job,
    exc: BaseException | None,
    result: object,
    policy: BackoffPolicy | None = None,
) -> bool:
    if job.cancel_requested_at is not None or job.expired:
        exc = Cancel()
        result = None
    outcome = decide(job, exc, result, policy)
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
