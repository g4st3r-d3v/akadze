"""Write run outcomes in batches: one transaction for many finished jobs."""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from akadze.hooks import Hooks
from akadze.job import _COLUMNS, Job, columns, job_from_mapping, owns, run_hot
from akadze.outcome import BackoffPolicy, decide, push_error
from akadze.ready import expired

logger = logging.getLogger("akadze")

# A full buffer is written at once instead of waiting for the interval.
MAX_BATCH = 200

_CURRENT = text(
    f"""
    SELECT {_COLUMNS},
           {expired("jobs")} AS expired
    FROM akadze.jobs
    WHERE id = ANY(CAST(:ids AS bigint[]))
    """
)


def _apply_many(returning: str) -> Any:
    return text(
        f"""
    UPDATE akadze.jobs AS jobs
    SET state = batch.state,
        attempt = batch.attempt,
        snoozes = batch.snoozes,
        run_at = coalesce(now() + batch.delay, jobs.run_at),
        worker_id = CASE WHEN batch.state = 'queued' THEN NULL ELSE jobs.worker_id END,
        started_at = CASE WHEN batch.state = 'queued' THEN NULL ELSE jobs.started_at END,
        finished_at = CASE WHEN batch.state = 'queued' THEN NULL ELSE now() END,
        errors = CAST(batch.errors AS jsonb),
        result = CAST(batch.result AS jsonb)
    FROM unnest(
        CAST(:ids AS bigint[]),
        CAST(:run_counts AS integer[]),
        CAST(:states AS text[]),
        CAST(:attempts AS integer[]),
        CAST(:snoozes AS integer[]),
        CAST(:delays AS interval[]),
        CAST(:errors AS text[]),
        CAST(:results AS text[])
    ) AS batch(id, run_count, state, attempt, snoozes, delay, errors, result)
    WHERE jobs.id = batch.id
      AND jobs.run_count = batch.run_count
      AND jobs.state = 'running'
    RETURNING {returning}
    """
    )


# Full rows only when hooks need them; the log line needs the id and state.
_APPLY_MANY = _apply_many(f"{columns('jobs')}, false AS expired")
_APPLY_MANY_SLIM = _apply_many("jobs.id, jobs.state")


class _Unbatchable(Exception):
    """This outcome goes to the single path (its result cannot be stored as jsonb)."""


@dataclass
class _Pending:
    job: Job
    exc: BaseException | None
    result: object
    policy: BackoffPolicy | None
    done: asyncio.Future[None] = field(
        default_factory=lambda: asyncio.get_running_loop().create_future()
    )


class OutcomeBatch:
    """Collects finished runs and writes them together.

    A batch is written as soon as every run in flight is waiting in it (no
    other run could join), when 200 wait, or after ``delay`` at the latest.
    Each run waits for its batch to commit, so a run is in flight until its
    outcome is written, as without batching. If a batch fails, every run in
    it gets the error and writes its outcome on its own, which keeps the
    per-job handling of transient errors and unstorable results.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        hooks: Hooks,
        delay: timedelta,
        in_flight: Callable[[], int],
    ) -> None:
        self.engine = engine
        self.hooks = hooks
        self.delay = delay.total_seconds()
        self._in_flight = in_flight
        self._pending: list[_Pending] = []
        self._flusher: asyncio.Task[None] | None = None
        self._full: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def flush_now(self) -> None:
        """Write what waits without waiting for the interval (shutdown, stale heartbeat)."""

        if self._full is not None:
            self._full.set()

    def _bind_loop(self) -> asyncio.Event:
        loop = asyncio.get_running_loop()
        if self._full is None or self._loop is not loop:
            # A worker reused in a new event loop: the old loop's state is gone.
            self._loop, self._full, self._flusher, self._pending = loop, asyncio.Event(), None, []
        return self._full

    async def record(
        self,
        job: Job,
        exc: BaseException | None,
        result: object,
        policy: BackoffPolicy | None,
    ) -> None:
        """Wait until this run's outcome is written; raise what failed its batch."""

        full = self._bind_loop()
        pending = _Pending(job, exc, result, policy)
        self._pending.append(pending)
        if len(self._pending) >= min(MAX_BATCH, max(self._in_flight(), 1)):
            full.set()
        if self._flusher is None or self._flusher.done():
            # A fresh context: hooks of every row must not see this run's contextvars.
            self._flusher = asyncio.create_task(self._flush_soon(), context=contextvars.Context())
        await asyncio.shield(pending.done)

    async def _flush_soon(self) -> None:
        full = self._bind_loop()
        try:
            try:
                await asyncio.wait_for(full.wait(), self.delay)
            except TimeoutError:
                pass
            while self._pending:
                batch, self._pending = self._pending[:MAX_BATCH], self._pending[MAX_BATCH:]
                full.clear()
                try:
                    await self._write(batch)
                except Exception as exc:
                    _settle(batch, exc)
                else:
                    _settle(batch, None)
        except BaseException as exc:
            # Cancelled: nothing else will write these, so let each run write its own.
            _settle(self._pending, exc)
            self._pending = []
            raise

    async def _write(self, batch: list[_Pending]) -> None:
        async with self.engine.begin() as connection:
            await self._apply(connection, batch)

    async def _apply(self, connection: AsyncConnection, batch: list[_Pending]) -> None:
        batch.sort(key=lambda pending: pending.job.id)  # one lock order for every batch
        found = await run_hot(connection, _CURRENT, {"ids": [p.job.id for p in batch]})
        current = {job.id: job for job in map(job_from_mapping, found.mappings())}
        values: dict[str, list[Any]] = {
            name: []
            for name in (
                "ids",
                "run_counts",
                "states",
                "attempts",
                "snoozes",
                "delays",
                "errors",
                "results",
            )
        }
        before: dict[int, Job] = {}
        for pending in batch:
            job = current.get(pending.job.id)
            if not owns(job, pending.job):
                logger.info("job %s finish discarded", pending.job.id)
                continue
            outcome = decide(job, pending.exc, pending.result, pending.policy)
            if outcome.result_json is not None and "\\u0000" in outcome.result_json:
                # jsonb rejects NUL: it would fail the whole batch.
                pending.done.set_exception(_Unbatchable())
                continue
            before[job.id] = job
            values["ids"].append(job.id)
            values["run_counts"].append(job.run_count)
            values["states"].append(outcome.state)
            values["attempts"].append(outcome.attempt)
            values["snoozes"].append(outcome.snoozes)
            values["delays"].append(outcome.delay)
            values["errors"].append(json.dumps(push_error(list(job.errors), outcome)))
            values["results"].append(outcome.result_json)
        if not before:
            return
        if not self.hooks.on_transition:
            slim = await run_hot(connection, _APPLY_MANY_SLIM, values)
            for row in slim.mappings():
                logger.info("job %s running -> %s", row["id"], row["state"])
            return
        updated = await run_hot(connection, _APPLY_MANY, values)
        for row in updated.mappings():
            finished = job_from_mapping(row)
            old = before[finished.id]
            await self.hooks.ran_transition(connection, finished, old.state, finished.state)
            logger.info("job %s %s -> %s", finished.id, old.state, finished.state)


def _settle(batch: list[_Pending], exc: BaseException | None) -> None:
    for pending in batch:
        if pending.done.done():
            continue
        if exc is None:
            pending.done.set_result(None)
        else:
            pending.done.set_exception(exc)
