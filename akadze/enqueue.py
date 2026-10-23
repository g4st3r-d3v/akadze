"""Insert a job into the caller's transaction."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from akadze.exc import AkadzeError, DuplicateJob, EnqueueError
from akadze.hooks import Hooks
from akadze.job import _COLUMNS, job_from_mapping

_ACTIVE_UNIQUE_KEY = "jobs_unique_key_active_idx"

_PRIORITY_MIN = -32768
_PRIORITY_MAX = 32767
_MAX_ATTEMPTS_MAX = 2147483647
_BATCH_ROWS = 1000
_META_MAX_BYTES = 8192


async def insert_job(
    session: AsyncConnection | AsyncSession,
    *,
    task: str,
    queue: str,
    priority: int,
    max_attempts: int,
    args: dict[str, Any],
    unique_key: str | None,
    limit_key: str | None,
    delay: timedelta | None,
    run_at: datetime | None,
    expires_at: datetime | None,
    hooks: Hooks,
    meta: Mapping[str, Any] | None = None,
) -> None:
    meta_json = dump_meta(meta)
    check_options(priority, max_attempts, delay, run_at, expires_at)
    connection = await _connection(session)
    run_at_sql = _run_at_sql(delay, run_at)
    statement = text(
        f"""
        INSERT INTO akadze.jobs (
            task, queue, priority, state, args, max_attempts, run_at, unique_key,
            limit_key, expires_at, meta
        )
        VALUES (
            :task, :queue, :priority, 'queued', CAST(:args AS jsonb), :max_attempts,
            {run_at_sql}, :unique_key, :limit_key, :expires_at, CAST(:meta AS jsonb)
        )
        RETURNING {_COLUMNS}, false AS expired
        """
    )
    parameters = {
        "task": task,
        "queue": queue,
        "priority": priority,
        "args": json.dumps(args),
        "max_attempts": max_attempts,
        "delay": delay,
        "run_at": run_at,
        "unique_key": unique_key,
        "limit_key": limit_key or None,
        "expires_at": expires_at,
        "meta": meta_json,
    }
    try:
        async with connection.begin_nested():
            result = await connection.execute(statement, parameters)
            row = result.mappings().one()
            await hooks.ran_enqueue(connection, job_from_mapping(row))
    except IntegrityError as exc:
        if _ACTIVE_UNIQUE_KEY not in str(exc.orig):
            raise
        raise DuplicateJob("unique_key is already active") from None


async def insert_jobs(
    session: AsyncConnection | AsyncSession,
    *,
    task: str,
    queue: str,
    priority: int,
    max_attempts: int,
    args: Sequence[dict[str, Any]],
    limit_key: str | None,
    delay: timedelta | None,
    run_at: datetime | None,
    expires_at: datetime | None,
    hooks: Hooks,
    meta: Mapping[str, Any] | None = None,
) -> None:
    """Insert many jobs with shared options in the caller's transaction, in order."""

    meta_json = dump_meta(meta)
    check_options(priority, max_attempts, delay, run_at, expires_at)
    connection = await _connection(session)
    if not args:
        return
    returning = f"RETURNING {_COLUMNS}, false AS expired" if hooks.on_enqueue else ""
    statement = text(
        f"""
        INSERT INTO akadze.jobs (
            task, queue, priority, state, args, max_attempts, run_at, limit_key, expires_at,
            meta
        )
        SELECT :task, :queue, :priority, 'queued', item.value, :max_attempts,
               {_run_at_sql(delay, run_at)}, :limit_key, :expires_at, CAST(:meta AS jsonb)
        FROM jsonb_array_elements(CAST(:args AS jsonb)) WITH ORDINALITY AS item (value, position)
        ORDER BY item.position
        {returning}
        """
    )
    shared = {
        "task": task,
        "queue": queue,
        "priority": priority,
        "max_attempts": max_attempts,
        "delay": delay,
        "run_at": run_at,
        "limit_key": limit_key or None,
        "expires_at": expires_at,
        "meta": meta_json,
    }
    async with connection.begin_nested():
        for start in range(0, len(args), _BATCH_ROWS):
            chunk = json.dumps(list(args[start : start + _BATCH_ROWS]))
            result = await connection.execute(statement, {**shared, "args": chunk})
            if not returning:
                continue
            rows = sorted(result.mappings().all(), key=lambda row: int(row["id"]))
            for row in rows:
                await hooks.ran_enqueue(connection, job_from_mapping(row))


def check_options(
    priority: int,
    max_attempts: int,
    delay: timedelta | None,
    run_at: datetime | None,
    expires_at: datetime | None,
) -> None:
    """Reject enqueue options that cannot form a valid job row. Raises EnqueueError."""

    if delay is not None and run_at is not None:
        raise EnqueueError("pass delay or run_at, not both")
    if delay is not None and delay < timedelta(0):
        raise EnqueueError("delay must not be negative")
    if run_at is not None and run_at.tzinfo is None:
        raise EnqueueError("run_at must be timezone-aware")
    if expires_at is not None and expires_at.tzinfo is None:
        raise EnqueueError("expires_at must be timezone-aware")
    if not _PRIORITY_MIN <= priority <= _PRIORITY_MAX:
        raise EnqueueError("priority must fit in smallint")
    if not 1 <= max_attempts <= _MAX_ATTEMPTS_MAX:
        raise EnqueueError("max_attempts must be between 1 and 2147483647")


def _run_at_sql(delay: timedelta | None, run_at: datetime | None) -> str:
    if delay is not None:
        return "now() + CAST(:delay AS interval)"
    if run_at is not None:
        return ":run_at"
    return "now()"


def dump_meta(meta: Mapping[str, Any] | None) -> str:
    """Validate job metadata and return it as JSON text. Raises EnqueueError.

    Postgres jsonb is the target: keys must be str at every level, and NaN,
    infinities, NUL characters and lone surrogates are rejected up front.
    """

    if meta is None:
        return "{}"
    if not isinstance(meta, Mapping) or not _str_keys(meta):
        raise EnqueueError("meta must be a mapping with str keys")
    try:
        encoded = json.dumps(dict(meta), separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        size = len(encoded.encode())
    except (TypeError, ValueError, UnicodeEncodeError):
        raise EnqueueError("meta must be JSON-serializable") from None
    if "\\u0000" in encoded:
        raise EnqueueError("meta must not contain NUL characters")
    if size > _META_MAX_BYTES:
        raise EnqueueError(f"meta must be at most {_META_MAX_BYTES} bytes as JSON")
    return encoded


def _str_keys(value: object) -> bool:
    if isinstance(value, Mapping):
        return all(isinstance(key, str) and _str_keys(item) for key, item in value.items())
    if isinstance(value, list | tuple):
        return all(_str_keys(item) for item in value)
    return True


async def request_cancel(session: AsyncConnection | AsyncSession, job_id: int) -> None:
    """Ask a queued or running job to stop. The worker applies the cancellation.

    A queued job is cancelled without running. A running job is not interrupted:
    if its body returns, it ends ``succeeded``; if the body raises, ``cancelled``.
    """

    connection = await _connection(session)
    await connection.execute(
        text(
            """
            UPDATE akadze.jobs
            SET cancel_requested_at = now()
            WHERE id = :id AND state IN ('queued', 'running')
            """
        ),
        {"id": job_id},
    )


async def requeue(session: AsyncConnection | AsyncSession, job_id: int) -> None:
    """Return a failed job to the queue. Keeps attempt, errors, and args."""

    connection = await _connection(session)
    statement = text(
        """
        UPDATE akadze.jobs
        SET state = 'queued',
            run_at = now(),
            worker_id = NULL,
            started_at = NULL,
            finished_at = NULL,
            cancel_requested_at = NULL,
            max_attempts = greatest(max_attempts, attempt + 1)
        WHERE id = :id AND state = 'failed'
        """
    )
    try:
        async with connection.begin_nested():
            result = await connection.execute(statement, {"id": job_id})
            if result.rowcount != 1:
                raise AkadzeError("job is not failed")
    except IntegrityError as exc:
        if _ACTIVE_UNIQUE_KEY not in str(exc.orig):
            raise
        raise DuplicateJob("unique_key is already active") from None


async def _connection(session: AsyncConnection | AsyncSession) -> AsyncConnection:
    if isinstance(session, AsyncConnection):
        connection = session
    elif isinstance(session, AsyncSession):
        connection = await session.connection()
    else:
        raise EnqueueError("session must be an AsyncConnection or AsyncSession")
    if not connection.in_transaction():
        raise EnqueueError("enqueue must run inside the caller's transaction")
    return connection
