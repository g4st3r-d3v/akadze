"""A job row. Queries only read."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection

_COLUMN_NAMES = (
    "id",
    "task",
    "queue",
    "priority",
    "state",
    "args",
    "run_count",
    "attempt",
    "max_attempts",
    "snoozes",
    "unique_key",
    "worker_id",
    "errors",
    "result",
    "cancel_requested_at",
    "expires_at",
)
_COLUMNS = ", ".join(_COLUMN_NAMES)


def columns(alias: str) -> str:
    return ", ".join(f"{alias}.{name}" for name in _COLUMN_NAMES)


@dataclass(frozen=True)
class Job:
    id: int
    task: str
    queue: str
    priority: int
    state: str
    args: dict[str, Any]
    run_count: int
    attempt: int
    max_attempts: int
    snoozes: int
    unique_key: str | None
    worker_id: UUID | None
    errors: list[dict[str, str]]
    result: Any
    cancel_requested_at: datetime | None
    expires_at: datetime | None
    expired: bool = False


def _decode_json(value: Any) -> Any:
    if isinstance(value, str) and value[:1] in "{[":
        return json.loads(value)
    return value


def job_from_mapping(row: RowMapping) -> Job:
    args = _decode_json(row["args"])
    errors = _decode_json(row["errors"])
    result = _decode_json(row["result"])
    worker_id = row["worker_id"]
    expired = bool(row["expired"]) if "expired" in row else False
    return Job(
        id=int(row["id"]),
        task=str(row["task"]),
        queue=str(row["queue"]),
        priority=int(row["priority"]),
        state=str(row["state"]),
        args=dict(args),
        run_count=int(row["run_count"]),
        attempt=int(row["attempt"]),
        max_attempts=int(row["max_attempts"]),
        snoozes=int(row["snoozes"]),
        unique_key=None if row["unique_key"] is None else str(row["unique_key"]),
        worker_id=None if worker_id is None else UUID(str(worker_id)),
        errors=list(errors),
        result=result,
        cancel_requested_at=row["cancel_requested_at"],
        expires_at=row["expires_at"],
        expired=expired,
    )


async def running_jobs(connection: AsyncConnection, worker_id: UUID) -> list[Job]:
    rows = await connection.execute(
        text(
            f"""
            SELECT {_COLUMNS},
                   (expires_at IS NOT NULL AND expires_at <= now()) AS expired
            FROM akadze.jobs
            WHERE state = 'running' AND worker_id = :worker_id
            ORDER BY id
            """
        ),
        {"worker_id": worker_id},
    )
    return [job_from_mapping(row) for row in rows.mappings()]


async def count_running(connection: AsyncConnection, worker_id: UUID, queue: str) -> int:
    count = await connection.scalar(
        text(
            """
            SELECT count(*)
            FROM akadze.jobs
            WHERE state = 'running' AND worker_id = :worker_id AND queue = :queue
            """
        ),
        {"worker_id": worker_id, "queue": queue},
    )
    return int(count or 0)


async def job_by_id(connection: AsyncConnection, job_id: int) -> Job | None:
    result = await connection.execute(
        text(
            f"""
            SELECT {_COLUMNS},
                   (expires_at IS NOT NULL AND expires_at <= now()) AS expired
            FROM akadze.jobs
            WHERE id = :id
            """
        ),
        {"id": job_id},
    )
    row = result.mappings().first()
    if row is None:
        return None
    return job_from_mapping(row)
