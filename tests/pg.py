from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg


@asynccontextmanager
async def connection(database_url: str) -> AsyncIterator[asyncpg.Connection]:
    opened = await asyncpg.connect(database_url)
    try:
        yield opened
    finally:
        await opened.close()


async def job_row(database_url: str, task: str) -> asyncpg.Record | None:
    async with connection(database_url) as opened:
        return await opened.fetchrow("SELECT * FROM akadze.jobs WHERE task = $1", task)


async def job_rows(database_url: str) -> list[asyncpg.Record]:
    async with connection(database_url) as opened:
        return list(await opened.fetch("SELECT * FROM akadze.jobs ORDER BY id"))


def as_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value
