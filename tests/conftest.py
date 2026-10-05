from __future__ import annotations

import os
from collections.abc import AsyncIterator

import asyncpg
import pytest


@pytest.fixture
async def database_url() -> AsyncIterator[str]:
    url = os.environ.get("AKADZE_DATABASE_URL", "").strip()
    if not url:
        pytest.fail("AKADZE_DATABASE_URL is required; tests run against real Postgres")
    await _drop_schema(url)
    yield url
    await _drop_schema(url)


async def _drop_schema(url: str) -> None:
    conn = await asyncpg.connect(url)
    try:
        await conn.execute("DROP SCHEMA IF EXISTS akadze CASCADE")
    finally:
        await conn.close()
