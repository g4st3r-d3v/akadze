"""Shared fixtures for the akadze suite.

Pattern (see also tests/README.md):
- Arrange with factories / domain helpers (`insert_job`, `apply_limit`, `pause`).
- Act once (one command or one query under test).
- Assert on domain state — never mock Postgres / the SQLAlchemy engine.
- Use pytest.mark.parametrize for state/filter/limit edges; pytest-mock only
  for non-DB boundaries (hooks, clocks, etc.).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from urllib.parse import urlparse

import asyncpg
import pytest
from faker import Faker

from akadze import Akadze, migrate
from tests.factories import fake as shared_fake


@pytest.fixture
def fake() -> Iterator[Faker]:
    """Process-wide Faker with unique cleared between tests."""

    shared_fake.unique.clear()
    yield shared_fake


@pytest.fixture
async def database_url() -> AsyncIterator[str]:
    url = os.environ.get("AKADZE_DATABASE_URL", "").strip()
    if not url:
        pytest.fail("AKADZE_DATABASE_URL is required; tests run against real Postgres")
    _assert_disposable(url)
    await _drop_schema(url)
    yield url
    await _drop_schema(url)


def _assert_disposable(url: str) -> None:
    name = urlparse(url).path.lstrip("/").split("/", 1)[0]
    if name == "akadze" or name.endswith("_test"):
        return
    pytest.fail(
        "Refusing to drop schema akadze in database "
        f"{name!r}. Use a database named akadze or ending with _test."
    )


@pytest.fixture
async def app(database_url: str) -> AsyncIterator[Akadze]:
    await migrate(database_url)
    application = Akadze(database_url=database_url)
    try:
        yield application
    finally:
        await application.aclose()


async def _drop_schema(url: str) -> None:
    conn = await asyncpg.connect(url)
    try:
        await conn.execute("DROP SCHEMA IF EXISTS akadze CASCADE")
    finally:
        await conn.close()
