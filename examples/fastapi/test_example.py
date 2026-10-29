"""Runs the example against a real Postgres: ``pytest examples/fastapi``.

Uses ``AKADZE_DATABASE_URL`` and recreates schema ``akadze`` and the example
table in it, so point it at a test database.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
from main import api, app
from orders import metadata
from setup_db import main as setup_database
from sqlalchemy import text

from akadze.testing import assert_enqueued, drain


@pytest.fixture(autouse=True)
async def database() -> AsyncIterator[None]:
    async with app.engine.begin() as connection:
        await connection.execute(text("DROP SCHEMA IF EXISTS akadze CASCADE"))
        await connection.run_sync(metadata.drop_all)
    await setup_database()
    yield
    await app.aclose()


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=api)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as opened:
        yield opened


async def test_an_order_is_confirmed_by_the_worker(client: httpx.AsyncClient) -> None:
    # Arrange
    created = (await client.post("/orders", json={"email": "ada@example.com"})).json()
    await assert_enqueued(app, "orders.confirm", order_id=created["id"])

    # Act
    await drain(app)

    # Assert
    order = (await client.get(f"/orders/{created['id']}")).json()
    assert order["status"] == "confirmed"
