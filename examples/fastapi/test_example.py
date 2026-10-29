"""Runs the example against a real Postgres: ``pytest`` in this directory.

Needs ``AKADZE_DATABASE_URL`` pointing at a database whose name ends in
``_test``: each test drops schema ``akadze`` and the example's
``example_orders`` table there and sets both up again.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from urllib.parse import urlparse

import httpx
import pytest
from sqlalchemy import text

if not urlparse(os.environ.get("AKADZE_DATABASE_URL", "")).path.endswith("_test"):
    pytest.exit("set AKADZE_DATABASE_URL to a database whose name ends in _test", returncode=2)

from main import api, app  # noqa: E402
from orders import metadata  # noqa: E402
from setup_db import main as setup_database  # noqa: E402

from akadze import list_jobs  # noqa: E402
from akadze.testing import assert_enqueued, drain  # noqa: E402


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


async def test_creating_an_order_enqueues_its_confirmation(client: httpx.AsyncClient) -> None:
    # Arrange — an empty queue

    # Act
    response = await client.post("/orders", json={"email": "ada@example.com"})

    # Assert
    assert response.status_code == 201
    await assert_enqueued(app, "orders.confirm", order_id=response.json()["id"])


async def test_the_worker_confirms_the_order_and_queues_a_receipt(
    client: httpx.AsyncClient,
) -> None:
    # Arrange
    created = (await client.post("/orders", json={"email": "ada@example.com"})).json()

    # Act
    await drain(app)

    # Assert
    order = (await client.get(f"/orders/{created['id']}")).json()
    assert order["status"] == "confirmed"
    receipts = await list_jobs(app.engine, queue="mail", state="succeeded")
    assert [job.task for job in receipts] == ["orders.receipt"]
