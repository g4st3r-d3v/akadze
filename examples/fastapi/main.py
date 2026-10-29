"""FastAPI service that enqueues a job in the same transaction as its own write.

Run the API with ``uvicorn main:api`` and the worker with
``akadze worker main:app --queue default --queue mail``.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from orders import orders
from pydantic import BaseModel
from sqlalchemy import insert, select

from akadze import Akadze

DATABASE_URL = os.environ.get(
    "AKADZE_DATABASE_URL", "postgresql://akadze:akadze@localhost:5432/akadze"
)

app = Akadze(database_url=DATABASE_URL)
app.include("tasks")


class NewOrder(BaseModel):
    email: str


class Order(BaseModel):
    id: int
    email: str
    status: str


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await app.aclose()


api = FastAPI(lifespan=lifespan)


@api.post("/orders", status_code=201)
async def create_order(body: NewOrder) -> Order:
    async with app.engine.begin() as connection:
        order_id = await connection.scalar(
            insert(orders).values(email=body.email).returning(orders.c.id)
        )
        assert order_id is not None
        # Same transaction: if anything below fails, neither the order nor the job exists.
        await app.tasks["orders.confirm"].using(session=connection).enqueue(order_id=order_id)
    return Order(id=order_id, email=body.email, status="new")


@api.get("/orders/{order_id}")
async def read_order(order_id: int) -> Order:
    async with app.engine.connect() as connection:
        row = (await connection.execute(select(orders).where(orders.c.id == order_id))).first()
    if row is None:
        raise HTTPException(status_code=404)
    return Order(id=row.id, email=row.email, status=row.status)
