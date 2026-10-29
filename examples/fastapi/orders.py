"""The application's own table. akadze keeps its tables in schema ``akadze``."""

from __future__ import annotations

from sqlalchemy import Column, Integer, MetaData, String, Table

metadata = MetaData()

orders = Table(
    "example_orders",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("email", String, nullable=False),
    Column("status", String, nullable=False, server_default="new"),
)
