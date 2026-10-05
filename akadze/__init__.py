"""Postgres task queue. The broker is PostgreSQL."""

from akadze.schema import migrate

__all__ = ["migrate"]
