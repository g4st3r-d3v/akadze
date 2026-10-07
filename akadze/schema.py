"""Versioned SQL migrations for the akadze schema."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

_MIGRATIONS = Path(__file__).resolve().parent / "sql"
_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext('akadze.migrate'))"
_BOOTSTRAP_SQL = (
    "CREATE SCHEMA IF NOT EXISTS akadze",
    """
    CREATE TABLE IF NOT EXISTS akadze.schema_migrations (
        version text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
    )
    """,
)


class MigrateError(Exception):
    """Migration failed. The message does not include the database URL."""


def async_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql+asyncpg://"):
        return database_url
    if database_url.startswith("postgresql://"):
        return "postgresql+asyncpg://" + database_url.removeprefix("postgresql://")
    if database_url.startswith("postgres://"):
        return "postgresql+asyncpg://" + database_url.removeprefix("postgres://")
    raise ValueError("database URL must use the postgresql scheme")


def redact_database_url(message: str, database_url: str) -> str:
    redacted = message.replace(database_url, "postgresql://***")
    try:
        redacted = redacted.replace(async_database_url(database_url), "postgresql://***")
    except ValueError:
        pass
    redacted = re.sub(r"://[^\s/@]+:[^\s/@]+@", "://***@", redacted)
    password = urlparse(database_url).password
    if password is not None and len(password) >= 4:
        redacted = redacted.replace(password, "***")
    return redacted


def _engine(database_url: str) -> AsyncEngine:
    return create_async_engine(
        async_database_url(database_url),
        poolclass=NullPool,
        connect_args={"statement_cache_size": 0},
    )


async def migrate(database_url: str) -> None:
    """Apply pending migrations. This command does not return database state."""

    scripts = _migration_scripts()
    _reject_dollar_quotes(scripts)
    async with _connection(database_url) as conn:
        async with conn.begin():
            await _apply(conn, scripts)


async def applied_versions(database_url: str) -> list[str]:
    """Return applied migration ids. This query does not write."""

    async with _connection(database_url) as conn:
        async with conn.begin():
            await conn.execute(text("SET TRANSACTION READ ONLY"))
            return await _versions(conn)


@asynccontextmanager
async def _connection(database_url: str) -> AsyncIterator[AsyncConnection]:
    engine = _engine(database_url)
    try:
        try:
            async with engine.connect() as conn:
                yield conn
        except (SQLAlchemyError, OSError) as exc:
            raise MigrateError(_public_db_error(exc, database_url)) from None
    finally:
        await engine.dispose()


async def _apply(conn: AsyncConnection, scripts: list[tuple[str, str]]) -> None:
    await conn.execute(text(_LOCK_SQL))
    for statement in _BOOTSTRAP_SQL:
        await conn.execute(text(statement))
    done = set(await _versions(conn))
    for version, script in scripts:
        if version in done:
            continue
        for statement in _statements(script):
            await conn.execute(text(statement))
        await conn.execute(
            text("INSERT INTO akadze.schema_migrations (version) VALUES (:version)"),
            {"version": version},
        )


async def _versions(conn: AsyncConnection) -> list[str]:
    exists = await conn.scalar(
        text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'akadze'
                  AND table_name = 'schema_migrations'
            )
            """
        )
    )
    if not exists:
        return []
    rows = await conn.execute(
        text("SELECT version FROM akadze.schema_migrations ORDER BY version")
    )
    return [str(row[0]) for row in rows]


def _migration_scripts() -> list[tuple[str, str]]:
    scripts = [
        (path.stem, path.read_text(encoding="utf-8")) for path in sorted(_MIGRATIONS.glob("*.sql"))
    ]
    if not scripts:
        raise MigrateError("no schema migrations packaged")
    return scripts


def _reject_dollar_quotes(scripts: list[tuple[str, str]]) -> None:
    for version, script in scripts:
        if "$$" in script:
            raise MigrateError(f"migration {version} contains dollar quotes")


def _public_db_error(exc: BaseException, database_url: str) -> str:
    message = str(exc).strip() or type(exc).__name__
    return redact_database_url(message, database_url)


def _statements(script: str) -> list[str]:
    return [statement.strip() for statement in script.split(";") if statement.strip()]
