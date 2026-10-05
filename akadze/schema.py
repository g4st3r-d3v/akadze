"""Versioned SQL migrations for the akadze schema."""

from __future__ import annotations

import re
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
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
    return re.sub(r"://[^\s/@]+:[^\s/@]+@", "://***@", redacted)


async def migrate(database_url: str) -> list[str]:
    """Apply pending migrations. Return the version ids applied in this call."""

    engine = create_async_engine(
        async_database_url(database_url),
        poolclass=NullPool,
        connect_args={"statement_cache_size": 0},
    )
    try:
        async with engine.begin() as conn:
            return await _migrate_locked(conn)
    except SQLAlchemyError as exc:
        raise MigrateError(redact_database_url(str(exc), database_url)) from None
    finally:
        await engine.dispose()


async def _migrate_locked(conn: AsyncConnection) -> list[str]:
    await conn.execute(text(_LOCK_SQL))
    for statement in _BOOTSTRAP_SQL:
        await conn.execute(text(statement))
    rows = await conn.execute(text("SELECT version FROM akadze.schema_migrations"))
    done = {str(row[0]) for row in rows}
    applied: list[str] = []
    for version, script in _migration_scripts():
        if version in done:
            continue
        for statement in split_sql(script):
            await conn.execute(text(statement))
        await conn.execute(
            text("INSERT INTO akadze.schema_migrations (version) VALUES (:version)"),
            {"version": version},
        )
        applied.append(version)
    return applied


def _migration_scripts() -> list[tuple[str, str]]:
    scripts = [
        (path.stem, path.read_text(encoding="utf-8")) for path in sorted(_MIGRATIONS.glob("*.sql"))
    ]
    if not scripts:
        raise MigrateError("no schema migrations packaged")
    return scripts


def split_sql(script: str) -> list[str]:
    """Split SQL on semicolons. Understands quotes and comments, not dollar quotes."""

    statements: list[str] = []
    buf: list[str] = []
    i = 0
    length = len(script)
    in_single = False
    in_double = False
    in_line = False
    in_block = False
    while i < length:
        char = script[i]
        nxt = script[i + 1] if i + 1 < length else ""
        if in_line:
            buf.append(char)
            if char == "\n":
                in_line = False
            i += 1
            continue
        if in_block:
            buf.append(char)
            if char == "*" and nxt == "/":
                buf.append(nxt)
                in_block = False
                i += 2
                continue
            i += 1
            continue
        if in_single:
            buf.append(char)
            if char == "'" and nxt == "'":
                buf.append(nxt)
                i += 2
                continue
            if char == "'":
                in_single = False
            i += 1
            continue
        if in_double:
            buf.append(char)
            if char == '"':
                in_double = False
            i += 1
            continue
        if char == "-" and nxt == "-":
            buf.extend("--")
            in_line = True
            i += 2
            continue
        if char == "/" and nxt == "*":
            buf.extend("/*")
            in_block = True
            i += 2
            continue
        if char == "'":
            in_single = True
            buf.append(char)
            i += 1
            continue
        if char == '"':
            in_double = True
            buf.append(char)
            i += 1
            continue
        if char == ";":
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
            i += 1
            continue
        buf.append(char)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements
