"""Versioned SQL migrations for the akadze schema."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

import asyncpg  # type: ignore[import-untyped]
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, InterfaceError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

_MIGRATIONS = Path(__file__).resolve().parent / "sql"
_PASSWORD_IN_URL = re.compile(r"://[^\s/@]+:[^\s/@]+@")
_ERROR_MESSAGE_LIMIT = 200
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
    redacted = _PASSWORD_IN_URL.sub("://***@", redacted)
    password = urlparse(database_url).password
    if password is not None and len(password) >= 4:
        redacted = redacted.replace(password, "***")
    return redacted


DB_ERRORS: tuple[type[BaseException], ...] = (
    SQLAlchemyError,
    OSError,
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
    asyncpg.exceptions.InternalClientError,
)
"""Errors that come from the database or the connection to it.

Raw asyncpg errors are listed because SQLAlchemy 2.0 does not translate the
ones raised while opening a connection (for example "the database system is
starting up").
"""

# Connection failure, shutdown or restart, too many connections, serialization
# failure, deadlock: running the same statement again can succeed.
_TRANSIENT_SQLSTATES = ("08", "57P", "53300", "40001", "40P01")


def is_transient_db_error(exc: BaseException) -> bool:
    """Whether a database error can go away when the statement is run again."""

    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return True
    for error in _error_chain(exc):
        if isinstance(error, OSError | asyncpg.InterfaceError | InterfaceError):
            return True
        if isinstance(error, asyncpg.exceptions.InternalClientError):
            return True
        sqlstate = getattr(error, "sqlstate", None)
        if isinstance(sqlstate, str) and sqlstate.startswith(_TRANSIENT_SQLSTATES):
            return True
    return False


def public_db_error(exc: BaseException) -> str:
    """Describe a database error for a log line: no SQL, no parameters, no password.

    Transient errors keep the first line of the driver message, which says what
    happened to the connection. Other errors keep only their name and SQLSTATE,
    because driver messages can quote the values that were sent.
    """

    name = type(exc).__name__
    if not is_transient_db_error(exc):
        sqlstate = next(
            (
                code
                for code in (getattr(error, "sqlstate", None) for error in _error_chain(exc))
                if isinstance(code, str)
            ),
            None,
        )
        return f"{name} (SQLSTATE {sqlstate})" if sqlstate else name
    cause = exc.orig if isinstance(exc, DBAPIError) and exc.orig is not None else exc
    detail = str(cause).strip()
    first = detail.splitlines()[0] if detail else ""
    first = _PASSWORD_IN_URL.sub("://***@", first)[:_ERROR_MESSAGE_LIMIT]
    return f"{name}: {first}" if first else name


def _error_chain(exc: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in chain and len(chain) < 5:
        chain.append(current)
        orig = current.orig if isinstance(current, DBAPIError) else None
        current = orig if orig is not None else current.__cause__
    return chain


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
