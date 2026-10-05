"""CLI entrypoints: worker, beat, migrate."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from akadze.__about__ import __version__
from akadze.schema import MigrateError, migrate, redact_database_url


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="akadze", description="Postgres-backed task queue")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("worker", help="Run a worker loop (claim + execute tasks)")
    sub.add_parser("beat", help="Run the scheduler (enqueue due periodic tasks)")
    sub.add_parser("migrate", help="Apply schema migrations")

    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    if args.command == "migrate":
        return _run_migrate()

    print(
        f"akadze {__version__}: `{args.command}` is not implemented yet.",
        file=sys.stderr,
    )
    return 1


def _run_migrate() -> int:
    database_url = os.environ.get("AKADZE_DATABASE_URL", "").strip()
    if not database_url:
        print("AKADZE_DATABASE_URL is not set", file=sys.stderr)
        return 2
    try:
        applied = asyncio.run(migrate(database_url))
    except (MigrateError, ValueError, OSError) as exc:
        print(f"migrate failed: {redact_database_url(str(exc), database_url)}", file=sys.stderr)
        return 1
    if applied:
        for version in applied:
            print(f"applied {version}")
    else:
        print("schema up to date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
