"""CLI entrypoints: worker, beat, migrate."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import signal
import sys

from akadze.__about__ import __version__
from akadze.app import Akadze
from akadze.schema import MigrateError, applied_versions, migrate, redact_database_url
from akadze.worker import Worker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="akadze", description="Postgres-backed task queue")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")

    worker = sub.add_parser("worker", help="Claim and run registered tasks")
    worker.add_argument("app", help="Import path of an Akadze app, for example demo:app")
    worker.add_argument("--slots", type=int, default=1)
    worker.add_argument("--queue", action="append", dest="queues")
    sub.add_parser("beat", help="Unused: periodic tasks run inside the worker")
    sub.add_parser("migrate", help="Apply schema migrations")

    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    if args.command == "migrate":
        return _run_migrate()
    if args.command == "beat":
        print(
            "periodic tasks run inside `akadze worker`; there is no beat process",
            file=sys.stderr,
        )
        return 1
    if args.command == "worker":
        return _run_worker(args.app, slots=args.slots, queues=args.queues)

    print(f"akadze {__version__}: `{args.command}` is not implemented yet.", file=sys.stderr)
    return 1


async def _newly_applied(database_url: str) -> list[str]:
    before = set(await applied_versions(database_url))
    await migrate(database_url)
    after = await applied_versions(database_url)
    return [version for version in after if version not in before]


def _run_migrate() -> int:
    database_url = os.environ.get("AKADZE_DATABASE_URL", "").strip()
    if not database_url:
        print("AKADZE_DATABASE_URL is not set", file=sys.stderr)
        return 2
    try:
        applied = asyncio.run(_newly_applied(database_url))
    except (MigrateError, ValueError, OSError) as exc:
        print(f"migrate failed: {redact_database_url(str(exc), database_url)}", file=sys.stderr)
        return 1
    if applied:
        for version in applied:
            print(f"applied {version}")
    else:
        print("schema up to date")
    return 0


def _run_worker(spec: str, *, slots: int, queues: list[str] | None) -> int:
    app = _load_app(spec)
    worker = Worker(app, slots=slots, queues=queues)

    async def _run() -> None:
        try:
            await _serve(worker)
        finally:
            await app.aclose()

    try:
        asyncio.run(_run())
    except (ValueError, OSError) as exc:
        print(f"worker failed: {exc}", file=sys.stderr)
        return 1
    return 0


async def _serve(worker: Worker) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await worker.serve(stop)


def _load_app(spec: str) -> Akadze:
    module_name, separator, attribute = spec.partition(":")
    if separator != ":" or not module_name or not attribute:
        print("app must look like package.module:app", file=sys.stderr)
        raise SystemExit(2)
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    module = importlib.import_module(module_name)
    app = getattr(module, attribute, None)
    if not isinstance(app, Akadze):
        print(f"{spec} is not an Akadze app", file=sys.stderr)
        raise SystemExit(2)
    return app


if __name__ == "__main__":
    raise SystemExit(main())
