"""CLI entrypoints: worker, beat, migrate, jobs, queues."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import signal
import sys
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from akadze.__about__ import __version__
from akadze.app import Akadze
from akadze.enqueue import requeue
from akadze.exc import AkadzeError
from akadze.queue import JobSummary, list_jobs
from akadze.queues import pause_queue, resume_queue
from akadze.schema import (
    MigrateError,
    applied_versions,
    async_database_url,
    migrate,
    redact_database_url,
)
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

    jobs = sub.add_parser("jobs", help="Inspect and manage jobs")
    jobs_sub = jobs.add_subparsers(dest="jobs_command")
    jobs_list = jobs_sub.add_parser("list", help="List job summaries")
    jobs_list.add_argument("--queue", default=None)
    jobs_list.add_argument("--state", default=None)
    jobs_list.add_argument("--limit", type=int, default=50)
    jobs_requeue = jobs_sub.add_parser("requeue", help="Return a failed job to the queue")
    jobs_requeue.add_argument("job_id", type=int)

    queues = sub.add_parser("queues", help="Pause and resume queues")
    queues_sub = queues.add_subparsers(dest="queues_command")
    queues_pause = queues_sub.add_parser("pause", help="Stop new claims from a queue")
    queues_pause.add_argument("name")
    queues_resume = queues_sub.add_parser("resume", help="Allow claims from a queue again")
    queues_resume.add_argument("name")

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
    if args.command == "jobs":
        if args.jobs_command == "list":
            return _run_jobs_list(queue=args.queue, state=args.state, limit=args.limit)
        if args.jobs_command == "requeue":
            return _run_jobs_requeue(args.job_id)
        jobs.print_help()
        return 0
    if args.command == "queues":
        if args.queues_command == "pause":
            return _run_queues_pause(args.name)
        if args.queues_command == "resume":
            return _run_queues_resume(args.name)
        queues.print_help()
        return 0

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


def _require_database_url() -> str | None:
    database_url = os.environ.get("AKADZE_DATABASE_URL", "").strip()
    if not database_url:
        print("AKADZE_DATABASE_URL is not set", file=sys.stderr)
        return None
    return database_url


def _run_jobs_list(*, queue: str | None, state: str | None, limit: int) -> int:
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        summaries = asyncio.run(_list_jobs(database_url, queue=queue, state=state, limit=limit))
    except (AkadzeError, ValueError, OSError, SQLAlchemyError) as exc:
        print(f"jobs list failed: {redact_database_url(str(exc), database_url)}", file=sys.stderr)
        return 1
    for summary in summaries:
        print(_format_job_line(summary))
    return 0


async def _list_jobs(
    database_url: str,
    *,
    queue: str | None,
    state: str | None,
    limit: int,
) -> tuple[JobSummary, ...]:
    engine = _cli_engine(database_url)
    try:
        return await list_jobs(engine, queue=queue, state=state, limit=limit)
    finally:
        await engine.dispose()


def _run_jobs_requeue(job_id: int) -> int:
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        asyncio.run(_requeue_job(database_url, job_id))
    except (AkadzeError, ValueError, OSError, SQLAlchemyError) as exc:
        message = redact_database_url(str(exc), database_url)
        print(f"jobs requeue failed: {message}", file=sys.stderr)
        return 1
    return 0


async def _requeue_job(database_url: str, job_id: int) -> None:
    engine = _cli_engine(database_url)
    try:
        async with engine.begin() as session:
            await requeue(session, job_id)
    finally:
        await engine.dispose()


def _run_queues_pause(name: str) -> int:
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        asyncio.run(_pause(database_url, name))
    except (AkadzeError, ValueError, OSError, SQLAlchemyError) as exc:
        message = redact_database_url(str(exc), database_url)
        print(f"queues pause failed: {message}", file=sys.stderr)
        return 1
    return 0


def _run_queues_resume(name: str) -> int:
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        asyncio.run(_resume(database_url, name))
    except (AkadzeError, ValueError, OSError, SQLAlchemyError) as exc:
        print(
            f"queues resume failed: {redact_database_url(str(exc), database_url)}",
            file=sys.stderr,
        )
        return 1
    return 0


async def _pause(database_url: str, queue: str) -> None:
    engine = _cli_engine(database_url)
    try:
        async with engine.begin() as session:
            await pause_queue(session, queue)
    finally:
        await engine.dispose()


async def _resume(database_url: str, queue: str) -> None:
    engine = _cli_engine(database_url)
    try:
        async with engine.begin() as session:
            await resume_queue(session, queue)
    finally:
        await engine.dispose()


def _cli_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(
        async_database_url(database_url),
        poolclass=NullPool,
        connect_args={"statement_cache_size": 0},
    )


def _format_job_line(job: JobSummary) -> str:
    return (
        f"id={job.id} task={job.task} queue={job.queue} state={job.state} "
        f"priority={job.priority} attempt={job.attempt} max_attempts={job.max_attempts} "
        f"run_at={_format_dt(job.run_at)} started_at={_format_dt(job.started_at)} "
        f"finished_at={_format_dt(job.finished_at)}"
    )


def _format_dt(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.isoformat()


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
