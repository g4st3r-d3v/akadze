"""CLI entrypoints: worker, beat, migrate, jobs, queues, limits, workers, metrics."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import signal
import sys
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from akadze.__about__ import __version__
from akadze.app import Akadze
from akadze.enqueue import request_cancel, requeue
from akadze.exc import AkadzeError
from akadze.limits import LimitSummary, clear_limit, list_limits
from akadze.metrics import render_metrics
from akadze.queue import JobSummary, list_jobs
from akadze.queues import pause_queue, resume_queue
from akadze.schema import (
    DB_ERRORS,
    MigrateError,
    applied_versions,
    async_database_url,
    is_transient_db_error,
    migrate,
    public_db_error,
    redact_database_url,
)
from akadze.worker import Worker, WorkerSummary, list_workers


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="akadze", description="Postgres-backed task queue")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")

    worker = sub.add_parser("worker", help="Claim and run registered tasks")
    worker.add_argument("app", help="Import path of an Akadze app, for example demo:app")
    worker.add_argument("--slots", type=int, default=1)
    worker.add_argument("--queue", action="append", dest="queues")
    worker.add_argument(
        "--fair",
        action="store_true",
        help="Within a priority, claim round-robin across limit_key groups",
    )
    sub.add_parser("beat", help="Unused: periodic tasks run inside the worker")
    sub.add_parser("migrate", help="Apply schema migrations")

    jobs = sub.add_parser("jobs", help="Inspect and manage jobs")
    jobs_sub = jobs.add_subparsers(dest="jobs_command")
    jobs_list = jobs_sub.add_parser("list", help="List job summaries")
    jobs_list.add_argument("--queue", default=None)
    jobs_list.add_argument("--state", default=None)
    jobs_list.add_argument("--limit", type=int, default=50)
    jobs_list.add_argument(
        "--meta",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Keep jobs whose meta has KEY equal to VALUE (JSON if it parses, else text)",
    )
    jobs_requeue = jobs_sub.add_parser("requeue", help="Return a failed job to the queue")
    jobs_requeue.add_argument("job_id", type=int)
    jobs_cancel = jobs_sub.add_parser(
        "cancel", help="Cancel a queued job; a running one is cancelled unless it returns"
    )
    jobs_cancel.add_argument("job_id", type=int)

    queues = sub.add_parser("queues", help="Pause and resume queues")
    queues_sub = queues.add_subparsers(dest="queues_command")
    queues_pause = queues_sub.add_parser("pause", help="Stop new claims from a queue")
    queues_pause.add_argument("name")
    queues_resume = queues_sub.add_parser("resume", help="Allow claims from a queue again")
    queues_resume.add_argument("name")

    limits = sub.add_parser("limits", help="Inspect and clear limit policies")
    limits_sub = limits.add_subparsers(dest="limits_command")
    limits_sub.add_parser("list", help="List limit policies")
    limits_clear = limits_sub.add_parser("clear", help="Delete a limits row by key")
    limits_clear.add_argument("key")

    workers = sub.add_parser("workers", help="Inspect registered workers")
    workers_sub = workers.add_subparsers(dest="workers_command")
    workers_sub.add_parser("list", help="List workers from akadze.workers")

    metrics = sub.add_parser("metrics", help="Print Prometheus exposition text")
    metrics_sub = metrics.add_subparsers(dest="metrics_command")
    metrics_sub.add_parser("render", help="Print Prometheus exposition text")

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
        return _run_worker(args.app, slots=args.slots, queues=args.queues, fair=args.fair)
    if args.command == "jobs":
        if args.jobs_command == "list":
            return _run_jobs_list(
                queue=args.queue, state=args.state, limit=args.limit, meta=args.meta
            )
        if args.jobs_command == "requeue":
            return _run_jobs_requeue(args.job_id)
        if args.jobs_command == "cancel":
            return _run_jobs_cancel(args.job_id)
        jobs.print_help()
        return 0
    if args.command == "queues":
        if args.queues_command == "pause":
            return _run_queues_pause(args.name)
        if args.queues_command == "resume":
            return _run_queues_resume(args.name)
        queues.print_help()
        return 0
    if args.command == "limits":
        if args.limits_command == "list":
            return _run_limits_list()
        if args.limits_command == "clear":
            return _run_limits_clear(args.key)
        limits.print_help()
        return 0
    if args.command == "workers":
        if args.workers_command == "list":
            return _run_workers_list()
        workers.print_help()
        return 0
    if args.command == "metrics":
        if args.metrics_command in (None, "render"):
            return _run_metrics()
        metrics.print_help()
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


def _run_jobs_list(
    *, queue: str | None, state: str | None, limit: int, meta: list[str] | None = None
) -> int:
    try:
        filters = _parse_meta(meta or [])
    except AkadzeError as exc:
        print(f"jobs list: {exc}", file=sys.stderr)
        return 2
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        summaries = asyncio.run(
            _list_jobs(database_url, queue=queue, state=state, limit=limit, meta=filters)
        )
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
    meta: dict[str, object] | None = None,
) -> tuple[JobSummary, ...]:
    engine = _cli_engine(database_url)
    try:
        return await list_jobs(engine, queue=queue, state=state, limit=limit, meta=meta)
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


def _run_jobs_cancel(job_id: int) -> int:
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        asyncio.run(_cancel_job(database_url, job_id))
    except (AkadzeError, ValueError, OSError, SQLAlchemyError) as exc:
        message = redact_database_url(str(exc), database_url)
        print(f"jobs cancel failed: {message}", file=sys.stderr)
        return 1
    return 0


async def _cancel_job(database_url: str, job_id: int) -> None:
    engine = _cli_engine(database_url)
    try:
        async with engine.begin() as session:
            state = await session.scalar(
                text("SELECT state FROM akadze.jobs WHERE id = :id FOR UPDATE"), {"id": job_id}
            )
            if state not in ("queued", "running"):
                raise AkadzeError("job is not active")
            await request_cancel(session, job_id)
    finally:
        await engine.dispose()


def _run_limits_list() -> int:
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        summaries = asyncio.run(_list_limits(database_url))
    except (AkadzeError, ValueError, OSError, SQLAlchemyError) as exc:
        print(f"limits list failed: {redact_database_url(str(exc), database_url)}", file=sys.stderr)
        return 1
    for summary in summaries:
        print(_format_limit_line(summary))
    return 0


async def _list_limits(database_url: str) -> tuple[LimitSummary, ...]:
    engine = _cli_engine(database_url)
    try:
        return await list_limits(engine)
    finally:
        await engine.dispose()


def _run_limits_clear(key: str) -> int:
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        asyncio.run(_clear_limit(database_url, key))
    except (AkadzeError, ValueError, OSError, SQLAlchemyError) as exc:
        message = redact_database_url(str(exc), database_url)
        print(f"limits clear failed: {message}", file=sys.stderr)
        return 1
    return 0


async def _clear_limit(database_url: str, key: str) -> None:
    engine = _cli_engine(database_url)
    try:
        async with engine.begin() as session:
            await clear_limit(session, key)
    finally:
        await engine.dispose()


def _run_workers_list() -> int:
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        summaries = asyncio.run(_list_workers(database_url))
    except (AkadzeError, ValueError, OSError, SQLAlchemyError) as exc:
        message = redact_database_url(str(exc), database_url)
        print(f"workers list failed: {message}", file=sys.stderr)
        return 1
    for summary in summaries:
        print(_format_worker_line(summary))
    return 0


async def _list_workers(database_url: str) -> tuple[WorkerSummary, ...]:
    engine = _cli_engine(database_url)
    try:
        return await list_workers(engine)
    finally:
        await engine.dispose()


def _run_metrics() -> int:
    database_url = _require_database_url()
    if database_url is None:
        return 2
    try:
        text = asyncio.run(_render_metrics(database_url))
    except (AkadzeError, ValueError, OSError, SQLAlchemyError) as exc:
        print(f"metrics failed: {redact_database_url(str(exc), database_url)}", file=sys.stderr)
        return 1
    sys.stdout.write(text)
    return 0


async def _render_metrics(database_url: str) -> str:
    engine = _cli_engine(database_url)
    try:
        return await render_metrics(engine)
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
        f"finished_at={_format_dt(job.finished_at)} "
        f"meta={json.dumps(dict(job.meta), separators=(',', ':'), sort_keys=True)}"
    )


def _parse_meta(pairs: list[str]) -> dict[str, object]:
    filters: dict[str, object] = {}
    for pair in pairs:
        key, sep, raw = pair.partition("=")
        if not sep or not key:
            raise AkadzeError(f"--meta expects KEY=VALUE, got {pair!r}")
        if key in filters:
            raise AkadzeError(f"--meta {key} given more than once")
        try:
            filters[key] = json.loads(raw)
        except ValueError:
            filters[key] = raw
    return filters


def _format_limit_line(limit: LimitSummary) -> str:
    return (
        f"key={limit.key} max_running={_format_optional_int(limit.max_running)} "
        f"max_starts={_format_optional_int(limit.max_starts)} "
        f"window={_format_window(limit.window)}"
    )


def _format_worker_line(worker: WorkerSummary) -> str:
    queues = ",".join(worker.queues) if worker.queues else "-"
    return (
        f"id={worker.id} hostname={worker.hostname} pid={worker.pid} "
        f"queues={queues} started_at={_format_dt(worker.started_at)} "
        f"heartbeat_at={_format_dt(worker.heartbeat_at)}"
    )


def _format_optional_int(value: int | None) -> str:
    if value is None:
        return "-"
    return str(value)


def _format_window(value: timedelta | None) -> str:
    if value is None:
        return "-"
    return str(value)


def _format_dt(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.isoformat()


def _run_worker(spec: str, *, slots: int, queues: list[str] | None, fair: bool) -> int:
    app = _load_app(spec)
    worker = Worker(app, slots=slots, queues=queues, fair=fair)

    async def _run() -> None:
        try:
            await _serve(worker)
        finally:
            await app.aclose()

    try:
        asyncio.run(_run())
    except ValueError as exc:
        print(f"worker failed: {exc}", file=sys.stderr)
        return 1
    except DB_ERRORS as exc:
        print(f"worker failed: {_worker_error(exc, app)}", file=sys.stderr)
        return 1
    if worker._abandon:
        print("worker stopped: no heartbeat succeeded within heartbeat_ttl", file=sys.stderr)
        return 1
    return 0


def _worker_error(exc: BaseException, app: Akadze) -> str:
    """One line for a database error that stopped the worker, without the password.

    Errors that are not transient (missing schema, rejected login) keep the
    first line of their message: at start-up it names the cause.
    """

    if is_transient_db_error(exc):
        return public_db_error(exc)
    first = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
    url = app.engine.url.render_as_string(hide_password=False)
    return f"{type(exc).__name__}: {redact_database_url(first, url)}"


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
