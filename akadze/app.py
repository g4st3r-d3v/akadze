"""Registry of tasks. The engine belongs to the worker, not to enqueue."""

from __future__ import annotations

import importlib
import inspect
import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from akadze.exc import AkadzeError
from akadze.hooks import Hooks
from akadze.outcome import BackoffPolicy
from akadze.periodic import Schedule, ScheduleSpec, check_schedule
from akadze.schema import async_database_url
from akadze.task import Task

logger = logging.getLogger("akadze.app")

_SCHEDULE_SPEC_KEYS = frozenset({"name", "cron", "every", "overlap", "queue", "timezone"})


class Akadze:
    """The application: an engine, the registered tasks and the schedules.

    Pass ``database_url`` and the app builds an async engine it owns (and
    closes in ``aclose``), with asyncpg's statement cache off so a
    transaction-mode pool such as PgBouncer can sit in front. Pass ``engine``
    instead to share a pool; the app then leaves it open. One of the two is
    required.

    ``grace`` is how late a periodic fire may still be enqueued (default one
    minute). ``retention`` is how long finished jobs and periodic runs are
    kept before the worker deletes them (default seven days).

    Example:
        ```python
        app = Akadze(database_url="postgresql://localhost/app")

        @app.task("mail.send", queue="mail", max_attempts=6)
        async def send_mail(user_id: int) -> None: ...
        ```
    """

    def __init__(
        self,
        *,
        engine: AsyncEngine | None = None,
        database_url: str | None = None,
        grace: timedelta = timedelta(minutes=1),
        retention: timedelta = timedelta(days=7),
    ) -> None:
        if engine is None and database_url is None:
            raise AkadzeError("Akadze needs an engine or a database_url")
        self.engine = engine or create_async_engine(
            async_database_url(database_url or ""),
            connect_args={"statement_cache_size": 0},
        )
        self._owns_engine = engine is None
        self.grace = grace
        self.retention = retention
        self.hooks = Hooks()
        self.tasks: dict[str, Task[Any]] = {}
        self.schedules: list[Schedule] = []

    def include(self, *modules: str) -> None:
        """Import each module and call its ``register(app)``.

        Modules own registration (``@app.task`` / ``@app.periodic`` /
        ``app.register``). There is no package walk and no
        import-for-side-effect convention.
        """
        for name in modules:
            module = importlib.import_module(name)
            register = getattr(module, "register", None)
            if not callable(register):
                raise AkadzeError(f"{name} has no callable register(app)")
            register(self)

    def register[**P](
        self,
        fn: Callable[P, Any],
        *,
        name: str,
        queue: str = "default",
        max_attempts: int = 3,
        timeout: timedelta | None = None,
        priority: int = 0,
        backoff: BackoffPolicy | None = None,
    ) -> Task[P]:
        """Bind a callable into the task registry without decorating on ``app``.

        Same options and validation as ``@app.task``. Returns the registered
        ``Task`` so the caller can enqueue it.

        ``timeout`` is not a hard limit for a sync callable: it runs in a
        thread, which keeps running after the timeout while the job is retried
        or failed, so a retry can overlap it, and the thread stays out of
        ``Worker.slots``. Registering one with ``timeout`` logs a warning on
        logger ``akadze.app``.
        """
        if max_attempts < 1:
            raise AkadzeError("max_attempts must be at least 1")
        if timeout is not None and timeout <= timedelta(0):
            raise AkadzeError("timeout must be positive")
        if backoff is not None and (not callable(backoff) or inspect.iscoroutinefunction(backoff)):
            raise AkadzeError("backoff must be a plain (not async) callable")
        if name in self.tasks:
            raise AkadzeError(f"task {name} is already registered")
        registered: Task[P] = Task(
            self,
            name,
            fn,
            queue=queue,
            max_attempts=max_attempts,
            timeout=timeout,
            priority=priority,
            backoff=backoff,
        )
        self.tasks[name] = registered
        if timeout is not None and not registered.is_async:
            logger.warning(
                "task %s is sync: its timeout does not stop its thread, "
                "which keeps running after the job is retried or failed",
                name,
            )
        return registered

    def task[**P](
        self,
        name: str,
        *,
        queue: str = "default",
        max_attempts: int = 3,
        timeout: timedelta | None = None,
        priority: int = 0,
        backoff: BackoffPolicy | None = None,
    ) -> Callable[[Callable[P, Any]], Task[P]]:
        """Register the decorated function as task ``name`` and return its ``Task``.

        ``queue`` is where its jobs go; workers claim from the queues they are
        started with. ``max_attempts`` is the budget including the first run.
        A run longer than ``timeout`` counts as an error and is retried.
        ``priority`` is the default for its jobs (higher is claimed first).
        ``backoff`` maps the attempt number to the delay before the next one
        (default: exponential with jitter, capped at 300 s). Arguments are
        validated by a model built from the signature. Same options as
        ``register``.
        """

        def decorate(fn: Callable[P, Any]) -> Task[P]:
            return self.register(
                fn,
                name=name,
                queue=queue,
                max_attempts=max_attempts,
                timeout=timeout,
                priority=priority,
                backoff=backoff,
            )

        return decorate

    def periodic(
        self,
        name: str,
        *,
        cron: str | None = None,
        every: timedelta | None = None,
        overlap: bool = True,
        queue: str = "default",
        max_attempts: int = 3,
        timeout: timedelta | None = None,
        timezone: str | None = None,
        backoff: BackoffPolicy | None = None,
    ) -> Callable[[Callable[..., Any]], Task[Any]]:
        """Register the decorated function as a task that runs on a schedule.

        Give exactly one of ``cron`` (five fields, UTC unless ``timezone``
        names an IANA zone) and ``every`` (a ``timedelta``). Every worker runs
        the schedule; each due fire is enqueued once across the cluster.
        ``overlap=False`` holds a fire back while the previous job is queued or
        running, for at most the app's ``grace``. The other options are those
        of ``task``.
        """
        check_schedule(cron, every, timezone)

        def decorate(fn: Callable[..., Any]) -> Task[Any]:
            registered = self.task(
                name,
                queue=queue,
                max_attempts=max_attempts,
                timeout=timeout,
                backoff=backoff,
            )(fn)
            self.schedules.append(
                Schedule(
                    name=name,
                    cron=cron,
                    every=every,
                    overlap=overlap,
                    queue=queue,
                    timezone=timezone,
                )
            )
            return registered

        return decorate

    def add_schedule(
        self,
        name: str,
        *,
        cron: str | None = None,
        every: timedelta | None = None,
        overlap: bool = True,
        queue: str | None = None,
        timezone: str | None = None,
    ) -> None:
        """Attach a periodic schedule to an already registered task.

        Same cron/every/overlap/queue/timezone semantics as ``@periodic``.
        ``queue`` defaults to the task's queue. Returns nothing.
        """
        if name not in self.tasks:
            raise AkadzeError(f"task {name} is not registered")
        check_schedule(cron, every, timezone)
        resolved = queue if queue is not None else self.tasks[name].queue
        self.schedules.append(
            Schedule(
                name=name,
                cron=cron,
                every=every,
                overlap=overlap,
                queue=resolved,
                timezone=timezone,
            )
        )

    def add_schedules(self, specs: Sequence[ScheduleSpec | Mapping[str, object]]) -> None:
        """Attach many schedules from already-parsed specs. Returns nothing."""

        for spec in specs:
            parsed = spec if isinstance(spec, ScheduleSpec) else _schedule_spec_from_mapping(spec)
            self.add_schedule(
                parsed.name,
                cron=parsed.cron,
                every=parsed.every,
                overlap=parsed.overlap,
                queue=parsed.queue,
                timezone=parsed.timezone,
            )

    async def aclose(self) -> None:
        """Dispose of the engine if the app built it from ``database_url``."""

        if self._owns_engine:
            await self.engine.dispose()


def _schedule_spec_from_mapping(raw: Mapping[str, object]) -> ScheduleSpec:
    unknown = set(raw) - _SCHEDULE_SPEC_KEYS
    if unknown:
        raise AkadzeError(f"unknown schedule keys: {sorted(unknown)}")
    if "name" not in raw:
        raise AkadzeError("schedule needs name")
    name = raw["name"]
    if not isinstance(name, str) or not name:
        raise AkadzeError("schedule name must be a non-empty str")
    cron = raw.get("cron")
    if cron is not None and not isinstance(cron, str):
        raise AkadzeError("schedule cron must be a str")
    every = raw.get("every")
    if every is not None and not isinstance(every, timedelta):
        raise AkadzeError("schedule every must be a timedelta")
    overlap = raw.get("overlap", True)
    if not isinstance(overlap, bool):
        raise AkadzeError("schedule overlap must be a bool")
    queue = raw.get("queue")
    if queue is not None and not isinstance(queue, str):
        raise AkadzeError("schedule queue must be a str")
    timezone = raw.get("timezone")
    if timezone is not None and not isinstance(timezone, str):
        raise AkadzeError("schedule timezone must be a str")
    return ScheduleSpec(
        name=name,
        cron=cron,
        every=every,
        overlap=overlap,
        queue=queue,
        timezone=timezone,
    )
