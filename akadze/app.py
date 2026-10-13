"""Registry of tasks. The engine belongs to the worker, not to enqueue."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping, Sequence
from datetime import timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from akadze.exc import AkadzeError
from akadze.hooks import Hooks
from akadze.periodic import Schedule, ScheduleSpec
from akadze.schema import async_database_url
from akadze.task import Task

_SCHEDULE_SPEC_KEYS = frozenset({"name", "cron", "every", "overlap", "queue"})


class Akadze:
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
    ) -> Task[P]:
        """Bind a callable into the task registry without decorating on ``app``.

        Same options and validation as ``@app.task``. Returns the registered
        ``Task`` so the caller can enqueue it.
        """
        if max_attempts < 1:
            raise AkadzeError("max_attempts must be at least 1")
        if timeout is not None and timeout <= timedelta(0):
            raise AkadzeError("timeout must be positive")
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
        )
        self.tasks[name] = registered
        return registered

    def task[**P](
        self,
        name: str,
        *,
        queue: str = "default",
        max_attempts: int = 3,
        timeout: timedelta | None = None,
        priority: int = 0,
    ) -> Callable[[Callable[P, Any]], Task[P]]:
        def decorate(fn: Callable[P, Any]) -> Task[P]:
            return self.register(
                fn,
                name=name,
                queue=queue,
                max_attempts=max_attempts,
                timeout=timeout,
                priority=priority,
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
    ) -> Callable[[Callable[..., Any]], Task[Any]]:
        if (cron is None) == (every is None):
            raise AkadzeError("periodic needs cron or every")
        if every is not None and every <= timedelta(0):
            raise AkadzeError("every must be positive")

        def decorate(fn: Callable[..., Any]) -> Task[Any]:
            registered = self.task(
                name,
                queue=queue,
                max_attempts=max_attempts,
                timeout=timeout,
            )(fn)
            self.schedules.append(
                Schedule(name=name, cron=cron, every=every, overlap=overlap, queue=queue)
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
    ) -> None:
        """Attach a periodic schedule to an already registered task.

        Same cron/every/overlap/queue semantics as ``@periodic``. ``queue``
        defaults to the task's queue. Returns nothing.
        """
        if name not in self.tasks:
            raise AkadzeError(f"task {name} is not registered")
        if (cron is None) == (every is None):
            raise AkadzeError("schedule needs cron or every")
        if every is not None and every <= timedelta(0):
            raise AkadzeError("every must be positive")
        resolved = queue if queue is not None else self.tasks[name].queue
        self.schedules.append(
            Schedule(name=name, cron=cron, every=every, overlap=overlap, queue=resolved)
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
            )

    async def aclose(self) -> None:
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
    return ScheduleSpec(name=name, cron=cron, every=every, overlap=overlap, queue=queue)
