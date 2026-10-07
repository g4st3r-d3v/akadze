"""Registry of tasks. The engine belongs to the worker, not to enqueue."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from akadze.exc import AkadzeError
from akadze.hooks import Hooks
from akadze.periodic import Schedule
from akadze.schema import async_database_url
from akadze.task import Task


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

    def task[**P](
        self,
        name: str,
        *,
        queue: str = "default",
        max_attempts: int = 3,
        timeout: timedelta | None = None,
        priority: int = 0,
    ) -> Callable[[Callable[P, Any]], Task[P]]:
        if max_attempts < 1:
            raise AkadzeError("max_attempts must be at least 1")
        if timeout is not None and timeout <= timedelta(0):
            raise AkadzeError("timeout must be positive")

        def decorate(fn: Callable[P, Any]) -> Task[P]:
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

    async def aclose(self) -> None:
        if self._owns_engine:
            await self.engine.dispose()
