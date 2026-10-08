"""A registered task and the enqueue options kept apart from its arguments."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, Generic, ParamSpec

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from akadze.args import argument_model, dump_arguments
from akadze.enqueue import insert_job
from akadze.exc import AkadzeError

P = ParamSpec("P")


class Task(Generic[P]):
    def __init__(
        self,
        app: Any,
        name: str,
        fn: Callable[P, Any],
        *,
        queue: str,
        max_attempts: int,
        timeout: timedelta | None,
        priority: int,
    ) -> None:
        self.app = app
        self.name = name
        self.fn = fn
        self.queue = queue
        self.max_attempts = max_attempts
        self.timeout = timeout
        self.priority = priority
        self.arguments: type[BaseModel] = argument_model(fn)

    def using(
        self,
        *,
        session: AsyncConnection | AsyncSession,
        delay: timedelta | None = None,
        run_at: datetime | None = None,
        priority: int | None = None,
        unique_key: str | None = None,
        expires_at: datetime | None = None,
    ) -> BoundTask[P]:
        return BoundTask(
            self,
            session=session,
            delay=delay,
            run_at=run_at,
            priority=self.priority if priority is None else priority,
            unique_key=unique_key,
            expires_at=expires_at,
        )

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        return self.fn(*args, **kwargs)


class BoundTask(Generic[P]):
    def __init__(
        self,
        task: Task[P],
        *,
        session: AsyncConnection | AsyncSession,
        delay: timedelta | None,
        run_at: datetime | None,
        priority: int,
        unique_key: str | None,
        expires_at: datetime | None,
    ) -> None:
        self._task = task
        self._session = session
        self._delay = delay
        self._run_at = run_at
        self._priority = priority
        self._unique_key = unique_key
        self._expires_at = expires_at

    async def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> None:
        if args:
            raise AkadzeError(f"pass arguments to {self._task.name} by name")
        payload = dump_arguments(self._task.arguments, kwargs, task=self._task.name)
        await insert_job(
            self._session,
            task=self._task.name,
            queue=self._task.queue,
            priority=self._priority,
            max_attempts=self._task.max_attempts,
            args=payload,
            unique_key=self._unique_key,
            delay=self._delay,
            run_at=self._run_at,
            expires_at=self._expires_at,
            hooks=self._task.app.hooks,
        )
