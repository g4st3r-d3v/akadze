"""A registered task and the enqueue options kept apart from its arguments."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta
from typing import Any, Generic, ParamSpec

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from akadze.args import argument_model, dump_arguments
from akadze.enqueue import check_options, insert_job, insert_jobs
from akadze.exc import EnqueueError
from akadze.outcome import BackoffPolicy

P = ParamSpec("P")


class Task(Generic[P]):
    """A registered task. ``@app.task`` and ``app.register`` return one.

    Enqueue it through ``using``: ``await task.using(session=conn).enqueue(...)``.
    """

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
        backoff: BackoffPolicy | None = None,
    ) -> None:
        self.app = app
        self.name = name
        self.fn = fn
        self.queue = queue
        self.max_attempts = max_attempts
        self.timeout = timeout
        self.priority = priority
        self.backoff = backoff
        self.arguments: type[BaseModel] = argument_model(fn)
        # A sync fn runs in a thread; the worker and the timeout warning both read this.
        self.is_async = inspect.iscoroutinefunction(fn)

    def using(
        self,
        *,
        session: AsyncConnection | AsyncSession,
        delay: timedelta | None = None,
        run_at: datetime | None = None,
        priority: int | None = None,
        max_attempts: int | None = None,
        unique_key: str | None = None,
        limit_key: str | None = None,
        expires_at: datetime | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> BoundTask[P]:
        """Bind enqueue options; the jobs are inserted on ``session``, in its transaction.

        ``delay`` or ``run_at`` (timezone-aware) set the earliest start.
        ``priority`` and ``max_attempts`` override the task's defaults for
        these jobs. While a job with ``unique_key`` is queued or running,
        another enqueue with that key raises ``DuplicateJob``; keys are shared
        across all tasks and queues. ``limit_key``
        names the limits that apply (see ``set_limit``). A job not started by
        ``expires_at`` is cancelled. ``meta`` is a small JSON object with
        string keys, at most 8 KiB, for filtering and hooks; not for arguments
        or secrets.
        """
        return BoundTask(
            self,
            session=session,
            delay=delay,
            run_at=run_at,
            priority=self.priority if priority is None else priority,
            max_attempts=self.max_attempts if max_attempts is None else max_attempts,
            unique_key=unique_key,
            limit_key=limit_key,
            expires_at=expires_at,
            meta=meta,
        )

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        return self.fn(*args, **kwargs)


class BoundTask(Generic[P]):
    """A task bound to a session and enqueue options; ``Task.using`` returns one."""

    def __init__(
        self,
        task: Task[P],
        *,
        session: AsyncConnection | AsyncSession,
        delay: timedelta | None,
        run_at: datetime | None,
        priority: int,
        max_attempts: int,
        unique_key: str | None,
        limit_key: str | None,
        expires_at: datetime | None,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        self._task = task
        self._session = session
        self._delay = delay
        self._run_at = run_at
        self._priority = priority
        self._max_attempts = max_attempts
        self._unique_key = unique_key
        self._limit_key = limit_key
        self._expires_at = expires_at
        self._meta = meta

    async def enqueue(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """Insert one job with these arguments (by name). Returns nothing.

        Raises ``EnqueueError`` on positional or invalid arguments and invalid
        options, and ``DuplicateJob`` when ``unique_key`` is taken; neither
        inserts a row.
        ``DuplicateJob`` does not abort the caller's transaction.
        """
        if args:
            raise EnqueueError(f"pass arguments to {self._task.name} by name")
        payload = dump_arguments(self._task.arguments, kwargs, task=self._task.name)
        await insert_job(
            self._session,
            task=self._task.name,
            queue=self._task.queue,
            priority=self._priority,
            max_attempts=self._max_attempts,
            args=payload,
            unique_key=self._unique_key,
            limit_key=self._limit_key,
            delay=self._delay,
            run_at=self._run_at,
            expires_at=self._expires_at,
            hooks=self._task.app.hooks,
            meta=self._meta,
        )

    async def enqueue_many(self, items: Iterable[Mapping[str, Any]]) -> None:
        """Insert one job per item (arguments by name) with the shared options.

        All items are validated before anything is inserted. ``unique_key``
        applies to one job, so it is rejected here.
        """

        if self._unique_key is not None:
            raise EnqueueError("unique_key applies to one job; enqueue_many does not take it")
        if isinstance(items, Mapping | str | bytes):
            raise EnqueueError("enqueue_many takes an iterable of mappings, not one mapping")
        check_options(
            self._priority, self._max_attempts, self._delay, self._run_at, self._expires_at
        )
        payloads = []
        for item in items:
            if not isinstance(item, Mapping):
                kind = type(item).__name__
                raise EnqueueError(f"enqueue_many items must be mappings, got {kind}")
            payloads.append(dump_arguments(self._task.arguments, dict(item), task=self._task.name))
        await insert_jobs(
            self._session,
            task=self._task.name,
            queue=self._task.queue,
            priority=self._priority,
            max_attempts=self._max_attempts,
            args=payloads,
            limit_key=self._limit_key,
            delay=self._delay,
            run_at=self._run_at,
            expires_at=self._expires_at,
            hooks=self._task.app.hooks,
            meta=self._meta,
        )
