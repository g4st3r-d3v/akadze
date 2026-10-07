"""Hooks run in the transaction that changes a job, except the run hooks."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection

from akadze.job import Job

EnqueueHook = Callable[[AsyncConnection, Job], Awaitable[None] | None]
RunHook = Callable[[Job], Awaitable[None] | None]
TransitionHook = Callable[[AsyncConnection, Job, str, str], Awaitable[None] | None]


class Hooks:
    def __init__(self) -> None:
        self.on_enqueue: list[EnqueueHook] = []
        self.before_run: list[RunHook] = []
        self.after_run: list[RunHook] = []
        self.on_transition: list[TransitionHook] = []

    async def ran_enqueue(self, connection: AsyncConnection, job: Job) -> None:
        for hook in self.on_enqueue:
            await _call(hook, connection, job)

    async def run_before(self, job: Job) -> None:
        for hook in self.before_run:
            await _call(hook, job)

    async def run_after(self, job: Job) -> None:
        for hook in self.after_run:
            await _call(hook, job)

    async def ran_transition(
        self,
        connection: AsyncConnection,
        job: Job,
        from_state: str,
        to_state: str,
    ) -> None:
        for hook in self.on_transition:
            await _call(hook, connection, job, from_state, to_state)


async def _call(hook: Callable[..., Any], *args: Any) -> None:
    result = hook(*args)
    if inspect.isawaitable(result):
        await result
