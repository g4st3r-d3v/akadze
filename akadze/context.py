"""The job that is running on this task, and a transaction that finishes it."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from akadze.exc import AkadzeError

_Finish = Callable[[AsyncConnection], Awaitable[None]]


@dataclass
class _Run:
    engine: AsyncEngine
    finish: _Finish
    done: bool = False
    busy: bool = False


_current: ContextVar[_Run | None] = ContextVar("akadze_run", default=None)


class Context:
    """Handle for the job executing on this asyncio task."""

    def __init__(self, run: _Run) -> None:
        self._run = run

    @asynccontextmanager
    async def complete_tx(self) -> AsyncIterator[AsyncConnection]:
        """Yield a connection. On a clean exit the job is finished in that transaction."""

        if self._run.done:
            raise AkadzeError("job is already complete")
        if self._run.busy:
            raise AkadzeError("complete_tx() is already open")
        self._run.busy = True
        try:
            async with self._run.engine.begin() as connection:
                yield connection
                await self._run.finish(connection)
            self._run.done = True
        finally:
            self._run.busy = False


def current() -> Context:
    """Return the job running on this asyncio task."""

    run = _current.get()
    if run is None:
        raise AkadzeError("current() is only valid inside a running job")
    return Context(run)


def bind_run(engine: AsyncEngine, finish: _Finish) -> tuple[_Run, Token[_Run | None]]:
    run = _Run(engine=engine, finish=finish)
    return run, _current.set(run)


def reset_run(token: Token[_Run | None]) -> None:
    _current.reset(token)
