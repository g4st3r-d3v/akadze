from __future__ import annotations

import logging
import sys
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from akadze import Akadze, AkadzeError
from akadze.testing import drain
from tests.pg import as_json, job_row


async def test_register_binds_callable_into_tasks(app: Akadze) -> None:
    # Arrange
    async def greet(name: str) -> str:
        return f"hi {name}"

    # Act
    registered = app.register(greet, name="greet")

    # Assert
    assert "greet" in app.tasks
    assert app.tasks["greet"] is registered
    assert registered.fn is greet
    assert registered.queue == "default"
    assert registered.max_attempts == 3
    assert registered.timeout is None
    assert registered.priority == 0


async def test_register_accepts_task_options(app: Akadze) -> None:
    # Arrange
    async def work() -> None:
        return None

    # Act
    registered = app.register(
        work,
        name="work",
        queue="ops",
        max_attempts=5,
        timeout=timedelta(seconds=10),
        priority=2,
    )

    # Assert
    assert registered.queue == "ops"
    assert registered.max_attempts == 5
    assert registered.timeout == timedelta(seconds=10)
    assert registered.priority == 2


async def test_registered_task_runs_through_drain(app: Akadze, database_url: str) -> None:
    # Arrange
    async def greet(name: str) -> str:
        return f"hi {name}"

    task = app.register(greet, name="greet")
    async with app.engine.begin() as session:
        await task.using(session=session).enqueue(name="ada")

    # Act
    await drain(app)

    # Assert
    row = await job_row(database_url, "greet")
    assert row is not None
    assert row["state"] == "succeeded"
    assert as_json(row["result"]) == "hi ada"


async def test_register_closes_over_deps_for_di(app: Akadze, database_url: str) -> None:
    # Arrange — pure callable declared before the app exists
    async def greet(name: str, *, runner: SimpleNamespace) -> str:
        return f"{runner.prefix}{name}"

    deps = SimpleNamespace(prefix="hi ")

    async def _greet(name: str) -> str:
        return await greet(name, runner=deps)

    task = app.register(_greet, name="greet")
    async with app.engine.begin() as session:
        await task.using(session=session).enqueue(name="ada")

    # Act
    await drain(app)

    # Assert
    row = await job_row(database_url, "greet")
    assert row is not None
    assert row["state"] == "succeeded"
    assert as_json(row["result"]) == "hi ada"


async def test_register_then_add_schedule(app: Akadze) -> None:
    # Arrange
    async def tick() -> None:
        return None

    app.register(tick, name="tick", queue="ops")

    # Act
    app.add_schedule("tick", every=timedelta(minutes=1))

    # Assert
    assert len(app.schedules) == 1
    assert app.schedules[0].name == "tick"
    assert app.schedules[0].queue == "ops"


def test_register_rejects_duplicate_name(app: Akadze) -> None:
    # Arrange
    async def first() -> None:
        return None

    async def second() -> None:
        return None

    app.register(first, name="demo")

    # Act / Assert
    with pytest.raises(AkadzeError, match="task demo is already registered"):
        app.register(second, name="demo")


def test_register_rejects_invalid_max_attempts(app: Akadze) -> None:
    # Arrange
    async def demo() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="max_attempts must be at least 1"):
        app.register(demo, name="demo", max_attempts=0)


def test_register_rejects_non_positive_timeout(app: Akadze) -> None:
    # Arrange
    async def demo() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="timeout must be positive"):
        app.register(demo, name="demo", timeout=timedelta(0))


async def test_include_may_call_app_register(
    app: Akadze,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    package = tmp_path / "included_tasks"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "greet.py").write_text(
        "async def greet(name: str) -> str:\n"
        "    return f'hi {name}'\n"
        "\n"
        "def register(app):\n"
        "    app.register(greet, name='greet')\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    # Act
    try:
        app.include("included_tasks.greet")
    finally:
        for name in list(sys.modules):
            if name == "included_tasks" or name.startswith("included_tasks."):
                sys.modules.pop(name, None)

    # Assert
    assert "greet" in app.tasks


def _sync_work() -> None:
    return None


async def _async_work() -> None:
    return None


def _app_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "akadze.app" and record.levelno == logging.WARNING
    ]


def test_register_warns_once_for_a_sync_task_with_timeout(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Arrange
    app = Akadze(database_url="postgresql://unused/akadze")
    caplog.set_level(logging.WARNING, logger="akadze.app")

    # Act
    app.register(_sync_work, name="sync", timeout=timedelta(seconds=5))

    # Assert
    assert _app_warnings(caplog) == [
        "task sync is sync: its timeout does not stop its thread, "
        "which keeps running after the job is retried or failed"
    ]


@pytest.mark.parametrize(
    ("fn", "timeout"),
    [(_async_work, timedelta(seconds=5)), (_sync_work, None)],
    ids=["async-with-timeout", "sync-without-timeout"],
)
def test_register_does_not_warn_without_a_sync_timeout(
    caplog: pytest.LogCaptureFixture,
    fn: Callable[[], Any],
    timeout: timedelta | None,
) -> None:
    # Arrange
    app = Akadze(database_url="postgresql://unused/akadze")
    caplog.set_level(logging.WARNING, logger="akadze.app")

    # Act
    app.register(fn, name="work", timeout=timeout)

    # Assert
    assert _app_warnings(caplog) == []


async def test_sync_wrapper_returning_a_coroutine_is_awaited(app: Akadze) -> None:
    # Arrange
    seen: list[int] = []

    async def body(item: int) -> None:
        seen.append(item)

    def wrapper(item: int) -> Any:
        return body(item)

    registered = app.register(wrapper, name="wrapped")
    async with app.engine.begin() as session:
        await registered.using(session=session).enqueue(item=3)

    # Act
    await drain(app)

    # Assert
    assert seen == [3]
