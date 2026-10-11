from __future__ import annotations

import sys
from pathlib import Path

import pytest

from akadze import Akadze, AkadzeError
from akadze.testing import drain
from tests.pg import job_row


def _write_module(root: Path, package: str, module: str, body: str) -> None:
    package_dir = root / package
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / f"{module}.py").write_text(body, encoding="utf-8")


def _forget_package(package: str) -> None:
    prefix = f"{package}."
    for name in list(sys.modules):
        if name == package or name.startswith(prefix):
            sys.modules.pop(name, None)


async def test_include_registers_tasks_from_named_modules(
    app: Akadze,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    _write_module(
        tmp_path,
        "included_tasks",
        "greet",
        "def register(app):\n"
        "    @app.task('greet')\n"
        "    async def greet(name: str) -> str:\n"
        "        return f'hi {name}'\n",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    # Act
    try:
        app.include("included_tasks.greet")
    finally:
        _forget_package("included_tasks")

    # Assert
    assert "greet" in app.tasks


async def test_include_registers_periodics_from_named_modules(
    app: Akadze,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    _write_module(
        tmp_path,
        "included_tasks",
        "tick",
        "from datetime import timedelta\n"
        "\n"
        "def register(app):\n"
        "    @app.periodic('tick', every=timedelta(minutes=1))\n"
        "    async def tick() -> None:\n"
        "        return None\n",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    # Act
    try:
        app.include("included_tasks.tick")
    finally:
        _forget_package("included_tasks")

    # Assert
    assert "tick" in app.tasks
    assert any(schedule.name == "tick" for schedule in app.schedules)


async def test_included_task_runs_through_drain(
    app: Akadze,
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    _write_module(
        tmp_path,
        "included_tasks",
        "greet",
        "def register(app):\n"
        "    @app.task('greet')\n"
        "    async def greet(name: str) -> str:\n"
        "        return f'hi {name}'\n",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        app.include("included_tasks.greet")
    finally:
        _forget_package("included_tasks")
    async with app.engine.begin() as session:
        await app.tasks["greet"].using(session=session).enqueue(name="ada")

    # Act
    await drain(app)

    # Assert
    row = await job_row(database_url, "greet")
    assert row is not None
    assert row["state"] == "succeeded"


def test_include_rejects_a_module_without_register(
    app: Akadze,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    _write_module(
        tmp_path,
        "included_tasks",
        "empty",
        "VALUE = 1\n",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    # Act / Assert
    try:
        with pytest.raises(AkadzeError, match="has no callable register"):
            app.include("included_tasks.empty")
    finally:
        _forget_package("included_tasks")


def test_include_returns_nothing(
    app: Akadze,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    _write_module(
        tmp_path,
        "included_tasks",
        "noop",
        "def register(app):\n"
        "    return None\n",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    # Act
    try:
        result = app.include("included_tasks.noop")
    finally:
        _forget_package("included_tasks")

    # Assert
    assert result is None
