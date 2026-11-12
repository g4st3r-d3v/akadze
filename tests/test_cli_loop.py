from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

import pytest

from akadze.cli import _loop_runner


def test_loop_asyncio_is_asyncio_run() -> None:
    # Act
    runner = _loop_runner("asyncio")

    # Assert
    assert runner is asyncio.run


@pytest.mark.parametrize("loop", ["auto", "uvloop"])
def test_loop_takes_uvloop_when_it_is_installed(monkeypatch: pytest.MonkeyPatch, loop: str) -> None:
    # Arrange
    fake = SimpleNamespace(run=lambda coroutine: None)
    monkeypatch.setitem(sys.modules, "uvloop", fake)

    # Act
    runner = _loop_runner(loop)

    # Assert
    assert runner is fake.run


def test_loop_auto_falls_back_to_asyncio_without_uvloop(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange — None in sys.modules makes the import fail
    monkeypatch.setitem(sys.modules, "uvloop", None)

    # Act
    runner = _loop_runner("auto")

    # Assert
    assert runner is asyncio.run


def test_loop_uvloop_without_uvloop_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setitem(sys.modules, "uvloop", None)

    # Act / Assert
    with pytest.raises(ImportError):
        _loop_runner("uvloop")


def test_loop_auto_falls_back_when_uvloop_has_no_run(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Arrange — a uvloop too old for uvloop.run, or a module that shadows it
    monkeypatch.setitem(sys.modules, "uvloop", SimpleNamespace())

    # Act
    runner = _loop_runner("auto")

    # Assert
    assert runner is asyncio.run
    assert "uvloop not usable, running on asyncio" in capsys.readouterr().err
