from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from akadze import Akadze
from akadze.cli import _load_app


def test_worker_loads_an_app_from_the_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    (tmp_path / "cwd_demo.py").write_text(
        "from akadze import Akadze\n"
        "app = Akadze(database_url='postgresql://akadze:akadze@localhost:5432/akadze')\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    # Act
    loaded = _load_app("cwd_demo:app")

    # Assert
    try:
        assert isinstance(loaded, Akadze)
    finally:
        sys.modules.pop("cwd_demo", None)
        cwd = str(tmp_path)
        while cwd in sys.path:
            sys.path.remove(cwd)
        asyncio.run(loaded.aclose())
