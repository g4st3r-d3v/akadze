from __future__ import annotations

import os
import re
import subprocess
import sys

import pytest

from akadze import Akadze
from tests.factories import pause


def _cli(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "akadze.cli", *argv],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


@pytest.mark.parametrize("argv", [("metrics",), ("metrics", "render")], ids=["bare", "render"])
async def test_cli_metrics_prints_render_metrics_text(
    app: Akadze, database_url: str, fake, argv: tuple[str, ...]
) -> None:
    # Arrange
    secret = fake.password(length=24)

    @app.task("mail-job", queue="mail")
    async def mail_job(token: str) -> str:
        return token

    async with app.engine.begin() as session:
        await mail_job.using(session=session).enqueue(token=secret)
        await mail_job.using(session=session).enqueue(token=secret)
    await pause(app.engine, "mail")
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = database_url

    # Act
    result = _cli(env, *argv)

    # Assert
    assert result.returncode == 0, result.stderr
    assert 'akadze_jobs{queue="mail",state="queued"} 2' in result.stdout
    assert re.search(r"^akadze_queue_lag_seconds \d", result.stdout, re.MULTILINE)
    assert "akadze_worker_busy 0" in result.stdout
    assert 'akadze_queue_paused{queue="mail"} 1' in result.stdout
    assert secret not in result.stdout
    assert database_url not in result.stdout + result.stderr
    assert result.stdout.endswith("\n")


def test_cli_metrics_requires_database_url() -> None:
    # Arrange
    env = os.environ.copy()
    env.pop("AKADZE_DATABASE_URL", None)

    # Act
    result = _cli(env, "metrics")

    # Assert
    assert result.returncode == 2
    assert "AKADZE_DATABASE_URL is not set" in result.stderr


def test_cli_metrics_hides_password_on_failure() -> None:
    # Arrange
    env = os.environ.copy()
    env["AKADZE_DATABASE_URL"] = "postgresql://akadze:super-secret-pw@127.0.0.1:1/akadze"

    # Act
    result = _cli(env, "metrics")

    # Assert
    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "metrics failed:" in result.stderr
    assert "super-secret-pw" not in output
    assert "Traceback" not in output
