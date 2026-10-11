from __future__ import annotations

import re

from akadze import Akadze, pause_queue
from akadze.metrics import render_metrics


async def test_render_metrics_reports_jobs_lag_busy_and_pause(app: Akadze) -> None:
    # Arrange
    secret = "metrics-must-not-leak-this-arg"

    @app.task("mail-job", queue="mail")
    async def mail_job(token: str) -> str:
        return token

    @app.task("default-job", queue="default")
    async def default_job() -> None:
        return None

    async with app.engine.begin() as session:
        await mail_job.using(session=session).enqueue(token=secret)
        await mail_job.using(session=session).enqueue(token=secret)
        await mail_job.using(session=session).enqueue(token=secret)
        await default_job.using(session=session).enqueue()
        await pause_queue(session, "mail")

    # Act
    text = await render_metrics(app.engine)

    # Assert
    assert 'akadze_jobs{queue="mail",state="queued"} 3' in text
    assert 'akadze_jobs{queue="default",state="queued"} 1' in text
    assert re.search(r"^akadze_queue_lag_seconds \d", text, re.MULTILINE)
    assert "akadze_worker_busy 0" in text
    assert 'akadze_queue_paused{queue="mail"} 1' in text
    assert secret not in text


async def test_paused_queue_with_jobs_is_not_empty_in_metrics(app: Akadze) -> None:
    # Arrange
    @app.task("demo", queue="mail")
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
        await demo.using(session=session).enqueue()
        await pause_queue(session, "mail")

    # Act
    text = await render_metrics(app.engine)

    # Assert
    assert 'akadze_jobs{queue="mail",state="queued"} 2' in text
    assert 'akadze_queue_paused{queue="mail"} 1' in text


async def test_render_metrics_omits_lag_when_no_ready_job(app: Akadze) -> None:
    # Arrange — schema only, no ready queued jobs

    # Act
    text = await render_metrics(app.engine)

    # Assert
    assert "akadze_queue_lag_seconds" not in text
    assert "akadze_worker_busy 0" in text
    assert "akadze_queue_paused{" not in text


async def test_render_metrics_excludes_job_argument_text(app: Akadze) -> None:
    # Arrange
    secret = "super-secret-payload-xyz"

    @app.task("demo")
    async def demo(payload: str) -> str:
        return payload

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue(payload=secret)

    # Act
    text = await render_metrics(app.engine)

    # Assert
    assert secret not in text
