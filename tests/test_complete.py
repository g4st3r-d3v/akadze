from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from akadze import Akadze, AkadzeError, current
from akadze.job import Job
from akadze.worker import Worker
from tests.pg import connection, job_row


@pytest.fixture
async def notes(database_url: str) -> AsyncIterator[None]:
    async with connection(database_url) as opened:
        await opened.execute(
            """
            CREATE TABLE IF NOT EXISTS public.akadze_test_notes (
                body text PRIMARY KEY
            )
            """
        )
        await opened.execute("DELETE FROM public.akadze_test_notes")
    yield
    async with connection(database_url) as opened:
        await opened.execute("DROP TABLE IF EXISTS public.akadze_test_notes")


async def _note_count(database_url: str) -> int:
    async with connection(database_url) as opened:
        count = await opened.fetchval("SELECT count(*) FROM public.akadze_test_notes")
    return int(count)


async def test_complete_tx_commits_the_write_with_the_job(
    app: Akadze,
    database_url: str,
    notes: None,
) -> None:
    # Arrange
    @app.task("save")
    async def save() -> None:
        async with current().complete_tx() as tx:
            await tx.execute(text("INSERT INTO public.akadze_test_notes (body) VALUES ('kept')"))

    async with app.engine.begin() as session:
        await save.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "save")
    assert row is not None
    assert row["state"] == "succeeded"
    async with connection(database_url) as opened:
        body = await opened.fetchval("SELECT body FROM public.akadze_test_notes")
    assert body == "kept"


async def test_lost_run_rolls_back_the_write(
    app: Akadze,
    database_url: str,
    notes: None,
) -> None:
    # Arrange
    @app.task("save")
    async def save() -> None:
        async with current().complete_tx() as tx:
            await tx.execute(text("INSERT INTO public.akadze_test_notes (body) VALUES ('lost')"))
            async with connection(database_url) as opened:
                await opened.execute(
                    "UPDATE akadze.jobs SET run_count = run_count + 1 WHERE task = 'save'"
                )

    async with app.engine.begin() as session:
        await save.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "save")
    assert row is not None
    assert row["state"] == "running"
    assert await _note_count(database_url) == 0


async def test_error_inside_complete_tx_rolls_back_the_write(
    app: Akadze,
    database_url: str,
    notes: None,
) -> None:
    # Arrange
    @app.task("save")
    async def save() -> None:
        async with current().complete_tx() as tx:
            await tx.execute(text("INSERT INTO public.akadze_test_notes (body) VALUES ('nope')"))
            raise RuntimeError("nope")

    async with app.engine.begin() as session:
        await save.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "save")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 1
    assert await _note_count(database_url) == 0


async def test_cancel_during_complete_tx_rolls_back_the_write(
    app: Akadze,
    database_url: str,
    notes: None,
) -> None:
    # Arrange
    @app.task("save")
    async def save() -> None:
        async with current().complete_tx() as tx:
            await tx.execute(text("INSERT INTO public.akadze_test_notes (body) VALUES ('gone')"))
            async with connection(database_url) as opened:
                await opened.execute(
                    "UPDATE akadze.jobs SET cancel_requested_at = now() WHERE task = 'save'"
                )

    async with app.engine.begin() as session:
        await save.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "save")
    assert row is not None
    assert row["state"] == "cancelled"
    assert row["attempt"] == 0
    assert await _note_count(database_url) == 0


async def test_transition_hook_error_rolls_back_the_write(
    app: Akadze,
    database_url: str,
    notes: None,
) -> None:
    # Arrange
    def reject_success(
        _connection: AsyncConnection,
        _job: Job,
        _from_state: str,
        to_state: str,
    ) -> None:
        if to_state == "succeeded":
            raise RuntimeError("hook")

    app.hooks.on_transition.append(reject_success)

    @app.task("save")
    async def save() -> None:
        async with current().complete_tx() as tx:
            await tx.execute(text("INSERT INTO public.akadze_test_notes (body) VALUES ('hook')"))

    async with app.engine.begin() as session:
        await save.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "save")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 1
    assert await _note_count(database_url) == 0


async def test_nested_complete_tx_rolls_back_the_write(
    app: Akadze,
    database_url: str,
    notes: None,
) -> None:
    # Arrange
    @app.task("save")
    async def save() -> None:
        async with current().complete_tx() as tx:
            await tx.execute(text("INSERT INTO public.akadze_test_notes (body) VALUES ('nested')"))
            async with current().complete_tx():
                pass

    async with app.engine.begin() as session:
        await save.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "save")
    assert row is not None
    assert row["state"] == "queued"
    assert row["attempt"] == 1
    assert await _note_count(database_url) == 0


def test_current_is_only_valid_inside_a_job() -> None:
    # Act / Assert
    with pytest.raises(AkadzeError, match="inside a running job"):
        current()
