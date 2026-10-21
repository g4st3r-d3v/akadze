"""A task receives its arguments as the types its signature declares."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel

from akadze import Akadze
from akadze.testing import drain
from tests.pg import as_json, connection, job_row


class Point(BaseModel):
    x: int


@dataclass
class Window:
    start: datetime


async def test_task_receives_models_dataclasses_and_scalars_as_declared(app: Akadze) -> None:
    # Arrange
    seen: dict[str, object] = {}

    @app.task("typed")
    async def typed(point: Point, window: Window, ref: UUID, at: datetime) -> None:
        seen.update(point=point, window=window, ref=ref, at=at)

    ref = uuid4()
    at = datetime(2026, 10, 21, 9, 30, tzinfo=UTC)
    async with app.engine.begin() as session:
        await typed.using(session=session).enqueue(
            point=Point(x=1), window=Window(start=at), ref=ref, at=at
        )

    # Act
    await drain(app)

    # Assert
    assert seen == {"point": Point(x=1), "window": Window(start=at), "ref": ref, "at": at}


async def test_stored_arguments_that_do_not_validate_fail_the_job_at_once(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("strict", max_attempts=3)
    async def strict(count: int) -> None:
        return None

    async with connection(database_url) as opened:
        await opened.execute(
            """INSERT INTO akadze.jobs (task, args) VALUES ('strict', '{"count": "many"}')"""
        )

    # Act
    await drain(app)

    # Assert
    row = await job_row(database_url, "strict")
    assert row is not None
    assert row["state"] == "failed"
    assert row["attempt"] == 1
    assert as_json(row["errors"]) == [{"error": "Fail", "message": "invalid arguments"}]
