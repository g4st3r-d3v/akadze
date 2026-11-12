from __future__ import annotations

import argparse
import asyncio
from datetime import timedelta

import pytest

from akadze import Akadze
from akadze.cli import _milliseconds
from akadze.worker import Worker
from tests.pg import as_json, job_rows

BATCH = timedelta(milliseconds=10)


async def test_batched_outcomes_record_every_run(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo")
    async def demo(n: int) -> int:
        return n * 2

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue_many({"n": n} for n in range(30))
    worker = Worker(app, slots=30, outcome_batch=BATCH)

    # Act
    await worker.step()

    # Assert
    rows = await job_rows(database_url)
    assert {row["state"] for row in rows} == {"succeeded"}
    assert sorted(as_json(row["result"]) for row in rows) == [n * 2 for n in range(30)]


async def test_batched_outcomes_keep_retries(app: Akadze, database_url: str) -> None:
    # Arrange
    @app.task("demo", max_attempts=3)
    async def demo() -> None:
        raise ValueError("bad input")

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
    worker = Worker(app, outcome_batch=BATCH)

    # Act
    await worker.step()

    # Assert
    [row] = await job_rows(database_url)
    assert (row["state"], row["attempt"]) == ("queued", 1)
    assert as_json(row["errors"])[-1]["message"] == "bad input"


async def test_a_result_jsonb_rejects_goes_to_the_single_path(
    app: Akadze, database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange — one result has a NUL, which jsonb rejects; the others share a batch
    @app.task("demo")
    async def demo(n: int) -> str:
        return "a\x00b" if n == 0 else "ok"

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue_many({"n": n} for n in range(5))
    worker = Worker(app, slots=5, outcome_batch=BATCH)

    # Act
    await worker.step()

    # Assert
    rows = {as_json(row["args"])["n"]: row for row in await job_rows(database_url)}
    assert rows[0]["state"] == "failed"
    assert as_json(rows[0]["errors"])[0]["message"].startswith("outcome could not be stored")
    assert {rows[n]["state"] for n in range(1, 5)} == {"succeeded"}
    assert caplog.text.count("writing it alone") == 1


async def test_a_hook_error_in_a_batch_touches_only_its_row(
    app: Akadze, database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Arrange
    @app.task("demo")
    async def demo(n: int) -> int:
        return n

    def picky(connection: object, job: object, from_state: str, to_state: str) -> None:
        if to_state == "succeeded" and job.args["n"] == 1:  # type: ignore[attr-defined]
            raise RuntimeError("picky hook")

    app.hooks.on_transition.append(picky)
    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue_many({"n": n} for n in range(3))
    worker = Worker(app, slots=3, outcome_batch=BATCH)

    # Act
    await worker.step()

    # Assert — written in the batch, not by the per-job fallback
    assert {row["state"] for row in await job_rows(database_url)} == {"succeeded"}
    assert "writing it alone" not in caplog.text
    assert "picky" in caplog.text


@pytest.mark.parametrize("batch", [timedelta(0), timedelta(milliseconds=-1)])
def test_outcome_batch_must_be_positive(app: Akadze, batch: timedelta) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="outcome_batch must be positive"):
        Worker(app, outcome_batch=batch)


@pytest.mark.parametrize(("value", "expected"), [("10", 10.0), ("2.5ms", 2.5)])
def test_cli_outcome_batch_takes_milliseconds(value: str, expected: float) -> None:
    # Act
    parsed = _milliseconds(value)

    # Assert
    assert parsed == timedelta(milliseconds=expected)


@pytest.mark.parametrize("value", ["0", "0.0001", "inf", "1e20", "soon"])
def test_cli_outcome_batch_rejects_bad_values(value: str) -> None:
    # Act / Assert
    with pytest.raises(argparse.ArgumentTypeError):
        _milliseconds(value)


async def test_a_batch_is_written_once_every_run_in_flight_waits(
    app: Akadze, database_url: str
) -> None:
    # Arrange — an interval far longer than the test; the batch must not wait for it
    @app.task("demo")
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue_many({} for _ in range(5))
    worker = Worker(app, slots=5, outcome_batch=timedelta(minutes=5))

    # Act
    await asyncio.wait_for(worker.step(), 10)

    # Assert
    assert {row["state"] for row in await job_rows(database_url)} == {"succeeded"}
