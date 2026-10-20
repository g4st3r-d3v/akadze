from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from akadze import Akadze, AkadzeError
from akadze.outcome import backoff
from akadze.worker import Worker
from tests.pg import job_row


def _boom(attempt: int) -> timedelta:
    raise RuntimeError("policy bug")


@pytest.mark.parametrize("attempt", [1, 3, 6])
def test_default_backoff_is_exponential_with_jitter(attempt: int) -> None:
    # Arrange
    ceiling = timedelta(seconds=2 ** (attempt - 1))

    # Act
    delays = {backoff(attempt, None) for _ in range(200)}

    # Assert
    assert all(ceiling / 2 <= delay <= ceiling for delay in delays)
    assert len(delays) > 1


def test_default_backoff_stays_under_the_cap() -> None:
    # Arrange / Act
    delay = backoff(40, None)

    # Assert
    assert timedelta(seconds=150) <= delay <= timedelta(seconds=300)


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (lambda attempt: timedelta(minutes=attempt), timedelta(minutes=2)),
        (lambda attempt: timedelta(seconds=-5), timedelta(0)),
    ],
    ids=["policy", "negative-is-zero"],
)
def test_task_policy_sets_the_delay(policy: object, expected: timedelta) -> None:
    # Arrange / Act
    delay = backoff(2, None, policy)  # type: ignore[arg-type]

    # Assert
    assert delay == expected


def test_retry_delay_wins_over_the_policy() -> None:
    # Arrange / Act
    delay = backoff(2, timedelta(seconds=7), lambda attempt: timedelta(hours=1))

    # Assert
    assert delay == timedelta(seconds=7)


@pytest.mark.parametrize(
    "policy",
    [_boom, lambda attempt: 30],
    ids=["raises", "not-a-timedelta"],
)
def test_broken_policy_falls_back_to_the_default(policy: object) -> None:
    # Arrange / Act
    delay = backoff(3, None, policy)  # type: ignore[arg-type]

    # Assert
    assert timedelta(seconds=2) <= delay <= timedelta(seconds=4)


def test_register_rejects_a_non_callable_backoff(app: Akadze) -> None:
    # Arrange
    async def demo() -> None:
        return None

    # Act / Assert
    with pytest.raises(AkadzeError, match="backoff must be callable"):
        app.register(demo, name="demo", backoff=timedelta(seconds=5))  # type: ignore[arg-type]


async def test_worker_schedules_the_retry_with_the_task_policy(
    app: Akadze, database_url: str
) -> None:
    # Arrange
    @app.task("demo", backoff=lambda attempt: timedelta(minutes=10 * attempt))
    async def demo() -> None:
        raise RuntimeError("boom")

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()

    # Act
    await Worker(app).step()

    # Assert
    row = await job_row(database_url, "demo")
    assert row is not None
    assert row["state"] == "queued"
    wait = row["run_at"] - datetime.now(UTC)
    assert timedelta(minutes=9) < wait <= timedelta(minutes=10)
