"""Decide the next job state. This function does not touch the database."""

from __future__ import annotations

import json
import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

from akadze.exc import Cancel, Fail, Retry, Snooze
from akadze.job import Job
from akadze.schema import PASSWORD_IN_URL

_MAX_BACKOFF_SECONDS = 300
_JITTER_FLOOR = 0.5
_MAX_POLICY_DELAY = timedelta(days=365)

logger = logging.getLogger("akadze.outcome")

BackoffPolicy = Callable[[int], timedelta]


class Unstored(Fail):
    """The body returned, but its outcome could not be stored. A cancel does not apply."""


_MESSAGE_LIMIT = 500


@dataclass(frozen=True)
class Outcome:
    state: str
    attempt: int
    snoozes: int
    delay: timedelta | None
    error_type: str | None
    error_message: str | None
    result_json: str | None


def decide(
    job: Job,
    exc: BaseException | None,
    result: object,
    policy: BackoffPolicy | None = None,
) -> Outcome:
    stopped = job.cancel_requested_at is not None or job.expired
    if stopped and not isinstance(exc, Unstored) and (exc is not None or not _is_json(result)):
        # A cancel or the expiry came while the body ran. A body that returned
        # a storable result keeps it (its side effects happened); one that
        # raised, whatever it raised, or returned a result that cannot be
        # stored, is cancelled without spending an attempt.
        exc = Cancel()
    if isinstance(exc, Cancel):
        return _terminal(job, "cancelled", job.attempt, "Cancel", exc)
    if isinstance(exc, Snooze):
        return Outcome(
            state="queued",
            attempt=job.attempt,
            snoozes=job.snoozes + 1,
            delay=_non_negative(exc.delay),
            error_type=None,
            error_message=None,
            result_json=None,
        )
    if isinstance(exc, Fail):
        return _terminal(job, "failed", job.attempt + 1, "Fail", exc)
    if exc is not None:
        attempt = job.attempt + 1
        delay = exc.delay if isinstance(exc, Retry) else None
        if attempt >= job.max_attempts:
            return _terminal(job, "failed", attempt, type(exc).__name__, exc)
        return Outcome(
            state="queued",
            attempt=attempt,
            snoozes=job.snoozes,
            delay=backoff(attempt, delay, policy),
            error_type=type(exc).__name__,
            error_message=_message(exc),
            result_json=None,
        )
    try:
        result_json = None if result is None else json.dumps(result)
    except TypeError:
        attempt = job.attempt + 1
        if attempt >= job.max_attempts:
            return Outcome(
                "failed",
                attempt,
                job.snoozes,
                None,
                "ResultNotJson",
                "result is not JSON",
                None,
            )
        return Outcome(
            "queued",
            attempt,
            job.snoozes,
            backoff(attempt, None, policy),
            "ResultNotJson",
            "result is not JSON",
            None,
        )
    return Outcome("succeeded", job.attempt, job.snoozes, None, None, None, result_json)


def backoff(
    attempt: int,
    explicit: timedelta | None,
    policy: BackoffPolicy | None = None,
    jitter: Callable[[], float] = random.random,
) -> timedelta:
    """Delay before the next attempt: Retry's delay, else the task policy, else the default.

    The default is exponential (``2^(attempt-1)`` seconds, capped) with jitter in
    ``[50 %, 100 %]`` so a batch that failed together does not retry together.
    A policy that raises or returns a non-timedelta falls back to the default.
    Delays are clamped to ``[0, 365 days]``. ``jitter`` returns a float in
    ``[0, 1)``; tests pass a fixed one.
    """

    if explicit is not None:
        return _non_negative(explicit)
    if policy is not None:
        try:
            chosen = policy(attempt)
        except Exception:
            logger.warning("backoff policy raised; using the default", exc_info=True)
        else:
            if isinstance(chosen, timedelta):
                return min(_non_negative(chosen), _MAX_POLICY_DELAY)
            logger.warning("backoff policy returned %s; using the default", type(chosen).__name__)
    seconds = min(2 ** (attempt - 1), _MAX_BACKOFF_SECONDS)
    return timedelta(seconds=seconds * (_JITTER_FLOOR + (1 - _JITTER_FLOOR) * jitter()))


def push_error(errors: list[dict[str, str]], outcome: Outcome) -> list[dict[str, str]]:
    if outcome.error_type is None:
        return errors
    entry = {"error": outcome.error_type, "message": outcome.error_message or ""}
    return [*errors, entry][-10:]


def _is_json(result: object) -> bool:
    try:
        json.dumps(result)
    except TypeError:
        return False
    return True


def _terminal(job: Job, state: str, attempt: int, error_type: str, exc: BaseException) -> Outcome:
    return Outcome(state, attempt, job.snoozes, None, error_type, _message(exc), None)


def _non_negative(delay: timedelta) -> timedelta:
    return max(delay, timedelta(0))


def _message(exc: BaseException) -> str:
    text = PASSWORD_IN_URL.sub("://***@", str(exc).strip()).replace("\x00", "")
    return text[:_MESSAGE_LIMIT]
