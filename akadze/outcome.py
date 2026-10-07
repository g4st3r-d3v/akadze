"""Decide the next job state. This function does not touch the database."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import timedelta

from akadze.exc import Cancel, Fail, Retry, Snooze
from akadze.job import Job

_MAX_BACKOFF_SECONDS = 300
_MESSAGE_LIMIT = 500
_PASSWORD_IN_URL = re.compile(r"://[^\s/@]+:[^\s/@]+@")


@dataclass(frozen=True)
class Outcome:
    state: str
    attempt: int
    snoozes: int
    delay: timedelta | None
    error_type: str | None
    error_message: str | None
    result_json: str | None


def decide(job: Job, exc: BaseException | None, result: object) -> Outcome:
    if isinstance(exc, Cancel):
        return _terminal(job, "cancelled", job.attempt, "Cancel", exc)
    if isinstance(exc, Snooze):
        return Outcome(
            state="queued",
            attempt=job.attempt,
            snoozes=job.snoozes + 1,
            delay=_positive(exc.delay),
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
            delay=backoff(attempt, delay),
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
            backoff(attempt, None),
            "ResultNotJson",
            "result is not JSON",
            None,
        )
    return Outcome("succeeded", job.attempt, job.snoozes, None, None, None, result_json)


def backoff(attempt: int, explicit: timedelta | None) -> timedelta:
    if explicit is not None:
        return _positive(explicit)
    seconds = min(2 ** (attempt - 1), _MAX_BACKOFF_SECONDS)
    return timedelta(seconds=seconds)


def push_error(errors: list[dict[str, str]], outcome: Outcome) -> list[dict[str, str]]:
    if outcome.error_type is None:
        return errors
    entry = {"error": outcome.error_type, "message": outcome.error_message or ""}
    return [*errors, entry][-10:]


def _terminal(job: Job, state: str, attempt: int, error_type: str, exc: BaseException) -> Outcome:
    return Outcome(state, attempt, job.snoozes, None, error_type, _message(exc), None)


def _positive(delay: timedelta) -> timedelta:
    if delay < timedelta(0):
        raise ValueError("delay must not be negative")
    return delay


def _message(exc: BaseException) -> str:
    text = _PASSWORD_IN_URL.sub("://***@", str(exc).strip())
    return text[:_MESSAGE_LIMIT]
