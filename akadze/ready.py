"""When a queued job is ready: the SQL conditions claim, cancel and drain share.

Each function returns a parenthesised boolean SQL fragment over the
``akadze.jobs`` row named ``row``. Keep the claim window, the claim's re-check,
the worker's cancel step, ``testing.drain`` and the lag metric on these, so
they cannot drift apart.
"""

from __future__ import annotations


def due(row: str) -> str:
    """Queued and its ``run_at`` has come."""

    return f"({row}.state = 'queued' AND {row}.run_at <= now())"


def expired(row: str) -> str:
    """Its ``expires_at`` has passed."""

    return f"({row}.expires_at IS NOT NULL AND {row}.expires_at <= now())"


def to_cancel(row: str) -> str:
    """Cancel requested, or expired: the cancel step closes it, a claim does not."""

    return f"({row}.cancel_requested_at IS NOT NULL OR {expired(row)})"


def unpaused(queue: str) -> str:
    """The queue named by the SQL expression ``queue`` is not paused.

    Pass a bind parameter such as ``:queue`` when the query is about one queue:
    Postgres then checks the pause once, not per row.
    """

    return f"(NOT EXISTS (SELECT 1 FROM akadze.queue_pauses WHERE queue_pauses.queue = {queue}))"


def claimable(row: str, queue: str | None = None) -> str:
    """A claim may take it, limits and registered tasks aside."""

    return f"({due(row)} AND NOT {to_cancel(row)} AND {unpaused(queue or f'{row}.queue')})"
