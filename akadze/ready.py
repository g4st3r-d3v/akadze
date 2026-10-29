"""When a queued job is ready: the SQL conditions claim, cancel and drain share.

Each function returns a boolean SQL fragment over the ``akadze.jobs`` row
named ``row``. Keep the claim window, the claim's re-check, the worker's
cancel step and ``testing.drain`` on these, so they cannot drift apart.
"""

from __future__ import annotations


def due(row: str) -> str:
    """Queued and its ``run_at`` has come."""

    return f"{row}.state = 'queued' AND {row}.run_at <= now()"


def to_cancel(row: str) -> str:
    """Cancel requested, or ``expires_at`` passed: the cancel step closes it, a claim does not."""

    return (
        f"({row}.cancel_requested_at IS NOT NULL"
        f" OR ({row}.expires_at IS NOT NULL AND {row}.expires_at <= now()))"
    )


def unpaused(row: str) -> str:
    """Its queue is not paused."""

    return f"NOT EXISTS (SELECT 1 FROM akadze.queue_pauses WHERE queue_pauses.queue = {row}.queue)"


def claimable(row: str) -> str:
    """A claim may take it, limits and registered tasks aside."""

    return f"{due(row)} AND NOT {to_cancel(row)} AND {unpaused(row)}"
