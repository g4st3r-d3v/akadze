from __future__ import annotations

import akadze

# The public Python names. A change here is an API change: see the harness note
# "Публичная поверхность и критерии Beta" (minor bump, release notes, deprecation).
PUBLIC_NAMES = frozenset(
    {
        "Akadze",
        "AkadzeError",
        "BackoffPolicy",
        "Cancel",
        "DuplicateJob",
        "EnqueueError",
        "Fail",
        "JobSummary",
        "LimitSummary",
        "QueueSnapshot",
        "Retry",
        "Snooze",
        "StateCount",
        "Worker",
        "WorkerSummary",
        "applied_versions",
        "clear_limit",
        "current",
        "list_jobs",
        "list_limits",
        "list_workers",
        "migrate",
        "pause_queue",
        "queue_snapshot",
        "request_cancel",
        "requeue",
        "resume_queue",
        "set_limit",
    }
)


def test_package_exports_exactly_the_public_names() -> None:
    # Arrange / Act
    exported = frozenset(akadze.__all__)

    # Assert
    assert exported == PUBLIC_NAMES


def test_every_public_name_resolves() -> None:
    # Arrange / Act
    missing = [name for name in akadze.__all__ if not hasattr(akadze, name)]

    # Assert
    assert missing == []
