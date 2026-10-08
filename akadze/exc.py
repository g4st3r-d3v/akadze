"""Failures a caller can catch. Task control flow uses Retry, Snooze, Fail, and Cancel."""

from __future__ import annotations

from datetime import timedelta


class AkadzeError(Exception):
    """The caller used akadze incorrectly."""


class EnqueueError(AkadzeError):
    """The job was not inserted."""


class DuplicateJob(EnqueueError):
    """An active job already uses this unique_key."""


class Retry(Exception):
    """Run the job again later. This spends an attempt."""

    def __init__(self, delay: timedelta | None = None) -> None:
        super().__init__("retry")
        self.delay = delay


class Snooze(Exception):
    """Run the job again later. This does not spend an attempt."""

    def __init__(self, delay: timedelta) -> None:
        super().__init__("snooze")
        self.delay = delay


class Fail(Exception):
    """Stop the job. Further attempts are not used."""

    def __init__(self, reason: str = "") -> None:
        super().__init__(reason)


class Cancel(Exception):
    """Stop the job without spending an attempt."""

    def __init__(self, reason: str = "") -> None:
        super().__init__(reason)
