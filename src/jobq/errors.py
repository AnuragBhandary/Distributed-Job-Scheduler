"""Exceptions shared by tasks, the worker and the client SDK."""

from __future__ import annotations

from typing import Any


class JobQError(Exception):
    """Base class for every jobq exception."""


class PermanentError(JobQError):
    """Raise from a task to send the job straight to the dead-letter queue (no more retries)."""


class RetryLater(JobQError):
    """Raise from a task to retry after an explicit delay instead of the backoff schedule."""

    def __init__(self, delay_s: float, message: str = "retry requested by task") -> None:
        super().__init__(message)
        self.delay_s = max(0.0, float(delay_s))


class JobTimeout(JobQError):
    """The task ran longer than the job's ``timeout_s``."""


class UnknownTask(JobQError):
    """No handler is registered under the job's task name on this worker."""


class APIError(JobQError):
    """The jobq API returned an error response."""

    def __init__(self, status_code: int, detail: Any) -> None:
        super().__init__(f"HTTP {status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail


class JobFailed(JobQError):
    """``AsyncResult.get()`` found the job dead or cancelled."""

    def __init__(self, job: dict[str, Any]) -> None:
        super().__init__(f"job {job['id']} ended as {job['status']}: {job.get('last_error')}")
        self.job = job
