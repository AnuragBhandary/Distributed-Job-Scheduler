"""Celery-style Python client.

    from jobq import JobQ

    app = JobQ("http://localhost:8000", api_key="jq_...")

    @app.task(max_attempts=5, queue="emails")
    def send_email(to: str, subject: str) -> None: ...

    result = send_email.delay("a@b.com", subject="hi")    # enqueue now
    send_email.apply_async(args=["a@b.com"], countdown=60)  # enqueue for later
    result.get(timeout=30)                                  # wait for the return value

The same decorated module is imported by workers (``jobq worker --tasks mymodule``), which is
how a task name maps to code on both sides.

Every enqueue carries an ``Idempotency-Key`` (generated when the caller does not pass one), so the
client can retry timeouts, 5xx responses and 429s without ever creating a duplicate job.
"""

from __future__ import annotations

import functools
import os
import random
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any, Generic, ParamSpec, TypeVar, overload
from uuid import uuid4

import httpx

from jobq.errors import APIError, JobFailed
from jobq.tasks import Registry, TaskSpec, default_registry

P = ParamSpec("P")
R = TypeVar("R")

TERMINAL = frozenset({"succeeded", "dead", "cancelled"})
_RETRYABLE_STATUS = frozenset({429, 502, 503, 504})


class JobQClient:
    """Thin synchronous HTTP client for the jobq REST API, with safe automatic retries."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout_s: float = 10.0,
        max_retries: int = 4,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"X-API-Key": api_key, "User-Agent": "jobq-python"},
            timeout=timeout_s,
            transport=transport,
        )
        self.max_retries = max_retries

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> JobQClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        for attempt in range(self.max_retries + 1):
            last = attempt == self.max_retries
            try:
                response = self._http.request(method, path, **kwargs)
            except httpx.TransportError:
                if last:
                    raise
                time.sleep(random.uniform(0, min(5.0, 0.1 * 2**attempt)))
                continue
            if response.status_code in _RETRYABLE_STATUS and not last:
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else min(5.0, 0.1 * 2**attempt)
                time.sleep(random.uniform(delay, delay * 1.2))
                continue
            if response.is_error:
                try:
                    detail = response.json().get("detail")
                except ValueError:
                    detail = response.text
                raise APIError(response.status_code, detail)
            return response.json()
        raise AssertionError("unreachable")

    def enqueue(
        self,
        task: str,
        args: list[Any] | tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        queue: str | None = None,
        run_at: datetime | None = None,
        delay_s: float | None = None,
        max_attempts: int | None = None,
        timeout_s: float | None = None,
        backoff_base_s: float | None = None,
        backoff_max_s: float | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"task": task, "args": list(args), "kwargs": kwargs or {}}
        optional = {
            "queue": queue,
            "run_at": run_at.isoformat() if run_at else None,
            "delay_s": delay_s,
            "max_attempts": max_attempts,
            "timeout_s": timeout_s,
            "backoff_base_s": backoff_base_s,
            "backoff_max_s": backoff_max_s,
        }
        body.update({k: v for k, v in optional.items() if v is not None})
        headers = {"Idempotency-Key": idempotency_key or str(uuid4())}
        result: dict[str, Any] = self._request("POST", "/v1/jobs", json=body, headers=headers)
        return result

    def get(self, job_id: str) -> dict[str, Any]:
        result: dict[str, Any] = self._request("GET", f"/v1/jobs/{job_id}")
        return result

    def list_jobs(self, **filters: Any) -> dict[str, Any]:
        params = {k: v for k, v in filters.items() if v is not None}
        result: dict[str, Any] = self._request("GET", "/v1/jobs", params=params)
        return result

    def executions(self, job_id: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = self._request("GET", f"/v1/jobs/{job_id}/executions")
        return result

    def cancel(self, job_id: str) -> dict[str, Any]:
        result: dict[str, Any] = self._request("POST", f"/v1/jobs/{job_id}/cancel")
        return result

    def dead_letters(self, **filters: Any) -> dict[str, Any]:
        params = {k: v for k, v in filters.items() if v is not None}
        result: dict[str, Any] = self._request("GET", "/v1/dlq", params=params)
        return result

    def requeue(self, job_id: str) -> dict[str, Any]:
        result: dict[str, Any] = self._request("POST", f"/v1/dlq/{job_id}/requeue")
        return result

    def stats(self) -> dict[str, Any]:
        result: dict[str, Any] = self._request("GET", "/v1/stats")
        return result


class AsyncResult:
    """Handle to an enqueued job (named after Celery's equivalent)."""

    def __init__(self, client: JobQClient, job: dict[str, Any]) -> None:
        self._client = client
        self.job = job

    @property
    def id(self) -> str:
        return str(self.job["id"])

    @property
    def status(self) -> str:
        return str(self.job["status"])

    def refresh(self) -> AsyncResult:
        self.job = self._client.get(self.id)
        return self

    def ready(self) -> bool:
        return self.refresh().status in TERMINAL

    def get(self, timeout: float | None = None, poll_interval_s: float = 0.2) -> Any:
        """Block until the job finishes; return its result or raise ``JobFailed``."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self.ready():
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"job {self.id} still {self.status} after {timeout}s")
            time.sleep(poll_interval_s)
        if self.status != "succeeded":
            raise JobFailed(self.job)
        return self.job["result"]

    def cancel(self) -> AsyncResult:
        self.job = self._client.cancel(self.id)
        return self

    def executions(self) -> list[dict[str, Any]]:
        return self._client.executions(self.id)

    def __repr__(self) -> str:
        return f"<AsyncResult {self.id} {self.status}>"


class Task(Generic[P, R]):
    """A registered task. Calling it runs it locally; ``delay``/``apply_async`` enqueue it."""

    def __init__(self, app: JobQ, spec: TaskSpec) -> None:
        self.app = app
        self.spec = spec
        self.name = spec.name
        functools.update_wrapper(self, spec.fn)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        result: R = self.spec.fn(*args, **kwargs)
        return result

    def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        return self.apply_async(args=args, kwargs=kwargs)

    def apply_async(
        self,
        args: list[Any] | tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        countdown: float | None = None,
        eta: datetime | None = None,
        queue: str | None = None,
        idempotency_key: str | None = None,
        max_attempts: int | None = None,
        timeout_s: float | None = None,
    ) -> AsyncResult:
        spec = self.spec
        job = self.app.client.enqueue(
            spec.name,
            args,
            kwargs,
            queue=queue or spec.queue,
            run_at=eta,
            delay_s=countdown,
            max_attempts=max_attempts or spec.max_attempts,
            timeout_s=timeout_s or spec.timeout_s,
            backoff_base_s=spec.backoff_base_s,
            backoff_max_s=spec.backoff_max_s,
            idempotency_key=idempotency_key,
        )
        return AsyncResult(self.app.client, job)


class JobQ:
    """Application object: defines tasks and knows how to reach the API.

    ``base_url`` and ``api_key`` default to ``JOBQ_API_URL`` / ``JOBQ_API_KEY``; the HTTP client
    is created lazily, so workers can import task modules without any API configuration.
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        *,
        registry: Registry = default_registry,
        default_queue: str = "default",
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url
        self.api_key = api_key
        self.registry = registry
        self.default_queue = default_queue
        self._transport = transport
        self._client: JobQClient | None = None

    @property
    def client(self) -> JobQClient:
        if self._client is None:
            base_url = self.base_url or os.environ.get("JOBQ_API_URL", "http://localhost:8000")
            api_key = self.api_key or os.environ.get("JOBQ_API_KEY")
            if not api_key:
                raise RuntimeError("no API key: pass api_key= or set JOBQ_API_KEY")
            self._client = JobQClient(base_url, api_key, transport=self._transport)
        return self._client

    @overload
    def task(self, fn: Callable[P, R]) -> Task[P, R]: ...

    @overload
    def task(
        self,
        fn: None = None,
        *,
        name: str | None = None,
        queue: str | None = None,
        max_attempts: int = 3,
        timeout_s: float = 300.0,
        backoff_base_s: float = 1.0,
        backoff_max_s: float = 300.0,
        bind: bool = False,
    ) -> Callable[[Callable[P, R]], Task[P, R]]: ...

    def task(
        self,
        fn: Callable[P, R] | None = None,
        *,
        name: str | None = None,
        queue: str | None = None,
        max_attempts: int = 3,
        timeout_s: float = 300.0,
        backoff_base_s: float = 1.0,
        backoff_max_s: float = 300.0,
        bind: bool = False,
    ) -> Task[P, R] | Callable[[Callable[P, R]], Task[P, R]]:
        def decorate(func: Callable[P, R]) -> Task[P, R]:
            spec = TaskSpec(
                name=name or f"{func.__module__}.{func.__qualname__}",
                fn=func,
                queue=queue or self.default_queue,
                max_attempts=max_attempts,
                timeout_s=timeout_s,
                backoff_base_s=backoff_base_s,
                backoff_max_s=backoff_max_s,
                bind=bind,
            )
            return Task(self, self.registry.register(spec))

        return decorate(fn) if fn is not None else decorate
