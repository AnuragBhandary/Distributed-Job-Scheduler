"""The Celery-style client SDK, against a real API server."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from jobq import JobFailed, JobQ, JobQClient
from jobq.auth import create_api_key
from jobq.config import Settings
from jobq.errors import APIError
from jobq.tasks import Registry
from jobq.worker import Worker
from tests.conftest import LiveServer
from tests.helpers import running_scheduler, running_worker

registry = Registry()
app = JobQ(registry=registry)


@app.task
def multiply(a: int, b: int) -> int:
    return a * b


@app.task(name="client.always_fails", max_attempts=1)
def always_fails() -> None:
    raise RuntimeError("nope")


def test_calling_a_task_runs_it_locally() -> None:
    assert multiply(3, 4) == 12
    assert multiply.name == "tests.test_client.multiply"
    assert multiply.__name__ == "multiply"


async def test_delay_get_and_failures(
    live_server: LiveServer, settings: Settings, pool: Any, redis: Any
) -> None:
    _, key = await create_api_key(pool, "sdk", rate_per_s=1000, burst=1000)
    app.base_url, app.api_key, app._client = live_server.url, key, None
    worker = Worker(settings, registry=registry, pool=pool, redis=redis)
    async with running_worker(worker), running_scheduler(settings, pool, redis):
        result = await asyncio.to_thread(multiply.delay, 6, 7)
        assert result.status == "queued" and repr(result).startswith("<AsyncResult")
        assert await asyncio.to_thread(result.get, 10) == 42
        assert [e["status"] for e in await asyncio.to_thread(result.executions)] == ["succeeded"]

        failed = await asyncio.to_thread(always_fails.delay)
        with pytest.raises(JobFailed):
            await asyncio.to_thread(failed.get, 10)
        dlq = await asyncio.to_thread(app.client.dead_letters)
        assert [j["id"] for j in dlq["items"]] == [failed.id]
        requeued = await asyncio.to_thread(app.client.requeue, failed.id)
        assert requeued["status"] == "queued"

        later = await asyncio.to_thread(lambda: multiply.apply_async(args=[1, 1], countdown=60))
        assert later.status == "scheduled"
        with pytest.raises(TimeoutError):
            await asyncio.to_thread(later.get, 0.3, 0.1)
        assert (await asyncio.to_thread(later.cancel)).status == "cancelled"

        same = [
            await asyncio.to_thread(
                lambda: multiply.apply_async(args=[2, 2], idempotency_key="once")
            )
            for _ in range(2)
        ]
        assert same[0].id == same[1].id
        stats = await asyncio.to_thread(app.client.stats)
        assert stats["queues"][0]["queue"] == "default"
        listing = await asyncio.to_thread(lambda: app.client.list_jobs(status="succeeded"))
        assert len(listing["items"]) >= 1
    app.client.close()


def test_client_retries_transient_errors_with_same_idempotency_key() -> None:
    seen: list[tuple[int, str | None]] = []
    responses = iter([503, 429, 201])

    def handler(request: httpx.Request) -> httpx.Response:
        code = next(responses)
        seen.append((code, request.headers.get("idempotency-key")))
        if code == 201:
            return httpx.Response(201, json={"id": "j1", "status": "queued"})
        return httpx.Response(code, headers={"Retry-After": "0"} if code == 429 else {})

    with JobQClient("http://x", "k", transport=httpx.MockTransport(handler)) as client:
        assert client.enqueue("t")["id"] == "j1"
    assert [c for c, _ in seen] == [503, 429, 201]
    assert len({k for _, k in seen}) == 1  # every retry reused the same key


def test_client_retries_connection_errors_then_gives_up() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("refused")

    client = JobQClient("http://x", "k", max_retries=2, transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.ConnectError):
        client.get("abc")
    assert attempts == 3


def test_client_raises_api_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/text"):
            return httpx.Response(500, text="oops")
        return httpx.Response(404, json={"detail": "job not found"})

    client = JobQClient("http://x", "k", max_retries=0, transport=httpx.MockTransport(handler))
    with pytest.raises(APIError) as info:
        client.get("missing")
    assert info.value.status_code == 404 and info.value.detail == "job not found"
    with pytest.raises(APIError):
        client._request("GET", "/text")


def test_app_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JOBQ_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        _ = JobQ(registry=Registry()).client
    monkeypatch.setenv("JOBQ_API_KEY", "k")
    assert JobQ(registry=Registry()).client is not None
