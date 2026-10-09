"""REST API: submission, idempotency, tenancy, pagination, cancel, DLQ, auth, rate limits."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import asyncpg
import httpx
import redis.asyncio as aioredis
from asgi_lifespan import LifespanManager

from jobq.api.app import create_app
from jobq.auth import create_api_key, revoke_api_key
from jobq.config import Settings
from jobq.dispatch import stream_key
from jobq.ratelimit import RateLimiter


async def _enqueue(client: httpx.AsyncClient, **body: Any) -> httpx.Response:
    body.setdefault("task", "examples.echo")
    headers = {}
    if "idempotency_key" in body:
        headers["Idempotency-Key"] = body.pop("idempotency_key")
    return await client.post("/v1/jobs", json=body, headers=headers)


async def test_immediate_job_is_queued_and_dispatched(
    client: httpx.AsyncClient, redis: Any
) -> None:
    resp = await _enqueue(client, args=[1, 2], kwargs={"x": "y"}, queue="emails")
    assert resp.status_code == 201, resp.text
    job = resp.json()
    assert job["status"] == "queued" and job["queue"] == "emails" and job["attempts"] == 0
    assert resp.headers["location"] == f"/v1/jobs/{job['id']}"
    messages = await redis.xrange(stream_key("emails"))
    assert [m[1]["id"] for m in messages] == [job["id"]]


async def test_delayed_job_is_scheduled_not_dispatched(
    client: httpx.AsyncClient, redis: Any
) -> None:
    resp = await _enqueue(client, delay_s=60)
    job = resp.json()
    assert job["status"] == "scheduled"
    run_at = datetime.fromisoformat(job["run_at"])
    assert run_at > datetime.now(UTC) + timedelta(seconds=50)
    assert await redis.exists(stream_key("default")) == 0


async def test_run_at_in_the_past_runs_now(client: httpx.AsyncClient) -> None:
    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    assert (await _enqueue(client, run_at=past)).json()["status"] == "queued"


async def test_validation_errors(client: httpx.AsyncClient) -> None:
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    assert (await _enqueue(client, run_at=future, delay_s=5)).status_code == 422
    assert (await _enqueue(client, task="bad name!")).status_code == 422
    assert (await _enqueue(client, max_attempts=0)).status_code == 422
    assert (await _enqueue(client, run_at="2030-01-01T00:00:00")).status_code == 422  # naive


async def test_idempotent_replay_returns_original(client: httpx.AsyncClient, redis: Any) -> None:
    first = await _enqueue(client, args=[1], idempotency_key="order-42")
    second = await _enqueue(client, args=[1], idempotency_key="order-42")
    assert first.status_code == 201 and second.status_code == 200
    assert second.headers["idempotent-replayed"] == "true"
    assert first.json()["id"] == second.json()["id"]
    assert await redis.xlen(stream_key("default")) == 1  # dispatched once


async def test_idempotency_key_reuse_with_different_body_conflicts(
    client: httpx.AsyncClient,
) -> None:
    assert (await _enqueue(client, args=[1], idempotency_key="k")).status_code == 201
    resp = await _enqueue(client, args=[2], idempotency_key="k")
    assert resp.status_code == 409


async def test_concurrent_duplicate_submissions_create_one_job(
    client: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    responses = await asyncio.gather(
        *[_enqueue(client, args=[7], idempotency_key="race") for _ in range(20)]
    )
    assert sorted(r.status_code for r in responses) == [200] * 19 + [201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert await pool.fetchval("SELECT count(*) FROM jobs") == 1


async def test_get_job_and_tenant_isolation(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    job = (await _enqueue(client)).json()
    assert (await client.get(f"/v1/jobs/{job['id']}")).json()["id"] == job["id"]
    assert (await client.get(f"/v1/jobs/{uuid4()}")).status_code == 404

    _, other_key = await create_api_key(pool, "other", rate_per_s=100, burst=100)
    resp = await client.get(f"/v1/jobs/{job['id']}", headers={"X-API-Key": other_key})
    assert resp.status_code == 404  # another tenant's job is invisible, not forbidden


async def test_list_pagination_and_filters(client: httpx.AsyncClient) -> None:
    ids = [(await _enqueue(client, queue="a" if i % 2 else "b")).json()["id"] for i in range(7)]
    seen: list[str] = []
    cursor = None
    while True:
        params: dict[str, Any] = {"limit": 3}
        if cursor:
            params["cursor"] = cursor
        page = (await client.get("/v1/jobs", params=params)).json()
        seen += [j["id"] for j in page["items"]]
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert seen == list(reversed(ids))  # newest first, no gaps or duplicates
    only_a = (await client.get("/v1/jobs", params={"queue": "a"})).json()["items"]
    assert len(only_a) == 3
    queued = await client.get("/v1/jobs", params={"status": "queued", "task": "examples.echo"})
    assert len(queued.json()["items"]) == 7
    assert (await client.get("/v1/jobs", params={"cursor": "%%%"})).status_code == 400


async def test_cancel(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    job = (await _enqueue(client, delay_s=60)).json()
    resp = await client.post(f"/v1/jobs/{job['id']}/cancel")
    assert resp.status_code == 200 and resp.json()["status"] == "cancelled"
    again = await client.post(f"/v1/jobs/{job['id']}/cancel")
    assert again.status_code == 409
    assert (await client.post(f"/v1/jobs/{uuid4()}/cancel")).status_code == 404


async def test_dead_letter_queue_list_and_requeue(
    client: httpx.AsyncClient, pool: asyncpg.Pool, redis: Any
) -> None:
    job = (await _enqueue(client, max_attempts=2)).json()
    await pool.execute(
        "UPDATE jobs SET status = 'dead', attempts = 2, finished_at = now() WHERE id = $1",
        uuid_of(job),
    )
    dlq = (await client.get("/v1/dlq")).json()
    assert [j["id"] for j in dlq["items"]] == [job["id"]]

    resp = await client.post(f"/v1/dlq/{job['id']}/requeue")
    assert resp.status_code == 200 and resp.json()["status"] == "queued"
    row = await pool.fetchrow(
        "SELECT attempts, attempt_offset FROM jobs WHERE id = $1", uuid_of(job)
    )
    assert (row["attempts"], row["attempt_offset"]) == (2, 2)  # fresh budget, history kept
    assert await redis.xlen(stream_key("default")) == 2
    assert (await client.post(f"/v1/dlq/{job['id']}/requeue")).status_code == 409


async def test_executions_and_stats(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    job = (await _enqueue(client, queue="q1")).json()
    await pool.execute(
        """INSERT INTO executions (job_id, attempt, worker_id, lease_token, status, finished_at)
           VALUES ($1, 1, 'w1', $2, 'failed', now())""",
        uuid_of(job),
        uuid4(),
    )
    execs = (await client.get(f"/v1/jobs/{job['id']}/executions")).json()
    assert execs[0]["worker_id"] == "w1" and execs[0]["duration_ms"] is not None
    assert (await client.get(f"/v1/jobs/{uuid4()}/executions")).status_code == 404
    await _enqueue(client, queue="q1", delay_s=30)
    stats = (await client.get("/v1/stats")).json()
    assert stats == {"queues": [{"queue": "q1", "counts": {"scheduled": 1, "queued": 1}}]}


async def test_auth_required(client: httpx.AsyncClient, pool: asyncpg.Pool, api_key: Any) -> None:
    assert (await client.get("/v1/stats", headers={"X-API-Key": ""})).status_code == 401
    bad = "jq_0123456789ab_" + "x" * 30
    assert (await client.get("/v1/stats", headers={"X-API-Key": bad})).status_code == 401
    wrong_secret = api_key[1][:16] + "y" * 43
    assert (await client.get("/v1/stats", headers={"X-API-Key": wrong_secret})).status_code == 401
    bearer = {"X-API-Key": "", "Authorization": f"Bearer {api_key[1]}"}
    assert (await client.get("/v1/stats", headers=bearer)).status_code == 200
    await revoke_api_key(pool, api_key[0])
    assert (await client.get("/v1/stats")).status_code == 401


async def test_rate_limit(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    _, key = await create_api_key(pool, "tiny", rate_per_s=0.5, burst=3)
    codes = [
        (await client.get("/v1/stats", headers={"X-API-Key": key})).status_code for _ in range(5)
    ]
    assert codes == [200, 200, 200, 429, 429]
    resp = await client.get("/v1/stats", headers={"X-API-Key": key})
    assert int(resp.headers["retry-after"]) >= 1
    ok = await client.get("/v1/stats")
    assert "x-ratelimit-remaining" in ok.headers


async def test_rate_limiter_fails_open_when_redis_is_down(
    client: httpx.AsyncClient, redis: Any
) -> None:
    _break_redis(client._transport.app.state.jobq)  # type: ignore[attr-defined]
    resp = await _enqueue(client)
    assert resp.status_code == 201  # accepted; the sweeper dispatches it later
    assert resp.json()["status"] == "queued"


async def test_payload_limit_health_and_metrics(
    settings: Settings, pool: asyncpg.Pool, redis: Any
) -> None:
    app = create_app(settings.model_copy(update={"max_payload_bytes": 100}))
    _, key = await create_api_key(pool, "p", rate_per_s=100, burst=100)
    async with (
        LifespanManager(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t", headers={"X-API-Key": key}
        ) as http,
    ):
        resp = await http.post("/v1/jobs", json={"task": "t", "args": ["x" * 200]})
        assert resp.status_code == 413
        assert (await http.get("/healthz")).json() == {"status": "ok"}
        assert (await http.get("/readyz")).json() == {"postgres": "ok", "redis": "ok"}
        assert "jobq_enqueue_seconds" in (await http.get("/metrics")).text
        _break_redis(app.state.jobq)
        ready = await http.get("/readyz")
        assert ready.status_code == 503 and ready.json()["redis"].startswith("error")


async def test_bootstrap_key(settings: Settings, pool: asyncpg.Pool, redis: Any) -> None:
    key = "jq_0000000000ab_bootstrap-key-for-tests-only"
    app = create_app(settings.model_copy(update={"bootstrap_api_key": key}))
    async with (
        LifespanManager(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t", headers={"X-API-Key": key}
        ) as http,
    ):
        assert (await http.get("/v1/stats")).status_code == 200


def _break_redis(state: Any) -> None:
    """Point the app at a port where nothing listens, simulating a Redis outage."""
    dead = aioredis.from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.2)
    state.redis = dead
    state.limiter = RateLimiter(dead)


def uuid_of(job: dict[str, Any]) -> Any:
    from uuid import UUID

    return UUID(job["id"])


async def test_workers_and_throughput(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    job = (await _enqueue(client)).json()
    other = (await _enqueue(client)).json()
    for attempt, (worker, status, finished) in enumerate(
        [("w1", "failed", True), ("w2", "succeeded", True)], start=1
    ):
        await pool.execute(
            """INSERT INTO executions (job_id, attempt, worker_id, lease_token, status,
                                       finished_at)
               VALUES ($1, $2, $3, $4, $5, CASE WHEN $6 THEN now() END)""",
            uuid_of(job), attempt, worker, uuid4(), status, finished,
        )  # fmt: skip
    await pool.execute(
        """INSERT INTO executions (job_id, attempt, worker_id, lease_token, status, started_at)
           VALUES ($1, 1, 'w3', $2, 'running', now() - interval '2 hours')""",
        uuid_of(other),
        uuid4(),
    )
    workers = {w["worker_id"]: w for w in (await client.get("/v1/workers")).json()}
    assert set(workers) == {"w1", "w2", "w3"}  # w3 started long ago but is still running
    assert (workers["w1"]["failed"], workers["w2"]["succeeded"], workers["w3"]["running"]) == (
        1,
        1,
        1,
    )
    data = (await client.get("/v1/stats/throughput", params={"bucket_s": 5})).json()
    assert data["bucket_s"] == 5
    buckets = data["buckets"]
    assert sum(b["succeeded"] for b in buckets) == 1 and sum(b["failed"] for b in buckets) == 1
    assert all(int(b["start"][17:19]) % 5 == 0 for b in buckets)  # aligned to 5 s


async def test_dashboard_routes(client: httpx.AsyncClient) -> None:
    page = await client.get("/jobs/123")
    assert page.status_code == 200 and "<title>jobq" in page.text
    assert (await client.get("/v1/no-such-route")).status_code == 404
