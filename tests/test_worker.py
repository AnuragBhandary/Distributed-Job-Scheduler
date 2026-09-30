"""Worker behaviour: execution, retries, DLQ, timeouts, leases, fencing, shutdown."""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from jobq import JobContext, JobQ, PermanentError, RetryLater
from jobq import repository as repo
from jobq.config import Settings
from jobq.tasks import Registry
from jobq.worker import Worker
from tests.conftest import job_status, wait_until
from tests.helpers import enqueue, running_scheduler, running_worker

registry = Registry()
app = JobQ(registry=registry)
calls: dict[str, int] = {}
release_gate = threading.Event()


@app.task(name="t.add")
async def add(a: int, b: int) -> int:
    return a + b


@app.task(name="t.sync_mul")
def sync_mul(a: int, b: int) -> int:
    return a * b


@app.task(name="t.fail_times", bind=True)
async def fail_times(ctx: JobContext, key: str, times: int) -> int:
    calls[key] = calls.get(key, 0) + 1
    if ctx.attempt <= times:
        raise RuntimeError(f"attempt {ctx.attempt} failed")
    return ctx.attempt


@app.task(name="t.permanent")
async def permanent() -> None:
    raise PermanentError("bad input")


@app.task(name="t.retry_later")
async def retry_later() -> None:
    raise RetryLater(30)


@app.task(name="t.slow")
async def slow(seconds: float) -> str:
    await asyncio.sleep(seconds)
    return "done"


@app.task(name="t.unserializable")
async def unserializable() -> Any:
    return {1, 2}


@app.task(name="t.ledger", bind=True)
async def ledger(ctx: JobContext, value: int, seconds: float = 0.0) -> int:
    await asyncio.sleep(seconds)
    ctx.transactional(
        "INSERT INTO example_ledger (job_id, value) VALUES ($1, $2)", ctx.job_id, value
    )
    return value


@app.task(name="t.bad_side_effect", bind=True)
async def bad_side_effect(ctx: JobContext) -> None:
    ctx.transactional("INSERT INTO no_such_table VALUES (1)")


@pytest.fixture
def worker(settings: Settings, pool: asyncpg.Pool, redis: Any) -> Worker:
    return Worker(settings, registry=registry, queues=["default", "other"], pool=pool, redis=redis)


async def _wait_status(pool: asyncpg.Pool, job_id: UUID, status: str, timeout: float = 10) -> None:
    async def check() -> bool:
        return await job_status(pool, job_id) == status

    await wait_until(check, timeout)


async def _job(pool: asyncpg.Pool, job_id: UUID) -> asyncpg.Record:
    row = await pool.fetchrow("SELECT *, status::text AS status FROM jobs WHERE id = $1", job_id)
    assert row is not None
    return row


async def _executions(pool: asyncpg.Pool, job_id: UUID) -> list[str]:
    rows = await pool.fetch(
        "SELECT status::text FROM executions WHERE job_id = $1 ORDER BY attempt", job_id
    )
    return [r[0] for r in rows]


async def test_async_and_sync_tasks_succeed(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        a = await enqueue(pool, redis, api_key[0], "t.add", 2, b=3)
        m = await enqueue(pool, redis, api_key[0], "t.sync_mul", 4, 5, queue="other")
        await _wait_status(pool, a, "succeeded")
        await _wait_status(pool, m, "succeeded")
    job = await _job(pool, a)
    assert job["result"] == 5 and job["attempts"] == 1 and job["lease_token"] is None
    assert (await _job(pool, m))["result"] == 20
    assert await _executions(pool, a) == ["succeeded"]


async def test_transient_failures_retry_with_backoff_then_succeed(
    worker: Worker, settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    key = str(uuid4())
    async with running_worker(worker), running_scheduler(settings, pool, redis):
        job_id = await enqueue(pool, redis, api_key[0], "t.fail_times", key, 2, max_attempts=3)
        await _wait_status(pool, job_id, "succeeded")
    assert (await _job(pool, job_id))["result"] == 3
    assert calls[key] == 3
    assert await _executions(pool, job_id) == ["failed", "failed", "succeeded"]


async def test_failure_schedules_retry_in_the_future(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        job_id = await enqueue(
            pool, redis, api_key[0], "t.fail_times", str(uuid4()), 5,
            backoff_base_s=60, backoff_max_s=60, max_attempts=5,
        )  # fmt: skip
        await _wait_status(pool, job_id, "scheduled")
    job = await _job(pool, job_id)
    assert job["attempts"] == 1 and "attempt 1 failed" in job["last_error"]
    assert job["run_at"] > job["updated_at"]


async def test_exhausted_retries_go_to_dead_letter_queue(
    worker: Worker, settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker), running_scheduler(settings, pool, redis):
        job_id = await enqueue(
            pool, redis, api_key[0], "t.fail_times", str(uuid4()), 99, max_attempts=2
        )
        await _wait_status(pool, job_id, "dead")
    job = await _job(pool, job_id)
    assert job["attempts"] == 2 and job["finished_at"] is not None
    assert await _executions(pool, job_id) == ["failed", "failed"]


async def test_permanent_error_skips_retries(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        job_id = await enqueue(pool, redis, api_key[0], "t.permanent", max_attempts=5)
        await _wait_status(pool, job_id, "dead")
    assert (await _job(pool, job_id))["attempts"] == 1


async def test_retry_later_uses_requested_delay(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        job_id = await enqueue(pool, redis, api_key[0], "t.retry_later")
        await _wait_status(pool, job_id, "scheduled")
    job = await _job(pool, job_id)
    assert 25 < (job["run_at"] - job["updated_at"]).total_seconds() < 35


async def test_timeout_is_recorded(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        job_id = await enqueue(pool, redis, api_key[0], "t.slow", 5, timeout_s=0.2, max_attempts=1)
        await _wait_status(pool, job_id, "dead")
    assert await _executions(pool, job_id) == ["timed_out"]
    assert "JobTimeout" in (await _job(pool, job_id))["last_error"]


async def test_unknown_task_is_retryable(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        job_id = await enqueue(pool, redis, api_key[0], "t.nope", max_attempts=1)
        await _wait_status(pool, job_id, "dead")
    assert "UnknownTask" in (await _job(pool, job_id))["last_error"]


async def test_unserializable_result_is_stored_as_repr(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        job_id = await enqueue(pool, redis, api_key[0], "t.unserializable")
        await _wait_status(pool, job_id, "succeeded")
    assert (await _job(pool, job_id))["result"] == {"repr": "{1, 2}"}


async def test_transactional_side_effect_commits_with_success(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        job_id = await enqueue(pool, redis, api_key[0], "t.ledger", 7)
        await _wait_status(pool, job_id, "succeeded")
    rows = await pool.fetch("SELECT value FROM example_ledger WHERE job_id = $1", job_id)
    assert [r["value"] for r in rows] == [7]


async def test_failing_side_effect_rolls_back_and_retries(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        job_id = await enqueue(pool, redis, api_key[0], "t.bad_side_effect", max_attempts=1)
        await _wait_status(pool, job_id, "dead")
    assert "no_such_table" in (await _job(pool, job_id))["last_error"]


async def test_duplicate_messages_execute_once(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    from jobq import dispatch

    job_id = await enqueue(pool, redis, api_key[0], "t.ledger", 1, publish=False)
    await dispatch.dispatch(redis, [(job_id, "default")] * 5, 1000)  # 5 copies of the message
    async with running_worker(worker):
        await _wait_status(pool, job_id, "succeeded")
        await asyncio.sleep(0.3)
    assert await pool.fetchval("SELECT count(*) FROM example_ledger WHERE job_id = $1", job_id) == 1
    assert await _executions(pool, job_id) == ["succeeded"]


async def test_heartbeat_keeps_long_job_alive(
    worker: Worker, settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    # The job runs 2.5x longer than the lease; heartbeats must keep the reaper away.
    async with running_worker(worker), running_scheduler(settings, pool, redis):
        job_id = await enqueue(pool, redis, api_key[0], "t.slow", settings.lease_s * 2.5)
        await _wait_status(pool, job_id, "succeeded", timeout=15)
    assert await _executions(pool, job_id) == ["succeeded"]


async def test_lost_lease_cancels_job_and_fences_its_result(
    worker: Worker, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker):
        job_id = await enqueue(pool, redis, api_key[0], "t.ledger", 1, seconds=3)
        await _wait_status(pool, job_id, "running")
        # Simulate the reaper having taken the job away (e.g. after a long GC pause).
        await pool.execute(
            "UPDATE jobs SET status = 'scheduled', lease_token = NULL, "
            "run_at = now() + interval '1 hour' WHERE id = $1",
            job_id,
        )

        async def fenced() -> bool:
            return await _executions(pool, job_id) == ["fenced"]

        await wait_until(fenced, timeout=5)
    assert await pool.fetchval("SELECT count(*) FROM example_ledger") == 0
    assert await job_status(pool, job_id) == "scheduled"


async def test_zombie_completion_is_fenced(pool: asyncpg.Pool, api_key: Any, redis: Any) -> None:
    """Direct test of the fencing primitive: a stale lease token cannot complete a job."""
    job_id = await enqueue(pool, redis, api_key[0], "t.add", 1, 2, publish=False)
    old, new = uuid4(), uuid4()
    assert await repo.claim_job(pool, job_id, old, "zombie", 30)
    # The lease expires; the reaper reschedules and another worker claims with a new token.
    await pool.execute(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 s' WHERE id = $1", job_id
    )
    assert await repo.reap_expired_leases(pool, 10) == [(job_id, "scheduled")]
    await pool.execute("UPDATE jobs SET status = 'queued' WHERE id = $1", job_id)
    assert await repo.claim_job(pool, job_id, new, "healthy", 30)

    effect = [("INSERT INTO example_ledger (job_id, value) VALUES ($1, 1)", (job_id,))]
    assert not await repo.complete_success(pool, job_id, old, "zombie", effect)
    assert not await repo.complete_failure(
        pool, job_id, old, status="dead", delay_s=0, error="x", execution_status="failed"
    )
    assert await repo.complete_success(pool, job_id, new, 3, effect)
    assert await pool.fetchval("SELECT count(*) FROM example_ledger") == 1
    assert await _executions(pool, job_id) == ["lease_expired", "succeeded"]


async def test_concurrency_limit(
    settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    worker = Worker(settings, registry=registry, concurrency=2, pool=pool, redis=redis)
    ids = [await enqueue(pool, redis, api_key[0], "t.slow", 0.4) for _ in range(6)]
    peak = 0
    async with running_worker(worker):
        for _ in range(40):
            peak = max(peak, len(worker._processing))
            await asyncio.sleep(0.03)
        for job_id in ids:
            await _wait_status(pool, job_id, "succeeded")
    assert peak == 2


async def test_graceful_shutdown_hands_back_unfinished_jobs(
    settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    fast = settings.model_copy(update={"shutdown_grace_s": 0.3})
    worker = Worker(fast, registry=registry, pool=pool, redis=redis)
    async with running_worker(worker):
        quick = await enqueue(pool, redis, api_key[0], "t.slow", 0.1)
        stuck = await enqueue(pool, redis, api_key[0], "t.slow", 30, max_attempts=1)
        await _wait_status(pool, stuck, "running")
    assert await job_status(pool, quick) == "succeeded"
    job = await _job(pool, stuck)
    # Released for immediate pickup, and the interrupted attempt did not use up the budget.
    assert job["status"] == "scheduled" and job["attempt_offset"] == 1
    assert await _executions(pool, stuck) == ["interrupted"]


async def test_worker_survives_redis_data_loss(
    worker: Worker, settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    async with running_worker(worker), running_scheduler(settings, pool, redis):
        await asyncio.sleep(0.2)
        await redis.flushdb()  # streams and consumer groups vanish (NOGROUP on next read)
        job_id = await enqueue(pool, redis, api_key[0], "t.add", 1, 1, publish=False)
        # Never published: only the sweeper can rescue it.
        await _wait_status(pool, job_id, "succeeded", timeout=10)


async def test_worker_owns_connections_when_not_injected(
    settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    worker = Worker(settings, registry=registry)
    async with running_worker(worker):
        job_id = await enqueue(pool, redis, api_key[0], "t.add", 1, 1)
        await _wait_status(pool, job_id, "succeeded")
