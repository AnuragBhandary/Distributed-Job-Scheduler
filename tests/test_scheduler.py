"""Scheduler loops: promotion, lease reaping, lost-message sweeping, retention, gauges."""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest

from jobq import dispatch, metrics
from jobq.config import Settings
from jobq.scheduler import Scheduler
from tests.conftest import job_status
from tests.helpers import enqueue


@pytest.fixture
def scheduler(settings: Settings, pool: asyncpg.Pool, redis: Any) -> Scheduler:
    return Scheduler(settings, pool=pool, redis=redis)


async def _stream_ids(redis: Any, queue: str = "default") -> list[str]:
    return [m[1]["id"] for m in await redis.xrange(dispatch.stream_key(queue))]


async def test_promotes_only_due_jobs(
    scheduler: Scheduler, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    due = await enqueue(pool, redis, api_key[0], "t", delay_s=0.05)
    later = await enqueue(pool, redis, api_key[0], "t", delay_s=3600)
    assert await job_status(pool, due) == "scheduled"
    await asyncio.sleep(0.1)
    assert await scheduler.promote_due() == 1
    assert await job_status(pool, due) == "queued"
    assert await job_status(pool, later) == "scheduled"
    assert await _stream_ids(redis) == [str(due)]
    assert await scheduler.promote_due() == 0


async def test_concurrent_schedulers_never_double_promote(
    settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    ids = [await enqueue(pool, redis, api_key[0], "t", delay_s=0.01) for _ in range(300)]
    await asyncio.sleep(0.05)
    small = settings.model_copy(update={"batch_size": 25})
    replicas = [Scheduler(small, pool=pool, redis=redis) for _ in range(4)]

    async def drain(s: Scheduler) -> int:
        total = 0
        while n := await s.promote_due():
            total += n
        return total

    counts = await asyncio.gather(*(drain(s) for s in replicas))
    assert sum(counts) == 300
    streamed = await _stream_ids(redis)
    assert len(streamed) == 300 and set(streamed) == {str(i) for i in ids}


async def test_reaper_retries_then_dead_letters_expired_leases(
    scheduler: Scheduler, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    from uuid import uuid4

    from jobq import repository as repo

    job_id = await enqueue(pool, redis, api_key[0], "t", max_attempts=2, publish=False)
    for attempt in (1, 2):
        await pool.execute("UPDATE jobs SET status = 'queued' WHERE id = $1", job_id)
        assert await repo.claim_job(pool, job_id, uuid4(), "crashed-worker", 0.01)
        await asyncio.sleep(0.05)
        assert await scheduler.reap_expired() == 1
        expected = "scheduled" if attempt == 1 else "dead"
        assert await job_status(pool, job_id) == expected
    statuses = await pool.fetch("SELECT status::text FROM executions WHERE job_id = $1", job_id)
    assert [r[0] for r in statuses] == ["lease_expired", "lease_expired"]
    assert await scheduler.reap_expired() == 0


async def test_sweeper_redispatches_lost_messages_only(
    scheduler: Scheduler, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    await dispatch.ensure_group(redis, "default")
    lost = await enqueue(pool, redis, api_key[0], "t", publish=False)  # crash before XADD
    await asyncio.sleep(0.6)  # older than sweep_after_s
    # The group has read everything in the stream, yet the job is still queued -> lost.
    assert await scheduler.sweep_lost() == 1
    assert await _stream_ids(redis) == [str(lost)]

    # Now a job that is legitimately waiting in an unread backlog must be left alone.
    await redis.xgroup_setid(dispatch.stream_key("default"), dispatch.GROUP, id="0")
    await pool.execute("UPDATE jobs SET dispatched_at = now() - interval '5 s'")
    assert await scheduler.sweep_lost() == 0


async def test_sweeper_redispatches_after_redis_data_loss(
    scheduler: Scheduler, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    job_id = await enqueue(pool, redis, api_key[0], "t")
    await redis.flushdb()
    await pool.execute("UPDATE jobs SET dispatched_at = now() - interval '5 s'")
    assert await scheduler.sweep_lost() == 1
    assert await _stream_ids(redis) == [str(job_id)]
    assert await scheduler.sweep_lost() == 0  # dispatched_at was refreshed


async def test_sweeper_waits_while_no_worker_has_attached(
    scheduler: Scheduler, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    await enqueue(pool, redis, api_key[0], "t", queue="fresh")  # stream exists, no group
    await pool.execute("UPDATE jobs SET dispatched_at = now() - interval '5 s'")
    assert await scheduler.sweep_lost() == 0


async def test_purge_deletes_only_old_finished_jobs(
    scheduler: Scheduler, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    old = await enqueue(pool, redis, api_key[0], "t", publish=False)
    fresh = await enqueue(pool, redis, api_key[0], "t", publish=False)
    dead = await enqueue(pool, redis, api_key[0], "t", publish=False)
    await pool.execute(
        "UPDATE jobs SET status = 'succeeded', finished_at = now() - interval '30 days' "
        "WHERE id = $1",
        old,
    )
    await pool.execute(
        "UPDATE jobs SET status = 'succeeded', finished_at = now() WHERE id = $1", fresh
    )
    await pool.execute(
        "UPDATE jobs SET status = 'dead', finished_at = now() - interval '30 days' WHERE id = $1",
        dead,
    )
    assert await scheduler.purge_finished() == 1
    assert await job_status(pool, old) is None
    assert await job_status(pool, fresh) == "succeeded"
    assert await job_status(pool, dead) == "dead"  # the DLQ is never purged automatically


async def test_gauges(scheduler: Scheduler, pool: asyncpg.Pool, redis: Any, api_key: Any) -> None:
    job_id = await enqueue(pool, redis, api_key[0], "t", queue="g")
    await scheduler.refresh_gauges()
    assert metrics.JOBS_BY_STATUS.labels("g", "queued")._value.get() == 1
    await pool.execute("UPDATE jobs SET status = 'succeeded' WHERE id = $1", job_id)
    await scheduler.refresh_gauges()
    assert metrics.JOBS_BY_STATUS.labels("g", "queued")._value.get() == 0


async def test_run_loop_survives_errors_and_stops(
    settings: Settings, pool: asyncpg.Pool, redis: Any
) -> None:
    scheduler = Scheduler(settings.model_copy(update={"reap_interval_s": 0.01}))
    calls = 0

    async def broken() -> int:
        nonlocal calls
        calls += 1
        raise RuntimeError("database hiccup")

    scheduler.reap_expired = broken  # type: ignore[method-assign]
    task = asyncio.create_task(scheduler.run())
    await asyncio.sleep(0.3)
    scheduler.stop()
    await asyncio.wait_for(task, 5)
    assert calls >= 2  # kept retrying with backoff instead of dying
