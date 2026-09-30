"""The demo tasks shipped for docker compose and the benchmark, run through a real worker."""

from __future__ import annotations

from typing import Any

import asyncpg

from jobq.config import Settings
from jobq.examples import tasks as examples
from jobq.tasks import default_registry
from jobq.worker import Worker
from tests.conftest import job_status, wait_until
from tests.helpers import enqueue, running_scheduler, running_worker


def test_local_calls() -> None:
    assert examples.echo(1, a=2) == {"args": [1], "kwargs": {"a": 2}}
    assert examples.fib(10) == 55


async def test_examples_through_worker(
    settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: Any
) -> None:
    worker = Worker(settings, registry=default_registry, pool=pool, redis=redis)
    key = api_key[0]
    async with running_worker(worker), running_scheduler(settings, pool, redis):
        jobs = {
            "succeeded": [
                await enqueue(pool, redis, key, "examples.echo", 1),
                await enqueue(pool, redis, key, "examples.sleep", 0.01),
                await enqueue(pool, redis, key, "examples.fib", 20),
                await enqueue(pool, redis, key, "examples.flaky", p_fail=0.0),
                await enqueue(pool, redis, key, "examples.ledger", 5, work_ms=1),
            ],
            "dead": [
                await enqueue(pool, redis, key, "examples.fail", max_attempts=3),
                await enqueue(pool, redis, key, "examples.flaky", p_fail=1.0, max_attempts=2),
                await enqueue(
                    pool, redis, key, "examples.ledger", 1, p_fail=1.0, max_attempts=1
                ),
            ],
        }

        async def settled() -> bool:
            for expected, ids in jobs.items():
                for job_id in ids:
                    if await job_status(pool, job_id) != expected:
                        return False
            return True

        await wait_until(settled, timeout=15)
    assert await pool.fetchval("SELECT count(*) FROM example_ledger") == 1
    fail_attempts = await pool.fetchval("SELECT attempts FROM jobs WHERE id = $1", jobs["dead"][0])
    assert fail_attempts == 1  # PermanentError: no retries despite max_attempts=3
