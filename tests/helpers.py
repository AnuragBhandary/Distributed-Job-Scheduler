from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from jobq import dispatch
from jobq import repository as repo
from jobq.config import Settings
from jobq.scheduler import Scheduler
from jobq.worker import Worker


async def enqueue(
    pool: asyncpg.Pool,
    redis: Any,
    api_key_id: int,
    task: str,
    *args: Any,
    queue: str = "default",
    delay_s: float = 0.0,
    max_attempts: int = 3,
    timeout_s: float = 30.0,
    backoff_base_s: float = 0.05,
    backoff_max_s: float = 0.2,
    publish: bool = True,
    **kwargs: Any,
) -> UUID:
    row = await repo.insert_job(
        pool,
        job_id=uuid4(),
        api_key_id=api_key_id,
        queue=queue,
        task=task,
        args=list(args),
        kwargs=kwargs,
        idempotency_key=None,
        request_hash=None,
        run_at=None,
        delay_s=delay_s,
        max_attempts=max_attempts,
        timeout_s=timeout_s,
        backoff_base_s=backoff_base_s,
        backoff_max_s=backoff_max_s,
    )
    assert row is not None
    if publish and row["status"] == "queued":
        await dispatch.dispatch(redis, [(row["id"], queue)], 10_000)
    job_id: UUID = row["id"]
    return job_id


@asynccontextmanager
async def running_worker(worker: Worker) -> AsyncIterator[Worker]:
    task = asyncio.create_task(worker.run())
    try:
        yield worker
    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=15)


@asynccontextmanager
async def running_scheduler(
    settings: Settings, pool: asyncpg.Pool, redis: Any
) -> AsyncIterator[Scheduler]:
    scheduler = Scheduler(settings, pool=pool, redis=redis)
    task = asyncio.create_task(scheduler.run())
    try:
        yield scheduler
    finally:
        scheduler.stop()
        await asyncio.wait_for(task, timeout=10)
