"""The scheduler: background loops that keep jobs moving.

* promoter: 'scheduled' jobs whose run_at has passed -> 'queued' + stream message
* reaper:   'running' jobs whose lease expired (worker died) -> retry or dead-letter
* sweeper:  'queued' jobs whose stream message was lost -> dispatch again
* purger:   delete finished jobs past the retention window
* gauges:   job counts per queue/status for Prometheus

No leader election is needed: every loop claims rows with ``FOR UPDATE SKIP LOCKED`` or uses
conditional UPDATEs, so running several scheduler replicas is safe and gives high availability.
The worst case of two sweepers racing is a duplicate stream message, which workers ignore.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from contextlib import suppress
from uuid import UUID

import asyncpg
import redis.asyncio as aioredis

from jobq import dispatch, metrics
from jobq import repository as repo
from jobq.config import Settings
from jobq.db import create_pool

log = logging.getLogger("jobq.scheduler")


class Scheduler:
    def __init__(
        self,
        settings: Settings,
        *,
        pool: asyncpg.Pool | None = None,
        redis: aioredis.Redis | None = None,
    ) -> None:
        self.settings = settings
        self._pool = pool
        self._redis = redis
        self._owns_pool = pool is None
        self._owns_redis = redis is None
        self._stopping = asyncio.Event()
        self._known_gauges: set[tuple[str, str]] = set()

    @property
    def pool(self) -> asyncpg.Pool:
        assert self._pool is not None, "scheduler not started"
        return self._pool

    @property
    def redis(self) -> aioredis.Redis:
        assert self._redis is not None, "scheduler not started"
        return self._redis

    # -- one iteration of each loop (called directly by tests) -----------------------------------

    async def promote_due(self) -> int:
        rows = await repo.promote_due(self.pool, self.settings.batch_size)
        if rows:
            # Committed before XADD on purpose: a worker must never read a message for a job
            # that is not yet 'queued'. A crash in between is healed by the sweeper.
            await dispatch.dispatch(
                self.redis, ((r["id"], r["queue"]) for r in rows), self.settings.stream_maxlen
            )
            metrics.PROMOTED.inc(len(rows))
        return len(rows)

    async def reap_expired(self) -> int:
        reaped = await repo.reap_expired_leases(self.pool, self.settings.batch_size)
        for job_id, status in reaped:
            metrics.REAPED.labels("retry" if status == "scheduled" else "dead").inc()
            log.warning("reaped expired lease", extra={"job_id": str(job_id), "next": status})
        return len(reaped)

    async def sweep_lost(self) -> int:
        stale = await repo.stale_queued(
            self.pool, self.settings.sweep_after_s, self.settings.batch_size
        )
        if not stale:
            return 0
        by_queue: dict[str, list[asyncpg.Record]] = defaultdict(list)
        for row in stale:
            by_queue[row["queue"]].append(row)
        lost: list[UUID] = []
        for queue, rows in by_queue.items():
            position = await dispatch.stream_position(self.redis, queue)
            lost.extend(
                r["id"]
                for r in rows
                if dispatch.message_lost(
                    r["dispatched_ms"], position, self.settings.clock_skew_margin_ms
                )
            )
        if not lost:
            return 0
        rows = await repo.mark_redispatched(self.pool, lost)
        await dispatch.dispatch(
            self.redis, ((r["id"], r["queue"]) for r in rows), self.settings.stream_maxlen
        )
        metrics.REDISPATCHED.inc(len(rows))
        log.warning("re-dispatched lost jobs", extra={"count": len(rows)})
        return len(rows)

    async def purge_finished(self) -> int:
        deleted = await repo.purge_finished(
            self.pool, self.settings.retention_days, self.settings.batch_size
        )
        metrics.PURGED.inc(deleted)
        return deleted

    async def refresh_gauges(self) -> None:
        seen: set[tuple[str, str]] = set()
        for row in await repo.active_counts(self.pool):
            key = (row["queue"], row["status"])
            metrics.JOBS_BY_STATUS.labels(*key).set(row["count"])
            seen.add(key)
        for key in self._known_gauges - seen:
            metrics.JOBS_BY_STATUS.labels(*key).set(0)
        self._known_gauges |= seen

    # -- run forever -----------------------------------------------------------------------------

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        if self._pool is None:
            self._pool = await create_pool(self.settings)
        if self._redis is None:
            self._redis = dispatch.create_redis(self.settings)
        s = self.settings
        loops = [
            asyncio.create_task(self._loop("promote", self.promote_due, s.promote_interval_s)),
            asyncio.create_task(self._loop("reap", self.reap_expired, s.reap_interval_s)),
            asyncio.create_task(self._loop("sweep", self.sweep_lost, s.sweep_interval_s)),
            asyncio.create_task(self._loop("purge", self.purge_finished, s.purge_interval_s)),
            asyncio.create_task(self._loop("gauges", self.refresh_gauges, s.gauge_interval_s)),
        ]
        log.info("scheduler started")
        try:
            await self._stopping.wait()
        finally:
            for task in loops:
                task.cancel()
            for task in loops:
                with suppress(asyncio.CancelledError):
                    await task
            if self._owns_redis:
                await self.redis.aclose()
            if self._owns_pool:
                await self.pool.close()
            log.info("scheduler stopped")

    async def _loop(
        self, name: str, step: Callable[[], Awaitable[int | None]], interval_s: float
    ) -> None:
        failures = 0
        while True:
            try:
                done = await step()
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                failures += 1
                log.exception("scheduler loop failed", extra={"loop": name})
                await asyncio.sleep(min(30.0, interval_s * 2**failures))
                continue
            # A full batch means there is probably more work: go again without sleeping.
            if not (isinstance(done, int) and done >= self.settings.batch_size):
                await asyncio.sleep(interval_s)
