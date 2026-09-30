"""The worker: reads job ids from Redis Streams, leases them in PostgreSQL, runs the handler,
heartbeats while it runs, and records the outcome behind a fencing check.

Lifecycle of one job on a worker::

    XREADGROUP (NOACK) -> claim (UPDATE ... WHERE status='queued')  -- lose the race? drop it
                       -> run handler under timeout, heartbeat extends the lease
                       -> success: UPDATE ... WHERE lease_token=mine (+ side effects, one txn)
                       -> failure: reschedule with backoff or dead-letter, same fencing
                       -> lease lost mid-run: cancel handler, discard result

Delivery is at-least-once (a crashed worker's job is re-run after its lease expires). Fencing
plus ``JobContext.transactional`` make the job's committed effects exactly-once.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
import traceback
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, TypeVar
from uuid import UUID, uuid4

import asyncpg
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from jobq import dispatch, metrics
from jobq import repository as repo
from jobq.backoff import decide_failure
from jobq.config import Settings
from jobq.db import create_pool
from jobq.errors import JobTimeout, PermanentError, RetryLater, UnknownTask
from jobq.tasks import JobContext, Registry, TaskSpec, default_registry

log = logging.getLogger("jobq.worker")

T = TypeVar("T")

_TRANSIENT_DB_ERRORS: tuple[type[BaseException], ...] = (
    OSError,
    asyncpg.PostgresConnectionError,
    asyncpg.InterfaceError,
    asyncpg.CannotConnectNowError,
    asyncpg.TooManyConnectionsError,
)


@dataclass(slots=True)
class _Running:
    job_id: UUID
    lease_token: UUID
    task: asyncio.Task[Any] | None = None
    lease_lost: bool = False
    interrupted: bool = False


def _format_error(exc: BaseException, limit: int = 4000) -> str:
    text = "".join(traceback.format_exception(exc)).strip()
    return text if len(text) <= limit else "..." + text[-limit:]


def _jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return {"repr": repr(value)}


class Worker:
    def __init__(
        self,
        settings: Settings,
        *,
        registry: Registry = default_registry,
        queues: Sequence[str] = ("default",),
        concurrency: int | None = None,
        worker_id: str | None = None,
        pool: asyncpg.Pool | None = None,
        redis: aioredis.Redis | None = None,
    ) -> None:
        self.settings = settings
        self.registry = registry
        self.queues = list(dict.fromkeys(queues))
        self.concurrency = concurrency or settings.worker_concurrency
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:6]}"
        self._pool = pool
        self._redis = redis
        self._owns_pool = pool is None
        self._owns_redis = redis is None
        self._processing: set[asyncio.Task[None]] = set()
        self._running: dict[UUID, _Running] = {}
        self._stopping = asyncio.Event()
        self._slot_free = asyncio.Event()

    # -- lifecycle ------------------------------------------------------------------------------

    @property
    def pool(self) -> asyncpg.Pool:
        assert self._pool is not None, "worker not started"
        return self._pool

    @property
    def redis(self) -> aioredis.Redis:
        assert self._redis is not None, "worker not started"
        return self._redis

    def stop(self) -> None:
        """Stop taking new jobs; ``run()`` then drains in-flight jobs and returns."""
        self._stopping.set()
        self._slot_free.set()

    async def run(self) -> None:
        if self._pool is None:
            self._pool = await create_pool(self.settings)
        if self._redis is None:
            self._redis = dispatch.create_redis(self.settings)
        for queue in self.queues:
            await dispatch.ensure_group(self.redis, queue)
        log.info(
            "worker started",
            extra={
                "worker_id": self.worker_id,
                "queues": self.queues,
                "tasks": self.registry.names(),
            },
        )
        heartbeat = asyncio.create_task(self._heartbeat_loop(), name="jobq-heartbeat")
        try:
            await self._consume_loop()
        finally:
            await self._drain()
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
            await self._teardown()
            log.info("worker stopped", extra={"worker_id": self.worker_id})

    async def _teardown(self) -> None:
        with suppress(Exception):
            for queue in self.queues:
                await self.redis.xgroup_delconsumer(
                    dispatch.stream_key(queue), dispatch.GROUP, self.worker_id
                )
        if self._owns_redis:
            await self.redis.aclose()
        if self._owns_pool:
            await self.pool.close()

    async def _drain(self) -> None:
        """Graceful shutdown: let in-flight jobs finish within the grace period, then interrupt
        the rest and hand them back so another worker can pick them up immediately."""
        if not self._processing:
            return
        _, pending = await asyncio.wait(self._processing, timeout=self.settings.shutdown_grace_s)
        if pending:
            log.warning("interrupting jobs after shutdown grace", extra={"count": len(pending)})
            for running in self._running.values():
                if running.task is not None and running.task.cancel():
                    running.interrupted = True
            await asyncio.wait(pending, timeout=10)

    # -- consuming ------------------------------------------------------------------------------

    async def _consume_loop(self) -> None:
        streams = {dispatch.stream_key(q): ">" for q in self.queues}
        failures = 0
        while not self._stopping.is_set():
            free = self.concurrency - len(self._processing)
            if free <= 0:
                self._slot_free.clear()
                await self._slot_free.wait()
                continue
            try:
                response: Any = await self.redis.xreadgroup(
                    dispatch.GROUP,
                    self.worker_id,
                    streams,  # type: ignore[arg-type]
                    count=free,
                    block=self.settings.worker_block_ms,
                    noack=True,
                )
                failures = 0
            except ResponseError as exc:
                if "NOGROUP" not in str(exc):
                    raise
                # Redis lost its data (restart without persistence): recreate and carry on.
                # Jobs whose messages vanished are re-dispatched by the scheduler's sweeper.
                for queue in self.queues:
                    await dispatch.ensure_group(self.redis, queue)
                continue
            except (RedisConnectionError, RedisTimeoutError, OSError):
                failures += 1
                delay = min(5.0, 0.1 * 2**failures)
                log.warning("redis unavailable, backing off", extra={"delay_s": delay})
                await asyncio.sleep(delay)
                continue
            for _stream, messages in response or []:
                for _message_id, fields in messages:
                    self._spawn(UUID(fields["id"]))

    def _spawn(self, job_id: UUID) -> None:
        task = asyncio.create_task(self._process(job_id), name=f"jobq-job-{job_id}")
        self._processing.add(task)
        metrics.INFLIGHT.set(len(self._processing))

        def _done(t: asyncio.Task[None]) -> None:
            self._processing.discard(t)
            metrics.INFLIGHT.set(len(self._processing))
            self._slot_free.set()
            if not t.cancelled() and t.exception() is not None:
                log.error("job processing crashed", exc_info=t.exception())

        task.add_done_callback(_done)

    async def _with_db_retry(self, op: Callable[[], Awaitable[T]], attempts: int = 4) -> T:
        """Retry a status write through brief database blips. If it still fails, the lease
        expires and the reaper recovers the job (the attempt is then re-run)."""
        for attempt in range(1, attempts + 1):
            try:
                return await op()
            except _TRANSIENT_DB_ERRORS:
                if attempt == attempts:
                    raise
                await asyncio.sleep(0.2 * 2**attempt)
        raise AssertionError("unreachable")

    # -- processing one job ---------------------------------------------------------------------

    async def _process(self, job_id: UUID) -> None:
        token = uuid4()
        try:
            job = await repo.claim_job(
                self.pool, job_id, token, self.worker_id, self.settings.lease_s
            )
        except _TRANSIENT_DB_ERRORS:
            log.warning("claim failed; sweeper will re-dispatch", extra={"job_id": str(job_id)})
            return
        if job is None:
            metrics.CLAIM_MISSES.inc()
            return
        running = _Running(job_id, token)
        self._running[job_id] = running
        try:
            await self._execute_claimed(job, running)
        finally:
            self._running.pop(job_id, None)

    async def _execute_claimed(self, job: asyncpg.Record, running: _Running) -> None:
        job_id, token = running.job_id, running.lease_token
        task_name, queue = job["task"], job["queue"]
        metrics.QUEUE_DELAY_SECONDS.labels(queue).observe(
            max(0.0, (job["claimed_at"] - job["run_at"]).total_seconds())
        )
        ctx = JobContext(
            job_id=job_id, task=task_name, queue=queue, attempt=job["attempts"], lease_token=token
        )
        started = time.perf_counter()
        handler = asyncio.create_task(self._invoke(self.registry.get(task_name), ctx, job))
        running.task = handler
        await asyncio.wait({handler})
        metrics.JOB_RUN_SECONDS.labels(task_name).observe(time.perf_counter() - started)

        def finished(outcome: str) -> None:
            metrics.JOBS_FINISHED.labels(queue, task_name, outcome).inc()

        if handler.cancelled():
            if running.interrupted:
                await self._with_db_retry(lambda: repo.release_job(self.pool, job_id, token))
                finished("interrupted")
            else:  # the heartbeat found the lease gone and cancelled the handler
                await self._with_db_retry(
                    lambda: repo.mark_execution_fenced(
                        self.pool, job_id, token, "lease lost while running; result discarded"
                    )
                )
                finished("lease_lost")
            return

        exc = handler.exception()
        if exc is None:
            result = _jsonable(handler.result())
            try:
                committed = await self._with_db_retry(
                    lambda: repo.complete_success(
                        self.pool, job_id, token, result, ctx.side_effects
                    )
                )
            except asyncpg.PostgresError as side_effect_error:
                exc = side_effect_error  # a transactional side effect failed: retry the job
            else:
                if committed:
                    finished("succeeded")
                else:
                    await repo.mark_execution_fenced(
                        self.pool, job_id, token, "lease lost before commit; result discarded"
                    )
                    finished("fenced")
                return

        decision = decide_failure(
            attempts=job["attempts"],
            attempt_offset=job["attempt_offset"],
            max_attempts=job["max_attempts"],
            backoff_base_s=job["backoff_base_s"],
            backoff_max_s=job["backoff_max_s"],
            permanent=isinstance(exc, PermanentError),
            requested_delay_s=exc.delay_s if isinstance(exc, RetryLater) else None,
        )
        error = _format_error(exc)
        recorded = await self._with_db_retry(
            lambda: repo.complete_failure(
                self.pool,
                job_id,
                token,
                status=decision.status,
                delay_s=decision.delay_s,
                error=error,
                execution_status="timed_out" if isinstance(exc, JobTimeout) else "failed",
            )
        )
        if not recorded:
            finished("fenced")
        else:
            finished("retried" if decision.status == "scheduled" else "dead")
        log.info(
            "job attempt failed",
            extra={
                "job_id": str(job_id),
                "task": task_name,
                "attempt": job["attempts"],
                "next": decision.status if recorded else "fenced",
                "delay_s": round(decision.delay_s, 3),
                "error": f"{type(exc).__name__}: {exc}",
            },
        )

    async def _invoke(self, spec: TaskSpec | None, ctx: JobContext, job: asyncpg.Record) -> Any:
        if spec is None:
            # Retryable on purpose: during a rolling deploy another worker may know this task.
            raise UnknownTask(f"no handler registered for task {job['task']!r}")
        args = [ctx, *job["args"]] if spec.bind else list(job["args"])
        kwargs = job["kwargs"]
        deadline = asyncio.timeout(job["timeout_s"])
        try:
            async with deadline:
                if spec.is_async:
                    return await spec.fn(*args, **kwargs)
                # Sync handlers run in a thread. Python cannot kill a thread, so on timeout the
                # job is failed but the thread runs to completion in the background; its
                # transactional side effects are discarded because they never reach the commit.
                return await asyncio.to_thread(spec.fn, *args, **kwargs)
        except TimeoutError:
            if deadline.expired():
                raise JobTimeout(f"exceeded timeout of {job['timeout_s']}s") from None
            raise

    # -- heartbeats -----------------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.heartbeat_interval_s)
            await self.heartbeat()

    async def heartbeat(self) -> None:
        """Extend the leases of every running job in one query; cancel jobs whose lease is gone."""
        active = [r for r in self._running.values() if r.task is not None and not r.task.done()]
        if not active:
            return
        try:
            alive = await repo.extend_leases(
                self.pool,
                [r.job_id for r in active],
                [r.lease_token for r in active],
                self.settings.lease_s,
            )
        except _TRANSIENT_DB_ERRORS:
            log.warning("heartbeat failed", exc_info=True)
            return
        for running in active:
            if running.job_id in alive or running.task is None:
                continue
            if running.task.cancel():
                running.lease_lost = True
                metrics.LEASES_LOST.inc()
                log.warning("lease lost; cancelling job", extra={"job_id": str(running.job_id)})
