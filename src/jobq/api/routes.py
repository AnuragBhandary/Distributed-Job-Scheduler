"""REST endpoints under /v1. Every job is scoped to the API key that created it."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import time
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import asyncpg
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from redis.exceptions import RedisError

from jobq import dispatch, metrics
from jobq import repository as repo
from jobq.api.deps import AppState, get_state, rate_limited
from jobq.api.schemas import (
    ExecutionOut,
    JobCreate,
    JobOut,
    JobPage,
    JobStatus,
    QueueStats,
    Stats,
    Throughput,
    ThroughputBucket,
    WorkerOut,
)
from jobq.auth import Principal

log = logging.getLogger("jobq.api")

router = APIRouter(prefix="/v1", tags=["jobs"])


def _job_out(row: asyncpg.Record) -> JobOut:
    return JobOut.model_validate(dict(row))


def _encode_cursor(row: asyncpg.Record) -> str:
    raw = f"{row['created_at'].isoformat()}|{row['id']}".encode()
    return base64.urlsafe_b64encode(raw).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        created_at, job_id = base64.urlsafe_b64decode(cursor.encode()).decode().split("|")
        return datetime.fromisoformat(created_at), UUID(job_id)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid cursor") from exc


def _request_hash(body: JobCreate) -> bytes:
    canonical = json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).digest()


async def _dispatch_now(state: AppState, row: asyncpg.Record) -> None:
    try:
        await dispatch.dispatch(
            state.redis, [(row["id"], row["queue"])], state.settings.stream_maxlen
        )
    except RedisError:
        # The job is already durable in PostgreSQL; the sweeper will dispatch it once Redis is
        # back. A Redis outage costs latency, never a job.
        metrics.DISPATCH_ERRORS.inc()
        log.warning("dispatch failed; job left for the sweeper", extra={"job_id": str(row["id"])})


async def _check_payload_size(request: Request, state: AppState = Depends(get_state)) -> None:
    length = request.headers.get("content-length")
    if length is not None and length.isdigit() and int(length) > state.settings.max_payload_bytes:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "payload too large")


@router.post(
    "/jobs",
    status_code=status.HTTP_201_CREATED,
    response_model=JobOut,
    dependencies=[Depends(_check_payload_size)],
    responses={200: {"description": "Idempotent replay of an earlier request"}},
)
async def create_job(
    body: JobCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, max_length=255),
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> JobOut:
    """Enqueue a job. Send an ``Idempotency-Key`` header to make retries safe: repeating a
    request with the same key returns the original job instead of creating a duplicate."""
    started = time.perf_counter()
    request_hash = _request_hash(body) if idempotency_key else None
    row = await repo.insert_job(
        state.pool,
        job_id=uuid4(),
        api_key_id=principal.id,
        queue=body.queue,
        task=body.task,
        args=body.args,
        kwargs=body.kwargs,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        run_at=body.run_at,
        delay_s=body.delay_s or 0.0,
        max_attempts=body.max_attempts,
        timeout_s=body.timeout_s,
        backoff_base_s=body.backoff_base_s,
        backoff_max_s=body.backoff_max_s,
    )
    if row is None:
        assert idempotency_key is not None
        existing = await repo.get_job_by_idempotency_key(state.pool, principal.id, idempotency_key)
        if existing is None:  # pragma: no cover - purged between the insert and the read
            raise HTTPException(status.HTTP_409_CONFLICT, "idempotency key race; retry")
        if bytes(existing["request_hash"] or b"") != request_hash:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "Idempotency-Key was already used with a different request body",
            )
        metrics.IDEMPOTENT_REPLAYS.inc()
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replayed"] = "true"
        return _job_out(existing)
    if row["status"] == "queued":
        await _dispatch_now(state, row)
    metrics.JOBS_ENQUEUED.labels(body.queue).inc()
    metrics.ENQUEUE_SECONDS.observe(time.perf_counter() - started)
    response.headers["Location"] = f"/v1/jobs/{row['id']}"
    return _job_out(row)


@router.get("/jobs", response_model=JobPage)
async def list_jobs(
    status_filter: JobStatus | None = Query(default=None, alias="status"),
    queue: str | None = None,
    task: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str | None = None,
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> JobPage:
    rows = await repo.list_jobs(
        state.pool,
        principal.id,
        status=status_filter,
        queue=queue,
        task=task,
        limit=limit,
        after=_decode_cursor(cursor) if cursor else None,
    )
    next_cursor = _encode_cursor(rows[-1]) if len(rows) == limit else None
    return JobPage(items=[_job_out(r) for r in rows], next_cursor=next_cursor)


async def _owned_job(state: AppState, job_id: UUID, principal: Principal) -> asyncpg.Record:
    row = await repo.get_job(state.pool, job_id, principal.id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return row


@router.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(
    job_id: UUID,
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> JobOut:
    return _job_out(await _owned_job(state, job_id, principal))


@router.get("/jobs/{job_id}/executions", response_model=list[ExecutionOut])
async def list_executions(
    job_id: UUID,
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> list[ExecutionOut]:
    await _owned_job(state, job_id, principal)
    out = []
    for row in await repo.list_executions(state.pool, job_id):
        data: dict[str, Any] = dict(row)
        finished = data["finished_at"]
        data["duration_ms"] = (
            round((finished - data["started_at"]).total_seconds() * 1000, 3) if finished else None
        )
        out.append(ExecutionOut.model_validate(data))
    return out


@router.post("/jobs/{job_id}/cancel", response_model=JobOut)
async def cancel_job(
    job_id: UUID,
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> JobOut:
    row = await repo.cancel_job(state.pool, job_id, principal.id)
    if row is None:
        current = await _owned_job(state, job_id, principal)
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"job is {current['status']}; only pending jobs can be cancelled",
        )
    return _job_out(row)


@router.get("/dlq", response_model=JobPage, tags=["dead-letter queue"])
async def list_dead_letters(
    queue: str | None = None,
    task: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str | None = None,
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> JobPage:
    return await list_jobs("dead", queue, task, limit, cursor, principal, state)


@router.post("/dlq/{job_id}/requeue", response_model=JobOut, tags=["dead-letter queue"])
async def requeue_dead_letter(
    job_id: UUID,
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> JobOut:
    row = await repo.requeue_dead_job(state.pool, job_id, principal.id)
    if row is None:
        current = await _owned_job(state, job_id, principal)
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"job is {current['status']}; only dead jobs can be requeued"
        )
    await _dispatch_now(state, row)
    return _job_out(row)


@router.get("/stats", response_model=Stats, tags=["stats"])
async def stats(
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> Stats:
    queues: dict[str, dict[str, int]] = {}
    for row in await repo.job_counts(state.pool, principal.id):
        queues.setdefault(row["queue"], {})[row["status"]] = row["count"]
    return Stats(queues=[QueueStats(queue=q, counts=c) for q, c in queues.items()])


@router.get("/stats/throughput", response_model=Throughput, tags=["stats"])
async def stats_throughput(
    window_s: int = Query(default=300, ge=10, le=3600),
    bucket_s: int = Query(default=10, ge=1, le=600),
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> Throughput:
    """Finished attempts per bucket over the last ``window_s`` seconds (the dashboard's chart)."""
    rows = await repo.throughput(state.pool, principal.id, window_s, bucket_s)
    return Throughput(
        bucket_s=bucket_s,
        buckets=[
            ThroughputBucket(start=r["bucket"], succeeded=r["succeeded"], failed=r["failed"])
            for r in rows
        ],
    )


@router.get("/workers", response_model=list[WorkerOut], tags=["stats"])
async def workers(
    window_s: int = Query(default=300, ge=10, le=3600),
    principal: Principal = Depends(rate_limited),
    state: AppState = Depends(get_state),
) -> list[WorkerOut]:
    """Workers that ran this key's jobs in the last ``window_s`` seconds, or are running one."""
    rows = await repo.recent_workers(state.pool, principal.id, window_s)
    return [WorkerOut.model_validate(dict(r)) for r in rows]
