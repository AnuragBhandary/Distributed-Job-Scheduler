"""All SQL lives here. Every state transition is a single conditional UPDATE, so concurrent
API servers, workers and schedulers can race freely: the WHERE clause decides the winner.

Fencing: a worker's writes for a running job include ``lease_token = $token``. When a lease
expires and the reaper reschedules the job, the token is cleared, so a stalled ("zombie")
worker that wakes up later cannot commit a result or its side effects.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, TypeAlias
from uuid import UUID

import asyncpg

from jobq.backoff import decide_failure

Executor: TypeAlias = asyncpg.Pool | asyncpg.Connection

JOB_COLUMNS = """
    id, api_key_id, queue, task, args, kwargs, status::text AS status, idempotency_key,
    request_hash, run_at, attempts, attempt_offset, max_attempts, timeout_s, backoff_base_s,
    backoff_max_s, lease_owner, lease_expires_at, dispatched_at, result, last_error,
    created_at, updated_at, finished_at
"""

# ---------------------------------------------------------------------------------------------
# API side
# ---------------------------------------------------------------------------------------------


async def insert_job(
    db: Executor,
    *,
    job_id: UUID,
    api_key_id: int,
    queue: str,
    task: str,
    args: list[Any],
    kwargs: dict[str, Any],
    idempotency_key: str | None,
    request_hash: bytes | None,
    run_at: datetime | None,
    delay_s: float,
    max_attempts: int,
    timeout_s: float,
    backoff_base_s: float,
    backoff_max_s: float,
) -> asyncpg.Record | None:
    """Insert a job. Due jobs start as 'queued' (the caller dispatches them), future ones as
    'scheduled'. Returns None when the idempotency key already exists for this API key."""
    return await db.fetchrow(
        f"""
        INSERT INTO jobs (id, api_key_id, queue, task, args, kwargs, status, idempotency_key,
                          request_hash, run_at, max_attempts, timeout_s, backoff_base_s,
                          backoff_max_s, dispatched_at)
        SELECT $1, $2, $3, $4, $5, $6,
               CASE WHEN t.run_at <= now() THEN 'queued' ELSE 'scheduled' END::job_status,
               $7, $8, t.run_at, $11, $12, $13, $14,
               CASE WHEN t.run_at <= now() THEN now() END
        FROM (SELECT COALESCE($9::timestamptz, now() + make_interval(secs => $10))) AS t(run_at)
        ON CONFLICT ON CONSTRAINT jobs_idempotency DO NOTHING
        RETURNING {JOB_COLUMNS}
        """,
        job_id, api_key_id, queue, task, args, kwargs, idempotency_key, request_hash,
        run_at, delay_s, max_attempts, timeout_s, backoff_base_s, backoff_max_s,
    )  # fmt: skip


async def get_job_by_idempotency_key(
    db: Executor, api_key_id: int, idempotency_key: str
) -> asyncpg.Record | None:
    return await db.fetchrow(
        f"SELECT {JOB_COLUMNS} FROM jobs WHERE api_key_id = $1 AND idempotency_key = $2",
        api_key_id,
        idempotency_key,
    )


async def get_job(db: Executor, job_id: UUID, api_key_id: int) -> asyncpg.Record | None:
    return await db.fetchrow(
        f"SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1 AND api_key_id = $2", job_id, api_key_id
    )


async def list_jobs(
    db: Executor,
    api_key_id: int,
    *,
    status: str | None = None,
    queue: str | None = None,
    task: str | None = None,
    limit: int = 50,
    after: tuple[datetime, UUID] | None = None,
) -> list[asyncpg.Record]:
    """Newest first, keyset-paginated on (created_at, id): stable under concurrent inserts and
    O(limit) per page, unlike OFFSET."""
    after_ts, after_id = after if after else (None, None)
    return await db.fetch(
        f"""
        SELECT {JOB_COLUMNS} FROM jobs
        WHERE api_key_id = $1
          AND ($2::job_status IS NULL OR status = $2::job_status)
          AND ($3::text IS NULL OR queue = $3)
          AND ($4::text IS NULL OR task = $4)
          AND ($5::timestamptz IS NULL OR (created_at, id) < ($5::timestamptz, $6::uuid))
        ORDER BY created_at DESC, id DESC
        LIMIT $7
        """,
        api_key_id, status, queue, task, after_ts, after_id, limit,
    )  # fmt: skip


async def list_executions(db: Executor, job_id: UUID) -> list[asyncpg.Record]:
    return await db.fetch(
        """
        SELECT attempt, worker_id, status::text AS status, error, started_at, finished_at
        FROM executions WHERE job_id = $1 ORDER BY attempt
        """,
        job_id,
    )


async def cancel_job(db: Executor, job_id: UUID, api_key_id: int) -> asyncpg.Record | None:
    """Cancel a job that has not started. Running jobs cannot be cancelled (409)."""
    return await db.fetchrow(
        f"""
        UPDATE jobs SET status = 'cancelled', finished_at = now(), dispatched_at = NULL,
                        updated_at = now()
        WHERE id = $1 AND api_key_id = $2 AND status IN ('scheduled', 'queued')
        RETURNING {JOB_COLUMNS}
        """,
        job_id,
        api_key_id,
    )


async def requeue_dead_job(db: Executor, job_id: UUID, api_key_id: int) -> asyncpg.Record | None:
    """Move a dead-lettered job back to the ready queue with a fresh retry budget."""
    return await db.fetchrow(
        f"""
        UPDATE jobs SET status = 'queued', attempt_offset = attempts, run_at = now(),
                        dispatched_at = now(), finished_at = NULL, updated_at = now()
        WHERE id = $1 AND api_key_id = $2 AND status = 'dead'
        RETURNING {JOB_COLUMNS}
        """,
        job_id,
        api_key_id,
    )


async def job_counts(db: Executor, api_key_id: int) -> list[asyncpg.Record]:
    return await db.fetch(
        """
        SELECT queue, status::text AS status, count(*) AS count
        FROM jobs WHERE api_key_id = $1 GROUP BY queue, status ORDER BY queue, status
        """,
        api_key_id,
    )


async def recent_workers(db: Executor, api_key_id: int, window_s: float) -> list[asyncpg.Record]:
    """Workers that ran this key's jobs recently (or are running one now), busiest first."""
    return await db.fetch(
        """
        SELECT e.worker_id,
               count(*) FILTER (WHERE e.status = 'running') AS running,
               count(*) FILTER (WHERE e.status = 'succeeded') AS succeeded,
               count(*) FILTER (WHERE e.status NOT IN ('running', 'succeeded')) AS failed,
               max(coalesce(e.finished_at, e.started_at)) AS last_seen
        FROM executions e JOIN jobs j ON j.id = e.job_id
        WHERE j.api_key_id = $1
          AND (e.started_at > now() - make_interval(secs => $2) OR e.status = 'running')
        GROUP BY e.worker_id
        ORDER BY running DESC, last_seen DESC
        """,
        api_key_id,
        window_s,
    )


async def throughput(
    db: Executor, api_key_id: int, window_s: float, bucket_s: int
) -> list[asyncpg.Record]:
    """Finished attempts per time bucket: succeeded vs failed (failed, timed out, lease expired,
    fenced, interrupted). Retries show up as failed attempts followed by a success."""
    return await db.fetch(
        """
        SELECT date_bin(make_interval(secs => $3), e.finished_at, 'epoch') AS bucket,
               count(*) FILTER (WHERE e.status = 'succeeded') AS succeeded,
               count(*) FILTER (WHERE e.status <> 'succeeded') AS failed
        FROM executions e JOIN jobs j ON j.id = e.job_id
        WHERE j.api_key_id = $1
          -- bounds the index range scan; attempts that ran over an hour are left out
          AND e.started_at > now() - make_interval(secs => $2) - interval '1 hour'
          AND e.finished_at > now() - make_interval(secs => $2)
        GROUP BY bucket ORDER BY bucket
        """,
        api_key_id,
        window_s,
        bucket_s,
    )


# ---------------------------------------------------------------------------------------------
# Worker side
# ---------------------------------------------------------------------------------------------


async def claim_job(
    db: Executor, job_id: UUID, lease_token: UUID, worker_id: str, lease_s: float
) -> asyncpg.Record | None:
    """Atomically take the lease on a queued job and open its execution record.

    One statement, one round trip. Returns None if the job is no longer 'queued' (duplicate
    stream message, cancelled, or another worker won the race).
    """
    return await db.fetchrow(
        """
        WITH claimed AS (
            UPDATE jobs
            SET status = 'running', attempts = attempts + 1, lease_token = $2, lease_owner = $3,
                lease_expires_at = now() + make_interval(secs => $4), dispatched_at = NULL,
                updated_at = now()
            WHERE id = $1 AND status = 'queued'
            RETURNING *
        ), opened AS (
            INSERT INTO executions (job_id, attempt, worker_id, lease_token, status)
            SELECT id, attempts, $3, $2, 'running' FROM claimed
        )
        SELECT id, queue, task, args, kwargs, run_at, attempts, attempt_offset, max_attempts,
               timeout_s, backoff_base_s, backoff_max_s, now() AS claimed_at
        FROM claimed
        """,
        job_id,
        lease_token,
        worker_id,
        lease_s,
    )


async def extend_leases(
    db: Executor, job_ids: Sequence[UUID], lease_tokens: Sequence[UUID], lease_s: float
) -> set[UUID]:
    """Heartbeat for all of a worker's running jobs in one statement. Returns the ids whose
    lease is still held; any missing id has lost its lease."""
    rows = await db.fetch(
        """
        UPDATE jobs AS j
        SET lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
        FROM unnest($1::uuid[], $2::uuid[]) AS l(id, token)
        WHERE j.id = l.id AND j.lease_token = l.token AND j.status = 'running'
        RETURNING j.id
        """,
        list(job_ids),
        list(lease_tokens),
        lease_s,
    )
    return {r["id"] for r in rows}


async def complete_success(
    pool: asyncpg.Pool,
    job_id: UUID,
    lease_token: UUID,
    result: Any,
    side_effects: Sequence[tuple[str, tuple[Any, ...]]] = (),
) -> bool:
    """Mark the job succeeded and apply the task's transactional side effects atomically.

    Returns False (and applies nothing) when the lease was lost: the fencing check and the
    side effects share one transaction, so a job's effects commit at most once.
    """
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            WITH done AS (
                UPDATE jobs
                SET status = 'succeeded', result = $3, finished_at = now(), updated_at = now(),
                    lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
                WHERE id = $1 AND lease_token = $2 AND status = 'running'
                RETURNING id
            )
            UPDATE executions AS e SET status = 'succeeded', finished_at = now()
            FROM done WHERE e.job_id = done.id AND e.lease_token = $2
            RETURNING e.id
            """,
            job_id,
            lease_token,
            result,
        )
        if row is None:
            return False
        for sql, args in side_effects:
            await conn.execute(sql, *args)
        return True


async def complete_failure(
    db: Executor,
    job_id: UUID,
    lease_token: UUID,
    *,
    status: str,
    delay_s: float,
    error: str,
    execution_status: str,
) -> bool:
    """Record a failed attempt: reschedule with backoff ('scheduled') or dead-letter ('dead').
    Fenced on the lease token; returns False if the lease was already lost."""
    row = await db.fetchrow(
        """
        WITH failed AS (
            UPDATE jobs
            SET status = $3::job_status,
                run_at = CASE WHEN $3::job_status = 'scheduled'
                              THEN now() + make_interval(secs => $4) ELSE run_at END,
                finished_at = CASE WHEN $3::job_status = 'dead' THEN now() END,
                last_error = $5, lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
                updated_at = now()
            WHERE id = $1 AND lease_token = $2 AND status = 'running'
            RETURNING id
        )
        UPDATE executions AS e
        SET status = $6::execution_status, error = $5, finished_at = now()
        FROM failed WHERE e.job_id = failed.id AND e.lease_token = $2
        RETURNING e.id
        """,
        job_id,
        lease_token,
        status,
        delay_s,
        error,
        execution_status,
    )
    return row is not None


async def release_job(db: Executor, job_id: UUID, lease_token: UUID) -> bool:
    """Give a job back on graceful shutdown: runnable immediately, and the interrupted attempt
    does not count against its retry budget."""
    row = await db.fetchrow(
        """
        WITH released AS (
            UPDATE jobs
            SET status = 'scheduled', run_at = now(), attempt_offset = attempt_offset + 1,
                lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
                updated_at = now()
            WHERE id = $1 AND lease_token = $2 AND status = 'running'
            RETURNING id
        )
        UPDATE executions AS e
        SET status = 'interrupted', error = 'worker shutdown', finished_at = now()
        FROM released WHERE e.job_id = released.id AND e.lease_token = $2
        RETURNING e.id
        """,
        job_id,
        lease_token,
    )
    return row is not None


async def mark_execution_fenced(db: Executor, job_id: UUID, lease_token: UUID, error: str) -> None:
    """Record that this attempt finished after losing its lease (its result was discarded)."""
    await db.execute(
        """
        UPDATE executions SET status = 'fenced', error = $3, finished_at = now()
        WHERE job_id = $1 AND lease_token = $2 AND status = 'running'
        """,
        job_id,
        lease_token,
        error,
    )


# ---------------------------------------------------------------------------------------------
# Scheduler side
# ---------------------------------------------------------------------------------------------


async def promote_due(db: Executor, limit: int) -> list[asyncpg.Record]:
    """Move due 'scheduled' jobs to 'queued'. SKIP LOCKED lets any number of scheduler
    replicas run this concurrently without double-promoting or blocking each other."""
    return await db.fetch(
        """
        WITH due AS (
            SELECT id FROM jobs
            WHERE status = 'scheduled' AND run_at <= now()
            ORDER BY run_at
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs AS j SET status = 'queued', dispatched_at = now(), updated_at = now()
        FROM due WHERE j.id = due.id
        RETURNING j.id, j.queue
        """,
        limit,
    )


async def reap_expired_leases(pool: asyncpg.Pool, limit: int) -> list[tuple[UUID, str]]:
    """Recover jobs whose worker stopped heartbeating (crashed, partitioned or stalled).

    Each one counts as a failed attempt: rescheduled with backoff, or dead-lettered when its
    retry budget is spent. Clearing lease_token fences out the old worker if it comes back.
    Returns (job_id, new_status) pairs.
    """
    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch(
            """
            SELECT id, lease_token, attempts, attempt_offset, max_attempts, backoff_base_s,
                   backoff_max_s
            FROM jobs
            WHERE status = 'running' AND lease_expires_at < now()
            ORDER BY lease_expires_at
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            """,
            limit,
        )
        if not rows:
            return []
        decisions = [
            decide_failure(
                attempts=r["attempts"],
                attempt_offset=r["attempt_offset"],
                max_attempts=r["max_attempts"],
                backoff_base_s=r["backoff_base_s"],
                backoff_max_s=r["backoff_max_s"],
            )
            for r in rows
        ]
        ids = [r["id"] for r in rows]
        error = "lease expired: worker stopped heartbeating"
        await conn.execute(
            """
            UPDATE jobs AS j
            SET status = u.status,
                run_at = CASE WHEN u.status = 'scheduled'
                              THEN now() + make_interval(secs => u.delay) ELSE j.run_at END,
                finished_at = CASE WHEN u.status = 'dead' THEN now() END,
                last_error = $4, lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
                updated_at = now()
            FROM unnest($1::uuid[], $2::job_status[], $3::float8[]) AS u(id, status, delay)
            WHERE j.id = u.id
            """,
            ids,
            [d.status for d in decisions],
            [d.delay_s for d in decisions],
            error,
        )
        await conn.execute(
            """
            UPDATE executions AS e
            SET status = 'lease_expired', error = $3, finished_at = now()
            FROM unnest($1::uuid[], $2::uuid[]) AS u(job_id, token)
            WHERE e.job_id = u.job_id AND e.lease_token = u.token AND e.status = 'running'
            """,
            ids,
            [r["lease_token"] for r in rows],
            error,
        )
        return [(job_id, d.status) for job_id, d in zip(ids, decisions, strict=True)]


async def stale_queued(db: Executor, older_than_s: float, limit: int) -> list[asyncpg.Record]:
    """Queued jobs dispatched longer ago than ``older_than_s`` (candidates for re-dispatch)."""
    return await db.fetch(
        """
        SELECT id, queue, (extract(epoch FROM dispatched_at) * 1000)::bigint AS dispatched_ms
        FROM jobs
        WHERE status = 'queued' AND dispatched_at < now() - make_interval(secs => $1)
        ORDER BY dispatched_at
        LIMIT $2
        """,
        older_than_s,
        limit,
    )


async def mark_redispatched(db: Executor, job_ids: Sequence[UUID]) -> list[asyncpg.Record]:
    return await db.fetch(
        """
        UPDATE jobs SET dispatched_at = now(), updated_at = now()
        WHERE id = ANY($1::uuid[]) AND status = 'queued'
        RETURNING id, queue
        """,
        list(job_ids),
    )


async def purge_finished(db: Executor, older_than_days: float, limit: int) -> int:
    """Retention: delete succeeded/cancelled jobs (and their executions) in bounded batches."""
    status = await db.execute(
        """
        DELETE FROM jobs WHERE id IN (
            SELECT id FROM jobs
            WHERE status IN ('succeeded', 'cancelled')
              AND finished_at < now() - make_interval(days => 1) * $1
            LIMIT $2
        )
        """,
        older_than_days,
        limit,
    )
    return int(status.split()[-1])


async def active_counts(db: Executor) -> list[asyncpg.Record]:
    return await db.fetch(
        """
        SELECT queue, status::text AS status, count(*) AS count FROM jobs
        WHERE status IN ('scheduled', 'queued', 'running', 'dead')
        GROUP BY queue, status
        """
    )
