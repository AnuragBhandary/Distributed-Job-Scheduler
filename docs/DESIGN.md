# jobq: Design

This document explains *why* jobq is built the way it is. Each section states the problem, the
decision, and the alternatives considered.

## 1. Requirements

**Functional**
- Submit a job (task name + JSON args) to run now or at/after a time.
- Workers execute jobs; failures retry with exponential backoff; jobs that exhaust their retries
  land in a dead-letter queue (DLQ) that can be inspected and requeued.
- Every attempt is recorded (worker, timing, error): execution history.
- REST API with API-key auth and per-key rate limiting; a Celery-style Python SDK.

**Non-functional**
- **No lost jobs**: once the API returns 2xx, the job will eventually reach a terminal state,
  even if any single process (API, scheduler, worker, Redis) crashes.
- **No duplicate effects**: a job's committed side effects happen once, even when the job itself
  is executed more than once (which at-least-once delivery makes unavoidable).
- **Horizontal scale**: any component can run as N replicas without coordination.
- Low enqueue latency (target p95 < 100 ms) at a sustained 2,000 jobs/min.

**Non-goals**: workflow DAGs/chaining, cron expressions, priorities, multi-region. See section 10.

## 2. Architecture

```
            ┌────────────── clients: SDK (task.delay) / curl ──────────────┐
            │                                                               │
            ▼  REST  (API key → token bucket in Redis)                      │
      ┌───────────┐  INSERT job (idempotent)   ┌────────────────────────┐   │
      │  API × N  │───────────────────────────▶│      PostgreSQL        │   │
      │  FastAPI  │  XADD job id (if due now)  │  jobs · executions ·   │   │
      └─────┬─────┘                            │  api_keys              │   │
            │                                  │  ── source of truth ── │   │
            ▼                                  └──▲──────────▲──────────┘   │
      ┌───────────┐   XREADGROUP (NOACK)          │ claim /    │ promote /
      │   Redis   │──────────────┐                │ heartbeat /│ reap /
      │  Streams  │              ▼                │ complete   │ sweep / purge
      │ (1/queue) │        ┌───────────┐          │            │
      └─────▲─────┘        │ Worker × N│──────────┘      ┌─────┴──────┐
            │              │  asyncio  │                 │Scheduler×N │
            └──────────────┴───────────┘◀── XADD ────────│  5 loops   │
                                                         └────────────┘
```

**The central decision: PostgreSQL is the source of truth; Redis is a lossy accelerator.**

- Every job and every state transition lives in PostgreSQL. Each transition is a single
  conditional `UPDATE ... WHERE status = <expected> [AND lease_token = <mine>]`, so concurrent
  actors race safely: the database decides the winner.
- Redis Streams carry only job ids ("this job is ready") so idle workers block on
  `XREADGROUP` and get work in ~1 ms instead of polling PostgreSQL. If Redis loses messages, or
  loses everything, the scheduler's sweeper notices from PostgreSQL and re-sends them.
- Redis also holds rate-limit buckets, which are disposable by nature.

### Why not just PostgreSQL (`SELECT ... FOR UPDATE SKIP LOCKED` polling)?
That design (used by Oban, good_job, pg-boss) is simpler and would work at this scale. Its
costs: every idle worker polls the database (N workers × poll rate queries/s even when idle), and
pickup latency is bounded by the poll interval. Push-based dispatch through Redis gives
~sub-millisecond pickup and zero idle load on PostgreSQL. The scheduler *does* use SKIP LOCKED
for its batch loops, where polling is cheap (one query per interval, not per worker).

### Why not Redis as the source of truth (like Celery/RQ/Sidekiq)?
Redis persistence is asynchronous (AOF `everysec` can lose ~1 s of writes; failover can lose
more), and it cannot express "update this job only if I still hold the lease" together with
"and insert my side effect" atomically with the business data. Keeping truth in PostgreSQL gives
durability, transactions, secondary indexes for the API (list/filter/paginate), and SQL for
audits (the benchmark verifies exactly-once with one query).

### Why not Kafka / SQS?
Kafka is a log, not a job queue: per-message acks, delays, retries with backoff and a DLQ all have
to be built on top, and partition-level ordering limits parallelism to the partition count. SQS
gives visibility timeouts (≈ leases) and DLQs, but has no fencing (a slow consumer can still act
after its message reappeared), limited delays (15 min), and nothing to query history. jobq
implements the same ideas explicitly so each one can be explained and tested.

## 3. Data model

`jobs` (one row per job): `status`, `run_at`, `attempts`, `max_attempts`, `attempt_offset`,
backoff parameters, lease (`lease_token`, `lease_owner`, `lease_expires_at`), `dispatched_at`,
`result`, `last_error`, `idempotency_key` + `request_hash`, timestamps.

`executions` (one row per attempt): `attempt`, `worker_id`, `lease_token`, `status`
(`running|succeeded|failed|timed_out|lease_expired|fenced|interrupted`), `error`, timings.
`UNIQUE (job_id, attempt)`.

**Partial indexes.** Each background loop only touches rows in one state, so each gets an index
over just those rows: `(run_at) WHERE status='scheduled'`, `(lease_expires_at) WHERE
status='running'`, `(dispatched_at) WHERE status='queued'`. These stay tiny even when the table
holds millions of finished jobs, so loop queries stay O(batch).

**Vacuum.** A queue table is update-heavy (each job is updated ~3-5 times), producing dead tuples
quickly. Autovacuum thresholds are lowered for `jobs` (2% instead of 20%) to prevent bloat.

## 4. Job state machine

```
                 delay/run_at in future
   POST /jobs ──────────────────────────▶ scheduled ──(run_at ≤ now: promoter)──┐
       │ due now                              ▲  ▲                             │
       ▼                                      │  │ retry with backoff          ▼
    queued ◀───────────── DLQ requeue ─────┐  │  │ (failure / lease expiry)  queued
       │  worker claim (conditional UPDATE)│  │  │
       ▼                                   │  │  │
    running ──success (fenced)──▶ succeeded│  │  │
       │ │                                 │  │  │
       │ └─failure, budget left────────────┼──┘  │
       │   failure, budget spent / Permanent ▶ dead
       └─lease expired (reaper)──────────────────┘
    scheduled|queued ──POST cancel──▶ cancelled
```

## 5. Core flows

### 5.1 Enqueue (API)
1. Authenticate (in-process cache → `api_keys` lookup by prefix → constant-time hash compare).
2. Rate-limit: Lua token bucket in Redis (atomic; uses Redis's clock so API replicas agree).
3. `INSERT ... ON CONFLICT (api_key_id, idempotency_key) DO NOTHING RETURNING *`. The status is
   computed in SQL: `queued` if `run_at <= now()`, else `scheduled`.
4. On conflict: load the existing job; if the stored `request_hash` differs → **409** (key reused
   for a different request), else return it with **200** + `Idempotent-Replayed: true`.
5. If queued: `XADD` its id. If Redis is down the request still succeeds: the job is durable and
   the sweeper dispatches it later. *A Redis outage costs latency, never a job.*

Two statements, three network round trips (Redis, PostgreSQL, Redis). That is the whole hot path.

**Idempotency** follows the Stripe/IETF `Idempotency-Key` pattern. The SDK *always* sends a key
(random if the caller gives none), which is what makes its automatic retries of timeouts, 5xx and
429 safe: a retry of a request whose response was lost cannot create a second job. The unique
constraint makes concurrent duplicates safe too (tested with 20 parallel identical requests).

### 5.2 Promote (scheduler)
```sql
WITH due AS (SELECT id FROM jobs WHERE status='scheduled' AND run_at <= now()
             ORDER BY run_at LIMIT $batch FOR UPDATE SKIP LOCKED)
UPDATE jobs SET status='queued', dispatched_at=now() FROM due WHERE jobs.id = due.id
RETURNING id, queue;
```
then one pipelined `XADD` per job. **Commit first, then XADD**: a worker must never receive a
message for a job that is not yet `queued` (its claim would fail and the message would be wasted).
A crash between commit and XADD leaves a `queued` job with no message; the sweeper fixes that.
If a batch is full the loop runs again immediately instead of sleeping (drains backlogs fast).

### 5.3 Claim, run, complete (worker)
- `XREADGROUP ... COUNT <free slots> BLOCK 1000 NOACK`. A worker only reads as many messages as it
  has free concurrency slots, so it never hoards work that other workers could run.
- **Claim** in one statement (a data-modifying CTE also opens the execution row):
  `UPDATE jobs SET status='running', attempts=attempts+1, lease_token=$new_uuid,
  lease_expires_at=now()+lease WHERE id=$1 AND status='queued'`. Zero rows → someone else has it
  (or it was cancelled, or this was a duplicate message): drop it.
- **Run** the handler under `asyncio.timeout(timeout_s)`. Sync handlers run in a thread.
- **Heartbeat**: one query per worker per interval extends *all* its leases
  (`UPDATE ... FROM unnest($ids, $tokens)`), instead of one query per job. Any job missing from
  the result has lost its lease → its handler is cancelled immediately.
- **Complete** with fencing: `UPDATE ... SET status='succeeded' WHERE id=$1 AND
  lease_token=$mine AND status='running'`; the task's transactional side effects run in the same
  transaction, after this check passes.
- **Fail**: `decide_failure()` → `scheduled` with `run_at = now() + backoff` or `dead`. Also fenced.

Timestamps for leases always come from PostgreSQL's `now()`, never worker clocks, so clock skew
between worker hosts cannot make a lease look valid or expired incorrectly.

### 5.4 Reap (scheduler)
Jobs `running` with `lease_expires_at < now()` are locked (`SKIP LOCKED`), and each is treated as
a failed attempt: rescheduled with backoff, or dead-lettered if its budget is spent. Their
execution rows become `lease_expired`. Clearing `lease_token` is what fences the old worker.

### 5.5 Sweep lost dispatches (scheduler)
A `queued` job's message can be lost: Redis restarted without the data, a crash between the
commit and XADD, or a worker died after reading the message but before claiming. Streams are
FIFO and stream ids embed a millisecond timestamp, so for each queue the sweeper reads the
consumer group's `last-delivered-id`:

- stream missing → lost (Redis lost its data);
- no consumer group yet → not lost (no worker has attached; the message is waiting);
- group has read *everything* (`last-delivered-id == last-generated-id`) → lost;
- group has read past a message added clearly after this job's dispatch time (+ a clock-skew
  margin) → lost, because FIFO delivery means this job's message would already have been handed
  out.

Otherwise the message is still in the backlog and is left alone. This avoids a naive "re-send
anything queued for > 30 s", which would flood the stream with duplicates exactly when the system
is already behind. Lost jobs get `dispatched_at = now()` and a fresh XADD; if two sweepers race,
the worst case is a duplicate message, which the conditional claim makes harmless.

### 5.6 Graceful shutdown (worker)
SIGTERM → stop reading → wait up to `shutdown_grace_s` for in-flight jobs → cancel the rest and
**release** them (`scheduled`, `run_at = now()`, `attempt_offset += 1` so the interrupted attempt
does not consume retry budget; execution `interrupted`) → remove the consumer from the group.

## 6. Delivery guarantees

| Property | Mechanism |
|---|---|
| Durability of accepted jobs | Committed to PostgreSQL before the API responds |
| No lost jobs | Leases + reaper (worker death), sweeper (lost messages), commit-before-dispatch |
| At-least-once execution | A job whose worker vanished is re-run after its lease expires |
| At-most-once *commit* | Fencing token (`lease_token`) on every completion write |
| Exactly-once *effects* | `ctx.transactional(sql)` runs in the completion transaction; for external APIs pass `ctx.idempotency_key` (the job id) downstream |
| No duplicate jobs from retries | `Idempotency-Key` + unique constraint + request-hash check |

"Exactly-once delivery" is impossible in a distributed system with failures (the two-generals
problem): if a worker completes the work and dies before recording it, *someone* has to decide
whether to run it again. jobq chooses to re-run (at-least-once) and makes the effect idempotent.

### Why fencing tokens matter
Leases alone are not enough. A worker can stall *longer than its lease* (GC pause, VM
suspension, network partition) and then wake up still believing it owns the job, while another
worker has already taken over. Without a fence, both would write results. (This is the argument
from Martin Kleppmann's "How to do distributed locking".) Here the fence is the random
`lease_token` checked on every write. `tests/test_chaos.py` reproduces it with real processes:
it `SIGSTOP`s a worker holding 10 jobs until the reaper hands them to another worker, then
`SIGCONT`s it. The zombie finishes all 10 handlers, and none of its writes land.

## 7. Failure modes

| Failure | What happens | Recovery time |
|---|---|---|
| Worker SIGKILL / OOM / host loss | Leases expire; reaper reschedules (counts as an attempt) | `lease_s` + reap interval |
| Worker stalls (GC, partition) | Same; its late writes are fenced | same |
| Worker dies between read and claim | Message lost (NOACK); sweeper re-sends | `sweep_after_s` |
| Scheduler dies | Other replicas continue (no leader); otherwise loops resume on restart | 0 with ≥2 replicas |
| API dies mid-request | Client retries with the same Idempotency-Key → at most one job | client retry |
| Redis down | Enqueue still succeeds (dispatch skipped, rate limiter fails open); workers back off and reconnect; sweeper re-dispatches | Redis downtime + `sweep_after_s` |
| Redis loses all data | Workers recreate groups on `NOGROUP`; sweeper re-sends every queued job | `sweep_after_s` |
| PostgreSQL down | API returns 5xx (nothing accepted that could be lost); workers retry status writes, then leases expire and jobs re-run | PG downtime + `lease_s` |
| Handler hangs | `timeout_s` fails the attempt (thread-based sync handlers keep running in the background, but their transactional effects can no longer commit) | `timeout_s` |
| Poison job | Retries with backoff until `max_attempts`, then DLQ; `PermanentError` skips retries | bounded |

## 8. Retry policy
Exponential backoff with **full jitter**: `delay = uniform(0, min(max, base · 2^(n−1)))`. Without
jitter, jobs that failed together (a dependency outage) retry together and cause synchronized
load spikes; full jitter spreads them (AWS Architecture Blog, "Exponential Backoff And Jitter").
Tasks can override the delay (`raise RetryLater(30)`) or skip retries (`raise PermanentError`).
Unknown task names are retryable on purpose: during a rolling deploy, a newer worker may know it.

## 9. Security
- API keys `jq_<12-hex prefix>_<256-bit secret>`: the prefix is an indexed lookup key; only
  `sha256(secret)` is stored. A fast hash is correct here, unlike passwords: the secret has 256
  bits of entropy, so brute force is infeasible and bcrypt would only add latency to every request.
  Compared with `hmac.compare_digest` (constant time). Keys can be revoked; the verification cache
  TTL (default 30 s) bounds how long a revoked key still works.
- Tenant isolation: every query filters on `api_key_id`; another tenant's job returns 404, not
  403, so ids cannot be probed.
- Payload size limit (256 KiB, 413), strict validation of names/queues, non-root container.

## 10. Scaling path and limitations

**Where the bottleneck is.** PostgreSQL writes: a successful job costs one insert and two
updates of its `jobs` row (plus one more update if it was scheduled for later) and one insert
and one update of its `executions` row.
In the peak benchmark PostgreSQL sustained ≈1,500 jobs/s using about one CPU core, with the
API processes as the actual bottleneck (see BENCHMARKS.md). The API, workers and schedulers scale
horizontally.

**Next steps if 100×:**
1. Partition `jobs` by time (or by status: hot "active" table + "archive"), making retention a
   cheap `DROP PARTITION` instead of `DELETE`.
2. Batch claims (claim N ids in one statement) and batch completions to cut round trips.
3. Shard by queue across PostgreSQL clusters; one stream per queue already maps cleanly.
4. PgBouncer in transaction mode between many workers and PostgreSQL.
5. Redis Cluster: streams are per-queue keys, so they shard naturally.

**Deliberate limitations**
- No priorities within a queue (use separate queues and dedicate workers).
- No cron/recurring schedules or workflows (chains, fan-in).
- Sync handlers cannot be forcibly killed on timeout (a Python limitation; use async handlers or
  a process pool for hard isolation).
- `GET /v1/stats` does a `GROUP BY` over the tenant's jobs; at large scale this should read
  counters maintained incrementally.
