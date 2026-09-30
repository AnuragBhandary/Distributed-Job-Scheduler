# Interview guide

How to learn this codebase, pitch it, and defend every decision in a system-design or
behavioural deep dive. Numbers are in [BENCHMARKS.md](BENCHMARKS.md); reasoning is in
[DESIGN.md](DESIGN.md).

## 1. Learning path (read in this order)

| # | File | What to be able to explain afterwards |
|---|---|---|
| 1 | `src/jobq/migrations/0001_init.sql` | Every column, the state enum, why each partial index exists |
| 2 | `src/jobq/backoff.py` | Full jitter; `attempt_offset`; when a job becomes dead |
| 3 | `src/jobq/repository.py` | Each SQL statement: what it races with and why it is still correct |
| 4 | `src/jobq/dispatch.py` | Streams, consumer groups, NOACK, `message_lost()` truth table |
| 5 | `src/jobq/api/routes.py` → `create_job` | Idempotency (200 vs 201 vs 409), commit-then-dispatch |
| 6 | `src/jobq/worker.py` | Claim → run → heartbeat → fenced complete; cancellation paths |
| 7 | `src/jobq/scheduler.py` | Five loops, why no leader election |
| 8 | `src/jobq/ratelimit.py`, `auth.py` | Token bucket in Lua; why sha256 not bcrypt |
| 9 | `src/jobq/client.py` | Celery-style API; why retries are safe |
| 10 | `tests/test_chaos.py` | SIGKILL and SIGSTOP experiments; what each assertion proves |
| 11 | `bench/run_benchmark.py` | Open-loop load, coordinated omission, the audit queries |

Exercise after each file: close it and re-derive the key statement or function on paper.

## 2. The 60-second pitch

> "I built a distributed job queue in Python: FastAPI for the API, PostgreSQL as the source of
> truth and Redis Streams for low-latency dispatch. The interesting part is correctness under
> failure. Workers take time-bounded leases on jobs and heartbeat them. If a worker dies, a
> reaper reschedules its jobs with exponential backoff and jitter, and exhausted jobs go to a
> dead-letter queue. Leases alone aren't safe, because a paused worker can wake up after its lease
> was handed to someone else, so every write carries a fencing token. A task's database side
> effects commit in the same transaction as that fenced check, which gives exactly-once effects on
> top of at-least-once execution. I proved it with chaos tests that SIGKILL workers and freeze
> them with SIGSTOP, and with a 50,000-job benchmark that kills a worker every minute and then
> audits the database for duplicates."

## 3. Numbers to know cold

- 50,000 jobs at 2,000/min for 25 min, 23 worker SIGKILLs, 5% of attempts failing on purpose.
- 0 unrecovered failures, 0 duplicate effects, 41 crash-interrupted jobs recovered in ≈11 s each.
- Enqueue p95 7.1 ms / p99 8.2 ms at the sustained rate.
- Peak ≈1,500 jobs/s end-to-end on one laptop; bottleneck = API CPU, not PostgreSQL fsync
  (`synchronous_commit=off` gave only +18%).
- 88 tests, 94% coverage; CI gate at 90%.

## 4. Resume bullets → evidence

| Claim | Where it is proven |
|---|---|
| Scheduled + immediate execution | `insert_job` status CASE; `promote_due`; `test_promotes_only_due_jobs` |
| Exponential-backoff retries | `backoff.py`; `test_transient_failures_retry_with_backoff_then_succeed` |
| Dead-letter handling | `/v1/dlq`, requeue with fresh budget; `test_dead_letter_queue_list_and_requeue` |
| Worker leases | `claim_job`, `extend_leases`, `reap_expired_leases`; `test_heartbeat_keeps_long_job_alive` |
| Idempotency keys | `test_concurrent_duplicate_submissions_create_one_job` (20 parallel identical requests → 1 job) |
| Persisted execution history | `executions` table; `GET /v1/jobs/{id}/executions` |
| 2,000 jobs/min over 50,000 jobs, failure rate, p95 enqueue | `docs/BENCHMARKS.md`, raw JSON in `bench/results/` |
| No duplicate execution across retry and crash-recovery tests | `test_chaos.py` and the benchmark audit (`duplicate_effects = 0`) |
| REST + Celery-style client, API keys, rate limiting | `api/`, `client.py`, `ratelimit.py`, `auth.py` |
| Containerized, >90% coverage | `Dockerfile`, `docker-compose.yml`, CI coverage gate |

Be precise about "no duplicate execution": say "no job's effects were committed twice". Jobs whose
worker was killed *were* executed again (that's what at-least-once means); the audit shows the
fencing made that invisible to the data.

## 5. Deep-dive questions and answers

**Walk me through what happens when I POST a job.**
Auth (cached key lookup, constant-time compare) → token bucket in Redis (Lua, atomic) → one
`INSERT ... ON CONFLICT DO NOTHING` that also decides `queued` vs `scheduled` in SQL → if queued,
`XADD` the id → 201. Three round trips total.

**What if the API crashes after the INSERT but before responding?**
The job exists. The client times out and retries with the *same* Idempotency-Key (the SDK always
sends one), and gets the existing job back with 200. No duplicate.

**What if it crashes after the INSERT but before the XADD?**
The job sits in `queued` with no message. The sweeper finds queued jobs older than
`sweep_after_s` whose message can no longer be delivered (comparing `dispatched_at` with the
consumer group's `last-delivered-id`) and re-sends them.

**Why commit before XADD, not after?**
Otherwise a fast worker could read the message before the row is visible, fail the conditional
claim, and drop the message. Commit-then-publish plus a sweeper is a lightweight transactional
outbox.

**How do two workers not run the same job?**
The claim is `UPDATE jobs SET status='running' ... WHERE id=$1 AND status='queued'`. Row-level
locking makes the second UPDATE wait for the first, then re-check the WHERE clause and match zero
rows. Only one worker gets `RETURNING` data.

**A worker is running a job and gets SIGKILLed. What happens?**
Heartbeats stop, `lease_expires_at` passes, the reaper (every second) locks expired rows with
`SKIP LOCKED`, marks the execution `lease_expired`, and reschedules the job with backoff (or
dead-letters it if its budget is spent). Another worker picks it up. Recovery ≈ lease length.

**How do you pick the lease length?**
It trades recovery time against false expiry. Heartbeat at about 1/3 of the lease, so two
consecutive missed heartbeats are tolerated. Defaults: lease 30 s, heartbeat 10 s (compose uses
10 s / 3 s to make chaos recovery faster). Long jobs are fine because the lease is renewed; the
lease is not a job timeout.

**What if the worker isn't dead, just slow, a 40-second GC pause?**
Then the lease expires and another worker takes the job. When the first one wakes up, every write
it attempts includes `lease_token = <its token>`, which no longer matches, so zero rows are
updated: fenced. Its heartbeat also notices and cancels the handler. The chaos test reproduces
this with SIGSTOP/SIGCONT.

**So is this exactly-once?**
Execution is at-least-once, which is unavoidable. *Effects* are exactly-once when they go through
`ctx.transactional()` (same transaction as the fenced completion) or when downstream calls are
idempotent using `ctx.idempotency_key` (the job id, stable across retries).

**Why NOACK on the stream? Isn't that dangerous?**
It's safe because Redis isn't the source of truth. With ACKs I'd need to track and reclaim the
pending-entries list (`XAUTOCLAIM`), which duplicates what leases in PostgreSQL already do. NOACK
means a message is "at-most-once", and the sweeper turns that back into "eventually delivered".

**Why Redis at all?**
Push-based pickup: workers block on XREADGROUP and wake up immediately, so there's no polling
load on PostgreSQL and no poll-interval latency. Plus a natural home for the shared rate limiter.
The trade-off is one more moving part, mitigated by designing so it can fail without losing jobs.

**How does the scheduler avoid double-promoting with multiple replicas?**
`FOR UPDATE SKIP LOCKED`: each replica locks a disjoint batch. There's no leader election, so
there's no failover delay and no split-brain. `test_concurrent_schedulers_never_double_promote`
runs 4 replicas over 300 jobs.

**What's full jitter and why does it matter?**
`uniform(0, min(cap, base·2^n))`. When a dependency fails, all its jobs fail at the same moment.
Plain exponential backoff retries them in synchronized waves; jitter spreads them out so the
dependency can recover.

**How is rate limiting implemented and why Lua?**
A token bucket per API key: refill = elapsed × rate, capped at burst; consume one token per
request. Read-modify-write must be atomic across API replicas, so it's a Lua script (Redis runs
scripts atomically). It uses Redis `TIME` so replicas with skewed clocks agree. If Redis is down
it fails open: availability of job submission beats strict limiting.

**Why sha256 for API keys and not bcrypt?**
bcrypt protects *low-entropy* secrets (passwords) against offline guessing. These secrets are 256
random bits, so guessing is infeasible, and a slow hash would add tens of ms to every request.

**How do you paginate `GET /jobs`?**
Keyset pagination on `(created_at, id)`, with the cursor encoding the last row. It's O(page size)
using the `(api_key_id, created_at DESC, id DESC)` index and stable under concurrent inserts,
unlike OFFSET.

**How would you scale to 100× the load?**
Measure first (PostgreSQL writes will dominate). Then: batch claims and completions; PgBouncer;
partition `jobs` by time so retention is `DROP PARTITION`; shard queues across database clusters;
Redis Cluster (one stream per queue shards naturally). API, workers and schedulers already scale
horizontally.

**What would you do differently / what's missing?**
Priorities, cron schedules, workflows (chains/fan-in), per-queue concurrency limits, a dashboard
UI, OpenTelemetry tracing across API → worker, and killing hung sync handlers (would need a
process pool).

**How do you know it works?**
88 tests at 94% coverage against real PostgreSQL and Redis (not mocks, because the guarantees
depend on real locking and stream semantics), multi-process chaos tests, and a benchmark whose
audit is a SQL query any reviewer can re-run.

## 6. Amazon Leadership Principles stories

Use STAR (Situation, Task, Action, Result). Keep each to about 2 minutes, and have the numbers ready.

- **Dive Deep**: *Leases weren't enough.* While designing crash recovery I realised a worker
  paused longer than its lease could still commit after another worker took over. I added fencing
  tokens and wrote a SIGSTOP/SIGCONT test that reproduces the zombie. Result: the zombie's writes
  are rejected and the ledger shows exactly one effect per job.
- **Insist on the Highest Standards**: *The benchmark had to be honest.* A closed-loop load
  generator hides server slowness (coordinated omission), so I built an open-loop one that reports
  latency from the intended send time too, and made the benchmark audit the database for
  duplicates and missing effects instead of trusting counters.
- **Invent and Simplify**: *No leader election.* Instead of adding a coordination service for the
  scheduler, every loop is written to be safe under concurrency (SKIP LOCKED, conditional
  updates). You can run any number of replicas.
- **Frugality**: The whole system runs on a laptop with open-source components and free CI; Redis
  is optional for correctness, so it can be a small single node.
- **Ownership / Are Right, A Lot**: The resume numbers were written as targets first. I built
  the benchmark to test them and committed to changing the resume if they didn't hold.
- **Bias for Action**: Shipped a minimal end-to-end path (API → stream → worker) first, then added
  leases, retries, the DLQ and chaos testing incrementally, with tests at every step.

## 7. Whiteboard version (draw in this order)

1. Client → API → PostgreSQL (jobs table) → 201.
2. Add Redis stream between API and workers; explain "hint vs truth".
3. Add worker lease + heartbeat + reaper; walk the SIGKILL story.
4. Add the fencing token; walk the GC-pause story.
5. Add the scheduler (promote, sweep) and the DLQ.
6. Finish with the guarantees table (DESIGN.md §6) and the scaling path (§10).
