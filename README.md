# jobq: distributed task scheduler and job queue

[![CI](https://github.com/AnuragBhandary/Distributed-Job-Scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/AnuragBhandary/Distributed-Job-Scheduler/actions/workflows/ci.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue)
![coverage](https://img.shields.io/badge/coverage-94%25-brightgreen)

A job platform in the spirit of Celery or Sidekiq, built around **correctness under failure**.
Jobs run now or on a schedule and retry with exponential backoff and jitter. Jobs that exhaust
their retries go to a dead-letter queue. Workers hold heartbeated **leases**, and every write is
**fenced**, so a crashed or frozen worker can never commit a job twice.

It comes with a React dashboard for watching queues, throughput and workers, following any job's
attempts, cancelling, enqueueing, and requeueing dead letters.

**Backend:** Python 3.12, asyncio, FastAPI, PostgreSQL (source of truth), Redis Streams
(dispatch) and Lua (rate limiting), Docker, GitHub Actions.
**Frontend:** React 19, TypeScript, Vite, Tailwind CSS, TanStack Query, React Router, React Hook
Form + Zod; tested with Vitest, Testing Library, MSW and Playwright.

### Measured results ([details](docs/BENCHMARKS.md))

| 50,000 jobs at 2,000/min, a worker `SIGKILL`ed every minute, 5% of attempts failing | |
|---|---|
| Unrecovered failures | **0** (50,000 / 50,000 succeeded; 2,633 retries and 41 crash-interrupted jobs recovered) |
| Duplicate side effects | **0** (audited in SQL: one ledger row per job) |
| Enqueue latency | **p95 7.1 ms**, p99 8.2 ms |
| Peak throughput (same laptop) | **≈ 1,500 jobs/s** end-to-end, pickup p95 3.5 ms |
| Dashboard end to end (Playwright, 4 runs) | A worker SIGKILLed mid-run each time: **all 200–500 jobs succeeded with exactly one ledger row each**, and the UI showed the 32 taken-over jobs' `lease_expired` attempt then the success on another worker |

## Architecture

```mermaid
flowchart LR
    C[Client SDK / REST] -->|POST /v1/jobs + Idempotency-Key| A[API x N<br/>FastAPI]
    A -->|token bucket, Lua| R[(Redis)]
    A -->|INSERT, idempotent| P[(PostgreSQL<br/>jobs, executions)]
    A -->|XADD job id| S[[Redis Streams<br/>one per queue]]
    S -->|XREADGROUP NOACK| W[Workers x N<br/>asyncio]
    W -->|claim lease, heartbeat,<br/>fenced complete| P
    SC[Scheduler x N] -->|promote due, reap expired leases,<br/>sweep lost messages, purge| P
    SC -->|XADD| S
```

**PostgreSQL is the source of truth; Redis only carries hints.** Every state change is a single
conditional `UPDATE` that the database arbitrates. Redis can lose messages, or all of its data,
without losing a job: the scheduler's sweeper notices and dispatches again.

| Guarantee | How |
|---|---|
| Accepted jobs are never lost | Committed before the 2xx; leases + reaper recover dead workers; sweeper recovers lost messages |
| No duplicate jobs from client retries | `Idempotency-Key` + unique constraint; reuse with a different body → 409 |
| At-least-once execution | An expired lease is rescheduled (backoff, or DLQ when the budget is spent) |
| Exactly-once *effects* | Fencing token on every write; `ctx.transactional(sql)` commits side effects in the same transaction as the fenced completion |
| Horizontal scaling, no leader | `FOR UPDATE SKIP LOCKED` and conditional updates everywhere; run any number of each component |

The full reasoning, alternatives considered (Postgres-only polling, Redis-as-truth, Kafka, SQS)
and failure-mode analysis are in **[docs/DESIGN.md](docs/DESIGN.md)**.

## Quick start

```bash
docker compose up -d --build --wait          # Postgres, Redis, API, scheduler, 3 workers
export KEY=jq_0000000000de_local-dev-key-not-for-production   # dev-only bootstrap key

curl -s -X POST localhost:8000/v1/jobs -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: order-42' -d '{"task": "examples.fib", "args": [50]}'
curl -s localhost:8000/v1/stats -H "X-API-Key: $KEY"
```

Dashboard: http://localhost:8000 (sign in with the key above) · API docs: http://localhost:8000/docs
· Prometheus metrics: `/metrics`.

## Dashboard

`web/` is a React + TypeScript app that the API serves at `/` (the Docker image builds it).

- **Overview:** job counts by status and queue, throughput over the last 5 minutes (succeeded vs
  failed attempts per 10 s), and the workers that ran your jobs recently, with quiet ones flagged.
  It refreshes every 2 seconds.
- **Jobs:** filter by status, queue and task. The filters live in the URL, so a view can be
  shared. Pages load with "Load more" on the API's keyset cursor.
- **Job:** arguments, result or error, and every attempt in order: which worker ran it, how long
  it took, and why it failed (`lease_expired`, `fenced` and so on, in plain words). A pending job
  can be cancelled after a confirmation step; a dead one can be requeued.
- **Dead letters:** select several or all, requeue them four at a time, with progress and a list
  of any that failed.
- **New job:** a validated form (Zod) with presets for the example tasks. The JSON arguments are
  checked before sending, and each submit carries a fresh `Idempotency-Key`, so a double click
  or a retry cannot create two jobs.

It signs in with an API key, checked against the API before being kept in `sessionStorage`. A
rejected key (for example one revoked meanwhile) signs you out with the reason. Each key gets its
own query cache, so signing out never shows another key's data.

```bash
make web-dev     # Vite on :5173 with hot reload, proxying /v1 to the API on :8000
make web-test    # ESLint, tsc, 38 Vitest tests against an MSW fake of the API
make e2e         # Playwright against the real stack (kills a worker on purpose)
```

### Python SDK (Celery-style)

```python
from jobq import JobContext, JobQ, PermanentError, RetryLater

app = JobQ("http://localhost:8000", api_key="jq_...")

@app.task(max_attempts=5, queue="emails", timeout_s=30)
def send_welcome_email(user_id: int) -> str:
    ...

@app.task(bind=True)
async def charge(ctx: JobContext, order_id: int, cents: int) -> None:
    # ctx.idempotency_key is the job id: stable across retries, so the provider dedupes
    await payments.charge(order_id, cents, idempotency_key=ctx.idempotency_key)
    # runs in the same transaction as the fenced completion: commits exactly once
    ctx.transactional("UPDATE orders SET paid = true WHERE id = $1", order_id)

result = send_welcome_email.delay(42)                      # run now
send_welcome_email.apply_async(args=[7], countdown=3600)   # run in an hour
result.get(timeout=10)                                     # wait for the return value
```

Workers import the same module: `jobq worker --tasks myapp.tasks --queues default,emails`.
Tasks can `raise RetryLater(30)` to pick their own delay or `raise PermanentError(...)` to skip
straight to the DLQ.

### REST API

| Method | Path | |
|---|---|---|
| `POST` | `/v1/jobs` | Enqueue (`delay_s` or `run_at`, `max_attempts`, `timeout_s`, backoff); `Idempotency-Key` header |
| `GET` | `/v1/jobs` | List, keyset-paginated; filter by `status`, `queue`, `task` |
| `GET` | `/v1/jobs/{id}` | Job status, result, last error |
| `GET` | `/v1/jobs/{id}/executions` | Every attempt: worker, outcome, error, duration |
| `POST` | `/v1/jobs/{id}/cancel` | Cancel a job that has not started |
| `GET` | `/v1/dlq` | Dead-letter queue |
| `POST` | `/v1/dlq/{id}/requeue` | Retry a dead job with a fresh retry budget |
| `GET` | `/v1/stats` | Job counts per queue and status |
| `GET` | `/v1/stats/throughput` | Succeeded vs failed attempts per bucket over a window (dashboard chart) |
| `GET` | `/v1/workers` | Workers that ran your jobs recently: running, done, failed, last seen |
| `GET` | `/healthz`, `/readyz`, `/metrics` | Liveness, readiness (Postgres + Redis), Prometheus |

Authenticate with `X-API-Key` or `Authorization: Bearer`. Rate limits come back in
`X-RateLimit-*` headers, and 429s carry `Retry-After`.

## Testing

```bash
make infra   # Postgres + Redis in Docker
make check   # ruff, mypy, 90 tests with coverage gate (94%)
make web-test && make e2e   # dashboard: 38 component tests (99% lines), 2 Playwright tests
```

The tests run against real PostgreSQL and Redis, because the guarantees depend on real row
locking and stream semantics. **`tests/test_chaos.py`** starts real worker processes and:

- **SIGKILLs a worker mid-job.** Its leases expire, the reaper reschedules the jobs and another
  worker finishes them. Each of the 80 jobs has exactly one ledger row.
- **Freezes a worker with SIGSTOP** until its 10 jobs are taken over, then SIGCONTs it. The
  "zombie" finishes all its handlers, but every write is fenced off: still exactly one row per job.

**`web/e2e/dashboard.spec.ts`** does the same through the browser against the compose stack.
It enqueues 200 ledger jobs, finds the worker running the most of them, `docker kill`s its
container, and waits for every job to succeed. Then it counts ledger rows in PostgreSQL (one per
job) and opens a taken-over job in the dashboard to check its `lease_expired` attempt. A second
test enqueues through the form, then dead-letters three jobs and requeues them from the UI.

## Project layout

```
src/jobq/
  api/            FastAPI app, routes, auth + rate-limit dependencies, schemas
  migrations/     SQL schema (applied by `jobq migrate`)
  repository.py   every SQL statement: claims, fencing, reaping, promotion
  dispatch.py     Redis Streams dispatch and lost-message detection
  worker.py       leases, heartbeats, timeouts, fenced completion, graceful shutdown
  scheduler.py    promote / reap / sweep / purge / gauge loops
  client.py       Celery-style SDK
  backoff.py      full-jitter backoff and the retry-or-dead decision
web/src/
  api/            fetch wrapper, API key session, TanStack Query hooks (polling, mutations)
  pages/          overview, jobs, job, dead letters, new job, sign in
  components/     throughput chart (SVG), attempt timeline, job table, badges
  test/fakeApi.ts in-memory fake of the REST API served through MSW
web/e2e/          Playwright: worker SIGKILL takeover, form enqueue, DLQ requeue
bench/            open-loop load generator + chaos monkey + database audit
docs/             DESIGN.md, BENCHMARKS.md, INTERVIEW.md
```
