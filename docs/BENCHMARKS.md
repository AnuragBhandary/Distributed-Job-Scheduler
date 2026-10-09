# Benchmarks

All numbers come from `bench/run_benchmark.py`. The raw output of every run is in
[`bench/results/`](../bench/results). Each run creates its own API key, and the verdict comes from
SQL over PostgreSQL (the audit section of the script), not from in-process counters.

## Environment

| | |
|---|---|
| Machine | MacBook Pro, Apple M5 Pro, 24 GB RAM |
| Containers | Colima VM with 8 vCPU / 8 GiB, Docker Compose (`docker-compose.yml` as committed) |
| Software | PostgreSQL 16.15, Redis 7.4.11 (AOF everysec), Python 3.12, uvicorn + uvloop |
| Topology | 1 PostgreSQL, 1 Redis, 1 scheduler, 3 workers × 32 concurrent jobs, API with 2 processes (4 for the peak run) |
| Worker settings | lease 10 s, heartbeat 3 s |

Everything, including the load generator, shares one laptop. These are not production numbers,
but they are a consistent baseline for comparing changes.

## 1. Sustained load with chaos: 50,000 jobs at 2,000 jobs/min

```bash
uv run python bench/run_benchmark.py --jobs 50000 --rate 2000 --work-ms 250 \
    --p-fail 0.05 --chaos-interval 60 --label sustained-2000pm-chaos
```

**Workload.** Every job is `examples.ledger`: 250 ms of simulated work, then one row written
through `ctx.transactional()` in the same transaction as the fenced completion. Faults injected:
- 5% of all attempts raise a transient error (retried with full-jitter backoff, 5 attempts max);
- 10% of jobs are scheduled 1-30 s in the future;
- **every 60 s a random worker container is `SIGKILL`ed** and restarted 5 s later (23 kills).

| Metric | Result |
|---|---|
| Submitted / accepted | 50,000 / 50,000 over 25 min 0 s (2,000.0 jobs/min), 0 client errors |
| Enqueue latency, client-measured | **p50 4.5 ms · p95 7.1 ms · p99 8.2 ms** (max 241 ms) |
| Enqueue latency from *intended* send time | p50 5.5 ms · p95 8.1 ms · p99 9.5 ms |
| Final state | **50,000 succeeded · 0 dead (0.000% unrecovered failures)** |
| Ledger rows / distinct jobs | 50,000 / 50,000 → **0 duplicate effects, 0 missing** |
| Transient failures retried | 2,633 failed attempts, all recovered |
| In-flight jobs lost to worker crashes | 41 leases expired → all reaped and re-run; median 11.1 s from the crashed attempt to its retry (lease 10 s + reaper tick) |
| Attempts per job | 1: 47,462 · 2: 2,408 · 3: 124 · 4: 6 |
| Worker processes involved | 26 (3 original + 23 restarts) |
| Pickup latency (run_at → claim, first attempts) | p50 1.6 ms · p95 131 ms · p99 229 ms |

Pickup p95/p99 are dominated by the 10% delayed jobs, which wait for the promoter's 250 ms tick
(`JOBQ_PROMOTE_INTERVAL_S`). Immediate jobs are pushed through the stream and are claimed in about
1 ms (see the peak run, which has no delayed jobs).

## 2. Peak throughput

```bash
JOBQ_API_WORKERS=4 docker compose up -d --wait api
# three generators inside the compose network, 20,000 jobs each, as fast as possible
docker run --rm --user "$(id -u)" --network distributed-job-scheduler_default \
    -v "$PWD/bench:/bench" --entrypoint python jobq:latest /bench/run_benchmark.py \
    --url http://api:8000 --dsn postgresql://jobq:jobq@postgres:5432/jobq \
    --jobs 20000 --rate 600000 --concurrency 64 --work-ms 0 --p-fail 0 --delayed-fraction 0 \
    --seed 1 --label peak-generator-1 --out /bench/results
```

| Metric | Result |
|---|---|
| Jobs | 60,000 (3 generators × 20,000), no-op task, no injected failures |
| End-to-end throughput | **≈ 1,500 jobs/s (≈ 90,000 jobs/min)**: 60,000 enqueued *and completed* in 39.8 s |
| Enqueue latency at saturation | p50 ≈ 79 ms · p95 ≈ 373 ms · p99 ≈ 610 ms (requests queue in the saturated API) |
| Pickup latency | p50 1.2 ms · p95 3.5 ms · p99 8.5 ms |
| Duplicate effects / dead jobs | 0 / 0 |

That is about 45× the sustained target of run 1. Workers kept pace with the API: jobs finished
within the same 40 s window in which they were submitted.

### Where the bottleneck is (and isn't)

- **API CPU.** Under peak load the API processes were the busiest containers: request parsing and
  validation, auth, the rate-limit script, an INSERT and an XADD per request. The API is stateless,
  so it scales horizontally.
- **Not PostgreSQL fsync.** With `synchronous_commit = off` (an experiment only; it is on in every
  reported run) throughput rose by only ~18%, so commit latency is not the limit at this scale.
  PostgreSQL CPU was about 1.1 of 8 cores.
- **Measurement pitfall found along the way.** A single load generator on the macOS host topped out
  near 500 requests/s because of Colima's host→VM port forwarding, while the containers were far
  from saturated. Moving the generators into the Docker network removed that artificial ceiling.
  Lesson: check where the time goes before blaming the system under test.

## Methodology notes

- **Open-loop generation.** Request *i* is due at `t0 + i / rate` whether or not earlier
  requests have returned. A closed-loop generator (send, wait, send) slows down when the server
  slows down and under-reports tail latency ("coordinated omission"). Latency is reported both from
  the actual send time and from the intended send time.
- **Idempotent submission.** Each request carries `Idempotency-Key: <run>-<i>`, so a generator
  retry can never inflate the job count.
- **The audit is SQL.** Exactly-once is checked as `count(rows) = count(DISTINCT job_id)` on the
  ledger, joined against job status. It also checks succeeded jobs with no effect and effects for
  jobs that did not succeed.

## 3. Dashboard end to end (Playwright)

`web/e2e/dashboard.spec.ts` against the compose stack (API, scheduler, 3 workers × 32
concurrency, `JOBQ_LEASE_S=10`), in headless Chromium. It signs in, enqueues N `examples.ledger`
jobs (3 s of work each), waits until one worker is running more than 5 of them, and SIGKILLs
that worker's container. Then it waits for every job to succeed and audits PostgreSQL. Last, it
opens a taken-over job in the dashboard: attempt 1 must show `lease expired` on the killed
worker, and the last attempt `succeeded`.

| Run | Jobs | Killed | Jobs taken over (attempts > 1) | Ledger rows / distinct jobs | Result |
|---|---|---|---|---|---|
| 1 | 200 | worker-2 | 32 | 200 / 200 | passed |
| 2 | 500 | worker-2 | 32 | 500 / 500 | passed |
| 3 | 500 | worker-2 | 32 | 500 / 500 | passed |
| 4 | 500 | worker-2 | 32 | 500 / 500 | passed |

32 is the killed worker's full concurrency: every job it was running was taken over once its
lease expired, and none was committed twice.

## Reproduce

```bash
make up          # build and start the stack
make bench       # run 1 (≈ 27 minutes)
make bench-smoke # 2,000 jobs with chaos, about a minute
make e2e         # run 3 with 200 jobs; E2E_JOBS=500 for the larger runs
```
