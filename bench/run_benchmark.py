"""End-to-end load + chaos benchmark against the docker compose stack.

    docker compose up -d --build
    uv run python bench/run_benchmark.py --jobs 50000 --rate 2000 --chaos-interval 60

What it does:
1. Creates a dedicated API key, so every job of this run can be audited afterwards.
2. Submits ``--jobs`` ``examples.ledger`` jobs at a fixed ``--rate`` (jobs/minute) with an
   *open-loop* generator: request i is due at t0 + i/rate whether or not earlier requests have
   finished, so a slow server cannot hide its latency by slowing the generator down
   ("coordinated omission"). Latency is reported both from the actual send time and from the
   intended send time.
3. Injects failures: each attempt fails with probability ``--p-fail`` (transient error, retried
   with backoff), a fraction of jobs are scheduled with a delay, and every ``--chaos-interval``
   seconds a random worker container is SIGKILLed and restarted a few seconds later.
4. Waits for every job to settle and audits PostgreSQL: unrecovered failures (dead jobs),
   duplicate or missing ledger rows (exactly-once effects), attempts, recovered leases and
   pickup latency.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import random
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
import httpx

from jobq.auth import create_api_key


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    k = (len(ordered) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def summary_ms(values: list[float]) -> dict[str, float]:
    ms = [v * 1000 for v in values]
    return {
        "count": len(ms),
        "mean": round(statistics.fmean(ms), 2) if ms else float("nan"),
        "p50": round(percentile(ms, 50), 2),
        "p95": round(percentile(ms, 95), 2),
        "p99": round(percentile(ms, 99), 2),
        "max": round(max(ms), 2) if ms else float("nan"),
    }


@dataclass
class Submission:
    service_latency: list[float] = field(default_factory=list)
    intended_latency: list[float] = field(default_factory=list)
    errors: dict[str, int] = field(default_factory=dict)
    accepted: int = 0


async def submit_all(args: argparse.Namespace, key: str, out: Submission) -> float:
    interval = 60.0 / args.rate
    limits = httpx.Limits(
        max_connections=args.concurrency, max_keepalive_connections=args.concurrency
    )
    semaphore = asyncio.Semaphore(args.concurrency)
    rng = random.Random(args.seed)
    async with httpx.AsyncClient(
        base_url=args.url, headers={"X-API-Key": key}, limits=limits, timeout=30
    ) as http:

        async def send(i: int, intended: float) -> None:
            body: dict[str, Any] = {
                "task": "examples.ledger",
                "args": [i],
                "kwargs": {"work_ms": args.work_ms, "p_fail": args.p_fail},
                "max_attempts": args.max_attempts,
                "backoff_base_s": 0.5,
                "backoff_max_s": 10,
            }
            if rng.random() < args.delayed_fraction:
                body["delay_s"] = round(rng.uniform(1, 30), 3)
            async with semaphore:
                for attempt in range(4):
                    started = time.perf_counter()
                    try:
                        resp = await http.post(
                            "/v1/jobs", json=body, headers={"Idempotency-Key": f"{key[-8:]}-{i}"}
                        )
                    except httpx.HTTPError as exc:
                        name = type(exc).__name__
                        out.errors[name] = out.errors.get(name, 0) + 1
                        await asyncio.sleep(0.2 * 2**attempt)
                        continue
                    done = time.perf_counter()
                    if resp.status_code in (200, 201):
                        if attempt == 0:
                            out.service_latency.append(done - started)
                            out.intended_latency.append(done - intended)
                        out.accepted += 1
                        return
                    code = f"http_{resp.status_code}"
                    out.errors[code] = out.errors.get(code, 0) + 1
                    await asyncio.sleep(0.2 * 2**attempt)

        tasks: list[asyncio.Task[None]] = []
        t0 = time.perf_counter()
        for i in range(args.jobs):
            intended = t0 + i * interval
            delay = intended - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(send(i, intended)))
            if i and i % 5000 == 0:
                elapsed = time.perf_counter() - t0
                print(f"  submitted {i:>6} jobs in {elapsed:7.1f}s", flush=True)
        await asyncio.gather(*tasks)
        return time.perf_counter() - t0


async def docker(*args: str) -> str:
    """Run a docker CLI command without blocking the event loop; returns stdout."""
    proc = await asyncio.to_thread(
        subprocess.run, ["docker", *args], capture_output=True, text=True
    )
    return proc.stdout


async def chaos_monkey(
    args: argparse.Namespace, events: list[dict[str, Any]], stop: asyncio.Event
) -> None:
    rng = random.Random(args.seed + 1)
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=args.chaos_interval)
            return
        except TimeoutError:
            pass
        ids = (await docker("compose", "ps", "-q", "worker")).split()
        if not ids:
            continue
        victim = rng.choice(ids)
        await docker("kill", "-s", "KILL", victim)
        now = datetime.now(UTC).isoformat()
        events.append({"t": now, "action": "SIGKILL", "container": victim[:12]})
        print(f"  chaos: SIGKILL worker {victim[:12]}", flush=True)
        await asyncio.sleep(args.chaos_restart_after)
        await docker("start", victim)
        now = datetime.now(UTC).isoformat()
        events.append({"t": now, "action": "restart", "container": victim[:12]})


async def wait_settled(conn: asyncpg.Connection, key_id: int, limit_s: float) -> None:
    deadline = time.monotonic() + limit_s
    while time.monotonic() < deadline:
        pending = await conn.fetchval(
            """
            SELECT count(*) FROM jobs
            WHERE api_key_id = $1 AND status IN ('scheduled', 'queued', 'running')
            """,
            key_id,
        )
        if pending == 0:
            return
        print(f"  waiting for {pending} unfinished jobs", flush=True)
        await asyncio.sleep(5)
    raise TimeoutError("jobs did not settle")


async def audit(conn: asyncpg.Connection, key_id: int) -> dict[str, Any]:
    status = {
        r["status"]: r["n"]
        for r in await conn.fetch(
            "SELECT status::text, count(*) AS n FROM jobs WHERE api_key_id = $1 GROUP BY 1", key_id
        )
    }
    ledger = await conn.fetchrow(
        """
        SELECT count(*) AS rows, count(DISTINCT l.job_id) AS jobs
        FROM example_ledger l JOIN jobs j ON j.id = l.job_id WHERE j.api_key_id = $1
        """,
        key_id,
    )
    missing = await conn.fetchval(
        """
        SELECT count(*) FROM jobs j WHERE j.api_key_id = $1 AND j.status = 'succeeded'
          AND NOT EXISTS (SELECT 1 FROM example_ledger l WHERE l.job_id = j.id)
        """,
        key_id,
    )
    orphan = await conn.fetchval(
        """
        SELECT count(*) FROM example_ledger l JOIN jobs j ON j.id = l.job_id
        WHERE j.api_key_id = $1 AND j.status <> 'succeeded'
        """,
        key_id,
    )
    executions = {
        r["status"]: r["n"]
        for r in await conn.fetch(
            """
            SELECT e.status::text, count(*) AS n FROM executions e JOIN jobs j ON j.id = e.job_id
            WHERE j.api_key_id = $1 GROUP BY 1
            """,
            key_id,
        )
    }
    attempts = {
        r["attempts"]: r["n"]
        for r in await conn.fetch(
            "SELECT attempts, count(*) AS n FROM jobs WHERE api_key_id = $1 GROUP BY 1 ORDER BY 1",
            key_id,
        )
    }
    pickup = [
        r["s"]
        for r in await conn.fetch(
            """
            SELECT extract(epoch FROM e.started_at - j.run_at)::float8 AS s
            FROM jobs j JOIN executions e ON e.job_id = j.id AND e.attempt = 1
            WHERE j.api_key_id = $1 AND j.attempts = 1 AND j.status = 'succeeded'
            """,
            key_id,
        )
    ]
    window = await conn.fetchrow(
        "SELECT min(created_at) AS first, max(finished_at) AS last FROM jobs WHERE api_key_id = $1",
        key_id,
    )
    total = sum(status.values())
    dead = status.get("dead", 0)
    return {
        "jobs": total,
        "status": status,
        "unrecovered_failures": dead,
        "unrecovered_failure_rate_pct": round(100 * dead / total, 4) if total else None,
        "ledger_rows": ledger["rows"],
        "ledger_distinct_jobs": ledger["jobs"],
        "duplicate_effects": ledger["rows"] - ledger["jobs"],
        "succeeded_without_effect": missing,
        "effects_for_unsucceeded_jobs": orphan,
        "executions": executions,
        "attempts_histogram": attempts,
        "pickup_latency_ms": summary_ms(pickup),
        "wall_clock_s": round((window["last"] - window["first"]).total_seconds(), 1),
    }


async def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--dsn", default="postgresql://jobq:jobq@localhost:5432/jobq")
    parser.add_argument("--jobs", type=int, default=50_000)
    parser.add_argument("--rate", type=float, default=2000, help="jobs per minute")
    parser.add_argument("--concurrency", type=int, default=64, help="max in-flight requests")
    parser.add_argument("--work-ms", type=float, default=50)
    parser.add_argument(
        "--p-fail", type=float, default=0.05, help="per-attempt failure probability"
    )
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--delayed-fraction", type=float, default=0.1)
    parser.add_argument("--chaos-interval", type=float, default=0, help="seconds; 0 disables")
    parser.add_argument("--chaos-restart-after", type=float, default=5)
    parser.add_argument("--settle-timeout", type=float, default=900)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--label", default="run")
    parser.add_argument("--out", type=Path, default=Path("bench/results"))
    args = parser.parse_args()

    conn = await asyncpg.connect(args.dsn)
    key_id, key = await create_api_key(
        conn, f"bench-{args.label}-{int(time.time())}", rate_per_s=100_000, burst=100_000
    )
    print(f"benchmark {args.label}: {args.jobs} jobs at {args.rate}/min, chaos every "
          f"{args.chaos_interval or 'never'}s", flush=True)  # fmt: skip

    submission = Submission()
    events: list[dict[str, Any]] = []
    stop = asyncio.Event()
    chaos = asyncio.create_task(chaos_monkey(args, events, stop)) if args.chaos_interval else None
    submit_seconds = await submit_all(args, key, submission)
    stop.set()
    if chaos:
        await chaos
        # make sure every killed worker is back before waiting for the backlog
        await docker("compose", "up", "-d", "worker")
    await wait_settled(conn, key_id, args.settle_timeout)
    result = {
        "label": args.label,
        "timestamp": datetime.now(UTC).isoformat(),
        "machine": {"platform": platform.platform(), "python": sys.version.split()[0]},
        "config": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
        "submission": {
            "accepted": submission.accepted,
            "errors": submission.errors,
            "duration_s": round(submit_seconds, 1),
            "achieved_rate_per_min": round(submission.accepted / submit_seconds * 60, 1),
            "enqueue_latency_ms": summary_ms(submission.service_latency),
            "enqueue_latency_from_intended_start_ms": summary_ms(submission.intended_latency),
        },
        "chaos_events": events,
        "audit": await audit(conn, key_id),
    }
    await conn.close()
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / f"{args.label}.json"
    path.write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps({k: result[k] for k in ("submission", "audit")}, indent=2, default=str))
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
