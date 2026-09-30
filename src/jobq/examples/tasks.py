"""Demo tasks, used by docker compose, the chaos tests and the benchmark.

jobq worker --tasks jobq.examples.tasks
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

from jobq import JobContext, JobQ, PermanentError

app = JobQ()


@app.task(name="examples.echo")
def echo(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return {"args": list(args), "kwargs": kwargs}


@app.task(name="examples.sleep")
async def sleep(seconds: float) -> float:
    await asyncio.sleep(seconds)
    return seconds


@app.task(name="examples.flaky", max_attempts=5, backoff_base_s=0.5)
async def flaky(p_fail: float = 0.5) -> str:
    """Fails transiently with probability ``p_fail`` per attempt."""
    if random.random() < p_fail:
        raise RuntimeError("transient failure (injected)")
    return "ok"


@app.task(name="examples.fail", max_attempts=1)
def fail(message: str = "permanent failure (injected)") -> None:
    raise PermanentError(message)


@app.task(name="examples.fib")
def fib(n: int) -> int:
    """CPU-bound and synchronous: runs in a worker thread."""
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


@app.task(name="examples.ledger", bind=True, max_attempts=5, backoff_base_s=0.2)
async def ledger(ctx: JobContext, value: int, work_ms: float = 0, p_fail: float = 0.0) -> int:
    """Simulated work, an optional injected failure, then one ledger row written atomically
    with the job's completion. Exactly one row per succeeded job proves exactly-once effects."""
    await asyncio.sleep(work_ms / 1000)
    if random.random() < p_fail:
        raise RuntimeError("transient failure (injected)")
    ctx.transactional(
        "INSERT INTO example_ledger (job_id, value) VALUES ($1, $2)", ctx.job_id, value
    )
    return value
