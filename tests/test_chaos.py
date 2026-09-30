"""Crash-recovery tests with real worker *processes*.

* SIGKILL a worker mid-job: its leases expire, the reaper reschedules the jobs, a surviving
  worker finishes them, and every job's side effect is committed exactly once.
* SIGSTOP a worker (a stand-in for a long GC pause or network partition) until its leases are
  taken over, then SIGCONT it: the "zombie" finishes its handlers but every write it attempts is
  fenced off, so still nothing is committed twice.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
from collections.abc import Iterator
from typing import Any

import asyncpg
import httpx
import pytest

from jobq.auth import create_api_key
from jobq.config import Settings
from tests.conftest import DATABASE_URL, REDIS_URL, LiveServer, wait_until
from tests.helpers import running_scheduler

pytestmark = pytest.mark.chaos

WORKER_ENV = {
    "JOBQ_DATABASE_URL": DATABASE_URL,
    "JOBQ_REDIS_URL": REDIS_URL,
    "JOBQ_LEASE_S": "2",
    "JOBQ_HEARTBEAT_INTERVAL_S": "0.5",
    "JOBQ_WORKER_BLOCK_MS": "100",
    "JOBQ_LOG_LEVEL": "WARNING",
}


class Workers:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen[bytes]] = []

    def start(self, concurrency: int = 32) -> subprocess.Popen[bytes]:
        proc = subprocess.Popen(
            [sys.executable, "-m", "jobq", "worker", "--tasks", "jobq.examples.tasks",
             "--concurrency", str(concurrency)],
            env={**os.environ, **WORKER_ENV},
            stdout=subprocess.DEVNULL,
        )  # fmt: skip
        self.procs.append(proc)
        return proc

    def stop_all(self) -> None:
        for proc in self.procs:
            if proc.poll() is None:
                proc.send_signal(signal.SIGCONT)
                proc.kill()
            proc.wait(timeout=10)


@pytest.fixture
def workers() -> Iterator[Workers]:
    group = Workers()
    yield group
    group.stop_all()


async def _submit(server: LiveServer, key: str, n: int, work_ms: int) -> list[str]:
    async with httpx.AsyncClient(base_url=server.url, headers={"X-API-Key": key}) as http:
        responses = await asyncio.gather(
            *[
                http.post(
                    "/v1/jobs",
                    json={
                        "task": "examples.ledger",
                        "args": [i],
                        "kwargs": {"work_ms": work_ms},
                        "max_attempts": 5,
                        "backoff_base_s": 0.1,
                        "backoff_max_s": 0.5,
                    },
                )
                for i in range(n)
            ]
        )
    assert all(r.status_code == 201 for r in responses)
    return [r.json()["id"] for r in responses]


async def _count(pool: asyncpg.Pool, sql: str) -> int:
    return int(await pool.fetchval(sql))


async def _assert_exactly_once(pool: asyncpg.Pool, n: int) -> None:
    assert await _count(pool, "SELECT count(*) FROM jobs WHERE status = 'succeeded'") == n
    assert await _count(pool, "SELECT count(*) FROM example_ledger") == n
    assert await _count(pool, "SELECT count(DISTINCT job_id) FROM example_ledger") == n


async def test_sigkill_worker_mid_job(
    live_server: LiveServer, settings: Settings, pool: asyncpg.Pool, redis: Any, workers: Workers
) -> None:
    _, key = await create_api_key(pool, "chaos", rate_per_s=1000, burst=1000)
    async with running_scheduler(settings, pool, redis):
        victim = workers.start()
        workers.start()
        await _submit(live_server, key, 80, work_ms=1500)

        async def victim_busy() -> bool:
            sql = "SELECT count(*) FROM jobs WHERE status = 'running' AND lease_owner LIKE $1"
            return bool(await pool.fetchval(sql, f"%-{victim.pid}-%"))

        await wait_until(victim_busy, timeout=15)
        victim.kill()  # SIGKILL: no cleanup, no lease release
        workers.start()

        async def all_done() -> bool:
            return await _count(pool, "SELECT count(*) FROM jobs WHERE status = 'succeeded'") == 80

        await wait_until(all_done, timeout=60, interval=0.2)

    await _assert_exactly_once(pool, 80)
    recovered = await _count(pool, "SELECT count(*) FROM executions WHERE status = 'lease_expired'")
    assert recovered >= 1  # the victim's in-flight jobs really were recovered by the reaper


async def test_stalled_zombie_worker_is_fenced(
    live_server: LiveServer, settings: Settings, pool: asyncpg.Pool, redis: Any, workers: Workers
) -> None:
    _, key = await create_api_key(pool, "zombie", rate_per_s=1000, burst=1000)
    async with running_scheduler(settings, pool, redis):
        zombie = workers.start()
        await _submit(live_server, key, 10, work_ms=2500)

        async def zombie_holds_all() -> bool:
            return await _count(pool, "SELECT count(*) FROM jobs WHERE status = 'running'") == 10

        await wait_until(zombie_holds_all, timeout=15)
        zombie.send_signal(signal.SIGSTOP)  # frozen: no heartbeats, handlers paused
        workers.start()

        async def taken_over() -> bool:
            return await _count(pool, "SELECT count(*) FROM jobs WHERE status = 'succeeded'") == 10

        await wait_until(taken_over, timeout=60, interval=0.2)
        zombie.send_signal(signal.SIGCONT)  # wakes up believing it still owns 10 jobs
        await asyncio.sleep(4)  # long enough for its handlers to finish and try to commit

    await _assert_exactly_once(pool, 10)
    zombie_attempts = await pool.fetch(
        "SELECT status::text FROM executions WHERE worker_id LIKE $1", f"%-{zombie.pid}-%"
    )
    assert len(zombie_attempts) == 10
    assert {r[0] for r in zombie_attempts} <= {"lease_expired", "fenced"}
