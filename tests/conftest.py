"""Fixtures. Tests run against real PostgreSQL and Redis (docker compose up -d postgres redis),
because the guarantees under test (row locks, SKIP LOCKED, Lua atomicity, stream semantics)
cannot be faked meaningfully."""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

import asyncpg
import httpx
import pytest
import uvicorn
from asgi_lifespan import LifespanManager

from jobq.api.app import create_app
from jobq.auth import create_api_key
from jobq.config import Settings
from jobq.db import create_pool, migrate
from jobq.dispatch import create_redis

DATABASE_URL = os.environ.get(
    "JOBQ_TEST_DATABASE_URL", "postgresql://jobq:jobq@localhost:5432/jobq_test"
)
REDIS_URL = os.environ.get("JOBQ_TEST_REDIS_URL", "redis://localhost:6379/15")


def make_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_url": DATABASE_URL,
        "redis_url": REDIS_URL,
        "db_pool_min": 1,
        "db_pool_max": 10,
        "lease_s": 2.0,
        "heartbeat_interval_s": 0.3,
        "worker_block_ms": 50,
        "worker_concurrency": 8,
        "shutdown_grace_s": 2.0,
        "promote_interval_s": 0.05,
        "reap_interval_s": 0.1,
        "sweep_interval_s": 0.2,
        "sweep_after_s": 0.5,
        "clock_skew_margin_ms": 100,
        "auth_cache_ttl_s": 0.0,
        "log_json": False,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


async def _prepare_database() -> None:
    base, _, name = DATABASE_URL.rpartition("/")
    admin = await asyncpg.connect(f"{base}/postgres")
    try:
        if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", name):
            await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    await migrate(DATABASE_URL)


@pytest.fixture(scope="session")
def database() -> str:
    asyncio.run(_prepare_database())
    return DATABASE_URL


async def _reset(pool: asyncpg.Pool) -> None:
    await pool.execute(
        "TRUNCATE jobs, executions, api_keys, example_ledger RESTART IDENTITY CASCADE"
    )


@pytest.fixture
def settings(database: str) -> Settings:
    return make_settings()


@pytest.fixture
async def pool(settings: Settings) -> AsyncIterator[asyncpg.Pool]:
    pool = await create_pool(settings)
    await _reset(pool)
    yield pool
    await pool.close()


@pytest.fixture
async def redis(settings: Settings) -> AsyncIterator[Any]:
    client = create_redis(settings)
    await client.flushdb()
    yield client
    await client.aclose()


@pytest.fixture
async def api_key(pool: asyncpg.Pool) -> tuple[int, str]:
    return await create_api_key(pool, "test", rate_per_s=1000, burst=1000)


@pytest.fixture
async def client(
    settings: Settings, pool: asyncpg.Pool, redis: Any, api_key: tuple[int, str]
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    async with (
        LifespanManager(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": api_key[1]},
        ) as http,
    ):
        yield http


async def wait_until(
    predicate: Callable[[], Awaitable[bool]], timeout: float = 10.0, interval: float = 0.05
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


async def job_status(pool: asyncpg.Pool, job_id: Any) -> str | None:
    value = await pool.fetchval("SELECT status::text FROM jobs WHERE id = $1", job_id)
    return str(value) if value is not None else None


class LiveServer:
    """Runs the real API under uvicorn in a background thread (for the sync client SDK and the
    multi-process chaos tests)."""

    def __init__(self, settings: Settings) -> None:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        config = uvicorn.Config(
            create_app(settings), host="127.0.0.1", port=self.port, log_level="warning"
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.url = f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> LiveServer:
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("live server did not start")
            time.sleep(0.02)
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


@pytest.fixture(scope="module")
def live_server(database: str) -> Iterator[LiveServer]:
    with LiveServer(make_settings()) as server:
        yield server
