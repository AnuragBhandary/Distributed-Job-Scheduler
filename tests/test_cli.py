"""CLI entry points and the migration runner."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from typing import Any

import asyncpg
import pytest

from jobq import cli
from jobq.auth import parse_key
from jobq.db import migrate, migration_files
from tests.conftest import DATABASE_URL, REDIS_URL

ENV = {"JOBQ_DATABASE_URL": DATABASE_URL, "JOBQ_REDIS_URL": REDIS_URL, "JOBQ_LOG_JSON": "false"}


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch, database: str) -> None:
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)


def test_migrations_are_ordered_and_idempotent(capsys: pytest.CaptureFixture[str]) -> None:
    versions = [v for v, _ in migration_files()]
    assert versions == sorted(versions) and versions[0] == "0001_init"
    assert asyncio.run(migrate(DATABASE_URL)) == []
    assert cli.main(["migrate"]) == 0
    assert "up to date" in capsys.readouterr().out


def test_create_key(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["create-key", "--name", "cli-test", "--rate", "5", "--burst", "10"]) == 0
    raw = capsys.readouterr().out.strip()
    assert parse_key(raw) is not None

    async def stored() -> Any:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            return await conn.fetchrow(
                "SELECT rate_per_s, burst FROM api_keys WHERE name = 'cli-test' ORDER BY id DESC"
            )
        finally:
            await conn.close()

    row = asyncio.run(stored())
    assert (row["rate_per_s"], row["burst"]) == (5.0, 10)


def test_split() -> None:
    assert cli._split(" a, b,,c ") == ["a", "b", "c"]


@pytest.mark.parametrize("command", [["scheduler"], ["worker", "--tasks", "jobq.examples.tasks"]])
def test_long_running_commands_stop_gracefully_on_sigterm(command: list[str]) -> None:
    proc = subprocess.Popen(
        [sys.executable, "-m", "jobq", *command],
        env={**os.environ, **ENV, "JOBQ_METRICS_PORT": "0"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    time.sleep(2.0)
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=20)
    assert proc.returncode == 0, out
    assert "stopped" in out


def test_api_command_serves_requests() -> None:
    import socket
    import urllib.request

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    proc = subprocess.Popen(
        [sys.executable, "-m", "jobq", "api", "--host", "127.0.0.1", "--port", str(port)],
        env={**os.environ, **ENV},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 15
        while True:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz") as resp:
                    assert resp.status == 200
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
    finally:
        proc.terminate()
        proc.wait(timeout=10)
