"""Command line entry point: ``jobq {api,worker,scheduler,migrate,create-key}``."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import signal
import sys
from collections.abc import Sequence

from jobq.config import Settings
from jobq.logs import configure_logging

log = logging.getLogger("jobq")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jobq", description="Distributed task scheduler")
    sub = parser.add_subparsers(dest="command", required=True)

    api = sub.add_parser("api", help="run the REST API")
    api.add_argument("--host")
    api.add_argument("--port", type=int)
    api.add_argument("--workers", type=int, default=1, help="uvicorn worker processes")

    worker = sub.add_parser("worker", help="run a worker")
    worker.add_argument(
        "--tasks", required=True, help="comma-separated modules that register tasks"
    )
    worker.add_argument("--queues", default="default", help="comma-separated queue names")
    worker.add_argument("--concurrency", type=int)

    sub.add_parser("scheduler", help="run the scheduler loops")
    sub.add_parser("migrate", help="apply database migrations")

    key = sub.add_parser("create-key", help="create an API key and print it once")
    key.add_argument("--name", required=True)
    key.add_argument("--rate", type=float, help="sustained requests per second")
    key.add_argument("--burst", type=int, help="bucket size")
    return parser


def _split(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


async def _run_until_signal(component: object) -> None:
    """Run a Worker/Scheduler; SIGINT/SIGTERM trigger its graceful stop."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, component.stop)  # type: ignore[attr-defined]
    await component.run()  # type: ignore[attr-defined]


async def _create_key(settings: Settings, name: str, rate: float, burst: int) -> str:
    from jobq.auth import create_api_key
    from jobq.db import create_pool

    pool = await create_pool(settings)
    try:
        _, raw = await create_api_key(pool, name, rate_per_s=rate, burst=burst)
        return raw
    finally:
        await pool.close()


def _start_metrics(settings: Settings) -> None:
    if settings.metrics_port:
        from prometheus_client import start_http_server

        start_http_server(settings.metrics_port)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = Settings()
    configure_logging(settings.log_level, settings.log_json)

    if args.command == "migrate":
        from jobq.db import migrate

        applied = asyncio.run(migrate(settings.database_url))
        print(f"applied: {', '.join(applied) if applied else 'nothing (up to date)'}")
        return 0

    if args.command == "create-key":
        raw = asyncio.run(
            _create_key(
                settings,
                args.name,
                args.rate or settings.default_rate_per_s,
                args.burst or settings.default_burst,
            )
        )
        print(raw)
        return 0

    if args.command == "api":
        import uvicorn

        uvicorn.run(
            "jobq.api.app:create_app",
            factory=True,
            host=args.host or settings.api_host,
            port=args.port or settings.api_port,
            workers=args.workers,
            access_log=settings.access_log,
            log_config=None,
        )
        return 0

    if args.command == "worker":
        from jobq.worker import Worker

        for module in _split(args.tasks):
            importlib.import_module(module)
        _start_metrics(settings)
        worker = Worker(settings, queues=_split(args.queues), concurrency=args.concurrency)
        asyncio.run(_run_until_signal(worker))
        return 0

    if args.command == "scheduler":
        from jobq.scheduler import Scheduler

        _start_metrics(settings)
        asyncio.run(_run_until_signal(Scheduler(settings)))
        return 0

    raise AssertionError(args.command)  # pragma: no cover


if __name__ == "__main__":
    sys.exit(main())
