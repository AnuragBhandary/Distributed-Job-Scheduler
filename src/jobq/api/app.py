"""FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from jobq import __version__
from jobq.api.deps import AppState
from jobq.api.routes import router
from jobq.auth import Authenticator, create_api_key
from jobq.config import Settings
from jobq.db import create_pool
from jobq.dispatch import create_redis
from jobq.ratelimit import RateLimiter


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = await create_pool(settings)
        redis = create_redis(settings)
        if settings.bootstrap_api_key:
            await create_api_key(
                pool,
                "bootstrap",
                rate_per_s=settings.default_rate_per_s,
                burst=settings.default_burst,
                raw_key=settings.bootstrap_api_key,
            )
        app.state.jobq = AppState(
            settings=settings,
            pool=pool,
            redis=redis,
            authenticator=Authenticator(pool, settings.auth_cache_ttl_s),
            limiter=RateLimiter(redis),
        )
        try:
            yield
        finally:
            await redis.aclose()
            await pool.close()

    app = FastAPI(
        title="jobq",
        version=__version__,
        summary="Distributed task scheduler and job queue",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.get("/healthz", tags=["ops"])
    async def healthz() -> dict[str, str]:
        """Liveness: the process is up."""
        return {"status": "ok"}

    @app.get("/readyz", tags=["ops"])
    async def readyz(request: Request) -> Response:
        """Readiness: PostgreSQL and Redis are reachable."""
        state: AppState = request.app.state.jobq
        checks: dict[str, str] = {}
        try:
            await state.pool.fetchval("SELECT 1")
            checks["postgres"] = "ok"
        except Exception as exc:
            checks["postgres"] = f"error: {exc}"
        try:
            await state.redis.ping()
            checks["redis"] = "ok"
        except Exception as exc:
            checks["redis"] = f"error: {exc}"
        ready = all(v == "ok" for v in checks.values())
        return JSONResponse(checks, status_code=200 if ready else 503)

    @app.get("/metrics", tags=["ops"], include_in_schema=False)
    async def prometheus_metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
