"""Shared API state plus the authentication and rate-limiting dependencies."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import asyncpg
import redis.asyncio as aioredis
from fastapi import Depends, Header, HTTPException, Request, Response, status
from redis.exceptions import RedisError

from jobq import metrics
from jobq.auth import Authenticator, Principal
from jobq.config import Settings
from jobq.ratelimit import RateLimiter

log = logging.getLogger("jobq.api")


@dataclass
class AppState:
    settings: Settings
    pool: asyncpg.Pool
    redis: aioredis.Redis
    authenticator: Authenticator
    limiter: RateLimiter


def get_state(request: Request) -> AppState:
    state: AppState = request.app.state.jobq
    return state


async def authenticate(
    state: AppState = Depends(get_state),
    x_api_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> Principal:
    raw = x_api_key or None
    if raw is None and authorization and authorization.lower().startswith("bearer "):
        raw = authorization[7:].strip()
    principal = await state.authenticator.authenticate(raw) if raw else None
    if principal is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return principal


async def rate_limited(
    response: Response,
    state: AppState = Depends(get_state),
    principal: Principal = Depends(authenticate),
) -> Principal:
    try:
        decision = await state.limiter.acquire(principal.id, principal.rate_per_s, principal.burst)
    except RedisError:
        # Fail open: an outage of the rate limiter must not take job submission down with it.
        log.warning("rate limiter unavailable; allowing request", exc_info=True)
        return principal
    response.headers["X-RateLimit-Limit"] = str(principal.burst)
    response.headers["X-RateLimit-Remaining"] = str(math.floor(decision.remaining))
    if not decision.allowed:
        metrics.RATE_LIMITED.inc()
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            detail="rate limit exceeded",
            headers={"Retry-After": str(max(1, math.ceil(decision.retry_after_s)))},
        )
    return principal
