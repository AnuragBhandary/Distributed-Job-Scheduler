"""Runtime configuration, read from ``JOBQ_*`` environment variables (or a ``.env`` file)."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="JOBQ_", env_file=".env", extra="ignore")

    # --- storage -----------------------------------------------------------------------------
    database_url: str = "postgresql://jobq:jobq@localhost:5432/jobq"
    redis_url: str = "redis://localhost:6379/0"
    db_pool_min: int = 2
    db_pool_max: int = 20

    # --- API ---------------------------------------------------------------------------------
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    access_log: bool = False
    max_payload_bytes: int = 256 * 1024
    auth_cache_ttl_s: float = 30.0
    default_rate_per_s: float = 100.0
    default_burst: int = 200
    # Optional fixed key created at API startup (local development / docker compose only).
    bootstrap_api_key: str | None = None

    # --- worker ------------------------------------------------------------------------------
    lease_s: float = 30.0
    heartbeat_interval_s: float = 10.0
    worker_concurrency: int = 16
    worker_block_ms: int = 1000
    shutdown_grace_s: float = 25.0

    # --- scheduler ---------------------------------------------------------------------------
    promote_interval_s: float = 0.25
    reap_interval_s: float = 1.0
    sweep_interval_s: float = 5.0
    # A queued job older than this whose stream message has already been delivered is
    # considered lost (worker died between read and claim, or Redis lost the message).
    sweep_after_s: float = 30.0
    # Tolerated clock difference between PostgreSQL now() and Redis stream ids.
    clock_skew_margin_ms: int = 2000
    purge_interval_s: float = 60.0
    retention_days: float = 7.0
    gauge_interval_s: float = 10.0
    batch_size: int = 500
    stream_maxlen: int = 1_000_000

    # --- observability -----------------------------------------------------------------------
    metrics_port: int | None = None
    log_level: str = "INFO"
    log_json: bool = True
