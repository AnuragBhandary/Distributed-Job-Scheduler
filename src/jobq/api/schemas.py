"""Request/response models for the REST API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, Field, model_validator

JobStatus = Literal["scheduled", "queued", "running", "succeeded", "dead", "cancelled"]

QUEUE_PATTERN = r"^[A-Za-z0-9_.:-]{1,64}$"
TASK_PATTERN = r"^[A-Za-z_][A-Za-z0-9_.:-]{0,199}$"


class JobCreate(BaseModel):
    task: str = Field(pattern=TASK_PATTERN, description="Registered task name")
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", pattern=QUEUE_PATTERN)
    run_at: AwareDatetime | None = Field(default=None, description="Run at (or after) this time")
    delay_s: float | None = Field(default=None, ge=0, le=365 * 86400, description="Run after N s")
    max_attempts: int = Field(default=3, ge=1, le=100)
    timeout_s: float = Field(default=300.0, gt=0, le=86400)
    backoff_base_s: float = Field(default=1.0, ge=0, le=3600)
    backoff_max_s: float = Field(default=300.0, ge=0, le=86400)

    @model_validator(mode="after")
    def _one_schedule(self) -> JobCreate:
        if self.run_at is not None and self.delay_s is not None:
            raise ValueError("set at most one of run_at and delay_s")
        return self


class JobOut(BaseModel):
    id: UUID
    queue: str
    task: str
    args: list[Any]
    kwargs: dict[str, Any]
    status: JobStatus
    idempotency_key: str | None
    run_at: datetime
    attempts: int
    max_attempts: int
    timeout_s: float
    result: Any | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None


class JobPage(BaseModel):
    items: list[JobOut]
    next_cursor: str | None


class ExecutionOut(BaseModel):
    attempt: int
    worker_id: str
    status: Literal[
        "running", "succeeded", "failed", "timed_out", "lease_expired", "fenced", "interrupted"
    ]
    error: str | None
    started_at: datetime
    finished_at: datetime | None
    duration_ms: float | None


class QueueStats(BaseModel):
    queue: str
    counts: dict[str, int]


class Stats(BaseModel):
    queues: list[QueueStats]
