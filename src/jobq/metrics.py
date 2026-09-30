"""Prometheus metrics. The API serves them on ``/metrics``; workers and schedulers on
``JOBQ_METRICS_PORT`` when it is set."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

_LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5)
_DELAY_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0)

# API
JOBS_ENQUEUED = Counter("jobq_jobs_enqueued_total", "Jobs accepted by the API", ["queue"])
IDEMPOTENT_REPLAYS = Counter(
    "jobq_idempotent_replays_total", "Enqueue requests answered from an existing idempotency key"
)
ENQUEUE_SECONDS = Histogram(
    "jobq_enqueue_seconds", "Server-side time to accept a job", buckets=_LATENCY_BUCKETS
)
RATE_LIMITED = Counter("jobq_rate_limited_total", "Requests rejected by the rate limiter")
DISPATCH_ERRORS = Counter(
    "jobq_dispatch_errors_total", "Failed Redis dispatches (recovered later by the sweeper)"
)

# worker
JOBS_FINISHED = Counter(
    "jobq_jobs_finished_total",
    "Job attempts by outcome (succeeded, retried, dead, fenced, lease_lost, interrupted)",
    ["queue", "task", "outcome"],
)
JOB_RUN_SECONDS = Histogram(
    "jobq_job_run_seconds", "Handler execution time", ["task"], buckets=_DELAY_BUCKETS
)
QUEUE_DELAY_SECONDS = Histogram(
    "jobq_queue_delay_seconds",
    "Time from run_at until a worker claimed the job",
    ["queue"],
    buckets=_DELAY_BUCKETS,
)
CLAIM_MISSES = Counter(
    "jobq_claim_misses_total", "Stream messages whose job was no longer claimable (duplicates)"
)
LEASES_LOST = Counter("jobq_leases_lost_total", "Running jobs whose lease was lost mid-execution")
INFLIGHT = Gauge("jobq_worker_inflight", "Jobs currently executing on this worker")

# scheduler
PROMOTED = Counter("jobq_promoted_total", "Scheduled jobs moved to the ready stream")
REAPED = Counter("jobq_reaped_total", "Expired leases recovered by the reaper", ["outcome"])
REDISPATCHED = Counter("jobq_redispatched_total", "Queued jobs re-sent after a lost dispatch")
PURGED = Counter("jobq_purged_total", "Finished jobs deleted by retention")
JOBS_BY_STATUS = Gauge("jobq_jobs", "Jobs by queue and status", ["queue", "status"])
