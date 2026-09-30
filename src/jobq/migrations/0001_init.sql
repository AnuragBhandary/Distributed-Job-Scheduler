-- jobq schema. PostgreSQL is the source of truth for every job; Redis only carries dispatch
-- hints (stream messages) and rate-limit buckets, so it can be lost without losing jobs.

CREATE TABLE api_keys (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT             NOT NULL,
    prefix        TEXT             NOT NULL UNIQUE,  -- public, used for lookup
    secret_hash   BYTEA            NOT NULL,         -- sha256(secret); secret never stored
    rate_per_s    DOUBLE PRECISION NOT NULL,
    burst         INTEGER          NOT NULL,
    created_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    revoked_at    TIMESTAMPTZ
);

-- scheduled: waiting for run_at (first run delayed, or retry backoff)
-- queued:    due and dispatched to the Redis stream, waiting for a worker
-- running:   leased by a worker; lease_token fences its writes
-- succeeded / dead / cancelled: terminal (dead = dead-letter queue)
CREATE TYPE job_status AS ENUM ('scheduled', 'queued', 'running', 'succeeded', 'dead', 'cancelled');

CREATE TABLE jobs (
    id                UUID PRIMARY KEY,
    api_key_id        BIGINT           NOT NULL REFERENCES api_keys (id),
    queue             TEXT             NOT NULL,
    task              TEXT             NOT NULL,
    args              JSONB            NOT NULL DEFAULT '[]',
    kwargs            JSONB            NOT NULL DEFAULT '{}',
    status            job_status       NOT NULL,
    idempotency_key   TEXT,
    request_hash      BYTEA,
    run_at            TIMESTAMPTZ      NOT NULL,
    attempts          INTEGER          NOT NULL DEFAULT 0,
    attempt_offset    INTEGER          NOT NULL DEFAULT 0,
    max_attempts      INTEGER          NOT NULL CHECK (max_attempts > 0),
    timeout_s         DOUBLE PRECISION NOT NULL CHECK (timeout_s > 0),
    backoff_base_s    DOUBLE PRECISION NOT NULL,
    backoff_max_s     DOUBLE PRECISION NOT NULL,
    lease_token       UUID,
    lease_owner       TEXT,
    lease_expires_at  TIMESTAMPTZ,
    dispatched_at     TIMESTAMPTZ,
    result            JSONB,
    last_error        TEXT,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    CONSTRAINT jobs_idempotency UNIQUE (api_key_id, idempotency_key)
);

-- Partial indexes: each background loop scans only the rows in the state it cares about, so
-- the indexes stay small no matter how many finished jobs the table holds.
CREATE INDEX jobs_due_idx      ON jobs (run_at)                     WHERE status = 'scheduled';
CREATE INDEX jobs_lease_idx    ON jobs (lease_expires_at)           WHERE status = 'running';
CREATE INDEX jobs_dispatch_idx ON jobs (dispatched_at)              WHERE status = 'queued';
CREATE INDEX jobs_finished_idx ON jobs (finished_at)                WHERE status IN ('succeeded', 'cancelled');
CREATE INDEX jobs_owner_idx    ON jobs (api_key_id, created_at DESC, id DESC);

-- A queue table is update-heavy: vacuum it more eagerly than the default 20% dead tuples.
ALTER TABLE jobs SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.05);

CREATE TYPE execution_status AS ENUM (
    'running', 'succeeded', 'failed', 'timed_out', 'lease_expired', 'fenced', 'interrupted'
);

-- One row per attempt: the job's execution history.
CREATE TABLE executions (
    id           BIGSERIAL PRIMARY KEY,
    job_id       UUID             NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt      INTEGER          NOT NULL,
    worker_id    TEXT             NOT NULL,
    lease_token  UUID             NOT NULL,
    status       execution_status NOT NULL,
    error        TEXT,
    started_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    CONSTRAINT executions_attempt UNIQUE (job_id, attempt)
);
