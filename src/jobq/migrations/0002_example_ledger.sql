-- Demo table written by the `examples.ledger` task through JobContext.transactional().
-- The chaos tests and the benchmark count rows per job here to prove that a job's side effect
-- is committed exactly once, even when workers crash or stall mid-job. Deliberately no UNIQUE
-- constraint on job_id: a duplicate must be visible, not silently rejected.
CREATE TABLE example_ledger (
    id          BIGSERIAL PRIMARY KEY,
    job_id      UUID        NOT NULL,
    value       BIGINT      NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX example_ledger_job_idx ON example_ledger (job_id);
