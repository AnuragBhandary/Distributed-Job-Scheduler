-- Indexes for the dashboard's recent-activity queries (workers seen and throughput over the last
-- few minutes). Both read only recent or running executions, so they stay index range scans as
-- the history grows.
CREATE INDEX executions_started_idx ON executions (started_at);
CREATE INDEX executions_running_idx ON executions (worker_id) WHERE status = 'running';
