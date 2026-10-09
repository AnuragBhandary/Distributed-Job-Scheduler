// Response shapes of the jobq REST API (src/jobq/api/schemas.py).

export const JOB_STATUSES = ["scheduled", "queued", "running", "succeeded", "dead", "cancelled"] as const;
export type JobStatus = (typeof JOB_STATUSES)[number];

export interface Job {
  id: string;
  queue: string;
  task: string;
  args: unknown[];
  kwargs: Record<string, unknown>;
  status: JobStatus;
  idempotency_key: string | null;
  run_at: string;
  attempts: number;
  max_attempts: number;
  timeout_s: number;
  result: unknown;
  last_error: string | null;
  created_at: string;
  updated_at: string;
  finished_at: string | null;
}

export interface JobPage {
  items: Job[];
  next_cursor: string | null;
}

export type ExecutionStatus =
  | "running"
  | "succeeded"
  | "failed"
  | "timed_out"
  | "lease_expired"
  | "fenced"
  | "interrupted";

export interface Execution {
  attempt: number;
  worker_id: string;
  status: ExecutionStatus;
  error: string | null;
  started_at: string;
  finished_at: string | null;
  duration_ms: number | null;
}

export interface Stats {
  queues: { queue: string; counts: Partial<Record<JobStatus, number>> }[];
}

export interface Throughput {
  bucket_s: number;
  buckets: { start: string; succeeded: number; failed: number }[];
}

export interface Worker {
  worker_id: string;
  running: number;
  succeeded: number;
  failed: number;
  last_seen: string;
}

export interface NewJob {
  task: string;
  queue: string;
  args: unknown[];
  kwargs: Record<string, unknown>;
  delay_s?: number;
  run_at?: string;
  max_attempts: number;
  timeout_s: number;
}

export const isTerminal = (status: JobStatus) =>
  status === "succeeded" || status === "dead" || status === "cancelled";
