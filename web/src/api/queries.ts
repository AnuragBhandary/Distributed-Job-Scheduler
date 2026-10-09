// TanStack Query hooks: polling for live views, mutations that refresh what they change.

import {
  keepPreviousData,
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";

import { http } from "./http";
import { isTerminal, type Execution, type Job, type JobPage, type JobStatus, type NewJob, type Stats, type Throughput, type Worker } from "./types";

export const POLL_MS = 2_000;

export interface JobFilters {
  status?: JobStatus;
  queue?: string;
  task?: string;
}

function query(params: Record<string, string | number | undefined>) {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== "") q.set(k, String(v));
  const s = q.toString();
  return s ? `?${s}` : "";
}

export const useStats = () =>
  useQuery({ queryKey: ["stats"], queryFn: () => http.get<Stats>("/v1/stats"), refetchInterval: POLL_MS });

export const useThroughput = () =>
  useQuery({
    queryKey: ["throughput"],
    queryFn: () => http.get<Throughput>("/v1/stats/throughput?window_s=300&bucket_s=10"),
    refetchInterval: POLL_MS,
  });

export const useWorkers = () =>
  useQuery({ queryKey: ["workers"], queryFn: () => http.get<Worker[]>("/v1/workers"), refetchInterval: POLL_MS });

/** Newest first, keyset-paginated ("Load more"); every loaded page is refreshed while polling. */
export function useJobs(filters: JobFilters, pageSize = 50) {
  return useInfiniteQuery({
    queryKey: ["jobs", filters, pageSize],
    queryFn: ({ pageParam }) =>
      http.get<JobPage>(`/v1/jobs${query({ ...filters, limit: pageSize, cursor: pageParam ?? undefined })}`),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.next_cursor,
    refetchInterval: POLL_MS,
    placeholderData: keepPreviousData,
  });
}

export function useDeadLetters(pageSize = 100) {
  return useJobs({ status: "dead" }, pageSize);
}

export function useJob(id: string) {
  return useQuery({
    queryKey: ["job", id],
    queryFn: () => http.get<Job>(`/v1/jobs/${id}`),
    // poll until the job reaches a terminal state
    refetchInterval: (q) => (q.state.data && isTerminal(q.state.data.status) ? false : 1_000),
    retry: false,
  });
}

export function useExecutions(id: string, live: boolean) {
  return useQuery({
    queryKey: ["executions", id],
    queryFn: () => http.get<Execution[]>(`/v1/jobs/${id}/executions`),
    refetchInterval: live ? 1_000 : false,
  });
}

function useJobMutation(action: (id: string) => Promise<Job>) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: action,
    onSuccess: (job) => {
      client.setQueryData(["job", job.id], job);
      void client.invalidateQueries({ queryKey: ["jobs"] });
      void client.invalidateQueries({ queryKey: ["executions", job.id] });
      void client.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export const useCancel = () => useJobMutation((id) => http.post<Job>(`/v1/jobs/${id}/cancel`));
export const useRequeue = () => useJobMutation((id) => http.post<Job>(`/v1/dlq/${id}/requeue`));

export function useEnqueue() {
  const client = useQueryClient();
  return useMutation({
    // The Idempotency-Key makes a double submit or a retry return the same job, not a second one.
    mutationFn: ({ job, idempotencyKey }: { job: NewJob; idempotencyKey?: string }) =>
      http.post<Job>("/v1/jobs", job, idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {}),
    onSuccess: (job) => {
      client.setQueryData(["job", job.id], job);
      void client.invalidateQueries({ queryKey: ["jobs"] });
    },
  });
}

/** Requeue many dead jobs, a few at a time; reports progress and collects failures. */
export async function requeueMany(
  ids: string[],
  onProgress: (done: number) => void,
  concurrency = 4,
): Promise<{ id: string; error: string }[]> {
  const failures: { id: string; error: string }[] = [];
  let next = 0;
  let done = 0;
  async function lane() {
    while (next < ids.length) {
      const id = ids[next++]!;
      try {
        await http.post<Job>(`/v1/dlq/${id}/requeue`);
      } catch (error) {
        failures.push({ id, error: (error as Error).message });
      }
      onProgress(++done);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, ids.length) }, lane));
  return failures;
}
