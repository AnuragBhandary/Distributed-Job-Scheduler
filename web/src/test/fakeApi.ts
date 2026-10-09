// An in-memory stand-in for the jobq REST API, served to the app through MSW. Tests seed jobs,
// then drive the UI; the handlers apply the same rules as the real API (auth, filters, keyset
// pages, cancel only pending jobs, requeue only dead ones, idempotency keys).

import { http, HttpResponse, type HttpResponseResolver } from "msw";
import { setupServer } from "msw/node";

import type { Execution, Job, JobStatus, Worker } from "../api/types";

export const GOOD_KEY = "jq_0123456789ab_secret";

let jobs: Job[] = [];
let executions = new Map<string, Execution[]>();
let workers: Worker[] = [];
let throughputBuckets: { start: string; succeeded: number; failed: number }[] = [];
let n = 0;
export const requests: { method: string; url: URL; headers: Headers; body: unknown }[] = [];
export const failRequeue = new Set<string>();

export function resetApi() {
  jobs = [];
  executions = new Map();
  workers = [];
  throughputBuckets = [];
  n = 0;
  requests.length = 0;
  failRequeue.clear();
}

export function seedJob(overrides: Partial<Job> = {}): Job {
  n++;
  const created = new Date(Date.UTC(2026, 9, 9, 12, 0, n)).toISOString();
  const job: Job = {
    id: `${n.toString(16).padStart(8, "0")}-0000-4000-8000-${String(n).padStart(12, "0")}`,
    queue: "default",
    task: "examples.echo",
    args: [],
    kwargs: {},
    status: "succeeded",
    idempotency_key: null,
    run_at: created,
    attempts: 1,
    max_attempts: 3,
    timeout_s: 300,
    result: null,
    last_error: null,
    created_at: created,
    updated_at: created,
    finished_at: null,
    ...overrides,
  };
  jobs.push(job);
  return job;
}

export const seedExecutions = (id: string, list: Execution[]) => executions.set(id, list);
export const seedWorkers = (list: Worker[]) => (workers = list);
export const seedThroughput = (list: typeof throughputBuckets) => (throughputBuckets = list);
export const jobById = (id: string) => jobs.find((j) => j.id === id);

const unauthorized = () => HttpResponse.json({ detail: "invalid or missing API key" }, { status: 401 });

function authed(handler: HttpResponseResolver): HttpResponseResolver {
  return async (info) => {
    const { request } = info;
    let body: unknown = undefined;
    if (request.method === "POST") body = await request.clone().json().catch(() => undefined);
    requests.push({ method: request.method, url: new URL(request.url), headers: request.headers, body });
    if (request.headers.get("X-API-Key") !== GOOD_KEY) return unauthorized();
    return handler(info);
  };
}

const newestFirst = (a: Job, b: Job) => b.created_at.localeCompare(a.created_at) || b.id.localeCompare(a.id);

export const handlers = [
  http.get("*/v1/stats", authed(() => {
    const queues = new Map<string, Partial<Record<JobStatus, number>>>();
    for (const j of jobs) {
      const counts = queues.get(j.queue) ?? {};
      counts[j.status] = (counts[j.status] ?? 0) + 1;
      queues.set(j.queue, counts);
    }
    return HttpResponse.json({ queues: [...queues].map(([queue, counts]) => ({ queue, counts })) });
  })),
  http.get("*/v1/stats/throughput", authed(({ request }) => {
    const bucket_s = Number(new URL(request.url).searchParams.get("bucket_s") ?? 10);
    return HttpResponse.json({ bucket_s, buckets: throughputBuckets });
  })),
  http.get("*/v1/workers", authed(() => HttpResponse.json(workers))),
  http.get("*/v1/jobs", authed(({ request }) => {
    const q = new URL(request.url).searchParams;
    const limit = Number(q.get("limit") ?? 50);
    const cursor = q.get("cursor");
    let list = jobs
      .filter((j) => (!q.get("status") || j.status === q.get("status")) && (!q.get("queue") || j.queue === q.get("queue")) && (!q.get("task") || j.task === q.get("task")))
      .sort(newestFirst);
    if (cursor) list = list.slice(list.findIndex((j) => j.id === cursor) + 1);
    const items = list.slice(0, limit);
    return HttpResponse.json({ items, next_cursor: items.length === limit && list.length > limit ? items.at(-1)!.id : null });
  })),
  http.get("*/v1/jobs/:id", authed(({ params }) => {
    const job = jobById(String(params.id));
    return job ? HttpResponse.json(job) : HttpResponse.json({ detail: "job not found" }, { status: 404 });
  })),
  http.get("*/v1/jobs/:id/executions", authed(({ params }) => HttpResponse.json(executions.get(String(params.id)) ?? []))),
  http.post("*/v1/jobs", authed(async ({ request }) => {
    const key = request.headers.get("Idempotency-Key");
    const existing = key ? jobs.find((j) => j.idempotency_key === key) : undefined;
    if (existing) return HttpResponse.json(existing, { status: 200 });
    const body = (await request.json()) as Partial<Job> & { delay_s?: number };
    if (body.task === "bad.task") return HttpResponse.json({ detail: [{ msg: "task is not registered" }] }, { status: 422 });
    const job = seedJob({ ...body, status: body.delay_s ? "scheduled" : "queued", attempts: 0, idempotency_key: key });
    return HttpResponse.json(job, { status: 201 });
  })),
  http.post("*/v1/jobs/:id/cancel", authed(({ params }) => {
    const job = jobById(String(params.id));
    if (!job) return HttpResponse.json({ detail: "job not found" }, { status: 404 });
    if (job.status !== "scheduled" && job.status !== "queued")
      return HttpResponse.json({ detail: `job is ${job.status}; only pending jobs can be cancelled` }, { status: 409 });
    job.status = "cancelled";
    return HttpResponse.json(job);
  })),
  http.post("*/v1/dlq/:id/requeue", authed(({ params }) => {
    const job = jobById(String(params.id));
    if (!job || failRequeue.has(job.id)) return HttpResponse.json({ detail: "job is running; only dead jobs can be requeued" }, { status: 409 });
    if (job.status !== "dead") return HttpResponse.json({ detail: `job is ${job.status}; only dead jobs can be requeued` }, { status: 409 });
    job.status = "queued";
    return HttpResponse.json(job);
  })),
];

export const server = setupServer(...handlers);
