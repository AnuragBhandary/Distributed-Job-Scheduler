// The dashboard against the real stack: API, scheduler, Redis, PostgreSQL and 3 worker containers.
//
// The main test SIGKILLs a worker while it is running jobs. Those jobs' leases expire, the
// scheduler gives them to other workers, and every job still commits its side effect exactly
// once (one example_ledger row per job, counted in PostgreSQL). The dashboard must show it: the
// overview counts the jobs as succeeded, and a reclaimed job's page shows the lease_expired
// attempt followed by the successful one on another worker.

import { exec } from "node:child_process";
import { resolve } from "node:path";
import { promisify } from "node:util";

import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

const run = promisify(exec);
const REPO = resolve(import.meta.dirname, "..", "..");
const KEY = process.env.E2E_API_KEY ?? "jq_0000000000de_local-dev-key-not-for-production";
const JOBS = Number(process.env.E2E_JOBS ?? 200);
const headers = { "X-API-Key": KEY };

async function signIn(page: Page) {
  await page.goto("/");
  await page.getByLabel("API key").fill(KEY);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
}

async function enqueue(api: APIRequestContext, body: object): Promise<string> {
  const res = await api.post("/v1/jobs", { data: body, headers });
  expect(res.status()).toBe(201);
  return ((await res.json()) as { id: string }).id;
}

async function job(api: APIRequestContext, id: string) {
  return (await (await api.get(`/v1/jobs/${id}`, { headers })).json()) as { status: string; attempts: number };
}

async function sh(cmd: string) {
  return (await run(cmd, { cwd: REPO })).stdout.trim();
}

test("a worker is SIGKILLed mid-run: every job still succeeds exactly once, and the UI shows the takeover", async ({
  page,
  request,
}, testInfo) => {
  await signIn(page);
  const ids = await Promise.all(
    Array.from({ length: JOBS }, (_, i) =>
      enqueue(request, { task: "examples.ledger", args: [i], kwargs: { work_ms: 3000 }, max_attempts: 5 }),
    ),
  );

  // Kill the worker that is running the most of our jobs, as soon as it has some.
  let victim = "";
  await expect
    .poll(async () => {
      const workers = (await (await request.get("/v1/workers", { headers })).json()) as { worker_id: string; running: number }[];
      victim = workers.sort((a, b) => b.running - a.running)[0]?.worker_id ?? "";
      return workers[0]?.running ?? 0;
    }, { timeout: 30_000 })
    .toBeGreaterThan(5);
  const host = victim.split("-")[0]!; // worker ids start with the container's hostname
  const container = await sh(`docker ps --filter "id=${host}" --format "{{.Names}}"`);
  expect(container).toMatch(/worker/);
  await sh(`docker kill -s SIGKILL ${container}`);

  // Wait for every job to finish, then restore the worker.
  await expect
    .poll(async () => (await Promise.all(ids.map((id) => job(request, id)))).filter((j) => j.status === "succeeded").length, {
      timeout: 120_000,
      intervals: [1_000],
    })
    .toBe(JOBS);
  await sh(`docker start ${container}`);

  // Exactly once: one ledger row per job, no more, no fewer.
  const list = ids.map((id) => `'${id}'`).join(",");
  const [rows, distinct] = (
    await sh(`docker compose exec -T postgres psql -U jobq -d jobq -tA -F, -c "SELECT count(*), count(DISTINCT job_id) FROM example_ledger WHERE job_id IN (${list})"`)
  ).split(",").map(Number);
  expect({ rows, distinct }).toEqual({ rows: JOBS, distinct: JOBS });

  // The UI: a reclaimed job shows the lost attempt, then the success on another worker.
  const jobs = await Promise.all(ids.map(async (id) => ({ id, ...(await job(request, id)) })));
  const reclaimed = jobs.filter((j) => j.attempts > 1);
  expect(reclaimed.length).toBeGreaterThan(0);
  await page.goto(`/jobs/${reclaimed[0]!.id}`);
  await expect(page.getByTestId("job-status")).toHaveText("succeeded");
  await expect(page.getByTestId("attempt-1")).toContainText("lease expired");
  await expect(page.getByTestId("attempt-1")).toContainText(victim);
  await expect(page.getByText("The worker stopped heartbeating")).toBeVisible();
  await expect(page.getByTestId(`attempt-${reclaimed[0]!.attempts}`)).toContainText("succeeded");

  // And the overview's throughput chart shows the failed (reclaimed) attempts.
  await page.goto("/");
  await expect.poll(async () => Number(await page.getByTestId("tp-failed").textContent())).toBeGreaterThanOrEqual(reclaimed.length);

  const summary = { jobs: JOBS, killed: container, reclaimed: reclaimed.length, ledgerRows: rows, distinctJobs: distinct };
  await testInfo.attach("summary.json", { body: JSON.stringify(summary, null, 2), contentType: "application/json" });
  console.log(JSON.stringify(summary));
});

test("enqueue from the form, then dead-letter and requeue from the UI", async ({ page, request }) => {
  await signIn(page);

  // Enqueue through the form and land on the job, which runs.
  await page.getByRole("link", { name: "New job" }).click();
  await page.getByRole("button", { name: "Echo" }).click();
  await page.getByLabel("args (JSON array)").fill('["from playwright"]');
  await page.getByRole("button", { name: "Enqueue job" }).click();
  await expect(page).toHaveURL(/\/jobs\/[0-9a-f-]{36}$/);
  await expect(page.getByTestId("job-status")).toHaveText("succeeded");
  await expect(page.getByTestId("job-result")).toContainText("from playwright");

  // Jobs that always fail end up in the dead-letter queue.
  const tag = `e2e-${Date.now()}`;
  const dead = await Promise.all([1, 2, 3].map((i) => enqueue(request, { task: "examples.fail", kwargs: { message: `${tag}-${i}` }, max_attempts: 1 })));
  await expect.poll(async () => (await Promise.all(dead.map((id) => job(request, id)))).every((j) => j.status === "dead")).toBe(true);

  await page.getByRole("link", { name: "Dead letters" }).click();
  for (const id of dead) await page.getByLabel(`Select job ${id.slice(0, 8)}`).check();
  await page.getByRole("button", { name: "Requeue selected (3)" }).click();
  // Requeued with a fresh budget: each runs once more (and, being designed to fail, dies again).
  await expect.poll(async () => (await Promise.all(dead.map((id) => job(request, id)))).map((j) => j.attempts), { timeout: 30_000 }).toEqual([2, 2, 2]);
  await page.goto(`/jobs/${dead[0]}`);
  await expect(page.getByTestId("attempt-2")).toContainText("failed");
  await expect(page.getByTestId("attempt-1")).toContainText(`${tag}-1`); // history kept
});
