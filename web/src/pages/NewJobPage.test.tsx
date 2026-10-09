import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { requests } from "../test/fakeApi";
import { renderAt } from "../test/render";
import { jobFormSchema, toNewJob } from "./NewJobPage";

const posts = () => requests.filter((r) => r.method === "POST" && r.url.pathname === "/v1/jobs");

describe("NewJobPage", () => {
  it("rejects arguments that are not the right JSON", async () => {
    renderAt("/new");
    const args = await screen.findByLabelText("args (JSON array)");
    await userEvent.clear(args);
    await userEvent.type(args, '{{"not": "an array"}');
    await userEvent.clear(screen.getByLabelText("Queue"));
    await userEvent.click(screen.getByRole("button", { name: "Enqueue job" }));
    expect(await screen.findByText(/Must be a JSON array/)).toBeInTheDocument();
    expect(screen.getByText(/1-64 of letters/)).toBeInTheDocument();
    expect(posts()).toHaveLength(0);
  });

  it("enqueues with an idempotency key and opens the new job", async () => {
    const { router } = renderAt("/new");
    await userEvent.click(await screen.findByRole("button", { name: "Flaky (retries)" }));
    await userEvent.click(screen.getByLabelText("After a delay"));
    await userEvent.click(screen.getByRole("button", { name: "Enqueue job" }));
    await vi.waitFor(() => expect(router.state.location.pathname).toMatch(/^\/jobs\/[0-9a-f-]+$/));
    const [post] = posts();
    expect(post!.body).toEqual({ task: "examples.flaky", queue: "default", args: [], kwargs: { p_fail: 0.7 }, max_attempts: 5, timeout_s: 300, delay_s: 10 });
    expect(post!.headers.get("Idempotency-Key")).toMatch(/^[0-9a-f-]{36}$/);
    expect(await screen.findByTestId("job-status")).toHaveTextContent("scheduled");
  });

  it("shows the API's validation error", async () => {
    renderAt("/new");
    const task = await screen.findByLabelText("Task");
    await userEvent.clear(task);
    await userEvent.type(task, "bad.task");
    await userEvent.click(screen.getByRole("button", { name: "Enqueue job" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("task is not registered");
  });
});

describe("job form schema", () => {
  const base = { task: "t", queue: "q", args: "[]", kwargs: "{}", when: "now", delay_s: "0", run_at: "", max_attempts: "3", timeout_s: "60", idempotency_key: "" };

  it("needs a valid time to schedule at one, and converts it to UTC", () => {
    expect(jobFormSchema.safeParse({ ...base, when: "at" }).success).toBe(false);
    const parsed = jobFormSchema.parse({ ...base, when: "at", run_at: "2026-12-31T23:30" });
    expect(toNewJob(parsed).run_at).toBe(new Date("2026-12-31T23:30").toISOString());
    expect(toNewJob(jobFormSchema.parse(base))).not.toHaveProperty("delay_s");
  });

  it("rejects a kwargs array", () => {
    expect(jobFormSchema.safeParse({ ...base, kwargs: "[1]" }).success).toBe(false);
  });
});
