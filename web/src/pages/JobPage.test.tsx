import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { jobById, seedExecutions, seedJob } from "../test/fakeApi";
import { renderAt } from "../test/render";

describe("JobPage", () => {
  it("shows details, the result and every attempt with what went wrong", async () => {
    const job = seedJob({ status: "succeeded", attempts: 2, args: [5], kwargs: { n: 1 }, result: { ok: true } });
    seedExecutions(job.id, [
      { attempt: 1, worker_id: "worker-a", status: "lease_expired", error: "lease expired", started_at: job.created_at, finished_at: job.created_at, duration_ms: 10_000 },
      { attempt: 2, worker_id: "worker-b", status: "succeeded", error: null, started_at: job.created_at, finished_at: job.created_at, duration_ms: 42 },
    ]);
    renderAt(`/jobs/${job.id}`);
    expect(await screen.findByTestId("job-attempts")).toHaveTextContent("2/3");
    expect(screen.getByTestId("job-result")).toHaveTextContent('"ok": true');
    expect(await screen.findByTestId("attempt-1")).toHaveTextContent(/lease expired.*worker-a/s);
    expect(screen.getByText(/stopped heartbeating/)).toBeInTheDocument();
    expect(screen.getByTestId("attempt-2")).toHaveTextContent("42 ms");
    expect(screen.queryByRole("button", { name: /Cancel|Requeue/ })).not.toBeInTheDocument();
  });

  it("cancels a pending job after confirmation", async () => {
    const job = seedJob({ status: "scheduled", attempts: 0 });
    renderAt(`/jobs/${job.id}`);
    await userEvent.click(await screen.findByRole("button", { name: "Cancel job" }));
    await userEvent.click(screen.getByRole("button", { name: "Keep" }));
    expect(jobById(job.id)!.status).toBe("scheduled");
    await userEvent.click(screen.getByRole("button", { name: "Cancel job" }));
    await userEvent.click(screen.getByRole("button", { name: "Confirm cancel" }));
    await vi.waitFor(() => expect(screen.getByTestId("job-status")).toHaveTextContent("cancelled"));
    expect(jobById(job.id)!.status).toBe("cancelled");
  });

  it("shows the API's reason when an action is refused", async () => {
    const job = seedJob({ status: "queued", attempts: 0 });
    renderAt(`/jobs/${job.id}`);
    await userEvent.click(await screen.findByRole("button", { name: "Cancel job" }));
    jobById(job.id)!.status = "running"; // a worker claimed it in the meantime
    await userEvent.click(screen.getByRole("button", { name: "Confirm cancel" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("only pending jobs can be cancelled");
  });

  it("requeues a dead job", async () => {
    const job = seedJob({ status: "dead", last_error: "PermanentError: boom" });
    renderAt(`/jobs/${job.id}`);
    expect(await screen.findByText("PermanentError: boom")).toBeInTheDocument();
    expect(await screen.findByText("No attempts yet.")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Requeue" }));
    await vi.waitFor(() => expect(screen.getByTestId("job-status")).toHaveTextContent("queued"));
  });

  it("says when the job does not exist", async () => {
    renderAt("/jobs/00000000-0000-4000-8000-999999999999");
    expect(await screen.findByText("Job not found")).toBeInTheDocument();
  });
});
