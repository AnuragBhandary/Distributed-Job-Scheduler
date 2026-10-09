import { screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { seedJob, seedThroughput, seedWorkers } from "../test/fakeApi";
import { renderAt } from "../test/render";

describe("OverviewPage", () => {
  it("totals jobs by status, per queue, with workers and throughput", async () => {
    seedJob({ status: "succeeded" });
    seedJob({ status: "succeeded", queue: "emails" });
    seedJob({ status: "dead", queue: "emails" });
    seedJob({ status: "running" });
    const now = Date.now();
    seedWorkers([
      { worker_id: "worker-a", running: 3, succeeded: 40, failed: 1, last_seen: new Date(now - 2_000).toISOString() },
      { worker_id: "worker-b", running: 0, succeeded: 7, failed: 0, last_seen: new Date(now - 120_000).toISOString() },
    ]);
    const bucket = new Date(Math.floor((now - 20_000) / 10_000) * 10_000).toISOString();
    seedThroughput([{ start: bucket, succeeded: 12, failed: 3 }]);

    renderAt("/");
    await vi.waitFor(() => expect(screen.getByTestId("total-succeeded")).toHaveTextContent("2"));
    expect(screen.getByTestId("total-dead")).toHaveTextContent("1");
    expect(screen.getByTestId("total-running")).toHaveTextContent("1");

    const queues = screen.getByRole("region", { name: "Queues" });
    expect(within(queues).getByRole("link", { name: "emails" })).toHaveAttribute("href", "/jobs?queue=emails");

    const rows = await screen.findAllByTestId("worker-row");
    expect(rows).toHaveLength(2);
    expect(within(rows[1]!).getByText("2m ago")).toHaveClass("text-amber-600"); // gone quiet

    expect(await screen.findByTestId("tp-succeeded")).toHaveTextContent("12");
    expect(screen.getByTestId("tp-failed")).toHaveTextContent("3");
    expect(screen.getByRole("img", { name: /12 succeeded, 3 failed/ })).toBeInTheDocument();
  });

  it("invites you to enqueue when there are no jobs", async () => {
    renderAt("/");
    expect(await screen.findByText(/No jobs yet/)).toBeInTheDocument();
    expect(await screen.findByText("No worker has run your jobs recently.")).toBeInTheDocument();
  });
});
