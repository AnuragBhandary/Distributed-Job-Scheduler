import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { failRequeue, jobById, seedJob } from "../test/fakeApi";
import { renderAt } from "../test/render";

describe("DeadLetterPage", () => {
  it("requeues the selected dead jobs and reports the ones that failed", async () => {
    const dead = Array.from({ length: 5 }, (_, i) => seedJob({ status: "dead", last_error: `error ${i}` }));
    seedJob({ status: "succeeded" });
    failRequeue.add(dead[2]!.id);
    renderAt("/dlq");
    expect(await screen.findAllByTestId("job-row")).toHaveLength(5);
    expect(screen.getByText("error 4")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Select all" }));
    await userEvent.click(screen.getByRole("button", { name: "Requeue selected (5)" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("1 could not be requeued");
    await vi.waitFor(() => expect(screen.getAllByTestId("job-row")).toHaveLength(1)); // only the failure is left
    expect(dead.filter((j) => jobById(j.id)!.status === "queued")).toHaveLength(4);
    expect(screen.getByRole("button", { name: "Requeue selected (1)" })).toBeEnabled(); // still selected
  });

  it("requeues one picked job", async () => {
    const [a] = [seedJob({ status: "dead" }), seedJob({ status: "dead" })];
    renderAt("/dlq");
    await userEvent.click(await screen.findByLabelText(`Select job ${a!.id.slice(0, 8)}`));
    await userEvent.click(screen.getByRole("button", { name: "Requeue selected (1)" }));
    await vi.waitFor(() => expect(jobById(a!.id)!.status).toBe("queued"));
  });

  it("is empty when nothing has died", async () => {
    renderAt("/dlq");
    expect(await screen.findByText(/No dead jobs/)).toBeInTheDocument();
  });
});
