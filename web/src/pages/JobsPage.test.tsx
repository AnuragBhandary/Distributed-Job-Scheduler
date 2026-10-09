import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { requests, seedJob } from "../test/fakeApi";
import { renderAt } from "../test/render";

describe("JobsPage", () => {
  it("filters through the URL and the API", async () => {
    seedJob({ status: "dead", task: "examples.fail" });
    seedJob({ status: "succeeded", queue: "emails" });
    seedJob({ status: "succeeded" });
    const { router } = renderAt("/jobs?status=succeeded");
    expect(await screen.findAllByTestId("job-row")).toHaveLength(2);

    await userEvent.type(screen.getByLabelText("Queue"), "emails");
    await userEvent.click(screen.getByRole("button", { name: "Apply" }));
    await vi.waitFor(() => expect(screen.getAllByTestId("job-row")).toHaveLength(1));
    expect(router.state.location.search).toBe("?status=succeeded&queue=emails");
    const last = requests.filter((r) => r.url.pathname === "/v1/jobs").at(-1)!;
    expect(Object.fromEntries(last.url.searchParams)).toMatchObject({ status: "succeeded", queue: "emails" });

    await userEvent.click(screen.getByRole("button", { name: "Clear" }));
    await vi.waitFor(() => expect(screen.getAllByTestId("job-row")).toHaveLength(3));
  });

  it("pages with Load more, newest first", async () => {
    for (let i = 0; i < 120; i++) seedJob();
    renderAt("/jobs");
    expect(await screen.findAllByTestId("job-row")).toHaveLength(50);
    await userEvent.click(screen.getByRole("button", { name: "Load more" }));
    await vi.waitFor(() => expect(screen.getAllByTestId("job-row")).toHaveLength(100));
    await userEvent.click(screen.getByRole("button", { name: "Load more" }));
    await vi.waitFor(() => expect(screen.getAllByTestId("job-row")).toHaveLength(120));
    expect(screen.queryByRole("button", { name: "Load more" })).not.toBeInTheDocument();
    const first = within(screen.getAllByTestId("job-row")[0]!).getByRole("link");
    expect(first).toHaveTextContent(/^00000078$/); // the newest (job 120), id shortened
  });

  it("says when nothing matches", async () => {
    renderAt("/jobs?status=dead");
    expect(await screen.findByText("No jobs match these filters.")).toBeInTheDocument();
  });
});
