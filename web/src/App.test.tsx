import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import { App } from "./App";
import { session } from "./api/session";
import { GOOD_KEY } from "./test/fakeApi";

describe("signing in and out", () => {
  it("validates the key's shape before calling the API", async () => {
    render(<App />);
    await userEvent.type(screen.getByLabelText("API key"), "not-a-key");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("jq_<12 hex characters>");
  });

  it("rejects a key the API refuses", async () => {
    render(<App />);
    await userEvent.type(screen.getByLabelText("API key"), "jq_0123456789ab_wrong");
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("That key was rejected.");
    expect(session.key()).toBeNull();
  });

  it("signs in, shows the dashboard, and signs out", async () => {
    render(<App />);
    await userEvent.type(screen.getByLabelText("API key"), GOOD_KEY);
    await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByRole("heading", { name: "Overview" })).toBeInTheDocument();
    expect(screen.getByText("jq_0123456789ab")).toBeInTheDocument(); // prefix only, never the secret
    await userEvent.click(screen.getByRole("button", { name: "Sign out" }));
    expect(await screen.findByRole("button", { name: "Sign in" })).toBeInTheDocument();
  });

  it("explains why it signed you out", () => {
    session.signOut("Your API key was rejected. It may have been revoked.");
    render(<App />);
    expect(screen.getByRole("status")).toHaveTextContent("revoked");
  });
});
