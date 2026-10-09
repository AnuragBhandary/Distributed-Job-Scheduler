import { http as mswHttp, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";

import { GOOD_KEY, server } from "../test/fakeApi";
import { ApiError, http } from "./http";
import { keyPrefix, session } from "./session";

describe("http + session", () => {
  it("sends the signed-in key and parses JSON", async () => {
    session.signIn(GOOD_KEY);
    expect(sessionStorage.getItem("jobq.apiKey")).toBe(GOOD_KEY);
    await expect(http.get("/v1/workers")).resolves.toEqual([]);
  });

  it("signs out with a reason when the key is rejected", async () => {
    session.signIn("jq_0123456789ab_revoked");
    await expect(http.get("/v1/stats")).rejects.toMatchObject({ status: 401 });
    expect(session.key()).toBeNull();
    expect(session.reason()).toMatch(/rejected/);
    expect(sessionStorage.getItem("jobq.apiKey")).toBeNull();
  });

  it("does not sign out when checking a different key fails", async () => {
    session.signIn(GOOD_KEY);
    await expect(http.get("/v1/stats", "jq_0123456789ab_other")).rejects.toBeInstanceOf(ApiError);
    expect(session.key()).toBe(GOOD_KEY);
  });

  it("turns FastAPI error details into readable messages", async () => {
    session.signIn(GOOD_KEY);
    await expect(http.post("/v1/jobs", { task: "bad.task" })).rejects.toThrow("task is not registered");
    await expect(http.post("/v1/jobs/nope/cancel")).rejects.toThrow("job not found");
    server.use(mswHttp.get("*/v1/workers", () => new HttpResponse("upstream down", { status: 502 })));
    await expect(http.get("/v1/workers")).rejects.toMatchObject({ status: 502 });
  });

  it("shows only the public part of a key", () => {
    expect(keyPrefix("jq_0000000000de_local-dev-key")).toBe("jq_0000000000de");
  });
});
