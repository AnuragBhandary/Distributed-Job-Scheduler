import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterAll, afterEach, beforeAll, beforeEach } from "vitest";

import { session } from "../api/session";
import { resetApi, server } from "./fakeApi";

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
beforeEach(() => resetApi());
afterEach(() => {
  cleanup();
  server.resetHandlers();
  session.signOut();
  sessionStorage.clear();
});
afterAll(() => server.close());
