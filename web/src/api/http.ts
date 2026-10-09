// fetch wrapper: absolute URLs on this origin, the API key header, JSON, typed errors.

import { session } from "./session";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  key = session.key(),
  extra: Record<string, string> = {},
): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json", ...extra };
  if (key) headers["X-API-Key"] = key;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const response = await fetch(new URL(path, window.location.origin), {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (response.status === 401 && key === session.key()) {
    session.signOut("Your API key was rejected. It may have been revoked.");
  }
  if (!response.ok) {
    let detail = response.statusText || `HTTP ${response.status}`;
    try {
      const data = (await response.json()) as { detail?: unknown };
      if (typeof data.detail === "string") detail = data.detail;
      else if (Array.isArray(data.detail)) detail = data.detail.map((d: { msg?: string }) => d.msg).join("; ");
    } catch {
      // not JSON
    }
    throw new ApiError(response.status, detail);
  }
  return (await response.json()) as T;
}

export const http = {
  get: <T>(path: string, key?: string) => request<T>("GET", path, undefined, key),
  post: <T>(path: string, body?: unknown, headers?: Record<string, string>) =>
    request<T>("POST", path, body ?? {}, session.key(), headers),
};
