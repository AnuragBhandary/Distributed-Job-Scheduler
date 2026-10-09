// The API key the dashboard signs in with.
//
// It is kept in sessionStorage, so it survives a reload but not closing the tab, and it is sent
// only as the X-API-Key header to this origin. Anything that can run script on the page could read
// it, which is why the API also rate-limits per key and keys can be revoked (`jobq revoke-key`).

const STORAGE_KEY = "jobq.apiKey";
const listeners = new Set<() => void>();
let current: string | null = read();
let reason: string | null = null;

function read(): string | null {
  try {
    return sessionStorage.getItem(STORAGE_KEY);
  } catch {
    return null; // storage blocked: keep the key in memory only
  }
}

function notify() {
  for (const listener of listeners) listener();
}

export const session = {
  key: () => current,
  /** Why the user was signed out (shown on the sign-in page), if not by choice. */
  reason: () => reason,
  signIn(key: string) {
    current = key;
    reason = null;
    try {
      sessionStorage.setItem(STORAGE_KEY, key);
    } catch {
      // ignore
    }
    notify();
  },
  signOut(why: string | null = null) {
    current = null;
    reason = why;
    try {
      sessionStorage.removeItem(STORAGE_KEY);
    } catch {
      // ignore
    }
    notify();
  },
  subscribe(listener: () => void) {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
};

/** "jq_0000000000de_local…" -> "jq_0000000000de": the public prefix, safe to show. */
export const keyPrefix = (key: string) => key.split("_").slice(0, 2).join("_");
