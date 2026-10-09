// Display helpers.

export function ago(iso: string, now: number): string {
  const s = Math.max(0, Math.round((now - Date.parse(iso)) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

export function duration(ms: number | null): string {
  if (ms === null) return "—";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
}

export const shortId = (id: string) => id.slice(0, 8);

export const json = (value: unknown) => JSON.stringify(value, null, 2);

/** One bar per bucket for the whole window, oldest first; missing buckets are zero. */
export function fillBuckets(
  buckets: { start: string; succeeded: number; failed: number }[],
  bucketS: number,
  windowS: number,
  now: number,
): { start: number; succeeded: number; failed: number }[] {
  const size = bucketS * 1000;
  const last = Math.floor(now / size) * size;
  const byStart = new Map(buckets.map((b) => [Math.floor(Date.parse(b.start) / size) * size, b]));
  const out = [];
  for (let t = last - (Math.ceil(windowS / bucketS) - 1) * size; t <= last; t += size) {
    const b = byStart.get(t);
    out.push({ start: t, succeeded: b?.succeeded ?? 0, failed: b?.failed ?? 0 });
  }
  return out;
}
