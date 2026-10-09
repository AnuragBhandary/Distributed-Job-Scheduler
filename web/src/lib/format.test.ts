import { describe, expect, it } from "vitest";

import { ago, duration, fillBuckets, shortId } from "./format";

const T = Date.parse("2026-10-09T12:00:00Z");

describe("format", () => {
  it.each([
    [5, "5s ago"],
    [125, "2m ago"],
    [7_300, "2h ago"],
    [200_000, "2d ago"],
    [-3, "0s ago"], // clock skew never shows "in the future"
  ])("ago(%i s) = %s", (s, text) => {
    expect(ago(new Date(T - s * 1000).toISOString(), T)).toBe(text);
  });

  it.each([
    [null, "—"],
    [12.4, "12 ms"],
    [1_540, "1.5 s"],
    [125_000, "2m 5s"],
  ])("duration(%s) = %s", (ms, text) => expect(duration(ms)).toBe(text));

  it("shortens ids", () => expect(shortId("0123456789abcdef")).toBe("01234567"));

  it("fills a full window of buckets, oldest first, zero where nothing ran", () => {
    const bars = fillBuckets(
      [
        { start: "2026-10-09T11:59:50Z", succeeded: 4, failed: 1 },
        { start: "2026-10-09T11:59:20Z", succeeded: 2, failed: 0 },
        { start: "2026-10-09T11:50:00Z", succeeded: 9, failed: 9 }, // outside the window
      ],
      10,
      60,
      T + 3_000,
    );
    expect(bars).toHaveLength(6);
    expect(bars[0]!.start).toBe(Date.parse("2026-10-09T11:59:10Z"));
    expect(bars.map((b) => [b.succeeded, b.failed])).toEqual([[0, 0], [2, 0], [0, 0], [0, 0], [4, 1], [0, 0]]);
  });
});
