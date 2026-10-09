import type { Throughput } from "../api/types";
import { fillBuckets } from "../lib/format";

const WINDOW_S = 300;

/** Stacked bars per bucket: successful attempts (green) under failed ones (red). */
/** `now` is when the data was fetched, which keeps rendering pure. */
export function ThroughputChart({ data, now }: { data: Throughput; now: number }) {
  const bars = fillBuckets(data.buckets, data.bucket_s, WINDOW_S, now);
  const peak = Math.max(1, ...bars.map((b) => b.succeeded + b.failed));
  const total = bars.reduce((n, b) => n + b.succeeded, 0);
  const failed = bars.reduce((n, b) => n + b.failed, 0);
  const latest = bars.at(-2) ?? bars[0]!; // the last complete bucket
  const w = 600;
  const h = 140;
  const slot = w / bars.length;
  const y = (v: number) => (v / peak) * (h - 4);
  return (
    <figure>
      <div className="mb-3 flex flex-wrap gap-x-6 gap-y-1 text-sm">
        <p>
          <span className="font-semibold tabular-nums" data-testid="tp-succeeded">{total}</span>{" "}
          <span className="text-slate-500">succeeded in 5 min</span>
        </p>
        <p>
          <span className="font-semibold tabular-nums" data-testid="tp-failed">{failed}</span>{" "}
          <span className="text-slate-500">failed attempts</span>
        </p>
        <p>
          <span className="font-semibold tabular-nums">{(latest.succeeded / data.bucket_s).toFixed(1)}</span>{" "}
          <span className="text-slate-500">jobs/s now</span>
        </p>
      </div>
      <svg
        viewBox={`0 0 ${w} ${h}`}
        className="h-36 w-full"
        role="img"
        aria-label={`Attempts per ${data.bucket_s} seconds over the last 5 minutes: ${total} succeeded, ${failed} failed`}
        preserveAspectRatio="none"
      >
        {bars.map((b, i) => (
          <g key={b.start}>
            <rect x={i * slot + 1} width={slot - 2} y={h - y(b.succeeded)} height={y(b.succeeded)} className="fill-emerald-500" />
            <rect
              x={i * slot + 1}
              width={slot - 2}
              y={h - y(b.succeeded) - y(b.failed)}
              height={y(b.failed)}
              className="fill-red-500"
            />
          </g>
        ))}
      </svg>
      <figcaption className="mt-1 flex justify-between text-xs text-slate-500">
        <span>5 min ago</span>
        <span>now</span>
      </figcaption>
    </figure>
  );
}
