import type { Execution } from "../api/types";
import { duration } from "../lib/format";
import { StatusBadge } from "./StatusBadge";

const explain: Partial<Record<Execution["status"], string>> = {
  lease_expired: "The worker stopped heartbeating (crashed or stalled); the scheduler reclaimed the job.",
  fenced: "A newer attempt held the lease, so this worker's late result was rejected.",
  timed_out: "The task ran past its timeout.",
  interrupted: "The worker shut down and released the job for another worker.",
};

export function ExecutionTimeline({ executions }: { executions: Execution[] }) {
  if (executions.length === 0) return <p className="text-sm text-slate-500">No attempts yet.</p>;
  return (
    <ol className="relative space-y-4 border-l border-slate-200 pl-5 dark:border-slate-700">
      {executions.map((e) => (
        <li key={e.attempt} data-testid={`attempt-${e.attempt}`}>
          <span aria-hidden className="absolute -left-1.5 mt-1.5 size-3 rounded-full border-2 border-white bg-slate-400 dark:border-slate-900" />
          <div className="flex flex-wrap items-center gap-2 text-sm">
            <span className="font-medium">Attempt {e.attempt}</span>
            <StatusBadge status={e.status} />
            <span className="text-slate-500">on</span>
            <code className="text-xs">{e.worker_id}</code>
            <span className="text-slate-500">· {duration(e.duration_ms)}</span>
          </div>
          {explain[e.status] && <p className="mt-1 text-xs text-slate-500">{explain[e.status]}</p>}
          {e.error && (
            <pre className="mt-1 overflow-x-auto rounded bg-red-50 p-2 text-xs text-red-800 dark:bg-red-950 dark:text-red-200">{e.error}</pre>
          )}
        </li>
      ))}
    </ol>
  );
}
