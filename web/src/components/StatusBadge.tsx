import type { ExecutionStatus, JobStatus } from "../api/types";

const tones: Record<JobStatus | ExecutionStatus, string> = {
  scheduled: "bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300",
  queued: "bg-sky-100 text-sky-800 dark:bg-sky-950 dark:text-sky-300",
  running: "bg-indigo-100 text-indigo-800 dark:bg-indigo-950 dark:text-indigo-300",
  succeeded: "bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300",
  dead: "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300",
  cancelled: "bg-slate-200 text-slate-700 dark:bg-slate-800 dark:text-slate-300",
  failed: "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300",
  timed_out: "bg-orange-100 text-orange-800 dark:bg-orange-950 dark:text-orange-300",
  lease_expired: "bg-orange-100 text-orange-800 dark:bg-orange-950 dark:text-orange-300",
  fenced: "bg-fuchsia-100 text-fuchsia-800 dark:bg-fuchsia-950 dark:text-fuchsia-300",
  interrupted: "bg-slate-200 text-slate-700 dark:bg-slate-800 dark:text-slate-300",
};

export function StatusBadge({ status }: { status: JobStatus | ExecutionStatus }) {
  return (
    <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${tones[status]}`}>
      {status === "running" && <span aria-hidden className="size-1.5 animate-pulse rounded-full bg-current" />}
      {status.replace("_", " ")}
    </span>
  );
}
