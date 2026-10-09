import { Link } from "react-router";

import type { Job } from "../api/types";
import { ago, shortId } from "../lib/format";
import { StatusBadge } from "./StatusBadge";

interface Props {
  jobs: Job[];
  selected?: Set<string>;
  onToggle?: (id: string) => void;
  showError?: boolean;
  /** When the list was fetched; "created 5s ago" is relative to it, keeping render pure. */
  now: number;
}

export function JobTable({ jobs, selected, onToggle, showError, now }: Props) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[40rem] text-sm">
        <thead className="text-left text-xs text-slate-500">
          <tr>
            {onToggle && <th className="w-8 pb-2"><span className="sr-only">Select</span></th>}
            <th className="pb-2 font-medium">Job</th>
            <th className="pb-2 font-medium">Task</th>
            <th className="pb-2 font-medium">Queue</th>
            <th className="pb-2 font-medium">Status</th>
            <th className="pb-2 text-right font-medium">Attempts</th>
            {showError ? <th className="pb-2 font-medium">Last error</th> : <th className="pb-2 text-right font-medium">Created</th>}
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.id} className="border-t border-slate-100 dark:border-slate-800" data-testid="job-row">
              {onToggle && (
                <td>
                  <input
                    type="checkbox"
                    aria-label={`Select job ${shortId(job.id)}`}
                    checked={selected?.has(job.id) ?? false}
                    onChange={() => onToggle(job.id)}
                  />
                </td>
              )}
              <td className="py-2"><Link to={`/jobs/${job.id}`} className="font-mono text-xs text-indigo-600 hover:underline dark:text-indigo-400">{shortId(job.id)}</Link></td>
              <td className="font-mono text-xs">{job.task}</td>
              <td>{job.queue}</td>
              <td><StatusBadge status={job.status} /></td>
              <td className="text-right tabular-nums">{job.attempts}/{job.max_attempts}</td>
              {showError ? (
                <td className="max-w-xs truncate text-xs text-red-700 dark:text-red-300" title={job.last_error ?? ""}>{job.last_error}</td>
              ) : (
                <td className="text-right text-xs text-slate-500">{ago(job.created_at, now)}</td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
