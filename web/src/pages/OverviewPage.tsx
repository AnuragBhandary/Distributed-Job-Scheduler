import { Link } from "react-router";

import { useStats, useThroughput, useWorkers } from "../api/queries";
import { JOB_STATUSES, type JobStatus } from "../api/types";
import { StatusBadge } from "../components/StatusBadge";
import { Empty, ErrorNote, Loading } from "../components/States";
import { ThroughputChart } from "../components/ThroughputChart";
import { ago } from "../lib/format";

const STALE_S = 30;

export function OverviewPage() {
  const stats = useStats();
  const throughput = useThroughput();
  const workers = useWorkers();

  const totals: Record<JobStatus, number> = Object.fromEntries(JOB_STATUSES.map((s) => [s, 0])) as Record<JobStatus, number>;
  for (const q of stats.data?.queues ?? []) for (const s of JOB_STATUSES) totals[s] += q.counts[s] ?? 0;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold tracking-tight">Overview</h1>

      <section aria-label="Totals" className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        {JOB_STATUSES.map((status) => (
          <Link key={status} to={`/jobs?status=${status}`} className="card block !p-4 transition hover:shadow-md">
            <StatusBadge status={status} />
            <p className="mt-2 text-2xl font-bold tabular-nums" data-testid={`total-${status}`}>
              {stats.data ? totals[status] : "—"}
            </p>
          </Link>
        ))}
      </section>

      <div className="grid gap-6 lg:grid-cols-[1.4fr_1fr]">
        <section aria-label="Throughput" className="card">
          <h2 className="mb-3 font-semibold">Throughput</h2>
          {throughput.error ? <ErrorNote error={throughput.error} /> : throughput.data ? <ThroughputChart data={throughput.data} now={throughput.dataUpdatedAt} /> : <Loading />}
        </section>

        <section aria-label="Workers" className="card">
          <h2 className="mb-3 font-semibold">Workers (last 5 min)</h2>
          {workers.error ? (
            <ErrorNote error={workers.error} />
          ) : !workers.data ? (
            <Loading />
          ) : workers.data.length === 0 ? (
            <p className="text-sm text-slate-500">No worker has run your jobs recently.</p>
          ) : (
            <table className="w-full text-sm">
              <thead className="text-left text-xs text-slate-500">
                <tr><th className="pb-2 font-medium">Worker</th><th className="pb-2 text-right font-medium">Running</th><th className="pb-2 text-right font-medium">Done</th><th className="pb-2 text-right font-medium">Failed</th><th className="pb-2 text-right font-medium">Seen</th></tr>
              </thead>
              <tbody>
                {workers.data.map((w) => {
                  const fetchedAt = workers.dataUpdatedAt;
                  const stale = fetchedAt - Date.parse(w.last_seen) > STALE_S * 1000;
                  return (
                    <tr key={w.worker_id} className="border-t border-slate-100 dark:border-slate-800" data-testid="worker-row">
                      <td className="py-1.5 font-mono text-xs">{w.worker_id}</td>
                      <td className="text-right tabular-nums">{w.running}</td>
                      <td className="text-right tabular-nums">{w.succeeded}</td>
                      <td className="text-right tabular-nums">{w.failed}</td>
                      <td className={`text-right text-xs ${stale ? "text-amber-600" : "text-slate-500"}`}>{ago(w.last_seen, fetchedAt)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </section>
      </div>

      <section aria-label="Queues" className="card overflow-x-auto">
        <h2 className="mb-3 font-semibold">Queues</h2>
        {stats.error ? (
          <ErrorNote error={stats.error} />
        ) : !stats.data ? (
          <Loading />
        ) : stats.data.queues.length === 0 ? (
          <Empty>No jobs yet. <Link to="/new" className="underline">Enqueue one</Link>.</Empty>
        ) : (
          <table className="w-full min-w-[36rem] text-sm">
            <thead className="text-left text-xs text-slate-500">
              <tr>
                <th className="pb-2 font-medium">Queue</th>
                {JOB_STATUSES.map((s) => <th key={s} className="pb-2 text-right font-medium">{s}</th>)}
              </tr>
            </thead>
            <tbody>
              {stats.data.queues.map((q) => (
                <tr key={q.queue} className="border-t border-slate-100 dark:border-slate-800">
                  <td className="py-1.5"><Link to={`/jobs?queue=${encodeURIComponent(q.queue)}`} className="font-medium hover:underline">{q.queue}</Link></td>
                  {JOB_STATUSES.map((s) => <td key={s} className="text-right tabular-nums">{q.counts[s] ?? 0}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}
