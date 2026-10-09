import { useState } from "react";
import { Link, useParams } from "react-router";

import { ApiError } from "../api/http";
import { useCancel, useExecutions, useJob, useRequeue } from "../api/queries";
import { isTerminal } from "../api/types";
import { ExecutionTimeline } from "../components/ExecutionTimeline";
import { StatusBadge } from "../components/StatusBadge";
import { ErrorNote, Loading } from "../components/States";
import { json } from "../lib/format";

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <dt className="text-xs text-slate-500">{label}</dt>
      <dd className="text-sm">{children}</dd>
    </div>
  );
}

const when = (iso: string | null) => (iso ? new Date(iso).toLocaleString() : "—");

export function JobPage() {
  const { jobId = "" } = useParams();
  const job = useJob(jobId);
  const live = !job.data || !isTerminal(job.data.status);
  const executions = useExecutions(jobId, live);
  const cancel = useCancel();
  const requeue = useRequeue();
  const [confirming, setConfirming] = useState(false);

  if (job.error instanceof ApiError && (job.error.status === 404 || job.error.status === 422)) {
    return (
      <div className="text-center">
        <h1 className="mb-2 text-2xl font-bold">Job not found</h1>
        <Link to="/jobs" className="underline">Back to jobs</Link>
      </div>
    );
  }
  if (job.error) return <ErrorNote error={job.error} />;
  if (!job.data) return <Loading />;

  const j = job.data;
  const actionError = cancel.error ?? requeue.error;
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="font-mono text-lg font-bold sm:text-xl">{j.id}</h1>
        <span data-testid="job-status"><StatusBadge status={j.status} /></span>
        <div className="ml-auto flex gap-2">
          {(j.status === "scheduled" || j.status === "queued") &&
            (confirming ? (
              <>
                <button type="button" className="btn bg-red-600 text-white hover:bg-red-500" disabled={cancel.isPending} onClick={() => cancel.mutate(j.id, { onSettled: () => setConfirming(false) })}>
                  Confirm cancel
                </button>
                <button type="button" className="btn btn-secondary" onClick={() => setConfirming(false)}>Keep</button>
              </>
            ) : (
              <button type="button" className="btn btn-secondary" onClick={() => setConfirming(true)}>Cancel job</button>
            ))}
          {j.status === "dead" && (
            <button type="button" className="btn btn-primary" disabled={requeue.isPending} onClick={() => requeue.mutate(j.id)}>
              Requeue
            </button>
          )}
        </div>
      </div>
      {actionError && <ErrorNote error={actionError} />}

      <div className="grid gap-4 lg:grid-cols-2">
        <section className="card space-y-4" aria-label="Details">
          <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3">
            <Field label="Task"><code>{j.task}</code></Field>
            <Field label="Queue">{j.queue}</Field>
            <Field label="Attempts"><span data-testid="job-attempts">{j.attempts}/{j.max_attempts}</span></Field>
            <Field label="Created">{when(j.created_at)}</Field>
            <Field label="Run at">{when(j.run_at)}</Field>
            <Field label="Finished">{when(j.finished_at)}</Field>
            <Field label="Timeout">{j.timeout_s} s</Field>
            <Field label="Idempotency key">{j.idempotency_key ? <code className="break-all text-xs">{j.idempotency_key}</code> : "—"}</Field>
          </dl>
          <div className="grid gap-3 sm:grid-cols-2">
            <div><h3 className="mb-1 text-xs text-slate-500">args</h3><pre className="overflow-x-auto rounded bg-slate-100 p-2 text-xs dark:bg-slate-800">{json(j.args)}</pre></div>
            <div><h3 className="mb-1 text-xs text-slate-500">kwargs</h3><pre className="overflow-x-auto rounded bg-slate-100 p-2 text-xs dark:bg-slate-800">{json(j.kwargs)}</pre></div>
          </div>
          {j.status === "succeeded" && (
            <div><h3 className="mb-1 text-xs text-slate-500">Result</h3><pre data-testid="job-result" className="overflow-x-auto rounded bg-emerald-50 p-2 text-xs dark:bg-emerald-950">{json(j.result)}</pre></div>
          )}
          {j.last_error && (
            <div><h3 className="mb-1 text-xs text-slate-500">{j.status === "succeeded" ? "Error from an earlier attempt" : "Last error"}</h3><pre className="overflow-x-auto rounded bg-red-50 p-2 text-xs text-red-800 dark:bg-red-950 dark:text-red-200">{j.last_error}</pre></div>
          )}
        </section>
        <section className="card" aria-label="Attempts">
          <h2 className="mb-3 font-semibold">Attempts</h2>
          {executions.error ? <ErrorNote error={executions.error} /> : executions.data ? <ExecutionTimeline executions={executions.data} /> : <Loading />}
        </section>
      </div>
    </div>
  );
}
