import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { requeueMany, useDeadLetters } from "../api/queries";
import { JobTable } from "../components/JobTable";
import { Empty, ErrorNote, Loading } from "../components/States";

export function DeadLetterPage() {
  const dead = useDeadLetters();
  const client = useQueryClient();
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null);
  const [failures, setFailures] = useState<{ id: string; error: string }[]>([]);

  const items = dead.data?.pages.flatMap((p) => p.items) ?? [];
  const visible = new Set(items.map((j) => j.id));
  const chosen = [...selected].filter((id) => visible.has(id)); // ignore jobs no longer dead

  function toggle(id: string) {
    setSelected((s) => {
      const next = new Set(s);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  async function requeueSelected() {
    setFailures([]);
    setProgress({ done: 0, total: chosen.length });
    const failed = await requeueMany(chosen, (done) => setProgress({ done, total: chosen.length }));
    setFailures(failed);
    setSelected(new Set(failed.map((f) => f.id)));
    setProgress(null);
    await client.invalidateQueries({ queryKey: ["jobs"] });
    void client.invalidateQueries({ queryKey: ["stats"] });
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-2xl font-bold tracking-tight">Dead letters</h1>
        <p className="text-sm text-slate-500">Jobs that used every attempt. Requeueing gives a job a fresh retry budget and keeps its history.</p>
      </div>
      <section className="card" aria-label="Dead jobs">
        {dead.error ? (
          <ErrorNote error={dead.error} />
        ) : dead.isPending ? (
          <Loading />
        ) : items.length === 0 ? (
          <Empty>No dead jobs. Nothing has run out of retries.</Empty>
        ) : (
          <>
            <div className="mb-3 flex flex-wrap items-center gap-2">
              <button type="button" className="btn btn-secondary" onClick={() => setSelected(chosen.length === items.length ? new Set() : new Set(visible))}>
                {chosen.length === items.length ? "Select none" : "Select all"}
              </button>
              <button type="button" className="btn btn-primary" disabled={chosen.length === 0 || progress !== null} onClick={() => void requeueSelected()}>
                {progress ? `Requeuing ${progress.done}/${progress.total}…` : `Requeue selected (${chosen.length})`}
              </button>
            </div>
            {failures.length > 0 && (
              <p role="alert" className="mb-3 text-sm text-red-600">
                {failures.length} could not be requeued: {failures[0]!.error}
              </p>
            )}
            <JobTable jobs={items} selected={selected} onToggle={toggle} showError now={dead.dataUpdatedAt} />
          </>
        )}
      </section>
    </div>
  );
}
