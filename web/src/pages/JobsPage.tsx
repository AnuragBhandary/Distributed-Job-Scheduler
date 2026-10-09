import { useSearchParams } from "react-router";

import { useJobs, type JobFilters } from "../api/queries";
import { JOB_STATUSES, type JobStatus } from "../api/types";
import { JobTable } from "../components/JobTable";
import { Empty, ErrorNote, Loading } from "../components/States";

/** Filters live in the URL, so a filtered view can be shared or bookmarked. */
export function JobsPage() {
  const [params, setParams] = useSearchParams();
  const filters: JobFilters = {
    status: (JOB_STATUSES as readonly string[]).includes(params.get("status") ?? "") ? (params.get("status") as JobStatus) : undefined,
    queue: params.get("queue") || undefined,
    task: params.get("task") || undefined,
  };
  const jobs = useJobs(filters);
  const items = jobs.data?.pages.flatMap((p) => p.items) ?? [];

  function apply(form: FormData) {
    const next = new URLSearchParams();
    for (const name of ["status", "queue", "task"]) {
      const value = String(form.get(name) ?? "").trim();
      if (value) next.set(name, value);
    }
    setParams(next);
  }

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-bold tracking-tight">Jobs</h1>
      <form
        key={params.toString()}
        action={apply}
        aria-label="Filters"
        className="card flex flex-wrap items-end gap-3 !p-4"
      >
        <label className="space-y-1 text-sm">
          <span className="block font-medium">Status</span>
          <select name="status" defaultValue={filters.status ?? ""} className="field">
            <option value="">any</option>
            {JOB_STATUSES.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>
        <label className="space-y-1 text-sm">
          <span className="block font-medium">Queue</span>
          <input name="queue" defaultValue={filters.queue ?? ""} className="field" placeholder="any" />
        </label>
        <label className="space-y-1 text-sm">
          <span className="block font-medium">Task</span>
          <input name="task" defaultValue={filters.task ?? ""} className="field font-mono" placeholder="any" />
        </label>
        <button type="submit" className="btn btn-primary">Apply</button>
        {params.size > 0 && (
          <button type="button" className="btn btn-secondary" onClick={() => setParams(new URLSearchParams())}>
            Clear
          </button>
        )}
      </form>

      <section className="card" aria-label="Job list">
        {jobs.error ? (
          <ErrorNote error={jobs.error} />
        ) : jobs.isPending ? (
          <Loading />
        ) : items.length === 0 ? (
          <Empty>No jobs match these filters.</Empty>
        ) : (
          <>
            <JobTable jobs={items} now={jobs.dataUpdatedAt} />
            <div className="mt-4 flex items-center justify-between text-sm text-slate-500">
              <span>{items.length} shown · refreshing every 2 s</span>
              {jobs.hasNextPage && (
                <button type="button" className="btn btn-secondary" onClick={() => void jobs.fetchNextPage()} disabled={jobs.isFetchingNextPage}>
                  {jobs.isFetchingNextPage ? "Loading…" : "Load more"}
                </button>
              )}
            </div>
          </>
        )}
      </section>
    </div>
  );
}
