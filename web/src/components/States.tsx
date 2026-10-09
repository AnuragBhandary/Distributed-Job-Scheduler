export function Loading({ label = "Loading…" }: { label?: string }) {
  return <p className="text-sm text-slate-500">{label}</p>;
}

export function ErrorNote({ error }: { error: Error }) {
  return (
    <p role="alert" className="rounded-lg bg-red-50 px-3 py-2 text-sm text-red-700 dark:bg-red-950 dark:text-red-300">
      {error.message}
    </p>
  );
}

export function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-2xl border border-dashed border-slate-300 p-8 text-center text-sm text-slate-500 dark:border-slate-700">
      {children}
    </div>
  );
}
