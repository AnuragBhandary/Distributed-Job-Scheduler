import { NavLink, Outlet } from "react-router";

import { keyPrefix, session } from "../api/session";

const links = [
  { to: "/", label: "Overview", end: true },
  { to: "/jobs", label: "Jobs" },
  { to: "/dlq", label: "Dead letters" },
  { to: "/new", label: "New job" },
];

export function Layout({ apiKey }: { apiKey: string }) {
  return (
    <div className="min-h-dvh">
      <header className="border-b border-slate-200 bg-white/80 backdrop-blur dark:border-slate-800 dark:bg-slate-900/80">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-x-6 gap-y-2 px-4 py-3">
          <span className="font-semibold tracking-tight">jobq</span>
          <nav aria-label="Main" className="order-3 flex w-full gap-1 overflow-x-auto sm:order-none sm:w-auto">
            {links.map((link) => (
              <NavLink
                key={link.to}
                to={link.to}
                end={link.end}
                className={({ isActive }) =>
                  `whitespace-nowrap rounded-lg px-3 py-1.5 text-sm font-medium ${
                    isActive
                      ? "bg-slate-900 text-white dark:bg-white dark:text-slate-900"
                      : "text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-800"
                  }`
                }
              >
                {link.label}
              </NavLink>
            ))}
          </nav>
          <div className="ml-auto flex items-center gap-3 text-sm">
            <code className="hidden text-xs text-slate-500 sm:inline" title="API key prefix">
              {keyPrefix(apiKey)}
            </code>
            <button type="button" className="btn btn-secondary" onClick={() => session.signOut()}>
              Sign out
            </button>
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-4 py-6">
        <Outlet />
      </main>
    </div>
  );
}
