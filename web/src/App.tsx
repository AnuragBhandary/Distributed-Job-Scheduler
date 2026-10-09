import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState, useSyncExternalStore } from "react";
import { createBrowserRouter, RouterProvider, type RouteObject } from "react-router";

import { session } from "./api/session";
import { Layout } from "./components/Layout";
import { DeadLetterPage } from "./pages/DeadLetterPage";
import { JobPage } from "./pages/JobPage";
import { JobsPage } from "./pages/JobsPage";
import { NewJobPage } from "./pages/NewJobPage";
import { OverviewPage } from "./pages/OverviewPage";
import { SignInPage } from "./pages/SignInPage";

export const pageRoutes: RouteObject[] = [
  { path: "/", element: <OverviewPage /> },
  { path: "/jobs", element: <JobsPage /> },
  { path: "/jobs/:jobId", element: <JobPage /> },
  { path: "/dlq", element: <DeadLetterPage /> },
  { path: "/new", element: <NewJobPage /> },
];

function Dashboard({ apiKey }: { apiKey: string }) {
  // One query cache and router per signed-in key: signing out drops another key's cached data.
  const [queryClient] = useState(() => new QueryClient({ defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: true } } }));
  const [router] = useState(() => createBrowserRouter([{ element: <Layout apiKey={apiKey} />, children: pageRoutes }]));
  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}

export function App() {
  const key = useSyncExternalStore(session.subscribe, session.key);
  return key ? <Dashboard key={key} apiKey={key} /> : <SignInPage />;
}
