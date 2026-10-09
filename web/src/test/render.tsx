import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router";

import { pageRoutes } from "../App";
import { session } from "../api/session";
import { Layout } from "../components/Layout";
import { GOOD_KEY } from "./fakeApi";

/** Render the signed-in dashboard at `path`, with an in-memory router. */
export function renderAt(path: string) {
  session.signIn(GOOD_KEY);
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter([{ element: <Layout apiKey={GOOD_KEY} />, children: pageRoutes }], {
    initialEntries: [path],
  });
  const view = render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
  return { ...view, router, queryClient };
}
