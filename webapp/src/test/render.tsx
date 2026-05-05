import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, type RenderOptions } from "@testing-library/react";
import type { ReactElement } from "react";
import { MemoryRouter } from "react-router-dom";

import { ToastProvider } from "../components/ToastProvider";

export function renderApp(ui: ReactElement, options?: RenderOptions & { route?: string }) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        staleTime: 0
      },
      mutations: {
        retry: false
      }
    }
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <ToastProvider>
        <MemoryRouter initialEntries={[options?.route ?? "/config"]}>{ui}</MemoryRouter>
      </ToastProvider>
    </QueryClientProvider>,
    options
  );
}
