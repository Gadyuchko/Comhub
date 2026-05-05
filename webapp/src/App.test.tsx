import { screen } from "@testing-library/react";

import App from "./App";
import { renderApp } from "./test/render";

describe("App shell", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(Response.json([]))));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders top navigation, local badge, and config sync indicator without side navigation", async () => {
    renderApp(<App />, { route: "/config" });

    expect(screen.getByRole("navigation", { name: "Primary" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Config" })).toHaveClass("active");
    expect(screen.getByRole("link", { name: "DLQ" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Dashboards" })).toBeInTheDocument();
    expect(screen.getByText("local")).toBeInTheDocument();
    expect(screen.getByText(/last config sync: observing/i)).toBeInTheDocument();
    expect(screen.queryByRole("navigation", { name: /side/i })).not.toBeInTheDocument();
  });

  it("renders exact DLQ and dashboard empty-state copy through routes", () => {
    renderApp(<App />, { route: "/dlq" });

    expect(screen.getByText("No failed events currently in dead-letter flow.")).toBeInTheDocument();

    renderApp(<App />, { route: "/dashboards" });

    expect(
      screen.getByText(
        "Open Grafana to inspect live pipeline behavior by source event, classification, handler, action status, lag, retries, and delivery outcomes."
      )
    ).toBeInTheDocument();
  });
});
