import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import App from "../App";
import type { MappingConfig } from "../api/types";
import { renderApp } from "../test/render";

const emptyCopy =
  "No source events configured yet. Create a source event to map, classify, and route Kafka records through ComHub.";

describe("Config route", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders empty copy, loading state, and Source Basics editor", async () => {
    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(Response.json([]))));

    renderApp(<App />, { route: "/config" });

    expect(screen.getByText("Loading source configuration state.")).toBeInTheDocument();
    expect(await screen.findAllByText(emptyCopy)).toHaveLength(1);
    expect(screen.getByLabelText("Topic")).toBeInTheDocument();
    expect(screen.getByLabelText("Source event type")).toBeInTheDocument();
    expect(screen.getByLabelText("Discriminator source")).toBeInTheDocument();
    expect(screen.getByLabelText("Discriminator key")).toBeInTheDocument();
  });

  it("renders list items, selection styling, disabled state, and mapping summary", async () => {
    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(Response.json([sampleConfig(), disabledConfig()]))));

    renderApp(<App />, { route: "/config" });

    const payment = await screen.findByRole("button", { name: /payment_failure/i });
    const invoice = screen.getByRole("button", { name: /invoice_overdue/i });

    expect(payment).toHaveClass("selected");
    expect(invoice).toHaveClass("disabled-source");
    expect(screen.getByText("payments.events.v1")).toBeInTheDocument();
    expect(screen.getAllByText("not observed")).toHaveLength(2);
    expect(screen.getAllByText("payments.events.v1 / payment_failure")).toHaveLength(2);
    expect(screen.getByText("3 of 3 mapped")).toBeInTheDocument();

    await userEvent.click(invoice);

    expect(invoice).toHaveClass("selected");
    expect(screen.getByDisplayValue("billing.events.v1")).toBeInTheDocument();
  });

  it("opens new source, preserves dirty edit while selecting, and restores it", async () => {
    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(Response.json([sampleConfig(), disabledConfig()]))));

    renderApp(<App />, { route: "/config" });

    await screen.findByRole("button", { name: /payment_failure/i });
    await userEvent.click(screen.getByRole("button", { name: "New Source" }));
    await userEvent.type(screen.getByLabelText("Topic"), "orders.events.v1");
    await userEvent.click(screen.getByRole("button", { name: /payment_failure/i }));
    await userEvent.click(screen.getByRole("button", { name: "New Source" }));

    expect(screen.getByDisplayValue("orders.events.v1")).toBeInTheDocument();
  });

  it("validates required fields inline", async () => {
    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(Response.json([]))));

    renderApp(<App />, { route: "/config" });

    await screen.findByText(emptyCopy);
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(screen.getByText("Topic is required.")).toBeInTheDocument();
    expect(screen.getByText("Source event type is required.")).toBeInTheDocument();
    expect(screen.getByText("Discriminator key is required.")).toBeInTheDocument();
  });

  it("blocks Save when enabled mapping is incomplete but allows disabled draft create", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(Response.json([]))
      .mockResolvedValueOnce(Response.json({ ...newDraft(), enabled: false }, { status: 201 }));
    vi.stubGlobal("fetch", fetchMock);

    renderApp(<App />, { route: "/config" });

    await screen.findByText(emptyCopy);
    await userEvent.type(screen.getByLabelText("Topic"), "orders.events.v1");
    await userEvent.type(screen.getByLabelText("Source event type"), "order_created");
    await userEvent.type(screen.getByLabelText("Discriminator key"), "eventType");
    await userEvent.click(screen.getByRole("switch", { name: "Enabled" }));
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(screen.getByText(/Enabled source configs require occurredAt, severity, and category mappings/i)).toBeInTheDocument();

    await userEvent.click(screen.getByRole("switch", { name: "Enabled" }));
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/source-configs", expect.objectContaining({ method: "POST" })));
  });

  it("uses PUT for existing source saves and percent-encodes path segments", async () => {
    const config = sampleConfig("payments.events.v1", "payment/failure");
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(Response.json([config]))
      .mockResolvedValueOnce(Response.json(config));
    vi.stubGlobal("fetch", fetchMock);

    renderApp(<App />, { route: "/config" });

    await screen.findByDisplayValue("payment/failure");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/source-configs/payments.events.v1/payment%2Ffailure",
        expect.objectContaining({ method: "PUT" })
      )
    );
  });

  it("renders 409 ProblemDetail discriminator conflict as an inline page banner", async () => {
    const problem = {
      title: "Discriminator conflict",
      status: 409,
      detail: "Topic 'orders.events.v1' already uses discriminator header/eventType"
    };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(Response.json([]))
      .mockResolvedValueOnce(Response.json(problem, { status: 409 }));
    vi.stubGlobal("fetch", fetchMock);

    renderApp(<App />, { route: "/config" });

    await screen.findByText(emptyCopy);
    await userEvent.type(screen.getByLabelText("Topic"), "orders.events.v1");
    await userEvent.type(screen.getByLabelText("Source event type"), "order_created");
    await userEvent.type(screen.getByLabelText("Discriminator key"), "eventType");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Discriminator conflict");
    expect(screen.getByText(problem.detail)).toBeInTheDocument();
  });
});

function sampleConfig(topic = "payments.events.v1", sourceEventType = "payment_failure"): MappingConfig {
  return {
    topic,
    sourceEventType,
    enabled: true,
    configSchemaVersion: 2,
    discriminator: {
      source: "header",
      key: "eventType"
    },
    mapping: {
      occurredAt: { source: "/occurredAt" },
      severity: { source: "/severity" },
      category: { source: "/category" },
      subject: { source: "/subject" },
      message: { source: "/message" },
      attributes: []
    },
    operations: {
      promotedAttributes: [{ sourceAttribute: "customerId", targetAttribute: "customerId" }],
      classification: [],
      routing: [{ handler: "primary-email", conditions: [], actions: [] }]
    }
  };
}

function disabledConfig(): MappingConfig {
  return {
    ...sampleConfig("billing.events.v1", "invoice_overdue"),
    enabled: false,
    mapping: { attributes: [] },
    operations: { promotedAttributes: [], classification: [], routing: [] }
  };
}

function newDraft(): MappingConfig {
  return {
    ...disabledConfig(),
    topic: "orders.events.v1",
    sourceEventType: "order_created",
    discriminator: {
      source: "header",
      key: "eventType"
    }
  };
}
