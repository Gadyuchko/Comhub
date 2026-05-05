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
    expect(screen.getAllByText("payments.events.v1").length).toBeGreaterThanOrEqual(1);
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

  it("renders mapping canvas sections in operations order without legacy delivery sections", async () => {
    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(Response.json([sampleConfig()]))));

    renderApp(<App />, { route: "/config" });

    const headings = await screen.findAllByRole("heading", { level: 2 });
    expect(headings.map((heading) => heading.textContent)).toEqual([
      "Sources",
      "Source Basics",
      "Canonical Mapping",
      "Promoted Attributes",
      "Classification",
      "Routing",
      "Validation & Save",
      "Sample Drop",
      "Dry-run trace"
    ]);
    expect(screen.queryByRole("heading", { name: "Delivery Settings" })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Responses" })).not.toBeInTheDocument();
    expect(screen.getByLabelText("System metadata")).toHaveTextContent("rawPayload");
    expect(screen.getByText("Operational severity used by classification, routing, and dashboards.")).toBeInTheDocument();
  });

  it("keeps Sample Drop local, offers JSON Pointer paths, and previews selected values", async () => {
    const config = { ...sampleConfig(), mapping: { ...sampleConfig().mapping, severity: undefined } };
    const fetchMock = vi.fn().mockResolvedValueOnce(Response.json([config])).mockResolvedValueOnce(Response.json(config));
    vi.stubGlobal("fetch", fetchMock);

    renderApp(<App />, { route: "/config" });

    await screen.findByDisplayValue("payment_failure");
    await userEvent.click(screen.getByLabelText("Sample JSON payload"));
    await userEvent.paste('{"severity":"WARN","nested":{"a/b":7}}');
    await userEvent.type(screen.getByLabelText("Sample headers"), "eventType=payment_failure");
    await userEvent.click(screen.getByLabelText("severity source path"));
    await userEvent.click(screen.getByRole("button", { name: "/severity" }));

    expect(screen.getByText("WARN")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "eventType" })).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("/api/source-configs/"), expect.any(Object)));
    const body = JSON.parse(String(fetchMock.mock.calls[1][1].body));
    expect(JSON.stringify(body)).not.toContain("nested");
    expect(body.mapping.severity.source).toBe("/severity");
  });

  it("edits attributes, promoted attributes, classification, routing, and serializes a complete v2 PUT payload", async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(Response.json([disabledConfig()])).mockImplementation(async (_url, init) => {
      return Response.json(JSON.parse(String(init?.body)));
    });
    vi.stubGlobal("fetch", fetchMock);

    renderApp(<App />, { route: "/config" });

    await screen.findByDisplayValue("billing.events.v1");
    await userEvent.type(screen.getByLabelText("severity source path"), "/severity");
    await userEvent.type(screen.getByLabelText("category source path"), "/category");

    await userEvent.click(screen.getByRole("button", { name: "Add attribute" }));
    await userEvent.type(screen.getByLabelText("Attribute key 1"), "customerId");
    await userEvent.type(screen.getByLabelText("Attribute path 1"), "/customer/id");

    await userEvent.click(screen.getByRole("button", { name: "Add promoted attribute" }));
    await userEvent.type(screen.getByLabelText("Name 1"), "customerId");
    await userEvent.type(screen.getByLabelText("Path 1"), "/customer/id");
    expect(screen.getByLabelText("Promoted type 1")).toHaveValue("string");

    await userEvent.click(screen.getByRole("button", { name: "Add classification rule" }));
    await userEvent.type(screen.getByLabelText("Classification code 1"), "BILLING");
    await userEvent.type(screen.getByLabelText("Severity set 1"), "CRITICAL");
    await userEvent.type(screen.getByLabelText("classification condition attribute 1.1"), "/severity");
    await userEvent.selectOptions(screen.getByLabelText("classification condition operator 1.1"), "eq");
    await userEvent.type(screen.getByLabelText("classification condition value 1.1"), "CRITICAL");
    expect(screen.getByText("when 1 condition AND")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Add routing rule" }));
    await userEvent.type(screen.getByLabelText("Route name 1"), "primary-email");
    await userEvent.type(screen.getByLabelText("routing condition attribute 1.1"), "classificationCode");
    await userEvent.selectOptions(screen.getByLabelText("routing condition operator 1.1"), "eq");
    await userEvent.type(screen.getByLabelText("routing condition value 1.1"), "BILLING");
    await userEvent.type(screen.getAllByLabelText("Target email 1")[0], "ops@example.com");
    expect(screen.getByLabelText("Default route name")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(fetchMock.mock.calls.length).toBeGreaterThanOrEqual(2));
    const [url, init] = fetchMock.mock.calls.find((call) => call[1]?.method === "PUT")!;
    const body = JSON.parse(String(init.body));
    expect(url).toBe("/api/source-configs/billing.events.v1/invoice_overdue");
    expect(init.method).toBe("PUT");
    expect(body.configSchemaVersion).toBe(2);
    expect(body.mapping.attributes).toEqual([{ targetAttribute: "customerId", source: "/customer/id" }]);
    expect(body.operations.promotedAttributes).toEqual([{ targetAttribute: "customerId", sourceAttribute: "/customer/id" }]);
    expect(body.operations.classification[0]).toMatchObject({
      code: "BILLING",
      handler: "CRITICAL",
      conditions: [{ attribute: "/severity", operator: "eq", value: "CRITICAL" }]
    });
    expect(body.operations.routing[0]).toMatchObject({
      handler: "primary-email",
      conditions: [{ attribute: "classificationCode", operator: "eq", value: "BILLING" }],
      actions: [{ type: "notify", channel: "email", target: "ops@example.com" }]
    });
  });

  it("shows field-local validation for invalid rows and blocks Save and Enable with inline reasons", async () => {
    vi.stubGlobal("fetch", vi.fn(() => Promise.resolve(Response.json([disabledConfig()]))));

    renderApp(<App />, { route: "/config" });

    await screen.findByDisplayValue("billing.events.v1");
    await userEvent.clear(screen.getByLabelText("severity source path"));
    await userEvent.type(screen.getByLabelText("severity source path"), "severity");
    expect(screen.getByText("Use a JSON Pointer like /severity.")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Add routing rule" }));
    await userEvent.type(screen.getByLabelText("Route name 1"), "bad-email");
    await userEvent.type(screen.getAllByLabelText("Target email 1")[0], "not-email");
    await userEvent.click(screen.getByRole("button", { name: "Save and Enable" }));

    expect(fetch).toHaveBeenCalledTimes(1);
    expect(screen.getByText("Use a valid email target.")).toBeInTheDocument();
    expect(screen.getByText("Fix highlighted fields before enabling.")).toBeInTheDocument();
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
