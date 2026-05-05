import { buildJsonPathTree, formatPointerValue, isValidJsonPointer, readJsonPointer } from "./jsonPointer";

describe("jsonPointer", () => {
  const document = {
    "": "slash-root",
    event_time: "2026-05-05T10:00:00Z",
    severity: "CRITICAL",
    nested: {
      "a/b": "escaped slash",
      "a~b": "escaped tilde",
      value: null
    },
    items: [{ id: 7 }, { id: 8 }]
  };

  it("evaluates empty pointer, slash pointer, escaped tokens, arrays, scalars, and nulls", () => {
    expect(formatPointerValue(readJsonPointer(document, ""))).toContain("event_time");
    expect(formatPointerValue(readJsonPointer(document, "/"))).toBe("slash-root");
    expect(formatPointerValue(readJsonPointer(document, "/nested/a~1b"))).toBe("escaped slash");
    expect(formatPointerValue(readJsonPointer(document, "/nested/a~0b"))).toBe("escaped tilde");
    expect(formatPointerValue(readJsonPointer(document, "/items/1/id"))).toBe("8");
    expect(formatPointerValue(readJsonPointer(document, "/nested/value"))).toBe("null");
  });

  it("reports missing and invalid pointers as not found", () => {
    expect(isValidJsonPointer("severity")).toBe(false);
    expect(readJsonPointer(document, "severity")).toEqual({ found: false, reason: "invalid" });
    expect(formatPointerValue(readJsonPointer(document, "/items/9/id"))).toBe("not found");
    expect(formatPointerValue(readJsonPointer(document, "/items/id"))).toBe("not found");
  });

  it("builds escaped JSON Pointer path nodes", () => {
    expect(buildJsonPathTree(document)).toEqual(
      expect.arrayContaining([
        { label: "payload", path: "" },
        { label: "event_time", path: "/event_time" },
        { label: "nested/a/b", path: "/nested/a~1b" },
        { label: "nested/a~b", path: "/nested/a~0b" },
        { label: "items/0/id", path: "/items/0/id" }
      ])
    );
  });
});
