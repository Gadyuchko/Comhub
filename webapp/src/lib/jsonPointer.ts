export type JsonPointerResult =
  | {
      found: true;
      value: unknown;
    }
  | {
      found: false;
      reason: "missing" | "invalid";
    };

export type JsonPathNode = {
  label: string;
  path: string;
};

export function isValidJsonPointer(pointer: string) {
  return pointer === "" || pointer.startsWith("/");
}

export function readJsonPointer(document: unknown, pointer: string): JsonPointerResult {
  if (!isValidJsonPointer(pointer)) {
    return { found: false, reason: "invalid" };
  }

  if (pointer === "") {
    return { found: true, value: document };
  }

  const tokens = pointer
    .slice(1)
    .split("/")
    .map((token) => token.replace(/~1/g, "/").replace(/~0/g, "~"));

  let current = document;
  for (const token of tokens) {
    if (Array.isArray(current)) {
      if (!/^(0|[1-9]\d*)$/.test(token)) {
        return { found: false, reason: "missing" };
      }

      const index = Number(token);
      if (index >= current.length) {
        return { found: false, reason: "missing" };
      }

      current = current[index];
      continue;
    }

    if (isRecord(current) && Object.prototype.hasOwnProperty.call(current, token)) {
      current = current[token];
      continue;
    }

    return { found: false, reason: "missing" };
  }

  return { found: true, value: current };
}

export function formatPointerValue(result: JsonPointerResult) {
  if (!result.found) {
    return "not found";
  }

  if (result.value === null) {
    return "null";
  }

  if (typeof result.value === "string") {
    return result.value;
  }

  if (typeof result.value === "number" || typeof result.value === "boolean") {
    return String(result.value);
  }

  return JSON.stringify(result.value);
}

export function buildJsonPathTree(document: unknown): JsonPathNode[] {
  const nodes: JsonPathNode[] = [];

  function visit(value: unknown, path: string, label: string) {
    nodes.push({ label, path });

    if (Array.isArray(value)) {
      value.forEach((item, index) => visit(item, `${path}/${index}`, `${label}/${index}`));
      return;
    }

    if (isRecord(value)) {
      Object.entries(value).forEach(([key, child]) => {
        const escaped = key.replace(/~/g, "~0").replace(/\//g, "~1");
        visit(child, `${path}/${escaped}`, path ? `${label}/${key}` : key);
      });
    }
  }

  visit(document, "", "payload");
  return nodes;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
