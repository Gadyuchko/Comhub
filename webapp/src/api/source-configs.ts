import type { ApiProblem, MappingConfig, ProblemDetail, SourceConfigRequest } from "./types";

const SOURCE_CONFIGS_PATH = "/api/source-configs";

export class ApiProblemError extends Error {
  readonly problem: ApiProblem;

  constructor(problem: ApiProblem) {
    super(problem.detail || problem.title);
    this.name = "ApiProblemError";
    this.problem = problem;
  }
}

export async function listSourceConfigs(): Promise<MappingConfig[]> {
  const response = await fetch(SOURCE_CONFIGS_PATH);
  return readJson<MappingConfig[]>(response);
}

export async function createSourceConfig(config: SourceConfigRequest): Promise<MappingConfig> {
  const response = await fetch(SOURCE_CONFIGS_PATH, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config)
  });

  return readJson<MappingConfig>(response);
}

export async function updateSourceConfig(config: SourceConfigRequest): Promise<MappingConfig> {
  const topic = encodeURIComponent(config.topic);
  const sourceEventType = encodeURIComponent(config.sourceEventType);
  const response = await fetch(`${SOURCE_CONFIGS_PATH}/${topic}/${sourceEventType}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config)
  });

  return readJson<MappingConfig>(response);
}

async function readJson<T>(response: Response): Promise<T> {
  if (response.ok) {
    return response.json() as Promise<T>;
  }

  throw new ApiProblemError(await parseProblem(response));
}

async function parseProblem(response: Response): Promise<ApiProblem> {
  let body: ProblemDetail = {};

  try {
    body = (await response.json()) as ProblemDetail;
  } catch {
    body = {};
  }

  return {
    status: body.status ?? response.status,
    title: body.title ?? "Request failed",
    detail: body.detail ?? response.statusText,
    fieldErrors: body.fieldErrors ?? {}
  };
}

export function problemFromUnknown(error: unknown): ApiProblem | null {
  if (error instanceof ApiProblemError) {
    return error.problem;
  }

  return null;
}
