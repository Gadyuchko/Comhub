export type DiscriminatorSource = "header" | "payload";

export type ConfigDiscriminator = {
  source: DiscriminatorSource;
  key: string;
};

export type CanonicalFieldMapping = {
  source?: string;
};

export type AttributeMapping = {
  targetAttribute: string;
  source: string;
};

export type CanonicalMapping = {
  occurredAt?: CanonicalFieldMapping;
  severity?: CanonicalFieldMapping;
  category?: CanonicalFieldMapping;
  subject?: CanonicalFieldMapping;
  message?: CanonicalFieldMapping;
  attributes: AttributeMapping[];
};

export type PromotedAttribute = {
  sourceAttribute: string;
  targetAttribute: string;
};

export type Condition = {
  attribute: string;
  operator: string;
  value: string;
};

export type ClassificationRule = {
  code: string;
  handler: string;
  conditions: Condition[];
};

export type RoutingAction = {
  type: string;
  channel: string;
  target: string;
};

export type RoutingRule = {
  handler: string;
  conditions: Condition[];
  actions: RoutingAction[];
};

export type OperationsConfig = {
  promotedAttributes: PromotedAttribute[];
  classification: ClassificationRule[];
  routing: RoutingRule[];
};

export type MappingConfig = {
  topic: string;
  sourceEventType: string;
  enabled: boolean;
  configSchemaVersion: number;
  discriminator: ConfigDiscriminator;
  mapping: CanonicalMapping;
  operations: OperationsConfig;
};

export type SourceConfigRequest = MappingConfig;

export type ProblemDetail = {
  type?: string;
  title?: string;
  status?: number;
  detail?: string;
  instance?: string;
  fieldErrors?: Record<string, string>;
};

export type ApiProblem = {
  status: number;
  title: string;
  detail: string;
  fieldErrors: Record<string, string>;
};
