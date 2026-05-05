import {
  AlertTriangle,
  Check,
  ChevronRight,
  Eye,
  GripVertical,
  Mail,
  Plus,
  Search,
  X,
  Zap
} from "lucide-react";
import { useEffect, useMemo, useState, type ReactNode } from "react";

import { problemFromUnknown } from "../api/source-configs";
import { useCreateSourceConfig, useSourceConfigs, useUpdateSourceConfig } from "../api/source-config-hooks";
import type {
  ApiProblem,
  AttributeMapping,
  ClassificationRule,
  ConfigDiscriminator,
  MappingConfig,
  PromotedAttribute,
  RoutingAction,
  RoutingRule,
  SourceConfigRequest
} from "../api/types";
import { InlineBanner } from "../components/InlineBanner";
import { QueryState } from "../components/QueryState";
import { useToast } from "../components/ToastProvider";
import { Dot, HelpBadge, Pill } from "../components/ui/atoms";
import { Button } from "../components/ui/button";
import {
  buildJsonPathTree,
  formatPointerValue,
  isValidJsonPointer,
  readJsonPointer,
  type JsonPathNode
} from "../lib/jsonPointer";
import { usePublishTopbarIdentity } from "../lib/topbarIdentity";

const CONFIG_EMPTY_COPY =
  "No source events configured yet. Create a source event to map, classify, and route Kafka records through ComHub.";

type CanonicalKey = "severity" | "category" | "occurredAt" | "subject" | "message";

type CanonicalFieldMeta = {
  key: CanonicalKey;
  label: string;
  required: boolean;
  impact: "behavior" | "context" | "visibility";
  help: string;
};

const CANONICAL_FIELDS: CanonicalFieldMeta[] = [
  {
    key: "severity",
    label: "Severity",
    required: true,
    impact: "behavior",
    help: "Drives alert priority and internal urgency handling. One of INFO, WARNING, ERROR, CRITICAL."
  },
  {
    key: "category",
    label: "Category",
    required: true,
    impact: "behavior",
    help: "Primary grouping key for routing rules, dashboards, and internal classification."
  },
  {
    key: "occurredAt",
    label: "Occurred At",
    required: true,
    impact: "context",
    help: "Business event timestamp used for timeline context and chronology."
  },
  {
    key: "subject",
    label: "Subject",
    required: false,
    impact: "visibility",
    help: "Short operator-facing label. Good for lists and cards. Not used for routing logic."
  },
  {
    key: "message",
    label: "Message",
    required: false,
    impact: "visibility",
    help: "Longer operator-facing summary shown in lists, alerts, and troubleshooting views."
  }
];

const SYSTEM_FIELDS = ["id", "sourceTopic", "sourcePartition", "sourceOffset", "receivedAt", "rawPayload"];
const OPERATORS = ["eq", "neq", "in", "gte", "gt", "lte", "lt"];
const MAX_RULES = 3;

type Draft = SourceConfigRequest;
type DraftMap = Record<string, Draft>;
type FieldErrors = Record<string, string>;

type SampleState = {
  payloadText: string;
  headersText: string;
  parsedPayload: unknown | null;
  payloadError: string;
};

type RuleTab = "classification" | "routing";

type PickerTarget =
  | { type: "canonical"; field: CanonicalKey }
  | { type: "attribute"; index: number }
  | { type: "promoted"; index: number }
  | { type: "condition"; group: RuleTab; ruleIndex: number; conditionIndex: number };

const emptyDraft: Draft = {
  topic: "",
  sourceEventType: "",
  enabled: false,
  configSchemaVersion: 2,
  discriminator: {
    source: "header",
    key: ""
  },
  mapping: {
    attributes: []
  },
  operations: {
    promotedAttributes: [],
    classification: [],
    routing: []
  }
};

const emptySample: SampleState = {
  payloadText: "",
  headersText: "",
  parsedPayload: null,
  payloadError: ""
};

export default function Config() {
  const configsQuery = useSourceConfigs();
  const createMutation = useCreateSourceConfig();
  const updateMutation = useUpdateSourceConfig();
  const toast = useToast();
  const configs = configsQuery.data ?? [];
  const [selectedKey, setSelectedKey] = useState<string>("");
  const [drafts, setDrafts] = useState<DraftMap>({ new: emptyDraft });
  const [fieldErrors, setFieldErrors] = useState<FieldErrors>({});
  const [pageProblem, setPageProblem] = useState<ApiProblem | null>(null);
  const [sample, setSample] = useState<SampleState>(emptySample);
  const [pickerTarget, setPickerTarget] = useState<PickerTarget | null>(null);
  const [tab, setTab] = useState<RuleTab>("classification");
  const [filterText, setFilterText] = useState<string>("");

  useEffect(() => {
    if (selectedKey === "" && configs.length > 0) {
      const firstConfig = configs[0];
      const key = configKey(firstConfig);
      setSelectedKey(key);
      setDrafts((current) => ({ ...current, [key]: normalizeDraft(current[key] ?? firstConfig) }));
    }
  }, [configs, selectedKey]);

  const selectedConfig = useMemo(
    () => configs.find((config) => configKey(config) === selectedKey),
    [configs, selectedKey]
  );

  const draft = drafts[selectedKey] ?? selectedConfig ?? emptyDraft;
  const selectedIsExisting = Boolean(selectedConfig);
  const hasSources = configs.length > 0;
  const payloadPaths = sample.parsedPayload === null
    ? []
    : buildJsonPathTree(sample.parsedPayload).filter((node) => node.path);
  const headerNames = parseHeaderNames(sample.headersText);
  const unsavedCount = countUnsaved(draft, selectedConfig);
  const requiredMappingMissing = !draft.mapping.severity?.source || !draft.mapping.category?.source;
  const filteredConfigs = filterConfigs(configs, filterText);

  usePublishTopbarIdentity(
    selectedIsExisting && draft.topic && draft.sourceEventType
      ? { topic: draft.topic, sourceEventType: draft.sourceEventType, enabled: draft.enabled }
      : null
  );

  function selectConfig(config: MappingConfig) {
    const key = configKey(config);
    setSelectedKey(key);
    setDrafts((current) => ({ ...current, [key]: normalizeDraft(current[key] ?? config) }));
    setFieldErrors({});
    setPageProblem(null);
    setPickerTarget(null);
    setSample(emptySample);
  }

  function startNewSource() {
    setSelectedKey("new");
    setDrafts((current) => ({ ...current, new: current.new ?? emptyDraft }));
    setFieldErrors({});
    setPageProblem(null);
    setPickerTarget(null);
    setSample(emptySample);
  }

  function updateDraft(nextDraft: Draft) {
    setDrafts((current) => ({ ...current, [selectedKey]: normalizeDraft(nextDraft) }));
  }

  function revertEdit() {
    setFieldErrors({});
    setPageProblem(null);
    setPickerTarget(null);
    setSample(emptySample);
    setDrafts((current) => ({
      ...current,
      [selectedKey]: normalizeDraft(selectedConfig ?? emptyDraft)
    }));
  }

  async function saveDraft(options: { enable: boolean } = { enable: false }) {
    setFieldErrors({});
    setPageProblem(null);

    const candidate = normalizeDraft({
      ...draft,
      enabled: options.enable ? true : draft.enabled,
      configSchemaVersion: 2
    });
    const localErrors = validateDraft(candidate);
    if (Object.keys(localErrors).length > 0) {
      setFieldErrors(localErrors);
      return;
    }

    if (options.enable || candidate.enabled) {
      const enableReasons = validateEnable(candidate);
      if (enableReasons.length > 0) {
        setPageProblem({
          status: 0,
          title: "Save and Enable blocked",
          detail: enableReasons.join(" "),
          fieldErrors: {}
        });
        return;
      }
    }

    try {
      const saved = selectedIsExisting
        ? await updateMutation.mutateAsync(candidate)
        : await createMutation.mutateAsync(candidate);
      const savedKey = configKey(saved);
      setSelectedKey(savedKey);
      setDrafts((current) => ({ ...current, [savedKey]: normalizeDraft(saved), new: emptyDraft }));
      toast.confirm(options.enable ? "Source configuration saved and enabled." : "Source configuration saved.");
    } catch (error) {
      const problem = problemFromUnknown(error);
      if (problem) {
        setFieldErrors(problem.fieldErrors);
        setPageProblem(problem);
        return;
      }

      setPageProblem({
        status: 0,
        title: "Save failed",
        detail: "Source configuration could not be saved.",
        fieldErrors: {}
      });
    }
  }

  function updateSamplePayload(payloadText: string) {
    if (!payloadText.trim()) {
      setSample((current) => ({ ...current, payloadText, parsedPayload: null, payloadError: "" }));
      return;
    }

    try {
      setSample((current) => ({
        ...current,
        payloadText,
        parsedPayload: JSON.parse(payloadText),
        payloadError: ""
      }));
    } catch {
      setSample((current) => ({
        ...current,
        payloadText,
        parsedPayload: null,
        payloadError: "Sample payload must be valid JSON."
      }));
    }
  }

  function pickPath(path: string) {
    if (!pickerTarget) {
      return;
    }

    if (pickerTarget.type === "canonical") {
      updateDraft({ ...draft, mapping: { ...draft.mapping, [pickerTarget.field]: { source: path } } });
    } else if (pickerTarget.type === "attribute") {
      updateDraft({
        ...draft,
        mapping: {
          ...draft.mapping,
          attributes: draft.mapping.attributes.map((attribute, index) =>
            index === pickerTarget.index ? { ...attribute, source: path } : attribute
          )
        }
      });
    } else if (pickerTarget.type === "promoted") {
      updateDraft({
        ...draft,
        operations: {
          ...draft.operations,
          promotedAttributes: draft.operations.promotedAttributes.map((attribute, index) =>
            index === pickerTarget.index ? { ...attribute, sourceAttribute: path } : attribute
          )
        }
      });
    } else {
      updateConditionPath(pickerTarget.group, pickerTarget.ruleIndex, pickerTarget.conditionIndex, path);
    }
  }

  function updateConditionPath(group: RuleTab, ruleIndex: number, conditionIndex: number, path: string) {
    if (group === "classification") {
      updateDraft({
        ...draft,
        operations: {
          ...draft.operations,
          classification: draft.operations.classification.map((rule, currentRuleIndex) =>
            currentRuleIndex === ruleIndex
              ? {
                  ...rule,
                  conditions: rule.conditions.map((condition, currentConditionIndex) =>
                    currentConditionIndex === conditionIndex ? { ...condition, attribute: path } : condition
                  )
                }
              : rule
          )
        }
      });
      return;
    }

    updateDraft({
      ...draft,
      operations: {
        ...draft.operations,
        routing: draft.operations.routing.map((rule, currentRuleIndex) =>
          currentRuleIndex === ruleIndex
            ? {
                ...rule,
                conditions: rule.conditions.map((condition, currentConditionIndex) =>
                  currentConditionIndex === conditionIndex ? { ...condition, attribute: path } : condition
                )
              }
            : rule
        )
      }
    });
  }

  const mutationPending = createMutation.isPending || updateMutation.isPending;
  const pageError = problemFromUnknown(configsQuery.error);
  const eyebrowEvent = draft.sourceEventType || "new source";

  return (
    <section className="page-frame">
      <div className="page-header-bar">
        <div className="page-header-text">
          <div className="eyebrow">SOURCE EVENTS / {eyebrowEvent}</div>
          <div className="page-header-title-row">
            <h1>Mapping &amp; operations</h1>
            {unsavedCount > 0 ? <Pill tone="warning">{unsavedCount} unsaved</Pill> : null}
            {requiredMappingMissing ? (
              <Pill tone="danger">severity + category unmapped · save blocked</Pill>
            ) : null}
          </div>
          <div className="page-header-subtitle">{identityLabel(draft)}</div>
        </div>
        <div className="page-header-spacer" />
        <div className="page-header-actions">
          <SourceSwitcher sourceEventType={eyebrowEvent} />
          <Button onClick={revertEdit} variant="ghost" disabled={mutationPending}>
            Revert
          </Button>
          <Button onClick={() => void saveDraft()} variant="outline" disabled={mutationPending}>
            Save draft
          </Button>
          <Button
            onClick={() => void saveDraft({ enable: true })}
            variant="primary"
            icon={<Check size={14} />}
            disabled={mutationPending}
          >
            Save &amp; enable
          </Button>
        </div>
      </div>

      <div className="config-grid config-grid-dense">
        <SourceConfigList
          configs={filteredConfigs}
          totalCount={configs.length}
          selectedKey={selectedKey}
          hasSources={hasSources}
          filterText={filterText}
          onFilterChange={setFilterText}
          onSelect={selectConfig}
          onNew={startNewSource}
        />

        <section className="editor-center">
          <QueryState
            isLoading={configsQuery.isLoading}
            isRefetching={configsQuery.isRefetching}
            isStale={configsQuery.isStale}
            isError={configsQuery.isError}
            errorMessage={pageError?.detail}
          />
          {pageProblem ? (
            <InlineBanner
              tone={pageProblem.status === 409 || pageProblem.status >= 500 ? "error" : "warning"}
              title={pageProblem.title}
            >
              {pageProblem.detail}
            </InlineBanner>
          ) : null}

          <SourceIdentitySection draft={draft} errors={fieldErrors} onChange={updateDraft} />

          <SampleDropNoteSection />

          <CanonicalMappingSection
            draft={draft}
            errors={fieldErrors}
            samplePayload={sample.parsedPayload}
            onChange={updateDraft}
            onTarget={setPickerTarget}
          />

          <PromotedAttributesSection
            draft={draft}
            errors={fieldErrors}
            samplePayload={sample.parsedPayload}
            onChange={updateDraft}
            onTarget={setPickerTarget}
          />

          <RuleTabsSection
            draft={draft}
            errors={fieldErrors}
            tab={tab}
            onTabChange={setTab}
            onChange={updateDraft}
            onTarget={setPickerTarget}
          />

          <ValidationSaveSection draft={draft} errors={fieldErrors} sample={sample} />
        </section>

        <aside className="config-rail">
          <SamplePayloadCard
            sample={sample}
            paths={payloadPaths}
            headers={headerNames}
            pickerActive={Boolean(pickerTarget)}
            onPayloadChange={updateSamplePayload}
            onHeadersChange={(headersText) => setSample((current) => ({ ...current, headersText }))}
            onPick={pickPath}
            onReplace={() => setSample(emptySample)}
          />
          <DryRunTraceCard draft={draft} sample={sample} />
        </aside>
      </div>
    </section>
  );
}

type SourceConfigListProps = {
  configs: MappingConfig[];
  totalCount: number;
  selectedKey: string;
  hasSources: boolean;
  filterText: string;
  onFilterChange: (value: string) => void;
  onSelect: (config: MappingConfig) => void;
  onNew: () => void;
};

function SourceConfigList({
  configs,
  totalCount,
  selectedKey,
  hasSources,
  filterText,
  onFilterChange,
  onSelect,
  onNew
}: SourceConfigListProps) {
  return (
    <aside className="panel source-list" aria-labelledby="sources-heading">
      <div className="sources-eyebrow">
        <h2 id="sources-heading" className="sources-eyebrow-label">
          Sources
        </h2>
        <span aria-hidden="true" className="sources-eyebrow-label">
          ({totalCount})
        </span>
        <div style={{ flex: 1 }} />
        <Button onClick={onNew} variant="ghost" icon={<Plus size={12} />}>
          New
        </Button>
      </div>
      <div className="source-search">
        <Search size={12} aria-hidden="true" />
        <input
          aria-label="Filter sources"
          placeholder="Filter…"
          value={filterText}
          onChange={(event) => onFilterChange(event.target.value)}
        />
      </div>
      <div className="source-list-body">
        {!hasSources ? <p className="muted">{CONFIG_EMPTY_COPY}</p> : null}
        {hasSources && configs.length === 0 ? <p className="muted">No sources match filter.</p> : null}
        {configs.map((config) => (
          <SourceListItem
            key={configKey(config)}
            config={config}
            selected={selectedKey === configKey(config)}
            onClick={() => onSelect(config)}
          />
        ))}
      </div>
    </aside>
  );
}

function SourceListItem({
  config,
  selected,
  onClick
}: {
  config: MappingConfig;
  selected: boolean;
  onClick: () => void;
}) {
  const tone = config.enabled ? "success" : "neutral";
  const className = [
    "dense-source-item",
    selected ? "selected" : "",
    config.enabled ? "" : "disabled-source"
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <button type="button" className={className} onClick={onClick}>
      <span className="dense-source-item-row">
        <Dot tone={tone} size={6} />
        <span className="dense-source-event">{config.sourceEventType}</span>
        <span className="dense-source-time">not observed</span>
      </span>
      <span className="dense-source-topic">{config.topic}</span>
    </button>
  );
}

function SourceSwitcher({ sourceEventType }: { sourceEventType: string }) {
  return (
    <div className="source-switcher" aria-label="Source switcher">
      <span className="source-switcher-label">Editing:</span>
      <strong>{sourceEventType}</strong>
      <ChevronRight size={12} aria-hidden="true" />
    </div>
  );
}

function SourceIdentitySection({
  draft,
  errors,
  onChange
}: {
  draft: Draft;
  errors: FieldErrors;
  onChange: (draft: Draft) => void;
}) {
  const updateDiscriminator = (discriminator: ConfigDiscriminator) => onChange({ ...draft, discriminator });

  return (
    <section aria-labelledby="source-identity-heading">
      <div className="section-header-dense">
        <h2 id="source-identity-heading">Source identity</h2>
        <span className="section-header-note">Config key: topic + source event type</span>
        <div className="section-header-spacer" />
        <Pill tone={draft.enabled ? "success" : "neutral"}>{draft.enabled ? "enabled" : "disabled"}</Pill>
      </div>

      <div className="identity-card">
        <div className="identity-card-grid">
          <Field label="Topic" error={errors.topic} dense>
            <input
              className="input mono"
              value={draft.topic}
              onChange={(event) => onChange({ ...draft, topic: event.target.value })}
            />
          </Field>
          <Field label="Source event type" error={errors.sourceEventType} dense>
            <input
              className="input mono"
              value={draft.sourceEventType}
              onChange={(event) => onChange({ ...draft, sourceEventType: event.target.value })}
            />
          </Field>
          <Field label="Discriminator source" error={errors["discriminator.source"]} dense>
            <select
              className="select"
              value={draft.discriminator.source}
              onChange={(event) =>
                updateDiscriminator({
                  ...draft.discriminator,
                  source: event.target.value as ConfigDiscriminator["source"]
                })
              }
            >
              <option value="header">header</option>
              <option value="payload">payload</option>
            </select>
          </Field>
          <Field label="Discriminator key" error={errors["discriminator.key"]} dense>
            <input
              className="input mono"
              value={draft.discriminator.key}
              onChange={(event) => updateDiscriminator({ ...draft.discriminator, key: event.target.value })}
            />
          </Field>
          <div className="field-dense">
            <span>Enabled</span>
            <button
              type="button"
              role="switch"
              aria-checked={draft.enabled}
              aria-label="Enabled"
              className={`toggle pill ${draft.enabled ? "on" : ""}`}
              onClick={() => onChange({ ...draft, enabled: !draft.enabled })}
            >
              <span>{draft.enabled ? "on" : "off"}</span>
              <span className="toggle-knob" aria-hidden="true" />
            </button>
          </div>
        </div>
        <div className="identity-warning">
          <AlertTriangle size={14} className="identity-warning-icon" aria-hidden="true" />
          <span>
            All configs on <code>{draft.topic || "this topic"}</code> must use the same discriminator. A mismatch returns{" "}
            <strong>409 conflict</strong>.
          </span>
        </div>
      </div>
    </section>
  );
}

function SampleDropNoteSection() {
  return (
    <section aria-labelledby="sample-drop-heading">
      <div className="section-header-dense">
        <h2 id="sample-drop-heading">Sample drop</h2>
        <span className="section-header-note">Local-only · not persisted on save</span>
      </div>
      <div className="sample-drop-note">
        Paste a JSON sample on the right to enable preview and path picking.
      </div>
    </section>
  );
}

function CanonicalMappingSection({
  draft,
  errors,
  samplePayload,
  onChange,
  onTarget
}: {
  draft: Draft;
  errors: FieldErrors;
  samplePayload: unknown | null;
  onChange: (draft: Draft) => void;
  onTarget: (target: PickerTarget) => void;
}) {
  function setCanonical(field: CanonicalKey, source: string) {
    onChange({ ...draft, mapping: { ...draft.mapping, [field]: source ? { source } : undefined } });
  }

  function addAttribute() {
    onChange({
      ...draft,
      mapping: {
        ...draft.mapping,
        attributes: [...draft.mapping.attributes, { targetAttribute: "", source: "" }]
      }
    });
  }

  function updateAttribute(index: number, attribute: AttributeMapping) {
    onChange({
      ...draft,
      mapping: {
        ...draft.mapping,
        attributes: draft.mapping.attributes.map((current, currentIndex) =>
          currentIndex === index ? attribute : current
        )
      }
    });
  }

  function removeAttribute(index: number) {
    onChange({
      ...draft,
      mapping: {
        ...draft.mapping,
        attributes: draft.mapping.attributes.filter((_, currentIndex) => currentIndex !== index)
      }
    });
  }

  const mappedRequired = CANONICAL_FIELDS.filter((field) => field.required && draft.mapping[field.key]?.source).length;
  const totalRequired = CANONICAL_FIELDS.filter((field) => field.required).length;

  return (
    <section aria-labelledby="canonical-mapping-heading">
      <div className="section-header-dense">
        <h2 id="canonical-mapping-heading">Canonical mapping</h2>
        <span className="section-header-note">Required first · hover labels for meaning</span>
        <div className="section-header-spacer" />
        <Pill tone={mappedRequired === totalRequired ? "success" : "warning"}>
          {mappedRequired} of {totalRequired} required
        </Pill>
      </div>

      <div className="mapping-table">
        <div className="mapping-table-header">
          <span>Canonical field</span>
          <span>Mode</span>
          <span>Source / rule</span>
          <span>Preview</span>
          <span>Impact</span>
        </div>

        {CANONICAL_FIELDS.map((field) => {
          const source = draft.mapping[field.key]?.source ?? "";
          const invalid = Boolean(source) && !isValidJsonPointer(source);
          const previewText = source ? previewValue(samplePayload, source) : "not found";
          const previewMuted = previewText === "not found";
          const impactTone =
            field.impact === "behavior" ? "warning" : field.impact === "context" ? "accent" : "info";

          return (
            <div key={field.key} className="mapping-row-table">
              <div className="mapping-row-label">
                {field.required ? (
                  <span className="mapping-row-label-required" aria-label="required">
                    •
                  </span>
                ) : null}
                <span className="mapping-row-label-text">{field.label}</span>
                <HelpBadge tip={field.help} label={`${field.label} help`} />
              </div>
              {/* TODO: Story 1.9 owns rule and static mode wiring. */}
              <select className="mode-select" defaultValue="direct" aria-label={`${field.key} mode`}>
                <option value="direct">direct</option>
              </select>
              <div className="mapping-row-source">
                <input
                  aria-label={`${field.key} source path`}
                  className={invalid ? "invalid" : undefined}
                  value={source}
                  onFocus={() => onTarget({ type: "canonical", field: field.key })}
                  onChange={(event) => setCanonical(field.key, event.target.value)}
                />
                {invalid ? <span className="field-error">Use a JSON Pointer like /severity.</span> : null}
              </div>
              <span className={`mapping-row-preview ${previewMuted ? "muted" : ""}`}>{previewText}</span>
              <div className="mapping-row-impact">
                <Pill tone={impactTone}>{field.impact}</Pill>
              </div>
            </div>
          );
        })}
      </div>

      <SystemMetadataStrip />

      <AttributesCard
        attributes={draft.mapping.attributes}
        errors={errors}
        samplePayload={samplePayload}
        onAdd={addAttribute}
        onUpdate={updateAttribute}
        onRemove={removeAttribute}
        onTarget={onTarget}
      />
    </section>
  );
}

function SystemMetadataStrip() {
  return (
    <div className="system-metadata-strip" aria-label="System metadata">
      <span className="system-metadata-eyebrow">System-populated metadata · read only</span>
      <div className="system-metadata-chips">
        {SYSTEM_FIELDS.map((field) => (
          <span key={field} className="system-metadata-chip">
            {field}
          </span>
        ))}
      </div>
    </div>
  );
}

function AttributesCard({
  attributes,
  errors,
  samplePayload,
  onAdd,
  onUpdate,
  onRemove,
  onTarget
}: {
  attributes: AttributeMapping[];
  errors: FieldErrors;
  samplePayload: unknown | null;
  onAdd: () => void;
  onUpdate: (index: number, attribute: AttributeMapping) => void;
  onRemove: (index: number) => void;
  onTarget: (target: PickerTarget) => void;
}) {
  return (
    <div className="attribute-card">
      <div className="attribute-card-header">
        <h3>Attributes</h3>
        <span className="section-header-note">Canonical extras · operator-facing keys</span>
        <div className="attribute-card-spacer" />
        <Button onClick={onAdd} variant="ghost" icon={<Plus size={12} />}>
          Add attribute
        </Button>
      </div>
      {attributes.length === 0 ? (
        <div className="attribute-row" style={{ gridTemplateColumns: "1fr" }}>
          <span className="muted">No additional attributes.</span>
        </div>
      ) : null}
      {attributes.map((attribute, index) => {
        const sourceInvalid = Boolean(attribute.source) && !isValidJsonPointer(attribute.source);
        const previewText = attribute.source ? previewValue(samplePayload, attribute.source) : "not found";
        return (
          <div key={index} className="attribute-row">
            <Field
              label={`Attribute key ${index + 1}`}
              error={errors[`mapping.attributes.${index}.targetAttribute`]}
            >
              <input
                value={attribute.targetAttribute}
                onChange={(event) => onUpdate(index, { ...attribute, targetAttribute: event.target.value })}
              />
            </Field>
            <Field
              label={`Attribute path ${index + 1}`}
              error={errors[`mapping.attributes.${index}.source`]}
            >
              <input
                className={sourceInvalid ? "invalid" : undefined}
                value={attribute.source}
                onFocus={() => onTarget({ type: "attribute", index })}
                onChange={(event) => onUpdate(index, { ...attribute, source: event.target.value })}
              />
            </Field>
            <span className="attribute-row-preview">{previewText}</span>
            <button
              type="button"
              className="icon-button"
              onClick={() => onRemove(index)}
              aria-label={`Remove attribute ${index + 1}`}
            >
              <X size={12} />
            </button>
          </div>
        );
      })}
    </div>
  );
}

function PromotedAttributesSection({
  draft,
  errors,
  samplePayload,
  onChange,
  onTarget
}: {
  draft: Draft;
  errors: FieldErrors;
  samplePayload: unknown | null;
  onChange: (draft: Draft) => void;
  onTarget: (target: PickerTarget) => void;
}) {
  function addPromoted() {
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        promotedAttributes: [
          ...draft.operations.promotedAttributes,
          { sourceAttribute: "", targetAttribute: "" }
        ]
      }
    });
  }

  function updatePromoted(index: number, promoted: PromotedAttribute) {
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        promotedAttributes: draft.operations.promotedAttributes.map((current, currentIndex) =>
          currentIndex === index ? promoted : current
        )
      }
    });
  }

  function removePromoted(index: number) {
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        promotedAttributes: draft.operations.promotedAttributes.filter(
          (_, currentIndex) => currentIndex !== index
        )
      }
    });
  }

  return (
    <section aria-labelledby="promoted-attributes-heading">
      <div className="section-header-dense">
        <h2 id="promoted-attributes-heading">Promoted attributes</h2>
        <span className="section-header-note">Dashboard dimensions · alert conditions</span>
        <div className="section-header-spacer" />
        <Pill tone="accent">{draft.operations.promotedAttributes.length}</Pill>
        <Button onClick={addPromoted} variant="ghost" icon={<Plus size={12} />}>
          Add promoted attribute
        </Button>
      </div>

      <div className="promoted-table">
        <div className="promoted-table-header">
          <span>Name</span>
          <span>Source</span>
          <span>Type</span>
          <span>Preview</span>
          <span>Mode</span>
          <span></span>
        </div>
        {draft.operations.promotedAttributes.length === 0 ? (
          <div className="promoted-row" style={{ gridTemplateColumns: "1fr" }}>
            <span className="muted">No promoted attributes.</span>
          </div>
        ) : null}
        {draft.operations.promotedAttributes.map((promoted, index) => {
          const invalid =
            promoted.sourceAttribute.startsWith("/") && !isValidJsonPointer(promoted.sourceAttribute);
          const preview =
            promoted.sourceAttribute.startsWith("/") ? previewValue(samplePayload, promoted.sourceAttribute) : "not found";
          return (
            <div key={index} className="promoted-row">
              <Field
                label={`Name ${index + 1}`}
                error={errors[`operations.promotedAttributes.${index}.targetAttribute`]}
              >
                <input
                  value={promoted.targetAttribute}
                  onChange={(event) => updatePromoted(index, { ...promoted, targetAttribute: event.target.value })}
                />
              </Field>
              <Field
                label={`Path ${index + 1}`}
                error={errors[`operations.promotedAttributes.${index}.sourceAttribute`]}
              >
                <input
                  className={invalid ? "invalid" : undefined}
                  value={promoted.sourceAttribute}
                  onFocus={() => onTarget({ type: "promoted", index })}
                  onChange={(event) => updatePromoted(index, { ...promoted, sourceAttribute: event.target.value })}
                />
              </Field>
              <Field label={`Type ${index + 1}`}>
                <select className="select" defaultValue="string" aria-label={`Promoted type ${index + 1}`}>
                  <option value="string">string</option>
                  <option value="number">number</option>
                  <option value="boolean">boolean</option>
                </select>
              </Field>
              <span className="promoted-row-preview">{preview}</span>
              {/* TODO: Story 1.9 wires conditional promotion. */}
              <Pill tone="neutral">direct</Pill>
              <button
                type="button"
                className="icon-button"
                onClick={() => removePromoted(index)}
                aria-label={`Remove promoted attribute ${index + 1}`}
              >
                <X size={12} />
              </button>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function RuleTabsSection({
  draft,
  errors,
  tab,
  onTabChange,
  onChange,
  onTarget
}: {
  draft: Draft;
  errors: FieldErrors;
  tab: RuleTab;
  onTabChange: (tab: RuleTab) => void;
  onChange: (draft: Draft) => void;
  onTarget: (target: PickerTarget) => void;
}) {
  return (
    <section aria-label="Classification and Routing">
      <div className="tab-strip" role="tablist" aria-label="Operations rule tabs">
        <button
          role="tab"
          aria-selected={tab === "classification"}
          className={`tab ${tab === "classification" ? "active" : ""}`}
          onClick={() => onTabChange("classification")}
        >
          Classification
          <span className="tab-count">
            {draft.operations.classification.length} + default
          </span>
        </button>
        <button
          role="tab"
          aria-selected={tab === "routing"}
          className={`tab ${tab === "routing" ? "active" : ""}`}
          onClick={() => onTabChange("routing")}
        >
          Routing
          <span className="tab-count">{draft.operations.routing.length} + default</span>
        </button>
        <span className="tab-strip-hint">first match wins · up to 3 rules (MVP)</span>
      </div>

      <div className="tab-panel" role="tabpanel">
        {tab === "classification" ? (
          <ClassificationPanel draft={draft} errors={errors} onChange={onChange} onTarget={onTarget} />
        ) : (
          <RoutingPanel draft={draft} errors={errors} onChange={onChange} onTarget={onTarget} />
        )}
      </div>
    </section>
  );
}

function ClassificationPanel({
  draft,
  errors,
  onChange,
  onTarget
}: {
  draft: Draft;
  errors: FieldErrors;
  onChange: (draft: Draft) => void;
  onTarget: (target: PickerTarget) => void;
}) {
  const rules = draft.operations.classification;
  const capped = rules.length >= MAX_RULES;

  function addRule() {
    if (capped) {
      return;
    }
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        classification: [
          ...rules,
          { code: "", handler: "", conditions: [{ attribute: "", operator: "eq", value: "" }] }
        ]
      }
    });
  }

  function updateRule(index: number, rule: ClassificationRule) {
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        classification: rules.map((current, currentIndex) => (currentIndex === index ? rule : current))
      }
    });
  }

  return (
    <>
      <div className="rule-card">
        <div className="rule-card-header classification">
          <span>#</span>
          <span>Name</span>
          <span>When</span>
          <span>Set</span>
          <span></span>
        </div>
        {rules.map((rule, ruleIndex) => (
          <div key={ruleIndex} className="rule-row classification">
            <span>{ruleIndex + 1}</span>
            <strong>{rule.code || "untitled"}</strong>
            <code className="rule-row-when">
              when {rule.conditions.length} condition{rule.conditions.length === 1 ? "" : "s"} AND
            </code>
            <div className="rule-set-cell">
              <Pill tone="warning">{rule.handler || "—"}</Pill>
              <Pill tone="accent">{rule.code || "—"}</Pill>
            </div>
            <div></div>
          </div>
        ))}
        <div className="rule-row classification rule-default-row">
          <span>—</span>
          <span>default (unmatched)</span>
          <code className="rule-default-when">*</code>
          <div className="rule-set-cell">
            <Pill tone="neutral">INFO</Pill>
            <Pill tone="neutral">unspecified</Pill>
          </div>
          <div></div>
        </div>
      </div>

      {rules.length === 0 ? null : <div style={{ height: 4 }} />}

      {rules.map((rule, ruleIndex) => (
        <div key={`editor-${ruleIndex}`} className="rule-editor">
          <div className="triple-row">
            <Field
              label={`Classification code ${ruleIndex + 1}`}
              error={errors[`operations.classification.${ruleIndex}.code`]}
            >
              <input
                className="input mono"
                value={rule.code}
                onChange={(event) => updateRule(ruleIndex, { ...rule, code: event.target.value })}
              />
            </Field>
            <Field label={`Severity set ${ruleIndex + 1}`}>
              <input
                className="input mono"
                value={rule.handler}
                onChange={(event) => updateRule(ruleIndex, { ...rule, handler: event.target.value })}
              />
            </Field>
          </div>
          <ConditionRows
            group="classification"
            ruleIndex={ruleIndex}
            conditions={rule.conditions}
            onChange={(conditions) => updateRule(ruleIndex, { ...rule, conditions })}
            onTarget={onTarget}
            errors={errors}
          />
        </div>
      ))}

      <Button
        onClick={addRule}
        variant="outline"
        icon={<Plus size={12} />}
        disabled={capped}
        aria-disabled={capped}
      >
        Add classification rule
      </Button>
      {capped ? <span className="muted">MVP limit: three explicit rules.</span> : null}
    </>
  );
}

function RoutingPanel({
  draft,
  errors,
  onChange,
  onTarget
}: {
  draft: Draft;
  errors: FieldErrors;
  onChange: (draft: Draft) => void;
  onTarget: (target: PickerTarget) => void;
}) {
  const rules = draft.operations.routing;
  const capped = rules.length >= MAX_RULES;

  function addRule() {
    if (capped) {
      return;
    }
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        routing: [
          ...rules,
          {
            handler: "",
            conditions: [{ attribute: "", operator: "eq", value: "" }],
            actions: [{ type: "notify", channel: "email", target: "" }]
          }
        ]
      }
    });
  }

  function updateRule(index: number, rule: RoutingRule) {
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        routing: rules.map((current, currentIndex) => (currentIndex === index ? rule : current))
      }
    });
  }

  return (
    <>
      <div className="rule-card">
        <div className="rule-card-header routing">
          <span>#</span>
          <span>Name</span>
          <span>When</span>
          <span>Action</span>
          <span></span>
        </div>
        {rules.map((rule, ruleIndex) => {
          const action = rule.actions[0];
          return (
            <div key={ruleIndex} className="rule-row routing">
              <span>{ruleIndex + 1}</span>
              <strong>{rule.handler || "untitled"}</strong>
              <code className="rule-row-when">
                when {rule.conditions.length} condition{rule.conditions.length === 1 ? "" : "s"} AND
              </code>
              <div className="rule-actions-cell">
                <Pill tone="accent" icon={<Zap size={11} />}>
                  notify
                </Pill>
                <Pill tone="neutral" icon={<Mail size={11} />}>
                  {action?.channel ?? "email"}
                </Pill>
                {action?.target ? <span className="code-chip">{action.target}</span> : null}
              </div>
              <div></div>
            </div>
          );
        })}
        <div className="rule-row routing rule-default-row">
          <span>—</span>
          <span>default (unmatched)</span>
          <code className="rule-default-when">*</code>
          <div className="rule-actions-cell">
            <Pill tone="neutral" icon={<Eye size={11} />}>
              visibility only
            </Pill>
          </div>
          <div></div>
        </div>
      </div>

      {rules.length === 0 ? null : <div style={{ height: 4 }} />}

      {rules.map((rule, ruleIndex) => (
        <div key={`editor-${ruleIndex}`} className="rule-editor">
          <div className="triple-row">
            <Field
              label={`Route name ${ruleIndex + 1}`}
              error={errors[`operations.routing.${ruleIndex}.handler`]}
            >
              <input
                className="input mono"
                value={rule.handler}
                onChange={(event) => updateRule(ruleIndex, { ...rule, handler: event.target.value })}
              />
            </Field>
          </div>
          <ConditionRows
            group="routing"
            ruleIndex={ruleIndex}
            conditions={rule.conditions}
            onChange={(conditions) => updateRule(ruleIndex, { ...rule, conditions })}
            onTarget={onTarget}
            errors={errors}
          />
          <ActionRows
            rule={rule}
            ruleIndex={ruleIndex}
            onChange={(actions) => updateRule(ruleIndex, { ...rule, actions })}
            errors={errors}
          />
        </div>
      ))}

      <Button
        onClick={addRule}
        variant="outline"
        icon={<Plus size={12} />}
        disabled={capped}
        aria-disabled={capped}
      >
        Add routing rule
      </Button>
      {capped ? <span className="muted">MVP limit: three explicit rules.</span> : null}
    </>
  );
}

function ConditionRows({
  group,
  ruleIndex,
  conditions,
  onChange,
  onTarget,
  errors
}: {
  group: RuleTab;
  ruleIndex: number;
  conditions: ClassificationRule["conditions"];
  onChange: (conditions: ClassificationRule["conditions"]) => void;
  onTarget: (target: PickerTarget) => void;
  errors: FieldErrors;
}) {
  return (
    <div className="stack">
      {conditions.map((condition, conditionIndex) => (
        <div key={conditionIndex} className="condition-row">
          <select
            className="select"
            aria-label={`${group} condition source ${ruleIndex + 1}.${conditionIndex + 1}`}
            defaultValue={condition.attribute.startsWith("/") ? "path" : "attr"}
          >
            <option value="path">path</option>
            <option value="attr">attr</option>
          </select>
          <input
            aria-label={`${group} condition attribute ${ruleIndex + 1}.${conditionIndex + 1}`}
            className={`input mono ${
              condition.attribute.startsWith("/") && !isValidJsonPointer(condition.attribute) ? "invalid" : ""
            }`}
            value={condition.attribute}
            onFocus={() => onTarget({ type: "condition", group, ruleIndex, conditionIndex })}
            onChange={(event) =>
              onChange(
                conditions.map((current, index) =>
                  index === conditionIndex ? { ...current, attribute: event.target.value } : current
                )
              )
            }
          />
          <select
            className="select"
            aria-label={`${group} condition operator ${ruleIndex + 1}.${conditionIndex + 1}`}
            value={condition.operator}
            onChange={(event) =>
              onChange(
                conditions.map((current, index) =>
                  index === conditionIndex ? { ...current, operator: event.target.value } : current
                )
              )
            }
          >
            {OPERATORS.map((operator) => (
              <option key={operator} value={operator}>
                {operator}
              </option>
            ))}
          </select>
          <input
            aria-label={`${group} condition value ${ruleIndex + 1}.${conditionIndex + 1}`}
            className="input mono"
            value={condition.value}
            onChange={(event) =>
              onChange(
                conditions.map((current, index) =>
                  index === conditionIndex ? { ...current, value: event.target.value } : current
                )
              )
            }
          />
          <button
            type="button"
            className="icon-button"
            onClick={() => onChange(conditions.filter((_, index) => index !== conditionIndex))}
            aria-label={`Remove ${group} condition ${ruleIndex + 1}.${conditionIndex + 1}`}
          >
            <X size={12} />
          </button>
          {errors[`operations.${group}.${ruleIndex}.conditions.${conditionIndex}.attribute`] ? (
            <span className="field-error">
              {errors[`operations.${group}.${ruleIndex}.conditions.${conditionIndex}.attribute`]}
            </span>
          ) : null}
        </div>
      ))}
      <Button
        onClick={() => onChange([...conditions, { attribute: "", operator: "eq", value: "" }])}
        variant="ghost"
        icon={<Plus size={12} />}
      >
        Add condition
      </Button>
    </div>
  );
}

function ActionRows({
  rule,
  ruleIndex,
  onChange,
  errors
}: {
  rule: RoutingRule;
  ruleIndex: number;
  onChange: (actions: RoutingAction[]) => void;
  errors: FieldErrors;
}) {
  return (
    <div className="stack">
      {rule.actions.map((action, actionIndex) => (
        <div key={actionIndex} className="triple-row">
          <Field label={`Action type ${actionIndex + 1}`}>
            <select
              className="select"
              value={action.type}
              onChange={(event) =>
                onChange(
                  rule.actions.map((current, index) =>
                    index === actionIndex ? { ...current, type: event.target.value } : current
                  )
                )
              }
            >
              <option value="notify">notify</option>
            </select>
          </Field>
          <Field label={`Channel ${actionIndex + 1}`}>
            <select
              className="select"
              value={action.channel}
              onChange={(event) =>
                onChange(
                  rule.actions.map((current, index) =>
                    index === actionIndex ? { ...current, channel: event.target.value } : current
                  )
                )
              }
            >
              <option value="email">email</option>
            </select>
          </Field>
          <Field
            label={`Target email ${actionIndex + 1}`}
            error={errors[`operations.routing.${ruleIndex}.actions.${actionIndex}.target`]}
          >
            <input
              className="input mono"
              value={action.target}
              onChange={(event) =>
                onChange(
                  rule.actions.map((current, index) =>
                    index === actionIndex ? { ...current, target: event.target.value } : current
                  )
                )
              }
            />
          </Field>
        </div>
      ))}
      <Button
        onClick={() => onChange([...rule.actions, { type: "notify", channel: "email", target: "" }])}
        variant="ghost"
        icon={<Plus size={12} />}
      >
        Add notify action
      </Button>
    </div>
  );
}

function ValidationSaveSection({
  draft,
  errors,
  sample
}: {
  draft: Draft;
  errors: FieldErrors;
  sample: SampleState;
}) {
  return (
    <section aria-labelledby="validation-save-heading" className="validation-summary-section">
      <div className="section-header-dense">
        <h2 id="validation-save-heading">Validation &amp; Save</h2>
        <span className="section-header-note">Required mappings, schema version, sample status</span>
      </div>
      <div className="summary-grid">
        <div className="summary-cell">
          <dt>Required canonical fields</dt>
          <dd>{countRequiredMappings(draft)} of 3 mapped</dd>
        </div>
        <div className="summary-cell">
          <dt>Config schema</dt>
          <dd>{draft.configSchemaVersion}</dd>
        </div>
        <div className="summary-cell">
          <dt>Sample persistence</dt>
          <dd>{sample.payloadText ? "local only" : "none"}</dd>
        </div>
        {Object.keys(errors).length > 0 ? (
          <div className="validation-reasons field-error">Fix highlighted fields before enabling.</div>
        ) : null}
      </div>
      <p className="muted mono">{identityLabel(draft)}</p>
    </section>
  );
}

function SamplePayloadCard({
  sample,
  paths,
  headers,
  pickerActive,
  onPayloadChange,
  onHeadersChange,
  onPick,
  onReplace
}: {
  sample: SampleState;
  paths: JsonPathNode[];
  headers: string[];
  pickerActive: boolean;
  onPayloadChange: (payload: string) => void;
  onHeadersChange: (headers: string) => void;
  onPick: (path: string) => void;
  onReplace: () => void;
}) {
  const isValid = sample.parsedPayload !== null && sample.payloadError === "";
  const isInvalid = sample.payloadText !== "" && Boolean(sample.payloadError);
  const showPre = isValid;

  return (
    <section className="sample-card" aria-labelledby="sample-payload-heading">
      <div className="sample-card-header">
        <h2 id="sample-payload-heading">Sample payload</h2>
        {isValid ? (
          <Pill tone="success" icon={<Check size={11} />}>
            valid
          </Pill>
        ) : null}
        {isInvalid ? <Pill tone="danger">invalid</Pill> : null}
        <div className="sample-card-header-spacer" />
        {sample.payloadText ? (
          <Button onClick={onReplace} variant="ghost">
            Replace
          </Button>
        ) : null}
      </div>

      {showPre ? (
        <pre className="sample-card-pre">{JSON.stringify(sample.parsedPayload, null, 2)}</pre>
      ) : (
        <div className="sample-card-textarea-host">
          <textarea
            aria-label="Sample JSON payload"
            className={`sample-card-textarea ${isInvalid ? "invalid" : ""}`}
            value={sample.payloadText}
            onChange={(event) => onPayloadChange(event.target.value)}
            placeholder={'{"severity":"CRITICAL","category":"billing"}'}
          />
          {sample.payloadError ? <span className="field-error">{sample.payloadError}</span> : null}
        </div>
      )}

      <div className="sample-card-headers">
        <h3>Headers</h3>
        <textarea
          aria-label="Sample headers"
          value={sample.headersText}
          onChange={(event) => onHeadersChange(event.target.value)}
          placeholder={"eventType=payment_failure\nsource=payments"}
        />
        {headers.length > 0 ? (
          <div className="path-chips">
            {headers.map((header) => (
              <button
                key={header}
                type="button"
                className="header-chip"
                onClick={() => onPick(header)}
                disabled={!pickerActive}
              >
                {header}
              </button>
            ))}
          </div>
        ) : null}
      </div>

      <div className="sample-card-paths">
        <h3>Discovered paths</h3>
        <div className="path-chips">
          {paths.length === 0 ? <span className="muted">Paste JSON to discover paths.</span> : null}
          {paths.map((node) => (
            <button
              key={node.path}
              type="button"
              draggable
              title="Drag into mapping, attributes, or rule operands"
              className="path-chip draggable"
              onClick={() => onPick(node.path)}
              disabled={!pickerActive}
            >
              <GripVertical size={10} aria-hidden="true" />
              {node.path}
            </button>
          ))}
        </div>
      </div>
    </section>
  );
}

function DryRunTraceCard({ draft, sample }: { draft: Draft; sample: SampleState }) {
  const firstClassification = draft.operations.classification[0];
  const firstRouting = draft.operations.routing[0];
  const firstAction = firstRouting?.actions[0];
  const stages: Array<{ key: string; label: string; value: string }> = [
    { key: "ingest", label: "Ingest", value: draft.topic || "source topic" },
    { key: "map", label: "Map", value: "canonical.v1" },
    { key: "classify", label: "Classify", value: firstClassification?.code || "pending" },
    { key: "route", label: "Route", value: firstAction?.channel || "pending" },
    { key: "deliver", label: "Deliver", value: stagesAreSatisfied(draft) ? "ok" : "pending" }
  ];
  const highlightIndex = firstClassification ? 2 : -1;

  const hasFire = Boolean(firstClassification && firstAction?.target);
  const severity = firstClassification?.handler ?? "INFO";
  const category = firstClassification?.code ?? "unspecified";

  return (
    <section className="dry-run-card" aria-labelledby="dry-run-trace-heading">
      <div className="dry-run-card-header">
        <h2 id="dry-run-trace-heading">Dry-run trace</h2>
        <Pill tone="accent">{sample.parsedPayload ? "uses pasted sample" : "no sample"}</Pill>
      </div>
      <div className="dry-run-stages">
        {stages.map((stage, index) => {
          const dim = stage.value === "pending";
          return (
            <div
              key={stage.key}
              className={`dry-run-stage ${index === highlightIndex ? "highlight" : ""}`}
            >
              <span className={`dry-run-stage-circle ${dim ? "dim" : ""}`}>{index + 1}</span>
              <span className="dry-run-stage-label">{stage.label}</span>
              <code className="dry-run-stage-value">{stage.value}</code>
              <Check size={12} className={`dry-run-stage-check ${dim ? "dim" : ""}`} aria-hidden="true" />
            </div>
          );
        })}
      </div>
      {hasFire ? (
        <div className="would-fire">
          <div className="would-fire-eyebrow">Would fire</div>
          <div className="would-fire-card">
            <div className="would-fire-line">
              <Mail size={12} aria-hidden="true" />
              <strong>Email · {firstAction?.target}</strong>
            </div>
            <div className="would-fire-detail">
              Subject — <strong>{draft.sourceEventType}</strong>
            </div>
            <div className="would-fire-detail">
              Severity — <strong className="severity">{severity}</strong> · Category — {category}
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}

function Field({
  label,
  error,
  children,
  dense
}: {
  label: string;
  error?: string;
  children: ReactNode;
  dense?: boolean;
}) {
  return (
    <label className={`field ${dense ? "field-dense" : ""}`}>
      <span>{label}</span>
      {children}
      {error ? <span className="field-error">{error}</span> : null}
    </label>
  );
}

function stagesAreSatisfied(draft: Draft) {
  return Boolean(
    draft.topic && draft.operations.classification[0] && draft.operations.routing[0]?.actions[0]?.target
  );
}

function validateDraft(draft: Draft): FieldErrors {
  const errors: FieldErrors = {};

  if (!draft.topic.trim()) {
    errors.topic = "Topic is required.";
  }

  if (!draft.sourceEventType.trim()) {
    errors.sourceEventType = "Source event type is required.";
  }

  if (!draft.discriminator.key.trim()) {
    errors["discriminator.key"] = "Discriminator key is required.";
  }

  collectPointerErrors(draft, errors);
  collectRuleErrors(draft, errors);
  return errors;
}

function collectPointerErrors(draft: Draft, errors: FieldErrors) {
  CANONICAL_FIELDS.forEach((field) => {
    const source = draft.mapping[field.key]?.source;
    if (source && !isValidJsonPointer(source)) {
      errors[`mapping.${field.key}.source`] = "Use a JSON Pointer like /severity.";
    }
  });

  draft.mapping.attributes.forEach((attribute, index) => {
    if (!attribute.targetAttribute.trim()) {
      errors[`mapping.attributes.${index}.targetAttribute`] = "Attribute key is required.";
    }
    if (attribute.source && !isValidJsonPointer(attribute.source)) {
      errors[`mapping.attributes.${index}.source`] = "Use a JSON Pointer like /customer/id.";
    }
  });

  draft.operations.promotedAttributes.forEach((attribute, index) => {
    if (!attribute.targetAttribute.trim()) {
      errors[`operations.promotedAttributes.${index}.targetAttribute`] = "Promoted name is required.";
    }
    if (attribute.sourceAttribute.startsWith("/") && !isValidJsonPointer(attribute.sourceAttribute)) {
      errors[`operations.promotedAttributes.${index}.sourceAttribute`] = "Use a JSON Pointer like /customer/id.";
    }
  });
}

function collectRuleErrors(draft: Draft, errors: FieldErrors) {
  draft.operations.routing.forEach((rule, ruleIndex) => {
    if (!rule.handler.trim()) {
      errors[`operations.routing.${ruleIndex}.handler`] = "Route name is required.";
    }
    rule.actions.forEach((action, actionIndex) => {
      if (action.channel === "email" && action.target && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(action.target)) {
        errors[`operations.routing.${ruleIndex}.actions.${actionIndex}.target`] = "Use a valid email target.";
      }
    });
  });
}

function validateEnable(config: Draft) {
  const reasons: string[] = [];

  if (!config.mapping.occurredAt?.source || !config.mapping.severity?.source || !config.mapping.category?.source) {
    reasons.push("Enabled source configs require occurredAt, severity, and category mappings before they can be enabled.");
  }

  if (!config.discriminator.source || !config.discriminator.key.trim()) {
    reasons.push("Save and Enable requires discriminator source and key.");
  }

  const validationErrors = validateDraft(config);
  if (Object.keys(validationErrors).length > 0) {
    reasons.push("Fix invalid mapping, rule, and action rows before enabling.");
  }

  return reasons;
}

function normalizeDraft(draft: Draft): Draft {
  return {
    ...draft,
    configSchemaVersion: 2,
    mapping: {
      ...draft.mapping,
      attributes: draft.mapping?.attributes ?? []
    },
    operations: {
      ...draft.operations,
      promotedAttributes: draft.operations?.promotedAttributes ?? [],
      classification: draft.operations?.classification ?? [],
      routing: draft.operations?.routing ?? []
    }
  };
}

function countRequiredMappings(config: Draft) {
  return [config.mapping.occurredAt?.source, config.mapping.severity?.source, config.mapping.category?.source].filter(Boolean).length;
}

function countUnsaved(draft: Draft, original: MappingConfig | undefined) {
  if (!original) {
    return 0;
  }
  let count = 0;
  if (draft.topic !== original.topic) count++;
  if (draft.sourceEventType !== original.sourceEventType) count++;
  if (draft.enabled !== original.enabled) count++;
  if (
    draft.discriminator.source !== original.discriminator.source ||
    draft.discriminator.key !== original.discriminator.key
  ) {
    count++;
  }
  for (const meta of CANONICAL_FIELDS) {
    if (JSON.stringify(draft.mapping[meta.key]) !== JSON.stringify(original.mapping?.[meta.key])) {
      count++;
    }
  }
  if (
    JSON.stringify(draft.mapping.attributes ?? []) !==
    JSON.stringify(original.mapping?.attributes ?? [])
  ) {
    count++;
  }
  if (
    JSON.stringify(draft.operations.promotedAttributes ?? []) !==
    JSON.stringify(original.operations?.promotedAttributes ?? [])
  ) {
    count++;
  }
  if (
    JSON.stringify(draft.operations.classification ?? []) !==
    JSON.stringify(original.operations?.classification ?? [])
  ) {
    count++;
  }
  if (
    JSON.stringify(draft.operations.routing ?? []) !== JSON.stringify(original.operations?.routing ?? [])
  ) {
    count++;
  }
  return count;
}

function configKey(config: Pick<MappingConfig, "topic" | "sourceEventType">) {
  return `${config.topic}::${config.sourceEventType}`;
}

function identityLabel(config: Pick<MappingConfig, "topic" | "sourceEventType">) {
  return `${config.topic || "topic"} / ${config.sourceEventType || "sourceEventType"}`;
}

function parseHeaderNames(headersText: string) {
  return headersText
    .split(/\r?\n/)
    .map((line) => line.split("=")[0]?.trim())
    .filter((header): header is string => Boolean(header));
}

function previewValue(samplePayload: unknown | null, pointer: string) {
  if (samplePayload === null) {
    return "not found";
  }

  return formatPointerValue(readJsonPointer(samplePayload, pointer));
}

function filterConfigs(configs: MappingConfig[], filterText: string) {
  const trimmed = filterText.trim().toLowerCase();
  if (!trimmed) {
    return configs;
  }
  return configs.filter((config) => {
    return (
      config.topic.toLowerCase().includes(trimmed) ||
      config.sourceEventType.toLowerCase().includes(trimmed)
    );
  });
}
