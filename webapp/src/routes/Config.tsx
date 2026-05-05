import { Plus, Save, X, Zap } from "lucide-react";
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
import { Button } from "../components/ui/button";
import { buildJsonPathTree, formatPointerValue, isValidJsonPointer, readJsonPointer, type JsonPathNode } from "../lib/jsonPointer";

const CONFIG_EMPTY_COPY =
  "No source events configured yet. Create a source event to map, classify, and route Kafka records through ComHub.";
const CANONICAL_FIELDS = ["severity", "category", "occurredAt", "subject", "message"] as const;
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
type PickerTarget =
  | { type: "canonical"; field: (typeof CANONICAL_FIELDS)[number] }
  | { type: "attribute"; index: number }
  | { type: "promoted"; index: number }
  | { type: "condition"; group: "classification" | "routing"; ruleIndex: number; conditionIndex: number };

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
  const payloadPaths = sample.parsedPayload === null ? [] : buildJsonPathTree(sample.parsedPayload).filter((node) => node.path);
  const headerNames = parseHeaderNames(sample.headersText);

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

  function cancelEdit() {
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
      setSample((current) => ({ ...current, payloadText, parsedPayload: JSON.parse(payloadText), payloadError: "" }));
    } catch {
      setSample((current) => ({ ...current, payloadText, parsedPayload: null, payloadError: "Sample payload must be valid JSON." }));
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

  function updateConditionPath(group: "classification" | "routing", ruleIndex: number, conditionIndex: number, path: string) {
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

  return (
    <section className="page-frame">
      <div className="config-grid config-grid-dense">
        <SourceConfigList
          configs={configs}
          selectedKey={selectedKey}
          hasSources={hasSources}
          onSelect={selectConfig}
          onNew={startNewSource}
        />
        <div className="panel editor">
          <div className="panel-header">
            <div>
              <div className="eyebrow">Source events / {draft.sourceEventType || "new source"}</div>
              <h1 className="panel-title page-title">Mapping &amp; operations</h1>
              <div className="muted mono">{selectedIsExisting ? identityLabel(draft) : "new source event"}</div>
            </div>
            <div className="source-item-row">
              <Button onClick={cancelEdit} icon={<X size={14} />} disabled={mutationPending}>
                Cancel
              </Button>
              <Button onClick={() => void saveDraft()} icon={<Save size={14} />} disabled={mutationPending}>
                Save
              </Button>
              <Button variant="primary" onClick={() => void saveDraft({ enable: true })} icon={<Zap size={14} />} disabled={mutationPending}>
                Save and Enable
              </Button>
            </div>
          </div>
          <div className="editor-body">
            <QueryState
              isLoading={configsQuery.isLoading}
              isRefetching={configsQuery.isRefetching}
              isStale={configsQuery.isStale}
              isError={configsQuery.isError}
              errorMessage={pageError?.detail}
            />
            {pageProblem ? (
              <InlineBanner tone={pageProblem.status === 409 || pageProblem.status >= 500 ? "error" : "warning"} title={pageProblem.title}>
                {pageProblem.detail}
              </InlineBanner>
            ) : null}
            <EditorSection title="Source Basics">
              <SourceBasicsEditor draft={draft} errors={fieldErrors} onChange={updateDraft} />
            </EditorSection>
            <EditorSection title="Canonical Mapping">
              <CanonicalMappingCanvas
                draft={draft}
                errors={fieldErrors}
                samplePayload={sample.parsedPayload}
                onChange={updateDraft}
                onTarget={setPickerTarget}
              />
            </EditorSection>
            <EditorSection title="Promoted Attributes">
              <PromotedAttributesEditor draft={draft} errors={fieldErrors} onChange={updateDraft} onTarget={setPickerTarget} />
            </EditorSection>
            <EditorSection title="Classification">
              <ClassificationEditor draft={draft} errors={fieldErrors} onChange={updateDraft} onTarget={setPickerTarget} />
            </EditorSection>
            <EditorSection title="Routing">
              <RoutingEditor draft={draft} errors={fieldErrors} onChange={updateDraft} onTarget={setPickerTarget} />
            </EditorSection>
            <EditorSection title="Validation & Save">
              <ValidationSummary draft={draft} errors={fieldErrors} sample={sample} />
              <p className="muted mono">{identityLabel(draft)}</p>
            </EditorSection>
          </div>
        </div>
        <aside className="panel sample-rail">
          <EditorSection title="Sample Drop">
            <SampleDrop
              sample={sample}
              paths={payloadPaths}
              headers={headerNames}
              pickerActive={Boolean(pickerTarget)}
              onPayloadChange={updateSamplePayload}
              onHeadersChange={(headersText) => setSample((current) => ({ ...current, headersText }))}
              onPick={pickPath}
            />
          </EditorSection>
          <DryRunTrace draft={draft} sample={sample} />
        </aside>
      </div>
    </section>
  );
}

type SourceConfigListProps = {
  configs: MappingConfig[];
  selectedKey: string;
  hasSources: boolean;
  onSelect: (config: MappingConfig) => void;
  onNew: () => void;
};

function SourceConfigList({ configs, selectedKey, hasSources, onSelect, onNew }: SourceConfigListProps) {
  return (
    <aside className="panel source-list">
      <div className="panel-header">
        <h2 className="panel-title">Sources</h2>
        <Button onClick={onNew} icon={<Plus size={14} />}>
          New Source
        </Button>
      </div>
      <div className="source-list-body">
        {!hasSources ? <p className="muted">{CONFIG_EMPTY_COPY}</p> : null}
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

function SourceListItem({ config, selected, onClick }: { config: MappingConfig; selected: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      className={`source-item ${selected ? "selected" : ""} ${config.enabled ? "" : "disabled-source"}`}
      onClick={onClick}
    >
      <span className="source-item-row">
        <span className={`status-dot ${config.enabled ? "" : "disabled"}`} aria-hidden="true" />
        <strong>{config.sourceEventType}</strong>
        <span className="source-item-time">not observed</span>
      </span>
      <span className="source-topic">{config.topic}</span>
      <span className="muted">
        {config.enabled ? "enabled" : "disabled"} · {config.discriminator.source}:{config.discriminator.key || "not set"}
      </span>
    </button>
  );
}

function EditorSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="editor-section" aria-labelledby={sectionId(title)}>
      <h2 id={sectionId(title)} className="section-title">
        {title}
      </h2>
      {children}
    </section>
  );
}

function SourceBasicsEditor({ draft, errors, onChange }: { draft: Draft; errors: FieldErrors; onChange: (draft: Draft) => void }) {
  const updateDiscriminator = (discriminator: ConfigDiscriminator) => onChange({ ...draft, discriminator });

  return (
    <div className="field-grid">
      <Field label="Topic" error={errors.topic}>
        <input className="input mono" value={draft.topic} onChange={(event) => onChange({ ...draft, topic: event.target.value })} />
      </Field>
      <Field label="Source event type" error={errors.sourceEventType}>
        <input
          className="input mono"
          value={draft.sourceEventType}
          onChange={(event) => onChange({ ...draft, sourceEventType: event.target.value })}
        />
      </Field>
      <Field label="Discriminator source" error={errors["discriminator.source"]}>
        <select
          className="select"
          value={draft.discriminator.source}
          onChange={(event) =>
            updateDiscriminator({ ...draft.discriminator, source: event.target.value as ConfigDiscriminator["source"] })
          }
        >
          <option value="header">header</option>
          <option value="payload">payload</option>
        </select>
      </Field>
      <Field label="Discriminator key" error={errors["discriminator.key"]}>
        <input className="input mono" value={draft.discriminator.key} onChange={(event) => updateDiscriminator({ ...draft.discriminator, key: event.target.value })} />
      </Field>
      <div className="field">
        <span className="field-label">Enabled</span>
        <div className="toggle-row">
          <button
            className={`toggle ${draft.enabled ? "on" : ""}`}
            type="button"
            role="switch"
            aria-checked={draft.enabled}
            aria-label="Enabled"
            onClick={() => onChange({ ...draft, enabled: !draft.enabled })}
          >
            <span />
          </button>
          <span>{draft.enabled ? "enabled" : "disabled"}</span>
        </div>
      </div>
    </div>
  );
}

function SampleDrop({
  sample,
  paths,
  headers,
  pickerActive,
  onPayloadChange,
  onHeadersChange,
  onPick
}: {
  sample: SampleState;
  paths: JsonPathNode[];
  headers: string[];
  pickerActive: boolean;
  onPayloadChange: (payload: string) => void;
  onHeadersChange: (headers: string) => void;
  onPick: (path: string) => void;
}) {
  return (
    <div className="sample-grid">
      <Field label="Sample JSON payload" error={sample.payloadError}>
        <textarea
          className="textarea mono"
          value={sample.payloadText}
          onChange={(event) => onPayloadChange(event.target.value)}
          rows={8}
          placeholder='{"severity":"CRITICAL","category":"billing"}'
        />
      </Field>
      <Field label="Sample headers">
        <textarea
          className="textarea mono"
          value={sample.headersText}
          onChange={(event) => onHeadersChange(event.target.value)}
          rows={8}
          placeholder={"eventType=payment_failure\nsource=payments"}
        />
      </Field>
      <div>
        <h3 className="subsection-title">Payload Paths</h3>
        <PathList empty="Paste valid JSON to inspect paths." paths={paths} pickerActive={pickerActive} onPick={onPick} />
      </div>
      <div>
        <h3 className="subsection-title">Headers</h3>
        <div className="path-list">
          {headers.length === 0 ? <span className="muted">Add headers as key=value lines.</span> : null}
          {headers.map((header) => (
            <button key={header} type="button" className="path-chip mono" onClick={() => onPick(header)} disabled={!pickerActive}>
              {header}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function CanonicalMappingCanvas({
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
  function setCanonical(field: (typeof CANONICAL_FIELDS)[number], source: string) {
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
        attributes: draft.mapping.attributes.map((current, currentIndex) => (currentIndex === index ? attribute : current))
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

  return (
    <div className="mapping-canvas">
      {CANONICAL_FIELDS.map((field) => {
        const source = draft.mapping[field]?.source ?? "";
        return (
          <div key={field} className={`mapping-row ${field === "severity" || field === "category" ? "emphasis-row" : ""}`}>
            <div>
              <FieldLabel label={field} help={canonicalHelp(field)} />
              <div className="chip-summary mono">{field} -&gt; {source || "source path"}</div>
            </div>
            <div>
              <input
                aria-label={`${field} source path`}
                className={`input mono ${source && !isValidJsonPointer(source) ? "invalid" : ""}`}
                value={source}
                onFocus={() => onTarget({ type: "canonical", field })}
                onChange={(event) => setCanonical(field, event.target.value)}
              />
              {source && !isValidJsonPointer(source) ? <span className="field-error">Use a JSON Pointer like /severity.</span> : null}
            </div>
            <code className="preview-value">{source ? previewValue(samplePayload, source) : "not found"}</code>
          </div>
        );
      })}
      <div className="metadata-grid" aria-label="System metadata">
        {SYSTEM_FIELDS.map((field) => (
          <div key={field} className="metadata-cell">
            <span>{field}</span>
            <strong>system populated</strong>
          </div>
        ))}
      </div>
      <div className="row-editor-header">
        <h3 className="subsection-title">Attributes</h3>
        <Button onClick={addAttribute} icon={<Plus size={14} />}>
          Add attribute
        </Button>
      </div>
      {draft.mapping.attributes.map((attribute, index) => (
        <div key={index} className="mapping-row">
          <Field label={`Attribute key ${index + 1}`} error={errors[`mapping.attributes.${index}.targetAttribute`]}>
            <input
              className="input mono"
              value={attribute.targetAttribute}
              onChange={(event) => updateAttribute(index, { ...attribute, targetAttribute: event.target.value })}
            />
          </Field>
          <Field label={`Attribute path ${index + 1}`} error={errors[`mapping.attributes.${index}.source`]}>
            <input
              className={`input mono ${attribute.source && !isValidJsonPointer(attribute.source) ? "invalid" : ""}`}
              value={attribute.source}
              onFocus={() => onTarget({ type: "attribute", index })}
              onChange={(event) => updateAttribute(index, { ...attribute, source: event.target.value })}
            />
          </Field>
          <code className="preview-value">{attribute.source ? previewValue(samplePayload, attribute.source) : "not found"}</code>
          <Button onClick={() => removeAttribute(index)} icon={<X size={14} />}>
            Remove
          </Button>
        </div>
      ))}
    </div>
  );
}

function PromotedAttributesEditor({
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
  function addPromoted() {
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        promotedAttributes: [...draft.operations.promotedAttributes, { sourceAttribute: "", targetAttribute: "" }]
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

  return (
    <div className="stack">
      <Button onClick={addPromoted} icon={<Plus size={14} />}>
        Add promoted attribute
      </Button>
      {draft.operations.promotedAttributes.map((promoted, index) => (
        <div key={index} className="triple-row">
          <Field label={`Name ${index + 1}`} error={errors[`operations.promotedAttributes.${index}.targetAttribute`]}>
            <input className="input mono" value={promoted.targetAttribute} onChange={(event) => updatePromoted(index, { ...promoted, targetAttribute: event.target.value })} />
          </Field>
          <Field label={`Path ${index + 1}`} error={errors[`operations.promotedAttributes.${index}.sourceAttribute`]}>
            <input
              className={`input mono ${promoted.sourceAttribute && !isValidJsonPointer(promoted.sourceAttribute) ? "invalid" : ""}`}
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
          <Button
            onClick={() =>
              onChange({
                ...draft,
                operations: {
                  ...draft.operations,
                  promotedAttributes: draft.operations.promotedAttributes.filter((_, currentIndex) => currentIndex !== index)
                }
              })
            }
            icon={<X size={14} />}
          >
            Remove
          </Button>
        </div>
      ))}
    </div>
  );
}

function ClassificationEditor({
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
  function addRule() {
    if (draft.operations.classification.length >= MAX_RULES) {
      return;
    }

    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        classification: [...draft.operations.classification, { code: "", handler: "", conditions: [{ attribute: "", operator: "eq", value: "" }] }]
      }
    });
  }

  function updateRule(index: number, rule: ClassificationRule) {
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        classification: draft.operations.classification.map((current, currentIndex) => (currentIndex === index ? rule : current))
      }
    });
  }

  return (
    <RuleStack
      kind="classification"
      capped={draft.operations.classification.length >= MAX_RULES}
      onAdd={addRule}
    >
      {draft.operations.classification.map((rule, ruleIndex) => (
        <div key={ruleIndex} className="rule-block">
          <div className="triple-row">
            <Field label={`Classification code ${ruleIndex + 1}`} error={errors[`operations.classification.${ruleIndex}.code`]}>
              <input className="input mono" value={rule.code} onChange={(event) => updateRule(ruleIndex, { ...rule, code: event.target.value })} />
            </Field>
            <Field label={`Severity set ${ruleIndex + 1}`}>
              <input className="input mono" value={rule.handler} onChange={(event) => updateRule(ruleIndex, { ...rule, handler: event.target.value })} />
            </Field>
            <div className="chip-summary mono">when {rule.conditions.length} condition{rule.conditions.length === 1 ? "" : "s"} AND</div>
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
      <Field label="Default classification_code">
        <input className="input mono" placeholder="UNCLASSIFIED" aria-label="Default classification_code" />
      </Field>
    </RuleStack>
  );
}

function RoutingEditor({
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
  function addRule() {
    if (draft.operations.routing.length >= MAX_RULES) {
      return;
    }

    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        routing: [
          ...draft.operations.routing,
          { handler: "", conditions: [{ attribute: "", operator: "eq", value: "" }], actions: [{ type: "notify", channel: "email", target: "" }] }
        ]
      }
    });
  }

  function updateRule(index: number, rule: RoutingRule) {
    onChange({
      ...draft,
      operations: {
        ...draft.operations,
        routing: draft.operations.routing.map((current, currentIndex) => (currentIndex === index ? rule : current))
      }
    });
  }

  return (
    <RuleStack kind="routing" capped={draft.operations.routing.length >= MAX_RULES} onAdd={addRule}>
      {draft.operations.routing.map((rule, ruleIndex) => (
        <div key={ruleIndex} className="rule-block">
          <div className="triple-row">
            <Field label={`Route name ${ruleIndex + 1}`} error={errors[`operations.routing.${ruleIndex}.handler`]}>
              <input className="input mono" value={rule.handler} onChange={(event) => updateRule(ruleIndex, { ...rule, handler: event.target.value })} />
            </Field>
            <div className="chip-summary mono">{rule.handler || "route"} · {rule.actions.length} action{rule.actions.length === 1 ? "" : "s"}</div>
          </div>
          <ConditionRows
            group="routing"
            ruleIndex={ruleIndex}
            conditions={rule.conditions}
            onChange={(conditions) => updateRule(ruleIndex, { ...rule, conditions })}
            onTarget={onTarget}
            errors={errors}
          />
          <ActionRows rule={rule} ruleIndex={ruleIndex} onChange={(actions) => updateRule(ruleIndex, { ...rule, actions })} errors={errors} />
        </div>
      ))}
      <div className="rule-block">
        <Field label="Default route name">
          <input className="input mono" placeholder="default-email" aria-label="Default route name" />
        </Field>
        <ActionRows rule={{ handler: "default", conditions: [], actions: [{ type: "notify", channel: "email", target: "" }] }} ruleIndex={-1} onChange={() => undefined} errors={{}} />
      </div>
    </RuleStack>
  );
}

function RuleStack({ kind, capped, onAdd, children }: { kind: string; capped: boolean; onAdd: () => void; children: ReactNode }) {
  return (
    <div className="stack">
      <Button onClick={onAdd} icon={<Plus size={14} />} disabled={capped}>
        Add {kind} rule
      </Button>
      {capped ? <span className="muted">MVP limit: three explicit rules.</span> : null}
      {children}
    </div>
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
  group: "classification" | "routing";
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
            className={`input mono ${condition.attribute.startsWith("/") && !isValidJsonPointer(condition.attribute) ? "invalid" : ""}`}
            value={condition.attribute}
            onFocus={() => onTarget({ type: "condition", group, ruleIndex, conditionIndex })}
            onChange={(event) =>
              onChange(conditions.map((current, index) => (index === conditionIndex ? { ...current, attribute: event.target.value } : current)))
            }
          />
          <select
            className="select"
            aria-label={`${group} condition operator ${ruleIndex + 1}.${conditionIndex + 1}`}
            value={condition.operator}
            onChange={(event) =>
              onChange(conditions.map((current, index) => (index === conditionIndex ? { ...current, operator: event.target.value } : current)))
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
              onChange(conditions.map((current, index) => (index === conditionIndex ? { ...current, value: event.target.value } : current)))
            }
          />
          <Button onClick={() => onChange(conditions.filter((_, index) => index !== conditionIndex))} icon={<X size={14} />}>
            Remove
          </Button>
          {errors[`operations.${group}.${ruleIndex}.conditions.${conditionIndex}.attribute`] ? (
            <span className="field-error">{errors[`operations.${group}.${ruleIndex}.conditions.${conditionIndex}.attribute`]}</span>
          ) : null}
        </div>
      ))}
      <Button onClick={() => onChange([...conditions, { attribute: "", operator: "eq", value: "" }])} icon={<Plus size={14} />}>
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
              onChange={(event) => onChange(rule.actions.map((current, index) => (index === actionIndex ? { ...current, type: event.target.value } : current)))}
            >
              <option value="notify">notify</option>
            </select>
          </Field>
          <Field label={`Channel ${actionIndex + 1}`}>
            <select
              className="select"
              value={action.channel}
              onChange={(event) => onChange(rule.actions.map((current, index) => (index === actionIndex ? { ...current, channel: event.target.value } : current)))}
            >
              <option value="email">email</option>
            </select>
          </Field>
          <Field label={`Target email ${actionIndex + 1}`} error={errors[`operations.routing.${ruleIndex}.actions.${actionIndex}.target`]}>
            <input
              className="input mono"
              value={action.target}
              onChange={(event) => onChange(rule.actions.map((current, index) => (index === actionIndex ? { ...current, target: event.target.value } : current)))}
            />
          </Field>
        </div>
      ))}
      {ruleIndex >= 0 ? (
        <Button onClick={() => onChange([...rule.actions, { type: "notify", channel: "email", target: "" }])} icon={<Plus size={14} />}>
          Add notify action
        </Button>
      ) : null}
    </div>
  );
}

function ValidationSummary({ draft, errors, sample }: { draft: Draft; errors: FieldErrors; sample: SampleState }) {
  return (
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
      {Object.keys(errors).length > 0 ? <div className="validation-reasons field-error">Fix highlighted fields before enabling.</div> : null}
    </div>
  );
}

function DryRunTrace({ draft, sample }: { draft: Draft; sample: SampleState }) {
  const stages = [
    { label: "Ingest", value: draft.topic || "source topic" },
    { label: "Map", value: "canonical.v1" },
    { label: "Classify", value: draft.operations.classification[0]?.code || "pending" },
    { label: "Route", value: draft.operations.routing[0]?.actions[0]?.channel || "pending" },
    { label: "Sample", value: sample.parsedPayload === null ? "not loaded" : "local only" }
  ];

  return (
    <EditorSection title="Dry-run trace">
      <div className="trace-list">
        {stages.map((stage, index) => (
          <div key={stage.label} className="trace-row">
            <span className="trace-index mono">{index + 1}</span>
            <span>{stage.label}</span>
            <code>{stage.value}</code>
          </div>
        ))}
      </div>
    </EditorSection>
  );
}

function Field({ label, error, children }: { label: string; error?: string; children: ReactNode }) {
  return (
    <label className="field">
      <span>{label}</span>
      {children}
      {error ? <span className="field-error">{error}</span> : null}
    </label>
  );
}

function FieldLabel({ label, help }: { label: string; help: string }) {
  return (
    <span className="field-label tooltip-host" tabIndex={0}>
      {label}
      <span role="tooltip" className="tooltip">
        {help}
      </span>
    </span>
  );
}

function PathList({
  empty,
  paths,
  pickerActive,
  onPick
}: {
  empty: string;
  paths: JsonPathNode[];
  pickerActive: boolean;
  onPick: (path: string) => void;
}) {
  return (
    <div className="path-list">
      {paths.length === 0 ? <span className="muted">{empty}</span> : null}
      {paths.map((node) => (
        <button key={node.path} type="button" className="path-chip mono" onClick={() => onPick(node.path)} disabled={!pickerActive}>
          {node.path}
        </button>
      ))}
    </div>
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
    const source = draft.mapping[field]?.source;
    if (source && !isValidJsonPointer(source)) {
      errors[`mapping.${field}.source`] = "Use a JSON Pointer like /severity.";
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

function canonicalHelp(field: string) {
  const descriptions: Record<string, string> = {
    severity: "Operational severity used by classification, routing, and dashboards.",
    category: "Stable category used to group related source events.",
    occurredAt: "Event timestamp from the source payload.",
    subject: "Short human-readable event subject.",
    message: "Longer event message rendered in operations views."
  };
  return descriptions[field] ?? "Canonical event field.";
}

function sectionId(title: string) {
  return title.toLowerCase().replace(/[^a-z0-9]+/g, "-");
}
