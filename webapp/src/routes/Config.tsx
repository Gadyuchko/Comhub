import { Plus, Save, X } from "lucide-react";
import { useEffect, useMemo, useState, type ReactNode } from "react";

import { problemFromUnknown } from "../api/source-configs";
import { useCreateSourceConfig, useSourceConfigs, useUpdateSourceConfig } from "../api/source-config-hooks";
import type { ApiProblem, ConfigDiscriminator, MappingConfig, SourceConfigRequest } from "../api/types";
import { InlineBanner } from "../components/InlineBanner";
import { QueryState } from "../components/QueryState";
import { useToast } from "../components/ToastProvider";
import { Button } from "../components/ui/button";

const CONFIG_EMPTY_COPY =
  "No source events configured yet. Create a source event to map, classify, and route Kafka records through ComHub.";

type Draft = SourceConfigRequest;
type DraftMap = Record<string, Draft>;
type FieldErrors = Record<string, string>;

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

  useEffect(() => {
    if (selectedKey === "" && configs.length > 0) {
      setSelectedKey(configKey(configs[0]));
    }
  }, [configs, drafts.new, selectedKey]);

  const selectedConfig = useMemo(
    () => configs.find((config) => configKey(config) === selectedKey),
    [configs, selectedKey]
  );

  const draft = drafts[selectedKey] ?? selectedConfig ?? emptyDraft;
  const selectedIsExisting = Boolean(selectedConfig);
  const hasSources = configs.length > 0;

  function selectConfig(config: MappingConfig) {
    const key = configKey(config);
    setSelectedKey(key);
    setDrafts((current) => ({ ...current, [key]: current[key] ?? config }));
    setFieldErrors({});
    setPageProblem(null);
  }

  function startNewSource() {
    setSelectedKey("new");
    setDrafts((current) => ({ ...current, new: current.new ?? emptyDraft }));
    setFieldErrors({});
    setPageProblem(null);
  }

  function updateDraft(nextDraft: Draft) {
    setDrafts((current) => ({ ...current, [selectedKey]: nextDraft }));
  }

  function cancelEdit() {
    setFieldErrors({});
    setPageProblem(null);
    setDrafts((current) => ({
      ...current,
      [selectedKey]: selectedConfig ?? emptyDraft
    }));
  }

  async function saveDraft() {
    setFieldErrors({});
    setPageProblem(null);

    const localErrors = validateDraft(draft);
    if (Object.keys(localErrors).length > 0) {
      setFieldErrors(localErrors);
      return;
    }

    if (draft.enabled && !hasRequiredMapping(draft)) {
      setPageProblem({
        status: 0,
        title: "Save blocked",
        detail: "Enabled source configs require occurredAt, severity, and category mappings before they can be enabled.",
        fieldErrors: {}
      });
      return;
    }

    try {
      const saved = selectedIsExisting
        ? await updateMutation.mutateAsync(draft)
        : await createMutation.mutateAsync(draft);
      const savedKey = configKey(saved);
      setSelectedKey(savedKey);
      setDrafts((current) => ({ ...current, [savedKey]: saved, new: emptyDraft }));
      toast.confirm("Source configuration saved.");
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

  const mutationPending = createMutation.isPending || updateMutation.isPending;
  const pageError = problemFromUnknown(configsQuery.error);

  return (
    <section className="page-frame">
      <div className="config-grid">
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
              <h1 className="panel-title">Source Basics</h1>
              <div className="muted mono">{selectedIsExisting ? identityLabel(draft) : "new source event"}</div>
            </div>
            <div className="source-item-row">
              <Button onClick={cancelEdit} icon={<X size={14} />} disabled={mutationPending}>
                Cancel
              </Button>
              <Button variant="primary" onClick={saveDraft} icon={<Save size={14} />} disabled={mutationPending}>
                Save
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
            <SourceBasicsEditor draft={draft} errors={fieldErrors} onChange={updateDraft} />
            <MappingSummary config={draft} existing={selectedIsExisting} />
          </div>
        </div>
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

type SourceListItemProps = {
  config: MappingConfig;
  selected: boolean;
  onClick: () => void;
};

function SourceListItem({ config, selected, onClick }: SourceListItemProps) {
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

type SourceBasicsEditorProps = {
  draft: Draft;
  errors: FieldErrors;
  onChange: (draft: Draft) => void;
};

function SourceBasicsEditor({ draft, errors, onChange }: SourceBasicsEditorProps) {
  const updateDiscriminator = (discriminator: ConfigDiscriminator) => onChange({ ...draft, discriminator });

  return (
    <div className="field-grid">
      <Field label="Topic" error={errors.topic}>
        <input
          className="input mono"
          value={draft.topic}
          onChange={(event) => onChange({ ...draft, topic: event.target.value })}
        />
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
        <input
          className="input mono"
          value={draft.discriminator.key}
          onChange={(event) => updateDiscriminator({ ...draft.discriminator, key: event.target.value })}
        />
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

function Field({ label, error, children }: { label: string; error?: string; children: ReactNode }) {
  return (
    <label className="field">
      <span>{label}</span>
      {children}
      {error ? <span className="field-error">{error}</span> : null}
    </label>
  );
}

function MappingSummary({ config, existing }: { config: Draft; existing: boolean }) {
  return (
    <section>
      <h2 className="panel-title">Mapping summary</h2>
      <p className="muted">
        {existing ? "Current mapping metadata for " : "Draft mapping metadata for "}
        <span className="mono">{identityLabel(config)}</span>
      </p>
      <dl className="summary-grid">
        <div className="summary-cell">
          <dt>Required canonical fields</dt>
          <dd>{countRequiredMappings(config)} of 3 mapped</dd>
        </div>
        <div className="summary-cell">
          <dt>Promoted attributes</dt>
          <dd>{config.operations.promotedAttributes.length}</dd>
        </div>
        <div className="summary-cell">
          <dt>Routing rules</dt>
          <dd>{config.operations.routing.length}</dd>
        </div>
      </dl>
    </section>
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

  return errors;
}

function hasRequiredMapping(config: Draft) {
  return Boolean(config.mapping.occurredAt?.source && config.mapping.severity?.source && config.mapping.category?.source);
}

function countRequiredMappings(config: Draft) {
  return [config.mapping.occurredAt?.source, config.mapping.severity?.source, config.mapping.category?.source].filter(Boolean)
    .length;
}

function isDirty(left: Draft, right: Draft) {
  return JSON.stringify(left) !== JSON.stringify(right);
}

function configKey(config: Pick<MappingConfig, "topic" | "sourceEventType">) {
  return `${config.topic}::${config.sourceEventType}`;
}

function identityLabel(config: Pick<MappingConfig, "topic" | "sourceEventType">) {
  return `${config.topic || "topic"} / ${config.sourceEventType || "sourceEventType"}`;
}
