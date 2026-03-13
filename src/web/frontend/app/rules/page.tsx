"use client";

import { useEffect, useMemo, useState } from "react";
import {
  getCurrentRule,
  getModelSettings,
  getRuleVersions,
  publishRuleVersion,
  saveRuleDraft,
  streamRuleDraftPreview,
  type CopilotExecutionSummary,
  type CopilotLogPayload,
  type ModelSettingsResponse,
  type RulePayload,
  type RuleSource,
  type RuleStatus,
  type RuleVersion,
  type RuleVersionListQuery,
} from "@/lib/api";
import { usePaginatedList } from "@/lib/use-paginated-list";
import {
  ALLOWED_RULE_OPERATORS,
  buildRuleDiffSummary,
  formatDateTime,
  toGuidedError,
  validateRuleDraft,
} from "@/lib/view-models";
import { ActionButton, EmptyState, InlineNotice, SectionHeader, StatusBadge, toneFromKeyword } from "@/components/ui";

interface NoticeState {
  tone: "info" | "success" | "warning" | "error";
  message: string;
}

type DraftViewMode = "issues" | "modified" | "all";
const VERSIONS_PAGE_SIZE = 30;
const DRAFT_ROW_HEIGHT = 272;
const DRAFT_VIEWPORT_HEIGHT = 640;
const MAX_COPILOT_PROMPT_CHARS = 2000;

const VERSION_STATUS_OPTIONS: Array<{ label: string; value: "all" | RuleStatus }> = [
  { label: "全部状态", value: "all" },
  { label: "published", value: "published" },
  { label: "draft", value: "draft" },
  { label: "archived", value: "archived" },
];

const VERSION_SOURCE_OPTIONS: Array<{ label: string; value: "all" | RuleSource }> = [
  { label: "全部来源", value: "all" },
  { label: "manual", value: "manual" },
  { label: "llm", value: "llm" },
  { label: "seed", value: "seed" },
];

function createEmptyRuleRow(): RulePayload["field_rules"][number] {
  return {
    field: "",
    operator: "eq",
    is_hard: false,
    operator_confidence: 0.8,
    hardness_confidence: 0.8,
    rationale: "",
  };
}

function clonePayload(payload: RulePayload): RulePayload {
  return {
    field_rules: payload.field_rules.map((row) => ({
      field: row.field,
      operator: row.operator,
      is_hard: row.is_hard,
      operator_confidence: row.operator_confidence,
      hardness_confidence: row.hardness_confidence,
      rationale: row.rationale ?? "",
    })),
  };
}

function parsePayloadSnapshot(snapshot: string): RulePayload {
  try {
    const parsed = JSON.parse(snapshot) as RulePayload;
    if (!parsed || !Array.isArray(parsed.field_rules)) {
      return { field_rules: [] };
    }
    return clonePayload(parsed);
  } catch {
    return { field_rules: [] };
  }
}

function normalizeVersions(rows: RuleVersion[]) {
  return [...rows].sort((a, b) => b.version_number - a.version_number);
}

function rowSignature(row: RulePayload["field_rules"][number] | undefined): string {
  if (!row) {
    return "__EMPTY__";
  }
  return JSON.stringify({
    field: row.field.trim(),
    operator: row.operator,
    is_hard: row.is_hard,
    operator_confidence: row.operator_confidence,
    hardness_confidence: row.hardness_confidence,
    rationale: (row.rationale ?? "").trim(),
  });
}

function countValidationItems(report: Record<string, unknown>, key: "errors" | "warnings"): number {
  const value = report[key];
  if (Array.isArray(value)) {
    return value.length;
  }
  return 0;
}

function draftModeLabel(mode: DraftViewMode): string {
  if (mode === "issues") return "仅错误/警告";
  if (mode === "modified") return "仅已修改";
  return "全部";
}

function asRecord(input: unknown): Record<string, unknown> | null {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return null;
  }
  return input as Record<string, unknown>;
}

function asString(input: unknown): string | null {
  return typeof input === "string" ? input : null;
}

function asNumber(input: unknown): number | null {
  if (typeof input !== "number" || Number.isNaN(input)) {
    return null;
  }
  return input;
}

function parseCopilotExecutionSummary(input: unknown): CopilotExecutionSummary | null {
  const payload = asRecord(input);
  if (!payload) return null;
  const stepName = asString(payload.step_name);
  const finalStatus = asString(payload.final_status);
  if (!stepName || (finalStatus !== "succeeded" && finalStatus !== "failed")) {
    return null;
  }

  const eventCounts = asRecord(payload.stream_event_counts) ?? {};
  const streamEventCounts: Record<string, number> = {};
  for (const [key, value] of Object.entries(eventCounts)) {
    const number = asNumber(value);
    if (number != null) {
      streamEventCounts[key] = number;
    }
  }
  const statusEvents = Array.isArray(payload.status_events)
    ? payload.status_events.filter((item): item is string => typeof item === "string")
    : [];

  return {
    step_name: stepName,
    request_started_at: asString(payload.request_started_at),
    request_finished_at: asString(payload.request_finished_at),
    duration_ms: asNumber(payload.duration_ms),
    final_status: finalStatus,
    response_received: Boolean(payload.response_received),
    fallback_used: Boolean(payload.fallback_used),
    failure_message: asString(payload.failure_message),
    reasoning_summary: asString(payload.reasoning_summary),
    reasoning_chars: asNumber(payload.reasoning_chars) ?? 0,
    stream_event_counts: streamEventCounts,
    status_events: statusEvents,
  };
}

export default function RulesPage() {
  const [draft, setDraft] = useState<RulePayload>({ field_rules: [] });
  const [editorSourceVersionId, setEditorSourceVersionId] = useState<string | null>(null);
  const [baselineSnapshot, setBaselineSnapshot] = useState<string>(JSON.stringify({ field_rules: [] }));
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<NoticeState>({ tone: "info", message: "正在加载规则版本..." });
  const [currentPublished, setCurrentPublished] = useState<RuleVersion | null>(null);
  const [editorInitialized, setEditorInitialized] = useState(false);

  const [draftMode, setDraftMode] = useState<DraftViewMode>("issues");
  const [draftScrollTop, setDraftScrollTop] = useState(0);
  const [rationaleEditorIndex, setRationaleEditorIndex] = useState<number | null>(null);
  const [rationaleEditorValue, setRationaleEditorValue] = useState("");
  const [expandedVersionIds, setExpandedVersionIds] = useState<Set<string>>(() => new Set());

  const [versionStatusFilter, setVersionStatusFilter] = useState<"all" | RuleStatus>("all");
  const [versionSourceFilter, setVersionSourceFilter] = useState<"all" | RuleSource>("all");
  const [versionKeyword, setVersionKeyword] = useState("");
  const [modelSettings, setModelSettings] = useState<ModelSettingsResponse | null>(null);
  const [copilotPrompt, setCopilotPrompt] = useState("");
  const [copilotReasoning, setCopilotReasoning] = useState("");
  const [copilotExecution, setCopilotExecution] = useState<CopilotExecutionSummary | null>(null);
  const [pendingCopilotLog, setPendingCopilotLog] = useState<CopilotLogPayload | null>(null);

  const versionQuery = useMemo<RuleVersionListQuery>(
    () => ({
      status: versionStatusFilter === "all" ? undefined : versionStatusFilter,
      source: versionSourceFilter === "all" ? undefined : versionSourceFilter,
      q: versionKeyword.trim() || undefined,
    }),
    [versionKeyword, versionSourceFilter, versionStatusFilter]
  );

  const {
    rows: versionsRaw,
    loading: versionsLoading,
    loadingMore: versionsLoadingMore,
    error: versionsError,
    hasMore: versionsHasMore,
    empty: versionsEmpty,
    reload: refreshVersions,
    loadMore: loadMoreVersions,
  } = usePaginatedList<RuleVersion, RuleVersionListQuery>({
    query: versionQuery,
    pageSize: VERSIONS_PAGE_SIZE,
    fetchPage: getRuleVersions,
  });

  const versions = useMemo(() => normalizeVersions(versionsRaw), [versionsRaw]);
  const published = useMemo(
    () => versions.find((row) => row.status === "published") ?? currentPublished,
    [currentPublished, versions]
  );

  const validation = useMemo(() => validateRuleDraft(draft), [draft]);
  const draftSnapshot = useMemo(() => JSON.stringify(draft), [draft]);
  const isDirty = draftSnapshot !== baselineSnapshot;
  const baselineDraft = useMemo(() => parsePayloadSnapshot(baselineSnapshot), [baselineSnapshot]);

  const rowIssues = useMemo(() => {
    const mapping = new Map<number, { errors: string[]; warnings: string[] }>();
    for (const issue of validation.errors) {
      const current = mapping.get(issue.row) ?? { errors: [], warnings: [] };
      current.errors.push(issue.message);
      mapping.set(issue.row, current);
    }
    for (const issue of validation.warnings) {
      const current = mapping.get(issue.row) ?? { errors: [], warnings: [] };
      current.warnings.push(issue.message);
      mapping.set(issue.row, current);
    }
    return mapping;
  }, [validation.errors, validation.warnings]);

  const modifiedRowIndices = useMemo(() => {
    const result = new Set<number>();
    const baselineRows = baselineDraft.field_rules;
    const maxLength = Math.max(draft.field_rules.length, baselineRows.length);
    for (let index = 0; index < maxLength; index += 1) {
      const current = draft.field_rules[index];
      if (!current) {
        continue;
      }
      const changed = rowSignature(current) !== rowSignature(baselineRows[index]);
      if (changed) {
        result.add(index);
      }
    }
    return result;
  }, [baselineDraft.field_rules, draft.field_rules]);

  const visibleRowIndices = useMemo(() => {
    const indices = draft.field_rules.map((_, index) => index);
    if (draftMode === "issues") {
      return indices.filter((index) => {
        const issues = rowIssues.get(index);
        return Boolean(issues && (issues.errors.length > 0 || issues.warnings.length > 0));
      });
    }
    if (draftMode === "modified") {
      return indices.filter((index) => modifiedRowIndices.has(index));
    }
    return indices;
  }, [draft.field_rules, draftMode, modifiedRowIndices, rowIssues]);

  const startIndex = Math.max(0, Math.floor(draftScrollTop / DRAFT_ROW_HEIGHT) - 4);
  const endIndex = Math.min(
    visibleRowIndices.length,
    Math.ceil((draftScrollTop + DRAFT_VIEWPORT_HEIGHT) / DRAFT_ROW_HEIGHT) + 4
  );
  const windowedIndices = visibleRowIndices.slice(startIndex, endIndex);
  const topSpacerHeight = startIndex * DRAFT_ROW_HEIGHT;
  const bottomSpacerHeight = Math.max(0, (visibleRowIndices.length - endIndex) * DRAFT_ROW_HEIGHT);

  const shouldShowDefaultVersionSlice =
    versionStatusFilter === "all" && versionSourceFilter === "all" && versionKeyword.trim().length === 0;
  const displayVersions = useMemo(() => {
    if (!shouldShowDefaultVersionSlice) {
      return versions;
    }
    const recent = versions.slice(0, 12);
    if (!published) {
      return recent;
    }
    if (recent.some((row) => row.id === published.id)) {
      return recent;
    }
    return [published, ...recent];
  }, [published, shouldShowDefaultVersionSlice, versions]);

  useEffect(() => {
    (async () => {
      const current = await getCurrentRule().catch(() => null);
      setCurrentPublished(current);
      if (current) {
        setNotice({ tone: "success", message: `已加载当前发布版本 v${current.version_number}` });
      } else {
        setNotice({ tone: "warning", message: "当前没有已发布版本，请先生成或保存草稿后发布。" });
      }
    })();
  }, []);

  useEffect(() => {
    void getModelSettings()
      .then((payload) => setModelSettings(payload))
      .catch(() => null);
  }, []);

  useEffect(() => {
    if (versionsError) {
      setNotice({ tone: "error", message: `原因：${versionsError}。下一步：确认 backend 服务后重试` });
    }
  }, [versionsError]);

  useEffect(() => {
    if (editorInitialized) {
      return;
    }
    const seed = currentPublished ?? versions[0] ?? null;
    if (!seed) {
      return;
    }
    const cloned = clonePayload(seed.payload);
    setDraft(cloned);
    setEditorSourceVersionId(seed.id);
    setBaselineSnapshot(JSON.stringify(cloned));
    setEditorInitialized(true);
  }, [currentPublished, editorInitialized, versions]);

  useEffect(() => {
    const handler = (event: BeforeUnloadEvent) => {
      if (!isDirty) return;
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [isDirty]);

  useEffect(() => {
    setDraftScrollTop(0);
  }, [draftMode, visibleRowIndices.length]);

  function updateRow(index: number, key: keyof RulePayload["field_rules"][number], value: string | boolean | number) {
    setDraft((prev) => {
      const rows = [...prev.field_rules];
      rows[index] = { ...rows[index], [key]: value };
      return { field_rules: rows };
    });
  }

  function loadVersionInEditor(version: RuleVersion) {
    const cloned = clonePayload(version.payload);
    setDraft(cloned);
    setEditorSourceVersionId(version.id);
    setBaselineSnapshot(JSON.stringify(cloned));
    setPendingCopilotLog(version.copilot_log ?? null);
    setCopilotReasoning(version.copilot_log?.reasoning_summary ?? "");
    setCopilotExecution(version.copilot_log?.execution_summary ?? null);
    setDraftMode("issues");
    setNotice({ tone: "info", message: `已加载版本 v${version.version_number} 到编辑器。` });
  }

  function addRow() {
    setDraft((prev) => ({ field_rules: [...prev.field_rules, createEmptyRuleRow()] }));
  }

  function removeRow(index: number) {
    setDraft((prev) => ({ field_rules: prev.field_rules.filter((_, rowIndex) => rowIndex !== index) }));
  }

  async function handleSaveDraft() {
    if (!validation.valid) {
      setNotice({ tone: "error", message: "草稿校验未通过，请先修复错误项。" });
      return;
    }

    setBusy(true);
    setNotice({ tone: "info", message: "正在保存草稿..." });

    try {
      const source: RuleSource = pendingCopilotLog ? "llm" : "manual";
      const saved = await saveRuleDraft(draft, "draft from UI", source, pendingCopilotLog ?? undefined);
      const cloned = clonePayload(saved.payload);
      setDraft(cloned);
      setEditorSourceVersionId(saved.id);
      setBaselineSnapshot(JSON.stringify(cloned));
      setPendingCopilotLog(saved.copilot_log ?? null);
      await refreshVersions();
      setNotice({ tone: "success", message: `草稿已保存为 v${saved.version_number}` });
    } catch (error) {
      setNotice({ tone: "error", message: toGuidedError(error, "修复草稿后重新保存") });
    } finally {
      setBusy(false);
    }
  }

  async function handleGenerateDraft() {
    if (!modelSettings?.has_api_key) {
      setNotice({ tone: "error", message: "原因：OpenAI API Key 未配置。下一步：先配置 OPENAI_API_KEY 后再生成。" });
      return;
    }
    if (copilotPrompt.length > MAX_COPILOT_PROMPT_CHARS) {
      setNotice({ tone: "error", message: `Prompt 超长，请限制在 ${MAX_COPILOT_PROMPT_CHARS} 字符以内。` });
      return;
    }

    setBusy(true);
    setCopilotReasoning("");
    setCopilotExecution(null);
    setPendingCopilotLog(null);
    setNotice({ tone: "info", message: "Copilot 正在生成规则预览（仅加载到编辑器，不自动入库）..." });

    try {
      let previewPayload: RulePayload | null = null;
      let executionSummary: CopilotExecutionSummary | null = null;
      let modelSnapshot = modelSettings.current_model;
      let reasoningBuffer = "";

      await streamRuleDraftPreview(copilotPrompt, ({ event, data }) => {
        if (event === "reasoning_summary_delta") {
          const text = asString(data.text) ?? "";
          if (!text) return;
          reasoningBuffer += text;
          setCopilotReasoning(reasoningBuffer);
          return;
        }
        if (event === "reasoning_summary") {
          const text = asString(data.text) ?? "";
          if (!text) return;
          reasoningBuffer = text;
          setCopilotReasoning(text);
          return;
        }
        if (event === "execution_summary") {
          const summary = parseCopilotExecutionSummary(data.summary);
          if (summary) {
            executionSummary = summary;
            setCopilotExecution(summary);
            if (summary.reasoning_summary) {
              reasoningBuffer = summary.reasoning_summary;
              setCopilotReasoning(summary.reasoning_summary);
            }
          }
          return;
        }
        if (event === "preview_payload") {
          const payload = asRecord(data.preview_payload);
          const fieldRules = payload?.field_rules;
          if (Array.isArray(fieldRules)) {
            previewPayload = { field_rules: fieldRules as RulePayload["field_rules"] };
          }
          const fromExecution = parseCopilotExecutionSummary(data.llm_execution_summary);
          if (fromExecution) {
            executionSummary = fromExecution;
            setCopilotExecution(fromExecution);
            if (fromExecution.reasoning_summary) {
              reasoningBuffer = fromExecution.reasoning_summary;
              setCopilotReasoning(fromExecution.reasoning_summary);
            }
          }
          const streamModel = asString(data.model);
          if (streamModel === "gpt-5.4" || streamModel === "gpt-5-mini") {
            modelSnapshot = streamModel;
          }
          return;
        }
        if (event === "error") {
          throw new Error(asString(data.message) ?? "Copilot 流式生成失败");
        }
      });

      if (!previewPayload) {
        throw new Error("Copilot 未返回可用规则预览");
      }

      const cloned = clonePayload(previewPayload);
      setDraft(cloned);
      setEditorSourceVersionId(null);
      setDraftMode("issues");
      const summaryForLog: CopilotExecutionSummary =
        executionSummary ?? {
          step_name: "rules_copilot_generate",
          request_started_at: null,
          request_finished_at: null,
          duration_ms: null,
          final_status: "succeeded",
          response_received: true,
          fallback_used: false,
          failure_message: null,
          reasoning_summary: reasoningBuffer || null,
          reasoning_chars: reasoningBuffer.length,
          stream_event_counts: {},
          status_events: [],
        };
      const nextLog: CopilotLogPayload = {
        prompt: copilotPrompt.trim() || "(empty prompt)",
        model: modelSnapshot,
        reasoning_summary: reasoningBuffer || summaryForLog.reasoning_summary,
        execution_summary: summaryForLog,
      };
      setPendingCopilotLog(nextLog);
      setNotice({ tone: "success", message: "规则预览已加载到编辑器。确认后点击“保存草稿”才会入库。" });
    } catch (error) {
      setNotice({ tone: "error", message: toGuidedError(error, "稍后重试生成，或手动编辑草稿") });
    } finally {
      setBusy(false);
    }
  }

  async function handlePublish(versionId: string) {
    const target = versions.find((row) => row.id === versionId);
    if (!target) {
      setNotice({ tone: "error", message: "未找到要发布的版本，请刷新列表后重试。" });
      return;
    }

    const diff = buildRuleDiffSummary(published?.payload ?? null, target.payload);
    const confirmed = window.confirm(
      `确认发布 v${target.version_number}？\n\n新增: ${diff.added}\n删除: ${diff.removed}\n变更: ${diff.changed}\n不变: ${diff.unchanged}`
    );
    if (!confirmed) {
      return;
    }

    setBusy(true);
    setNotice({ tone: "info", message: `正在发布 v${target.version_number}...` });
    try {
      await publishRuleVersion(versionId);
      await refreshVersions();
      const current = await getCurrentRule().catch(() => null);
      setCurrentPublished(current);
      setNotice({ tone: "success", message: `版本 v${target.version_number} 已发布。` });
    } catch (error) {
      setNotice({ tone: "error", message: toGuidedError(error, "确认版本状态后重试发布") });
    } finally {
      setBusy(false);
    }
  }

  function toggleVersionExpanded(versionId: string) {
    setExpandedVersionIds((prev) => {
      const next = new Set(prev);
      if (next.has(versionId)) {
        next.delete(versionId);
      } else {
        next.add(versionId);
      }
      return next;
    });
  }

  function openRationaleEditor(index: number) {
    setRationaleEditorIndex(index);
    setRationaleEditorValue(draft.field_rules[index]?.rationale ?? "");
  }

  function applyRationaleEditor() {
    if (rationaleEditorIndex == null) {
      return;
    }
    updateRow(rationaleEditorIndex, "rationale", rationaleEditorValue);
    setRationaleEditorIndex(null);
  }

  function clearVersionFilters() {
    setVersionStatusFilter("all");
    setVersionSourceFilter("all");
    setVersionKeyword("");
  }

  return (
    <div className="page-wrap grid gap-5">
      <section className="panel p-5 md:p-6">
        <SectionHeader
          title="字段规则工作台"
          subtitle="手工编辑或 Copilot 生成后，都需人工校验并发布。"
          right={
            <div className="flex flex-wrap items-center gap-2">
              {published ? <StatusBadge label={`已发布 v${published.version_number}`} tone="done" /> : null}
              <StatusBadge label={isDirty ? "未保存修改" : "已同步"} tone={isDirty ? "running" : "done"} />
            </div>
          }
        />

        <div className="mt-4 grid gap-3 lg:grid-cols-[1.4fr_1fr]">
          <div className="panel-soft p-3">
            <p className="m-0 text-xs font-semibold">Copilot Prompt（可选）</p>
            <textarea
              className="mt-2 h-28 w-full rounded-lg border border-white/20 bg-black/35 p-2 text-sm"
              value={copilotPrompt}
              onChange={(event) => setCopilotPrompt(event.target.value)}
              placeholder="例如：优先关注防护等级和眩光控制，软约束尽量覆盖能效指标。"
              maxLength={MAX_COPILOT_PROMPT_CHARS}
            />
            <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-xs">
              <span className="muted-text">
                已输入 {copilotPrompt.length}/{MAX_COPILOT_PROMPT_CHARS}
              </span>
              <div className="flex flex-wrap items-center gap-2">
                <StatusBadge label={`当前模型: ${modelSettings?.current_model ?? "loading"}`} tone="active" />
                <StatusBadge
                  label={modelSettings?.has_api_key ? "API Key 已配置" : "API Key 缺失"}
                  tone={modelSettings?.has_api_key ? "done" : "error"}
                />
              </div>
            </div>
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <ActionButton
                onClick={handleGenerateDraft}
                disabled={busy || !modelSettings?.has_api_key || copilotPrompt.length > MAX_COPILOT_PROMPT_CHARS}
                variant="primary"
              >
                Copilot 生成预览
              </ActionButton>
              <ActionButton onClick={handleSaveDraft} disabled={busy || !isDirty} variant="success">
                保存草稿
              </ActionButton>
              <ActionButton onClick={addRow} disabled={busy} variant="secondary">
                新增规则行
              </ActionButton>
            </div>
          </div>

          <div className="panel-soft p-3">
            <p className="m-0 text-xs font-semibold">LLM 实施摘要</p>
            <p className="mt-2 text-xs muted-text">
              仅展示 reasoning_summary。生成结束后只加载到编辑器，点击“保存草稿”才入库。
            </p>
            <pre className="json-box mt-2 max-h-36">{copilotReasoning || "暂无摘要"}</pre>
            {copilotExecution ? (
              <p className="mt-2 text-xs muted-text">
                状态: {copilotExecution.final_status} | 耗时: {copilotExecution.duration_ms ?? "-"}ms | 响应返回:{" "}
                {copilotExecution.response_received ? "是" : "否"}
              </p>
            ) : null}
            {pendingCopilotLog ? (
              <StatusBadge label="待保存 Copilot 日志" tone="running" className="mt-2" />
            ) : (
              <StatusBadge label="当前为手工草稿" tone="idle" className="mt-2" />
            )}
          </div>
        </div>

        <InlineNotice tone={notice.tone} message={notice.message} className="mt-3" />

        <div className="mt-3 grid gap-3 md:grid-cols-3">
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">规则总数</span>
              <StatusBadge label={`${draft.field_rules.length}`} tone="active" />
            </div>
            <div className="info-card-value">{draft.field_rules.length}</div>
            <p className="info-card-subtitle">编辑器当前草稿行数</p>
          </article>
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">校验错误</span>
              <StatusBadge label={`${validation.errors.length}`} tone={validation.errors.length > 0 ? "error" : "done"} />
            </div>
            <div className="info-card-value">{validation.errors.length}</div>
            <p className="info-card-subtitle">错误会阻止保存草稿</p>
          </article>
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">校验警告</span>
              <StatusBadge label={`${validation.warnings.length}`} tone={validation.warnings.length > 0 ? "running" : "idle"} />
            </div>
            <div className="info-card-value">{validation.warnings.length}</div>
            <p className="info-card-subtitle">警告不阻塞保存，但建议处理</p>
          </article>
        </div>
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader
          title="草稿编辑器"
          subtitle="虚拟滚动仅渲染可视行，默认显示“仅错误/警告”。"
          right={
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge
                label={
                  editorSourceVersionId
                    ? `编辑来源: v${versions.find((row) => row.id === editorSourceVersionId)?.version_number ?? "custom"}`
                    : "编辑来源: custom"
                }
                tone="active"
              />
              <StatusBadge label={`视图: ${draftModeLabel(draftMode)}`} tone="running" />
            </div>
          }
        />

        <div className="mt-3 flex flex-wrap items-center gap-2">
          <ActionButton onClick={() => setDraftMode("issues")} variant={draftMode === "issues" ? "primary" : "ghost"}>
            仅错误/警告
          </ActionButton>
          <ActionButton
            onClick={() => setDraftMode("modified")}
            variant={draftMode === "modified" ? "primary" : "ghost"}
          >
            仅已修改
          </ActionButton>
          <ActionButton onClick={() => setDraftMode("all")} variant={draftMode === "all" ? "primary" : "ghost"}>
            全部
          </ActionButton>
          <span className="text-xs muted-text">
            可见 {visibleRowIndices.length} / {draft.field_rules.length}
          </span>
        </div>

        {visibleRowIndices.length === 0 ? (
          <div className="mt-4">
            <EmptyState title="当前视图无可见行" description="可切换到“全部”查看，或继续编辑后再筛选。" />
          </div>
        ) : (
          <div
            className="mt-4 overflow-auto rounded-xl border border-white/10 px-2 py-2"
            style={{ maxHeight: `${DRAFT_VIEWPORT_HEIGHT}px` }}
            onScroll={(event) => setDraftScrollTop(event.currentTarget.scrollTop)}
          >
            {topSpacerHeight > 0 ? <div style={{ height: `${topSpacerHeight}px` }} /> : null}

            {windowedIndices.map((index) => {
              const row = draft.field_rules[index];
              const issues = rowIssues.get(index) ?? { errors: [], warnings: [] };
              const rationaleText = (row.rationale ?? "").trim();
              return (
                <article key={`draft-row-${index}`} className="timeline-item mb-3" style={{ minHeight: `${DRAFT_ROW_HEIGHT - 16}px` }}>
                  <div className="timeline-item-header">
                    <div className="timeline-item-title">
                      <span className="timeline-item-index">{index + 1}</span>
                      <div>
                        <h3 className="m-0 text-sm font-semibold">规则行 #{index + 1}</h3>
                        <p className="m-0 mt-1 text-xs muted-text">建议字段格式：`vw_bid_specs.xxx`</p>
                      </div>
                    </div>
                    <div className="flex flex-wrap items-center gap-2">
                      {modifiedRowIndices.has(index) ? <StatusBadge label="已修改" tone="running" /> : null}
                      {issues.errors.length > 0 ? <StatusBadge label={`${issues.errors.length} error`} tone="error" /> : null}
                      {issues.warnings.length > 0 ? <StatusBadge label={`${issues.warnings.length} warning`} tone="running" /> : null}
                      <ActionButton onClick={() => removeRow(index)} disabled={busy} variant="danger">
                        删除
                      </ActionButton>
                    </div>
                  </div>

                  <div className="mt-3 grid gap-2 xl:grid-cols-6">
                    <label className="block text-xs text-slate-200 xl:col-span-2">
                      字段名
                      <input
                        className="mt-1 w-full rounded-lg border border-white/20 bg-black/35 px-2 py-1.5 text-sm"
                        value={row.field}
                        onChange={(event) => updateRow(index, "field", event.target.value)}
                        placeholder="vw_bid_specs.direct_ugr"
                      />
                    </label>

                    <label className="block text-xs text-slate-200">
                      操作符
                      <select
                        className="mt-1 w-full rounded-lg border border-white/20 bg-black/35 px-2 py-1.5 text-sm"
                        value={row.operator}
                        onChange={(event) => updateRow(index, "operator", event.target.value)}
                      >
                        {ALLOWED_RULE_OPERATORS.map((operator) => (
                          <option key={operator} value={operator}>
                            {operator}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label className="block text-xs text-slate-200">
                      约束类型
                      <select
                        className="mt-1 w-full rounded-lg border border-white/20 bg-black/35 px-2 py-1.5 text-sm"
                        value={row.is_hard ? "hard" : "soft"}
                        onChange={(event) => updateRow(index, "is_hard", event.target.value === "hard")}
                      >
                        <option value="soft">soft</option>
                        <option value="hard">hard</option>
                      </select>
                    </label>

                    <label className="block text-xs text-slate-200">
                      操作符置信度
                      <input
                        type="number"
                        min={0}
                        max={1}
                        step={0.01}
                        className="mt-1 w-full rounded-lg border border-white/20 bg-black/35 px-2 py-1.5 text-sm"
                        value={row.operator_confidence}
                        onChange={(event) => updateRow(index, "operator_confidence", Number(event.target.value))}
                      />
                    </label>

                    <label className="block text-xs text-slate-200">
                      硬度置信度
                      <input
                        type="number"
                        min={0}
                        max={1}
                        step={0.01}
                        className="mt-1 w-full rounded-lg border border-white/20 bg-black/35 px-2 py-1.5 text-sm"
                        value={row.hardness_confidence}
                        onChange={(event) => updateRow(index, "hardness_confidence", Number(event.target.value))}
                      />
                    </label>
                  </div>

                  <div className="mt-3 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-white/10 bg-black/20 px-3 py-2">
                    <div className="min-w-0">
                      <p className="m-0 text-xs muted-text">rationale</p>
                      <p className="m-0 mt-1 truncate text-xs text-slate-200" title={rationaleText || "未填写"}>
                        {rationaleText || "未填写"}
                      </p>
                    </div>
                    <ActionButton onClick={() => openRationaleEditor(index)} variant="ghost">
                      编辑说明
                    </ActionButton>
                  </div>

                  {issues.errors.length > 0 ? (
                    <InlineNotice tone="error" message={issues.errors.join("；")} className="mt-3" />
                  ) : null}
                  {issues.warnings.length > 0 ? (
                    <InlineNotice tone="warning" message={issues.warnings.join("；")} className="mt-3" />
                  ) : null}
                </article>
              );
            })}

            {bottomSpacerHeight > 0 ? <div style={{ height: `${bottomSpacerHeight}px` }} /> : null}
          </div>
        )}

        {rowIssues.get(-1)?.warnings.length ? (
          <InlineNotice tone="warning" message={rowIssues.get(-1)?.warnings.join("；") ?? ""} className="mt-3" />
        ) : null}
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader
          title="版本历史"
          subtitle="默认展示发布版本 + 最近记录，支持按状态/来源筛选并分页加载"
          right={
            <ActionButton onClick={() => void refreshVersions()} disabled={versionsLoading} variant="ghost">
              {versionsLoading ? "刷新中..." : "刷新列表"}
            </ActionButton>
          }
        />

        <div className="mt-4 grid gap-2 lg:grid-cols-[12rem_12rem_1fr_auto]">
          <label className="text-xs text-slate-200">
            状态
            <select
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={versionStatusFilter}
              onChange={(event) => setVersionStatusFilter(event.target.value as "all" | RuleStatus)}
            >
              {VERSION_STATUS_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="text-xs text-slate-200">
            来源
            <select
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={versionSourceFilter}
              onChange={(event) => setVersionSourceFilter(event.target.value as "all" | RuleSource)}
            >
              {VERSION_SOURCE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="text-xs text-slate-200">
            关键词（ID/备注）
            <input
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={versionKeyword}
              onChange={(event) => setVersionKeyword(event.target.value)}
              placeholder="例如：manual / 版本ID"
            />
          </label>

          <div className="flex items-end">
            <ActionButton onClick={clearVersionFilters} variant="secondary" className="w-full">
              清空筛选
            </ActionButton>
          </div>
        </div>

        <div className="mt-4 space-y-3">
          {displayVersions.map((version) => {
            const diff = buildRuleDiffSummary(published?.payload ?? null, version.payload);
            const expanded = expandedVersionIds.has(version.id);
            const validationErrors = countValidationItems(version.validation_report, "errors");
            const validationWarnings = countValidationItems(version.validation_report, "warnings");
            return (
              <article key={version.id} className="timeline-item">
                <div className="timeline-item-header">
                  <div>
                    <div className="flex flex-wrap items-center gap-2">
                      <h3 className="m-0 text-sm font-semibold">版本 v{version.version_number}</h3>
                      <StatusBadge label={version.status} tone={toneFromKeyword(version.status)} />
                      <StatusBadge label={version.source} tone={toneFromKeyword(version.source)} />
                      {version.id === published?.id ? <StatusBadge label="当前发布" tone="done" /> : null}
                    </div>
                    <p className="m-0 mt-1 text-xs muted-text">创建时间：{formatDateTime(version.created_at)}</p>
                  </div>

                  <div className="flex flex-wrap items-center gap-2">
                    <ActionButton onClick={() => loadVersionInEditor(version)} disabled={busy} variant="ghost">
                      加载到编辑器
                    </ActionButton>
                    {version.status !== "published" ? (
                      <ActionButton onClick={() => handlePublish(version.id)} disabled={busy} variant="warning">
                        发布此版本
                      </ActionButton>
                    ) : null}
                    <ActionButton onClick={() => toggleVersionExpanded(version.id)} variant="secondary">
                      {expanded ? "收起详情" : "展开详情"}
                    </ActionButton>
                  </div>
                </div>

                <div className="mt-3 grid gap-2 md:grid-cols-4 text-xs text-slate-200">
                  <div className="panel-soft p-2">
                    新增字段 <strong>{diff.added}</strong>
                  </div>
                  <div className="panel-soft p-2">
                    删除字段 <strong>{diff.removed}</strong>
                  </div>
                  <div className="panel-soft p-2">
                    变更字段 <strong>{diff.changed}</strong>
                  </div>
                  <div className="panel-soft p-2">
                    未变更 <strong>{diff.unchanged}</strong>
                  </div>
                </div>

                {expanded ? (
                  <div className="mt-3 grid gap-2 md:grid-cols-3 text-xs text-slate-200">
                    <div className="panel-soft p-2">
                      校验错误 <strong>{validationErrors}</strong>
                    </div>
                    <div className="panel-soft p-2">
                      校验警告 <strong>{validationWarnings}</strong>
                    </div>
                    <div className="panel-soft p-2">
                      规则条目 <strong>{version.payload.field_rules.length}</strong>
                    </div>
                    <div className="panel-soft p-2 md:col-span-3">
                      备注：{version.note?.trim() || "（无）"}
                    </div>
                    <div className="panel-soft p-2 md:col-span-3">
                      版本 ID：
                      <span className="ml-1 font-mono">{version.id}</span>
                    </div>
                    {version.copilot_log ? (
                      <>
                        <div className="panel-soft p-2">
                          Copilot 模型 <strong>{version.copilot_log.model}</strong>
                        </div>
                        <div className="panel-soft p-2 md:col-span-2">
                          Copilot Prompt：
                          <span className="ml-1">{version.copilot_log.prompt || "（空）"}</span>
                        </div>
                        <div className="panel-soft p-2 md:col-span-3">
                          <p className="m-0 text-xs muted-text">reasoning_summary</p>
                          <pre className="json-box mt-2 max-h-32">
                            {version.copilot_log.reasoning_summary || "（无）"}
                          </pre>
                        </div>
                      </>
                    ) : null}
                  </div>
                ) : null}
              </article>
            );
          })}

          {versionsEmpty ? (
            <EmptyState title="暂无版本" description="先保存一份草稿，版本历史会自动出现。" />
          ) : null}
        </div>

        {!versionsEmpty ? (
          <div className="mt-4 flex flex-wrap items-center justify-between gap-2">
            <p className="text-xs muted-text">已加载 {versions.length} 条版本记录</p>
            {versionsHasMore ? (
              <ActionButton onClick={() => void loadMoreVersions()} disabled={versionsLoadingMore} variant="secondary">
                {versionsLoadingMore ? "加载中..." : "加载更多"}
              </ActionButton>
            ) : (
              <span className="text-xs muted-text">已到末页</span>
            )}
          </div>
        ) : null}
      </section>

      {rationaleEditorIndex != null ? (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/65 p-4">
          <div className="panel w-full max-w-2xl p-5">
            <SectionHeader
              title={`编辑 rationale（第 ${rationaleEditorIndex + 1} 行）`}
              subtitle="默认折叠展示，编辑后会立即写回草稿。"
            />
            <textarea
              className="mt-4 h-48 w-full rounded-lg border border-white/20 bg-black/35 p-3 text-sm"
              value={rationaleEditorValue}
              onChange={(event) => setRationaleEditorValue(event.target.value)}
              placeholder="说明这条规则的来源和业务意图"
            />
            <div className="mt-4 flex flex-wrap justify-end gap-2">
              <ActionButton onClick={() => setRationaleEditorIndex(null)} variant="ghost">
                取消
              </ActionButton>
              <ActionButton onClick={applyRationaleEditor} variant="success">
                保存说明
              </ActionButton>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
