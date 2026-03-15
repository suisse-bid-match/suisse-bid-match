"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useParams } from "next/navigation";
import { API_BASE, getJob, getJobResult, type JobResponse, type JobStepRow } from "@/lib/api";
import {
  buildStepProgressStates,
  calculateProgressPercent,
  formatDateTime,
  formatDuration,
  toGuidedError,
  type JobEventItem
} from "@/lib/view-models";
import { ActionButton, EmptyState, InlineNotice, SectionHeader, StatusBadge, toneFromKeyword } from "@/components/ui";
import { JobStepDetailPanel } from "@/components/job-step-detail-panel";

const STEP_ORDER = [
  "schema_snapshot",
  "step1_kb_bootstrap",
  "step2_extract_requirements",
  "step3_external_field_rules",
  "step4_merge_requirements_hardness",
  "step5_build_sql",
  "step6_execute_sql",
  "step7_rank_candidates"
];
const STRUCTURED_STEPS = new Set([
  "step2_extract_requirements",
  "step3_external_field_rules",
  "step4_merge_requirements_hardness",
  "step5_build_sql",
  "step6_execute_sql",
  "step7_rank_candidates"
]);
const STEP_RESULT_LIMIT = 20;

type EventPayload = Record<string, unknown>;
type LlmTraceMap = Record<string, { reasoningText: string; execution: LlmExecutionSummary | null }>;

interface LlmExecutionSummary {
  step_name: string;
  request_started_at: string | null;
  request_finished_at: string | null;
  duration_ms: number | null;
  final_status: "succeeded" | "failed";
  response_received: boolean;
  fallback_used: boolean;
  failure_message: string | null;
  reasoning_summary: string | null;
  reasoning_chars: number;
  stream_event_counts: Record<string, number>;
  status_events: string[];
}

function asRecord(input: unknown): Record<string, unknown> | null {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return null;
  }
  return input as Record<string, unknown>;
}

function asArray(input: unknown): unknown[] {
  return Array.isArray(input) ? input : [];
}

function asBoolean(input: unknown): boolean | null {
  if (typeof input !== "boolean") {
    return null;
  }
  return input;
}

function asNumber(input: unknown): number | null {
  if (typeof input !== "number" || Number.isNaN(input)) {
    return null;
  }
  return input;
}

function parseLlmExecutionSummary(input: unknown): LlmExecutionSummary | null {
  const payload = asRecord(input);
  if (!payload) return null;

  const stepName = typeof payload.step_name === "string" ? payload.step_name : null;
  const finalStatus = payload.final_status === "succeeded" || payload.final_status === "failed" ? payload.final_status : null;
  if (!stepName || !finalStatus) return null;

  const streamCountsRaw = asRecord(payload.stream_event_counts);
  const streamEventCounts: Record<string, number> = {};
  if (streamCountsRaw) {
    for (const [key, value] of Object.entries(streamCountsRaw)) {
      const count = asNumber(value);
      if (count != null) {
        streamEventCounts[key] = count;
      }
    }
  }

  const statusEvents = asArray(payload.status_events)
    .map((item) => (typeof item === "string" ? item : ""))
    .filter((item) => item.length > 0);

  return {
    step_name: stepName,
    request_started_at: typeof payload.request_started_at === "string" ? payload.request_started_at : null,
    request_finished_at: typeof payload.request_finished_at === "string" ? payload.request_finished_at : null,
    duration_ms: asNumber(payload.duration_ms),
    final_status: finalStatus,
    response_received: asBoolean(payload.response_received) ?? false,
    fallback_used: asBoolean(payload.fallback_used) ?? false,
    failure_message: typeof payload.failure_message === "string" ? payload.failure_message : null,
    reasoning_summary: typeof payload.reasoning_summary === "string" ? payload.reasoning_summary : null,
    reasoning_chars: asNumber(payload.reasoning_chars) ?? 0,
    stream_event_counts: streamEventCounts,
    status_events: statusEvents
  };
}

function extractLlmExecutionFromStepPayload(stepPayload: Record<string, unknown>): LlmExecutionSummary | null {
  const data = asRecord(stepPayload.data);
  if (!data) return null;
  return parseLlmExecutionSummary(data.llm_execution);
}

function executionPreview(summary: LlmExecutionSummary): string {
  const statusText = summary.final_status === "succeeded" ? "Succeeded" : "Failed";
  const durationText = summary.duration_ms != null ? `${summary.duration_ms}ms` : "-";
  const fallbackText = summary.fallback_used ? "Yes" : "No";
  return `Status: ${statusText} | Response received: ${summary.response_received ? "Yes" : "No"} | fallback: ${fallbackText} | Duration: ${durationText}`;
}

function mergeLlmTraces(base: LlmTraceMap, steps: JobStepRow[]): LlmTraceMap {
  const next: LlmTraceMap = { ...base };
  for (const step of steps) {
    const stepPayload = asRecord(step.payload);
    if (!stepPayload) continue;
    const execution = extractLlmExecutionFromStepPayload(stepPayload);
    if (!execution) continue;
    const existing = next[step.step_name] ?? { reasoningText: "", execution: null };
    const reasoningText = existing.reasoningText || execution.reasoning_summary || "";
    next[step.step_name] = { reasoningText, execution };
  }
  return next;
}

function getMessageFromPayload(eventType: string, payload: EventPayload): string {
  if (eventType === "step_update") {
    const stepName = typeof payload.step_name === "string" ? payload.step_name : "unknown_step";
    const stepStatus = typeof payload.step_status === "string" ? payload.step_status : "updated";
    return `${stepName} -> ${stepStatus}`;
  }
  if (eventType === "job_failed") {
    return `Job failed: ${typeof payload.message === "string" ? payload.message : "unknown error"}`;
  }
  if (eventType === "job_completed") {
    return "Job completed. Result generated.";
  }
  if (eventType === "job_started") {
    return "Job started";
  }
  if (eventType === "llm_progress") {
    const stepName = typeof payload.step_name === "string" ? payload.step_name : "llm";
    const status = typeof payload.status === "string" ? payload.status : "update";
    return `${stepName}: ${status}`;
  }
  return eventType;
}

export default function JobDetailPage() {
  const params = useParams<{ jobId: string }>();
  const jobId = params.jobId ?? "";

  const [job, setJob] = useState<JobResponse | null>(null);
  const [stepMap, setStepMap] = useState<Record<string, JobStepRow>>({});
  const [events, setEvents] = useState<JobEventItem[]>([]);
  const [llmTraces, setLlmTraces] = useState<LlmTraceMap>({});
  const [result, setResult] = useState<Record<string, unknown> | null>(null);
  const [errorMessage, setErrorMessage] = useState<string>("");
  const [sseConnected, setSseConnected] = useState(false);
  const [lastSyncedAt, setLastSyncedAt] = useState<string | null>(null);
  const [compactMode, setCompactMode] = useState(true);

  const lastSseErrorRef = useRef(0);
  const currentJobStatusRef = useRef<JobResponse["status"] | null>(null);

  useEffect(() => {
    currentJobStatusRef.current = job?.status ?? null;
  }, [job?.status]);

  const appendEvent = useCallback((event: Omit<JobEventItem, "createdAt" | "id"> & { createdAt?: string; id?: string }) => {
    const createdAt = event.createdAt ?? new Date().toISOString();
    const id = event.id ?? `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    setEvents((prev) => [{ ...event, createdAt, id }, ...prev].slice(0, 120));
  }, []);

  const appendLlmReasoning = useCallback((stepName: string, text: string) => {
    if (!text) return;
    setLlmTraces((prev) => {
      const current = prev[stepName] ?? { reasoningText: "", execution: null };
      return {
        ...prev,
        [stepName]: {
          ...current,
          reasoningText: (current.reasoningText + text).slice(-8000)
        }
      };
    });
  }, []);

  const setLlmReasoning = useCallback((stepName: string, text: string) => {
    if (!text) return;
    setLlmTraces((prev) => {
      const current = prev[stepName] ?? { reasoningText: "", execution: null };
      return {
        ...prev,
        [stepName]: {
          ...current,
          reasoningText: text.slice(-8000)
        }
      };
    });
  }, []);

  const upsertLlmExecution = useCallback((stepName: string, summary: LlmExecutionSummary) => {
    setLlmTraces((prev) => {
      const current = prev[stepName] ?? { reasoningText: "", execution: null };
      const reasoningText = current.reasoningText || summary.reasoning_summary || "";
      return {
        ...prev,
        [stepName]: {
          reasoningText,
          execution: summary
        }
      };
    });
  }, []);

  const mergeSteps = useCallback((steps: JobStepRow[]) => {
    setStepMap((prev) => {
      const merged = { ...prev };
      for (const step of steps) {
        merged[step.step_name] = step;
      }
      return merged;
    });
  }, []);

  const refreshSnapshot = useCallback(async () => {
    if (!jobId) return;

    try {
      const payload = await getJob(jobId);
      setJob(payload);
      mergeSteps(payload.steps);
      setLlmTraces((prev) => mergeLlmTraces(prev, payload.steps));
      setLastSyncedAt(new Date().toISOString());

      if (payload.status === "failed" && payload.error_message) {
        setErrorMessage(payload.error_message);
      }

      if (payload.status === "succeeded") {
        const output = await getJobResult(jobId);
        setResult(output);
      }
    } catch (error) {
      setErrorMessage(toGuidedError(error, "Refresh later, or check backend logs"));
    }
  }, [jobId, mergeSteps]);

  useEffect(() => {
    void refreshSnapshot();
  }, [refreshSnapshot]);

  useEffect(() => {
    if (!jobId) return;

    const source = new EventSource(`${API_BASE}/jobs/${jobId}/events`);

    source.onopen = () => {
      setSseConnected(true);
      appendEvent({ kind: "system", message: "SSE connected", rawPayload: null });
    };

    source.addEventListener("step_update", (event) => {
      const raw = JSON.parse((event as MessageEvent).data) as EventPayload;
      const stepName = typeof raw.step_name === "string" ? raw.step_name : "unknown";
      const stepStatus = typeof raw.step_status === "string" ? raw.step_status : "unknown";
      const updatedAt = typeof raw.updated_at === "string" ? raw.updated_at : null;
      const payload = asRecord(raw.data) ?? {};
      const execution = extractLlmExecutionFromStepPayload(payload);

      setStepMap((prev) => {
        const previous = prev[stepName];
        return {
          ...prev,
          [stepName]: {
            step_name: stepName,
            step_status: stepStatus,
            payload,
            updated_at: updatedAt ?? previous?.updated_at ?? null
          }
        };
      });
      if (execution) {
        upsertLlmExecution(stepName, execution);
      }

      appendEvent({
        id: (event as MessageEvent).lastEventId || undefined,
        kind: "step_update",
        stepName,
        message: getMessageFromPayload("step_update", raw),
        rawPayload: raw
      });

      if (["error", "failed", "fail"].includes(stepStatus.toLowerCase())) {
        setErrorMessage(`Step ${stepName} failed. Please inspect this step output.`);
      }
    });

    source.addEventListener("llm_progress", (event) => {
      const raw = JSON.parse((event as MessageEvent).data) as EventPayload;
      const stepName = typeof raw.step_name === "string" ? raw.step_name : "llm";
      const kind = typeof raw.kind === "string" ? raw.kind : "status";
      const text = typeof raw.text === "string" ? raw.text : "";
      const allowReasoningReplay = currentJobStatusRef.current === "running";

      if (allowReasoningReplay && kind === "reasoning_summary_delta" && text) {
        appendLlmReasoning(stepName, text);
      }

      if (allowReasoningReplay && kind === "reasoning_summary" && text) {
        setLlmReasoning(stepName, text);
      }

      if (kind === "execution_summary") {
        const summary = parseLlmExecutionSummary(raw.summary);
        if (summary) {
          upsertLlmExecution(stepName, summary);
        }
      }

      if (kind === "status") {
        appendEvent({
          id: (event as MessageEvent).lastEventId || undefined,
          kind: "llm_progress",
          stepName,
          message: getMessageFromPayload("llm_progress", raw),
          rawPayload: raw
        });
      }
    });

    source.addEventListener("job_started", (event) => {
      appendEvent({
        id: (event as MessageEvent).lastEventId || undefined,
        kind: "job_started",
        message: "Job execution started",
        rawPayload: null
      });
      setJob((prev) => {
        if (!prev) return prev;
        if (prev.status === "succeeded" || prev.status === "failed") {
          return prev;
        }
        return { ...prev, status: "running" };
      });
    });

    source.addEventListener("job_completed", (event) => {
      const raw = JSON.parse((event as MessageEvent).data) as EventPayload;
      appendEvent({
        id: (event as MessageEvent).lastEventId || undefined,
        kind: "job_completed",
        message: getMessageFromPayload("job_completed", raw),
        rawPayload: raw
      });
      void refreshSnapshot();
    });

    source.addEventListener("job_failed", (event) => {
      const raw = JSON.parse((event as MessageEvent).data) as EventPayload;
      appendEvent({
        id: (event as MessageEvent).lastEventId || undefined,
        kind: "job_failed",
        message: getMessageFromPayload("job_failed", raw),
        rawPayload: raw
      });
      if (typeof raw.message === "string") {
        setErrorMessage(raw.message);
      }
      void refreshSnapshot();
    });

    source.onerror = () => {
      setSseConnected(false);
      const now = Date.now();
      if (now - lastSseErrorRef.current > 15000) {
        appendEvent({
          kind: "system",
          message: "SSE disconnected. Auto-switched to low-frequency polling fallback.",
          rawPayload: null
        });
        lastSseErrorRef.current = now;
      }
    };

    return () => {
      source.close();
      setSseConnected(false);
    };
  }, [appendEvent, appendLlmReasoning, jobId, refreshSnapshot, setLlmReasoning, upsertLlmExecution]);

  useEffect(() => {
    if (!jobId) return;
    const timer = setInterval(() => {
      if (!sseConnected) {
        void refreshSnapshot();
      }
    }, 20000);

    return () => clearInterval(timer);
  }, [jobId, refreshSnapshot, sseConnected]);

  const timeline = useMemo(() => buildStepProgressStates(STEP_ORDER, stepMap), [stepMap]);
  const progress = useMemo(() => calculateProgressPercent(timeline), [timeline]);

  const finalOutput = useMemo(() => {
    if (!result) return null;
    const payload = asRecord(result.final_output);
    return payload;
  }, [result]);

  const finalSummary = useMemo(() => {
    if (!finalOutput) {
      return { tenderCount: 0, matchCount: 0, runId: "-" };
    }

    const tenderCount = asArray(finalOutput.tender_products).length;
    const matchCount = asArray(finalOutput.match_results).length;
    const runId = typeof finalOutput.run_id === "string" ? finalOutput.run_id : "-";

    return { tenderCount, matchCount, runId };
  }, [finalOutput]);

  const llmRows = useMemo(() => {
    return Object.entries(llmTraces)
      .filter(([, trace]) => Boolean(trace.reasoningText) || trace.execution !== null)
      .sort((a, b) => {
        const idxA = STEP_ORDER.indexOf(a[0]);
        const idxB = STEP_ORDER.indexOf(b[0]);
        const rankA = idxA === -1 ? Number.MAX_SAFE_INTEGER : idxA;
        const rankB = idxB === -1 ? Number.MAX_SAFE_INTEGER : idxB;
        return rankA - rankB;
      });
  }, [llmTraces]);

  if (!jobId) {
    return (
      <div className="page-wrap">
        <section className="panel p-6">
          <EmptyState title="Invalid Job ID" description="Open from the home history list, or create a new job." />
        </section>
      </div>
    );
  }

  return (
    <div className="page-wrap grid gap-5">
      <section className="panel p-5 md:p-6">
        <SectionHeader
          title="Job Execution Details"
          subtitle={`Job ID: ${jobId}`}
          right={<StatusBadge label={job?.status ?? "loading"} tone={toneFromKeyword(job?.status ?? "idle")} className="break-all" />}
        />

        <div className="mt-4 grid gap-3 md:grid-cols-4">
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">Job Status</span>
              <StatusBadge label={job?.status ?? "loading"} tone={toneFromKeyword(job?.status ?? "idle")} />
            </div>
            <div className="info-card-value">{job?.status ?? "loading"}</div>
            <p className="info-card-subtitle">Backend state is the single source of truth</p>
          </article>
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">Step Progress</span>
              <StatusBadge label={`${progress}%`} tone={progress === 100 ? "done" : "active"} />
            </div>
            <div className="info-card-value">{progress}%</div>
            <p className="info-card-subtitle">Only succeeded steps count as complete; running does not</p>
          </article>
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">Uploaded Files</span>
              <StatusBadge label={`${job?.file_count ?? 0}`} tone="active" />
            </div>
            <div className="info-card-value">{job?.file_count ?? 0}</div>
            <p className="info-card-subtitle">Uploading is disabled once status is running/succeeded</p>
          </article>
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">Connection</span>
              <StatusBadge label={sseConnected ? "SSE online" : "Polling fallback"} tone={sseConnected ? "done" : "running"} />
            </div>
            <div className="info-card-value">{sseConnected ? "Realtime" : "Fallback"}</div>
            <p className="info-card-subtitle">Last sync: {formatDateTime(lastSyncedAt)}</p>
          </article>
        </div>

        <div className="mt-4 h-3 w-full overflow-hidden rounded-full bg-slate-900/90">
          <div className="h-full bg-cyan-400 transition-all" style={{ width: `${progress}%` }} />
        </div>

        {errorMessage ? <InlineNotice tone="error" title="Execution Error" message={errorMessage} className="mt-4" /> : null}
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader
          title="Step Timeline"
          subtitle="step2-step7 use structured cards; step1 keeps raw JSON view."
          right={
            <ActionButton onClick={() => setCompactMode((prev) => !prev)} variant={compactMode ? "primary" : "secondary"}>
              Compact mode: {compactMode ? "On" : "Off"}
            </ActionButton>
          }
        />
        <div className="timeline mt-4">
          {timeline.map((row, index) => (
            <article key={row.stepName} className="timeline-item">
              <div className="timeline-item-header">
                <div className="timeline-item-title">
                  <span className="timeline-item-index">{index + 1}</span>
                  <div>
                    <h3 className="m-0 text-sm font-semibold">{row.displayName}</h3>
                    <p className="m-0 mt-1 text-xs muted-text">{row.stepName}</p>
                  </div>
                </div>
                <div className="timeline-meta">
                  <StatusBadge label={row.statusText} tone={toneFromKeyword(row.statusText)} />
                  <span className="text-xs muted-text">Duration: {formatDuration(row.durationMs)}</span>
                  <span className="text-xs muted-text">Updated: {formatDateTime(row.updatedAt)}</span>
                </div>
              </div>

              <p className="timeline-summary">{row.summary}</p>
              {row.errorMessage ? <InlineNotice tone="error" message={row.errorMessage} className="timeline-error" /> : null}

              {STRUCTURED_STEPS.has(row.stepName) ? (
                <JobStepDetailPanel step={row} compactMode={compactMode} resultLimit={STEP_RESULT_LIMIT} />
              ) : null}

              <details className="mt-2">
                <summary className="cursor-pointer text-xs text-cyan-100">View raw JSON</summary>
                <pre className="json-box">{JSON.stringify(row.payload ?? {}, null, 2)}</pre>
              </details>
            </article>
          ))}
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-3">
        <section className="panel p-5">
          <SectionHeader title="Event Stream" subtitle="Summary first, raw events on demand" />
          <div className="mt-3 rounded-xl border border-white/10 bg-black/25 p-3">
            <div className="flex flex-wrap items-center gap-2 text-xs muted-text">
              <span>Total events: {events.length}</span>
              <span>Last sync: {formatDateTime(lastSyncedAt)}</span>
            </div>
            <div className="mt-3 max-h-64 space-y-2 overflow-auto">
              {events.slice(0, 24).map((item) => (
                <div key={item.id} className="rounded-lg border border-white/10 bg-black/30 p-2">
                  <div className="flex flex-wrap items-center gap-2 text-xs">
                    <StatusBadge label={item.kind} tone={toneFromKeyword(item.kind)} />
                    <span className="muted-text">{formatDateTime(item.createdAt)}</span>
                  </div>
                  <p className="mb-0 mt-1 break-words text-sm text-slate-100">{item.message}</p>
                </div>
              ))}
              {events.length === 0 ? <EmptyState title="No Events Yet" description="After job start, step and status changes will appear here in realtime." /> : null}
            </div>
          </div>
          <details className="mt-3">
            <summary className="cursor-pointer text-xs text-cyan-100">View full event JSON</summary>
            <pre className="json-box">{JSON.stringify(events, null, 2)}</pre>
          </details>
        </section>

        <section className="panel p-5">
          <SectionHeader title="LLM Execution Summary" subtitle="Structured execution summary + realtime reasoning text (if available)" />
          {llmRows.length === 0 ? (
            <div className="mt-3">
              <EmptyState title="No LLM Summary Yet" description="Execution summaries appear here after step2/step7 LLM requests." />
            </div>
          ) : (
            <div className="mt-3 space-y-3">
              {llmRows.map(([stepName, trace]) => {
                const execution = trace.execution;
                const reasoningText = trace.reasoningText;
                const previewSource = reasoningText || (execution ? executionPreview(execution) : "");
                const preview = previewSource.length > 180 ? `${previewSource.slice(0, 180)}...` : previewSource;
                const badgeLabel = execution ? execution.final_status : "stream";
                const badgeTone = execution ? toneFromKeyword(execution.final_status) : "active";
                return (
                  <article key={stepName} className="rounded-xl border border-white/10 bg-black/25 p-3">
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-xs text-slate-300">{stepName}</span>
                      <StatusBadge label={badgeLabel} tone={badgeTone} />
                    </div>
                    <p className="mb-0 mt-2 text-sm text-slate-100">{preview}</p>
                    {execution ? (
                      <p className="mb-0 mt-2 text-xs muted-text">
                        {executionPreview(execution)} | Updated: {formatDateTime(execution.request_finished_at)}
                      </p>
                    ) : null}
                    <details className="mt-2">
                      <summary className="cursor-pointer text-xs text-cyan-100">View full details</summary>
                      <pre className="json-box">{reasoningText || "No reasoning text yet"}</pre>
                      <pre className="json-box">{JSON.stringify(execution ?? {}, null, 2)}</pre>
                    </details>
                  </article>
                );
              })}
            </div>
          )}
        </section>

        <section className="panel p-5">
          <SectionHeader title="Final Result" subtitle="Summary first, raw JSON on demand" />
          {finalOutput ? (
            <div className="mt-3 grid gap-2">
              <article className="info-card">
                <div className="info-card-top">
                  <span className="info-card-title">run_id</span>
                  <StatusBadge label="ready" tone="done" />
                </div>
                <div className="info-card-value break-all text-base">{finalSummary.runId}</div>
              </article>
              <article className="info-card">
                <div className="info-card-top">
                  <span className="info-card-title">Tender Item Count</span>
                  <StatusBadge label={`${finalSummary.tenderCount}`} tone="active" />
                </div>
                <div className="info-card-value">{finalSummary.tenderCount}</div>
              </article>
              <article className="info-card">
                <div className="info-card-top">
                  <span className="info-card-title">Candidate Count</span>
                  <StatusBadge label={`${finalSummary.matchCount}`} tone="active" />
                </div>
                <div className="info-card-value">{finalSummary.matchCount}</div>
              </article>
            </div>
          ) : (
            <div className="mt-3">
              <EmptyState title="Result Not Ready" description="Final output and summary appear automatically after job completion." />
            </div>
          )}

          <details className="mt-3">
            <summary className="cursor-pointer text-xs text-cyan-100">View raw JSON</summary>
            <pre className="json-box">{JSON.stringify(result ?? {}, null, 2)}</pre>
          </details>
        </section>
      </section>
    </div>
  );
}
