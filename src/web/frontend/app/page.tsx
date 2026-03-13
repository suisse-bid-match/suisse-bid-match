"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import type { JobListQuery, JobResponse, JobStatus, ModelSettingsResponse } from "@/lib/api";
import { createJob, getModelSettings, listJobs, startJob, uploadJobArchive, uploadJobFile } from "@/lib/api";
import { usePaginatedList } from "@/lib/use-paginated-list";
import {
  ActionButton,
  EmptyState,
  InlineNotice,
  SectionHeader,
  StatusBadge,
  toneFromKeyword,
} from "@/components/ui";
import { formatDateTime, mapJobStatusToFlow, toGuidedError, type JobFlowState } from "@/lib/view-models";

type NoticeTone = "info" | "success" | "warning" | "error";

interface NoticeState {
  tone: NoticeTone;
  message: string;
}

type ActionState = "idle" | "creating" | "uploading_files" | "uploading_archive" | "starting";
type StepState = "pending" | "active" | "done" | "error";
const HISTORY_PAGE_SIZE = 50;

const STATUS_OPTIONS: Array<{ value: "all" | JobStatus; label: string }> = [
  { value: "all", label: "All statuses" },
  { value: "created", label: "created" },
  { value: "uploading", label: "uploading" },
  { value: "ready", label: "ready" },
  { value: "running", label: "running" },
  { value: "succeeded", label: "succeeded" },
  { value: "failed", label: "failed" },
];

function toStepTone(step: StepState) {
  if (step === "done") return "done" as const;
  if (step === "active") return "active" as const;
  if (step === "error") return "error" as const;
  return "pending" as const;
}

function stepLabel(step: StepState) {
  if (step === "done") return "Done";
  if (step === "active") return "In progress";
  if (step === "error") return "Failed";
  return "Pending";
}

function parseStatusFilter(input: string | null): "all" | JobStatus {
  if (!input) {
    return "all";
  }
  if (STATUS_OPTIONS.some((row) => row.value === input)) {
    return input as "all" | JobStatus;
  }
  return "all";
}

function parseDateFilter(input: string | null): string {
  if (!input) {
    return "";
  }
  const match = input.match(/^(\d{4}-\d{2}-\d{2})/);
  return match?.[1] ?? "";
}

function toStartDayIso(dateText: string): string | undefined {
  return dateText ? `${dateText}T00:00:00Z` : undefined;
}

function toEndDayIso(dateText: string): string | undefined {
  return dateText ? `${dateText}T23:59:59Z` : undefined;
}

function HomePageContent() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null);
  const [actionState, setActionState] = useState<ActionState>("idle");
  const [uploadedFileCount, setUploadedFileCount] = useState(0);

  const [files, setFiles] = useState<File[]>([]);
  const [archive, setArchive] = useState<File | null>(null);

  const [statusFilter, setStatusFilter] = useState<"all" | JobStatus>(() =>
    parseStatusFilter(searchParams.get("status"))
  );
  const [keywordFilter, setKeywordFilter] = useState<string>(() => searchParams.get("q") ?? "");
  const [updatedFrom, setUpdatedFrom] = useState<string>(() => parseDateFilter(searchParams.get("updated_from")));
  const [updatedTo, setUpdatedTo] = useState<string>(() => parseDateFilter(searchParams.get("updated_to")));

  const [createNotice, setCreateNotice] = useState<NoticeState>({
    tone: "info",
    message: "Create a job first, then upload files.",
  });
  const [uploadNotice, setUploadNotice] = useState<NoticeState>({
    tone: "info",
    message: "Supports single PDF / DOCX / XLSX files, and ZIP archives.",
  });
  const [runNotice, setRunNotice] = useState<NoticeState>({
    tone: "info",
    message: "Start the matching job after upload is complete.",
  });
  const [globalNotice, setGlobalNotice] = useState<NoticeState | null>(null);
  const [modelSettings, setModelSettings] = useState<ModelSettingsResponse | null>(null);

  const totalBytes = useMemo(() => files.reduce((sum, row) => sum + row.size, 0), [files]);

  const flowState: JobFlowState = useMemo(() => {
    if (actionState === "creating") return "creating";
    if (actionState === "uploading_files" || actionState === "uploading_archive") return "uploading";
    if (actionState === "starting") return "starting";
    return mapJobStatusToFlow(jobStatus);
  }, [actionState, jobStatus]);

  const createStepState: StepState = useMemo(() => {
    if (actionState === "creating") return "active";
    if (createNotice.tone === "error") return "error";
    if (jobId) return "done";
    return "pending";
  }, [actionState, createNotice.tone, jobId]);

  const uploadStepState: StepState = useMemo(() => {
    if (!jobId) return "pending";
    if (actionState === "uploading_archive" || actionState === "uploading_files") return "active";
    if (uploadNotice.tone === "error") return "error";
    if (uploadedFileCount > 0) return "done";
    return "pending";
  }, [actionState, jobId, uploadNotice.tone, uploadedFileCount]);

  const runStepState: StepState = useMemo(() => {
    if (!jobId || uploadedFileCount === 0) return "pending";
    if (actionState === "starting") return "active";
    if (runNotice.tone === "error") return "error";
    if (jobStatus === "running" || jobStatus === "succeeded" || jobStatus === "failed") return "done";
    return "pending";
  }, [actionState, jobId, uploadedFileCount, runNotice.tone, jobStatus]);

  const historyQuery = useMemo<JobListQuery>(
    () => ({
      status: statusFilter === "all" ? undefined : statusFilter,
      q: keywordFilter.trim() || undefined,
      updated_from: toStartDayIso(updatedFrom),
      updated_to: toEndDayIso(updatedTo),
    }),
    [keywordFilter, statusFilter, updatedFrom, updatedTo]
  );

  const {
    rows: historyRows,
    loading: historyLoading,
    loadingMore: historyLoadingMore,
    error: historyError,
    hasMore: historyHasMore,
    empty: historyEmpty,
    reload: refreshHistoryJobs,
    loadMore: loadMoreHistoryJobs,
  } = usePaginatedList<JobResponse, JobListQuery>({
    query: historyQuery,
    pageSize: HISTORY_PAGE_SIZE,
    fetchPage: listJobs,
  });

  useEffect(() => {
    const nextStatus = parseStatusFilter(searchParams.get("status"));
    const nextKeyword = searchParams.get("q") ?? "";
    const nextUpdatedFrom = parseDateFilter(searchParams.get("updated_from"));
    const nextUpdatedTo = parseDateFilter(searchParams.get("updated_to"));
    setStatusFilter((prev) => (prev === nextStatus ? prev : nextStatus));
    setKeywordFilter((prev) => (prev === nextKeyword ? prev : nextKeyword));
    setUpdatedFrom((prev) => (prev === nextUpdatedFrom ? prev : nextUpdatedFrom));
    setUpdatedTo((prev) => (prev === nextUpdatedTo ? prev : nextUpdatedTo));
  }, [searchParams]);

  useEffect(() => {
    const params = new URLSearchParams(searchParams.toString());
    if (statusFilter === "all") {
      params.delete("status");
    } else {
      params.set("status", statusFilter);
    }
    if (keywordFilter.trim()) {
      params.set("q", keywordFilter.trim());
    } else {
      params.delete("q");
    }
    if (updatedFrom) {
      params.set("updated_from", updatedFrom);
    } else {
      params.delete("updated_from");
    }
    if (updatedTo) {
      params.set("updated_to", updatedTo);
    } else {
      params.delete("updated_to");
    }
    const nextQuery = params.toString();
    const currentQuery = searchParams.toString();
    if (nextQuery === currentQuery) {
      return;
    }
    const targetUrl = nextQuery ? `${pathname}?${nextQuery}` : pathname;
    router.replace(targetUrl as Parameters<typeof router.replace>[0], { scroll: false });
  }, [keywordFilter, pathname, router, searchParams, statusFilter, updatedFrom, updatedTo]);

  useEffect(() => {
    if (!historyError) {
      return;
    }
    setGlobalNotice({
      tone: "error",
      message: `Cause: ${historyError}. Next: check backend logs, then click "Refresh list" and retry.`,
    });
  }, [historyError]);

  useEffect(() => {
    void getModelSettings()
      .then((payload) => setModelSettings(payload))
      .catch(() => null);
  }, []);

  const canCreate = !jobId && actionState === "idle";
  const canUpload = !!jobId && actionState === "idle";
  const canStart = !!jobId && uploadedFileCount > 0 && actionState === "idle" && Boolean(modelSettings?.has_api_key);

  async function handleCreateJob() {
    setActionState("creating");
    setCreateNotice({ tone: "info", message: "Creating job..." });
    try {
      const row = await createJob();
      setJobId(row.id);
      setJobStatus(row.status);
      setUploadedFileCount(0);
      setFiles([]);
      setArchive(null);
      setCreateNotice({ tone: "success", message: `Job created: ${row.id}` });
      setUploadNotice({ tone: "info", message: "Upload files or a ZIP archive." });
      setRunNotice({ tone: "info", message: "Start the job after upload succeeds." });
      await refreshHistoryJobs();
      setGlobalNotice(null);
    } catch (error) {
      setCreateNotice({
        tone: "error",
        message: toGuidedError(error, "Retry job creation later. If it keeps failing, check backend connectivity."),
      });
    } finally {
      setActionState("idle");
    }
  }

  async function handleUploadFiles() {
    if (!jobId || files.length === 0) return;
    setActionState("uploading_files");
    setUploadNotice({ tone: "info", message: `Uploading ${files.length} file(s)...` });

    try {
      let response: JobResponse | null = null;
      for (const file of files) {
        const relativePath = (file as File & { webkitRelativePath?: string }).webkitRelativePath || file.name;
        response = await uploadJobFile(jobId, file, relativePath);
      }

      if (response) {
        setJobStatus(response.status);
        setUploadedFileCount(response.file_count);
      }

      setUploadNotice({ tone: "success", message: `Upload complete. Total files: ${response?.file_count ?? files.length}.` });
      setRunNotice({ tone: "info", message: "You can now start the job and view progress in the realtime page." });
      setFiles([]);
      await refreshHistoryJobs();
    } catch (error) {
      setUploadNotice({
        tone: "error",
        message: toGuidedError(error, "Confirm file format and size, then retry upload."),
      });
    } finally {
      setActionState("idle");
    }
  }

  async function handleUploadArchive() {
    if (!jobId || !archive) return;
    setActionState("uploading_archive");
    setUploadNotice({ tone: "info", message: `Uploading archive ${archive.name}...` });

    try {
      const response = await uploadJobArchive(jobId, archive);
      setJobStatus(response.status);
      setUploadedFileCount(response.file_count);
      if (response.warnings && response.warnings.length > 0) {
        const preview = response.warnings.slice(0, 3).join("; ");
        const remain = response.warnings.length > 3 ? `; and ${response.warnings.length - 3} more` : "";
        setUploadNotice({
          tone: "warning",
          message: `Upload succeeded. Total files: ${response.file_count}. Skipped ${response.warnings.length} unsupported file(s): ${preview}${remain}`,
        });
      } else {
        setUploadNotice({ tone: "success", message: `Archive extracted. Total files: ${response.file_count}.` });
      }
      setRunNotice({ tone: "info", message: "You can now start the job and view progress in the realtime page." });
      setArchive(null);
      await refreshHistoryJobs();
    } catch (error) {
      setUploadNotice({
        tone: "error",
        message: toGuidedError(error, "Check ZIP file formats (only pdf/docx/xlsx), then retry."),
      });
    } finally {
      setActionState("idle");
    }
  }

  async function handleStartJob() {
    if (!jobId) return;
    setActionState("starting");
    setRunNotice({ tone: "info", message: "Job submitted. Starting pipeline..." });

    try {
      await startJob(jobId);
      setJobStatus("running");
      setRunNotice({ tone: "success", message: "Job started. Redirecting to realtime page." });
      await refreshHistoryJobs();
      router.push(`/jobs/${jobId}`);
    } catch (error) {
      setRunNotice({
        tone: "error",
        message: toGuidedError(error, "Confirm files are uploaded and status is ready, then retry."),
      });
      setActionState("idle");
    }
  }

  async function handleCopyJobId(id: string) {
    try {
      await navigator.clipboard.writeText(id);
      setGlobalNotice({ tone: "success", message: `Copied Job ID: ${id}` });
    } catch {
      setGlobalNotice({ tone: "warning", message: "Copy failed. Please copy this Job ID manually." });
    }
  }

  function clearHistoryFilters() {
    setStatusFilter("all");
    setKeywordFilter("");
    setUpdatedFrom("");
    setUpdatedTo("");
  }

  return (
    <div className="page-wrap grid gap-5">
      <section className="panel p-5 md:p-6">
        <SectionHeader
          title="Tender Matching Task Console"
          subtitle='Follow "Create Job → Upload Files → Start Job" to avoid state confusion.'
          right={
            <>
              <StatusBadge label={`Flow state: ${flowState}`} tone={toneFromKeyword(flowState)} />
              {jobId ? <StatusBadge label={`Job: ${jobId}`} tone="active" className="break-all" /> : null}
            </>
          }
        />
        <div className="mt-4 grid gap-3 md:grid-cols-3">
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">Current Job Status</span>
              <StatusBadge label={jobStatus ?? "idle"} tone={toneFromKeyword(jobStatus ?? "idle")} />
            </div>
            <div className="info-card-value">{jobStatus ?? "idle"}</div>
            <p className="info-card-subtitle">Backend status is authoritative; frontend shows temporary transition states only</p>
          </article>
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">Uploaded Files</span>
              <StatusBadge label={uploadedFileCount > 0 ? "Ready" : "Pending upload"} tone={uploadedFileCount > 0 ? "done" : "pending"} />
            </div>
            <div className="info-card-value">{uploadedFileCount}</div>
            <p className="info-card-subtitle">Supports PDF / DOCX / XLSX. ZIP is extracted and validated server-side.</p>
          </article>
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">Selected File Size</span>
              <StatusBadge label={`${files.length} file(s)`} tone={files.length > 0 ? "active" : "idle"} />
            </div>
            <div className="info-card-value">{(totalBytes / 1024 / 1024).toFixed(2)} MB</div>
            <p className="info-card-subtitle">Counts only files currently selected in the uploader</p>
          </article>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-[1.35fr_1fr]">
        <div className="panel p-5 md:p-6">
          <SectionHeader title="Workflow Steps" subtitle="Each step has independent feedback, and errors include actionable next steps." />

          <div className="mt-4 grid gap-3">
            <article className="timeline-item">
              <div className="timeline-item-header">
                <div className="timeline-item-title">
                  <span className="timeline-item-index">1</span>
                  <div>
                    <h3 className="m-0 text-sm font-semibold">Create Job</h3>
                    <p className="m-0 mt-1 text-xs muted-text">Generate a Job ID used by all subsequent upload and execution actions</p>
                  </div>
                </div>
                <StatusBadge label={stepLabel(createStepState)} tone={toStepTone(createStepState)} />
              </div>
              <div className="mt-3 flex flex-wrap items-center gap-2">
                <ActionButton onClick={handleCreateJob} disabled={!canCreate} variant="primary">
                  {jobId ? "Job Created" : "Create Job"}
                </ActionButton>
              </div>
              <InlineNotice tone={createNotice.tone} message={createNotice.message} className="mt-3" />
            </article>

            <article className="timeline-item">
              <div className="timeline-item-header">
                <div className="timeline-item-title">
                  <span className="timeline-item-index">2</span>
                  <div>
                    <h3 className="m-0 text-sm font-semibold">Upload Files</h3>
                    <p className="m-0 mt-1 text-xs muted-text">Supports file-by-file upload and ZIP upload with backend safety checks</p>
                  </div>
                </div>
                <StatusBadge label={stepLabel(uploadStepState)} tone={toStepTone(uploadStepState)} />
              </div>

              <div className="mt-3 grid gap-3 lg:grid-cols-2">
                <div className="panel-soft p-3">
                  <p className="m-0 text-xs font-semibold">Upload Individual Files</p>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/20 bg-black/35 p-2 text-sm"
                    type="file"
                    accept=".pdf,.docx,.xlsx"
                    multiple
                    onChange={(event) => setFiles(Array.from(event.target.files ?? []))}
                  />
                  <p className="mt-2 text-xs muted-text">
                    Selected {files.length} file(s), {(totalBytes / 1024 / 1024).toFixed(2)} MB
                  </p>
                  <ActionButton
                    onClick={handleUploadFiles}
                    disabled={!canUpload || files.length === 0}
                    variant="secondary"
                    className="mt-2"
                  >
                    Upload Selected Files
                  </ActionButton>
                </div>

                <div className="panel-soft p-3">
                  <p className="m-0 text-xs font-semibold">Upload ZIP</p>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/20 bg-black/35 p-2 text-sm"
                    type="file"
                    accept=".zip"
                    onChange={(event) => setArchive(event.target.files?.[0] ?? null)}
                  />
                  <p className="mt-2 text-xs muted-text">Archive will be checked for path safety, extension rules, and size limits</p>
                  <ActionButton
                    onClick={handleUploadArchive}
                    disabled={!canUpload || !archive}
                    variant="secondary"
                    className="mt-2"
                  >
                    Upload & Extract ZIP
                  </ActionButton>
                </div>
              </div>

              <InlineNotice tone={uploadNotice.tone} message={uploadNotice.message} className="mt-3" />
            </article>

            <article className="timeline-item">
              <div className="timeline-item-header">
                <div className="timeline-item-title">
                  <span className="timeline-item-index">3</span>
                  <div>
                    <h3 className="m-0 text-sm font-semibold">Start Job</h3>
                    <p className="m-0 mt-1 text-xs muted-text">After start, enter the SSE realtime page to monitor steps and results</p>
                  </div>
                </div>
                <StatusBadge label={stepLabel(runStepState)} tone={toStepTone(runStepState)} />
              </div>

              <div className="mt-3 flex flex-wrap items-center gap-2">
                <ActionButton onClick={handleStartJob} disabled={!canStart} variant="success">
                  Start Matching Job
                </ActionButton>
              </div>
              <InlineNotice tone={runNotice.tone} message={runNotice.message} className="mt-3" />
              {modelSettings?.has_api_key === false ? (
                <InlineNotice
                  tone="warning"
                  message="Cause: OpenAI API key is not configured. Next: configure OPENAI_API_KEY in backend before starting."
                  className="mt-3"
                />
              ) : null}
            </article>
          </div>
        </div>

        <aside className="panel p-5 md:p-6">
          <SectionHeader title="Operation Tips" subtitle="Help you locate issues faster" />
          <ul className="mt-3 space-y-2 text-sm text-slate-200">
            <li>1. If upload fails, check the error message for "Cause + Next step" first.</li>
            <li>2. For ZIP uploads, validate file extensions locally first to avoid invalid retries.</li>
            <li>3. After starting a job, you can revisit the same job from history at any time.</li>
          </ul>
          {globalNotice ? <InlineNotice tone={globalNotice.tone} message={globalNotice.message} className="mt-4" /> : null}
        </aside>
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader
          title="Job History"
          subtitle="Server-side filtering + paginated loading, sorted by latest update by default"
          right={
            <ActionButton onClick={() => void refreshHistoryJobs()} disabled={historyLoading} variant="ghost">
              {historyLoading ? "Refreshing..." : "Refresh list"}
            </ActionButton>
          }
        />

        <div className="mt-4 grid gap-2 lg:grid-cols-[12rem_1fr_10rem_10rem_auto]">
          <label className="text-xs text-slate-200">
            Status
            <select
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value as "all" | JobStatus)}
            >
              {STATUS_OPTIONS.map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>

          <label className="text-xs text-slate-200">
            Keyword (Job ID)
            <input
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={keywordFilter}
              onChange={(event) => setKeywordFilter(event.target.value)}
              placeholder="e.g. f1bf27ec"
            />
          </label>

          <label className="text-xs text-slate-200">
            Updated from
            <input
              type="date"
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={updatedFrom}
              onChange={(event) => setUpdatedFrom(event.target.value)}
            />
          </label>

          <label className="text-xs text-slate-200">
            Updated to
            <input
              type="date"
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={updatedTo}
              onChange={(event) => setUpdatedTo(event.target.value)}
            />
          </label>

          <div className="flex items-end">
            <ActionButton onClick={clearHistoryFilters} variant="secondary" className="w-full">
              Clear filters
            </ActionButton>
          </div>
        </div>

        <div className="mt-4 overflow-auto rounded-xl border border-white/10">
          <table className="data-table min-w-[760px] table-fixed">
            <thead>
              <tr>
                <th className="w-[18rem]">Job ID</th>
                <th className="w-[8rem]">Status</th>
                <th className="w-[5.5rem]">Files</th>
                <th className="w-[5.5rem]">Steps</th>
                <th className="w-[9rem]">Updated At</th>
                <th className="w-[12rem]">Actions</th>
              </tr>
            </thead>
            <tbody>
              {historyRows.map((row) => (
                <tr key={row.id}>
                  <td className="font-mono text-xs">
                    <span className="block truncate" title={row.id}>
                      {row.id}
                    </span>
                  </td>
                  <td>
                    <StatusBadge label={row.status} tone={toneFromKeyword(row.status)} />
                  </td>
                  <td>{row.file_count}</td>
                  <td>{row.step_count}</td>
                  <td>{formatDateTime(row.updated_at)}</td>
                  <td>
                    <div className="flex flex-wrap gap-2">
                      <ActionButton variant="ghost" onClick={() => handleCopyJobId(row.id)}>
                        Copy ID
                      </ActionButton>
                      <ActionButton variant="secondary" onClick={() => router.push(`/jobs/${row.id}`)}>
                        View Job
                      </ActionButton>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {historyEmpty ? (
          <div className="mt-4">
            <EmptyState title="No Matching Jobs" description="Create a new job, upload files, and start the matching workflow." />
          </div>
        ) : null}

        {!historyEmpty ? (
          <div className="mt-4 flex flex-wrap items-center justify-between gap-2">
            <p className="text-xs muted-text">Loaded {historyRows.length} record(s)</p>
            {historyHasMore ? (
              <ActionButton onClick={() => void loadMoreHistoryJobs()} disabled={historyLoadingMore} variant="secondary">
                {historyLoadingMore ? "Loading..." : "Load more"}
              </ActionButton>
            ) : (
              <span className="text-xs muted-text">End of list</span>
            )}
          </div>
        ) : null}
      </section>
    </div>
  );
}

export default function HomePage() {
  return (
    <Suspense fallback={<div className="page-wrap py-8 text-sm text-slate-200">Loading task console...</div>}>
      <HomePageContent />
    </Suspense>
  );
}
