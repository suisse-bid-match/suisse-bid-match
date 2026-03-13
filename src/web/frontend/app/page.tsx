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
  { value: "all", label: "全部状态" },
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
  if (step === "done") return "已完成";
  if (step === "active") return "进行中";
  if (step === "error") return "失败";
  return "待执行";
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
    message: "先创建任务，再上传文件。",
  });
  const [uploadNotice, setUploadNotice] = useState<NoticeState>({
    tone: "info",
    message: "支持 PDF / DOCX / XLSX 单文件，以及 ZIP 压缩包。",
  });
  const [runNotice, setRunNotice] = useState<NoticeState>({
    tone: "info",
    message: "上传完成后可启动匹配任务。",
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
      message: `原因：${historyError}。下一步：检查后端服务日志后点击“刷新列表”重试`,
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
    setCreateNotice({ tone: "info", message: "正在创建任务..." });
    try {
      const row = await createJob();
      setJobId(row.id);
      setJobStatus(row.status);
      setUploadedFileCount(0);
      setFiles([]);
      setArchive(null);
      setCreateNotice({ tone: "success", message: `任务已创建：${row.id}` });
      setUploadNotice({ tone: "info", message: "请上传文件或压缩包。" });
      setRunNotice({ tone: "info", message: "上传成功后再启动任务。" });
      await refreshHistoryJobs();
      setGlobalNotice(null);
    } catch (error) {
      setCreateNotice({
        tone: "error",
        message: toGuidedError(error, "稍后重试创建任务，若持续失败请检查后端连接"),
      });
    } finally {
      setActionState("idle");
    }
  }

  async function handleUploadFiles() {
    if (!jobId || files.length === 0) return;
    setActionState("uploading_files");
    setUploadNotice({ tone: "info", message: `正在上传 ${files.length} 个文件...` });

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

      setUploadNotice({ tone: "success", message: `上传完成，共 ${response?.file_count ?? files.length} 个文件。` });
      setRunNotice({ tone: "info", message: "可启动任务，进入实时执行页面查看进度。" });
      setFiles([]);
      await refreshHistoryJobs();
    } catch (error) {
      setUploadNotice({
        tone: "error",
        message: toGuidedError(error, "确认文件格式和大小后重试上传"),
      });
    } finally {
      setActionState("idle");
    }
  }

  async function handleUploadArchive() {
    if (!jobId || !archive) return;
    setActionState("uploading_archive");
    setUploadNotice({ tone: "info", message: `正在上传压缩包 ${archive.name}...` });

    try {
      const response = await uploadJobArchive(jobId, archive);
      setJobStatus(response.status);
      setUploadedFileCount(response.file_count);
      if (response.warnings && response.warnings.length > 0) {
        const preview = response.warnings.slice(0, 3).join("；");
        const remain = response.warnings.length > 3 ? `；另有 ${response.warnings.length - 3} 条` : "";
        setUploadNotice({
          tone: "warning",
          message: `上传成功，累计文件 ${response.file_count} 个。已跳过 ${response.warnings.length} 个不支持文件：${preview}${remain}`,
        });
      } else {
        setUploadNotice({ tone: "success", message: `压缩包已解压，累计文件 ${response.file_count} 个。` });
      }
      setRunNotice({ tone: "info", message: "可启动任务，进入实时执行页面查看进度。" });
      setArchive(null);
      await refreshHistoryJobs();
    } catch (error) {
      setUploadNotice({
        tone: "error",
        message: toGuidedError(error, "检查 ZIP 内文件格式（仅 pdf/docx/xlsx）并重试"),
      });
    } finally {
      setActionState("idle");
    }
  }

  async function handleStartJob() {
    if (!jobId) return;
    setActionState("starting");
    setRunNotice({ tone: "info", message: "任务已提交，正在启动 pipeline..." });

    try {
      await startJob(jobId);
      setJobStatus("running");
      setRunNotice({ tone: "success", message: "任务已启动，正在跳转到实时页面。" });
      await refreshHistoryJobs();
      router.push(`/jobs/${jobId}`);
    } catch (error) {
      setRunNotice({
        tone: "error",
        message: toGuidedError(error, "确认任务已上传文件且状态为 ready 后重试"),
      });
      setActionState("idle");
    }
  }

  async function handleCopyJobId(id: string) {
    try {
      await navigator.clipboard.writeText(id);
      setGlobalNotice({ tone: "success", message: `已复制 Job ID：${id}` });
    } catch {
      setGlobalNotice({ tone: "warning", message: "复制失败，请手动复制该 Job ID。" });
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
          title="投标匹配任务控制台"
          subtitle="按照“创建任务 → 上传文件 → 启动任务”的顺序操作，避免状态混乱。"
          right={
            <>
              <StatusBadge label={`流程状态: ${flowState}`} tone={toneFromKeyword(flowState)} />
              {jobId ? <StatusBadge label={`Job: ${jobId}`} tone="active" className="break-all" /> : null}
            </>
          }
        />
        <div className="mt-4 grid gap-3 md:grid-cols-3">
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">当前任务状态</span>
              <StatusBadge label={jobStatus ?? "idle"} tone={toneFromKeyword(jobStatus ?? "idle")} />
            </div>
            <div className="info-card-value">{jobStatus ?? "idle"}</div>
            <p className="info-card-subtitle">以服务端状态为准，前端仅做短暂过渡态展示</p>
          </article>
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">已上传文件</span>
              <StatusBadge label={uploadedFileCount > 0 ? "已就绪" : "待上传"} tone={uploadedFileCount > 0 ? "done" : "pending"} />
            </div>
            <div className="info-card-value">{uploadedFileCount}</div>
            <p className="info-card-subtitle">支持 PDF / DOCX / XLSX，ZIP 会在服务端解压并校验</p>
          </article>
          <article className="info-card">
            <div className="info-card-top">
              <span className="info-card-title">本次选择文件体积</span>
              <StatusBadge label={`${files.length} file(s)`} tone={files.length > 0 ? "active" : "idle"} />
            </div>
            <div className="info-card-value">{(totalBytes / 1024 / 1024).toFixed(2)} MB</div>
            <p className="info-card-subtitle">仅统计当前文件选择器中的待上传文件</p>
          </article>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-[1.35fr_1fr]">
        <div className="panel p-5 md:p-6">
          <SectionHeader title="流程步骤" subtitle="每一步都有独立反馈，错误会带上可执行的下一步建议。" />

          <div className="mt-4 grid gap-3">
            <article className="timeline-item">
              <div className="timeline-item-header">
                <div className="timeline-item-title">
                  <span className="timeline-item-index">1</span>
                  <div>
                    <h3 className="m-0 text-sm font-semibold">创建任务</h3>
                    <p className="m-0 mt-1 text-xs muted-text">生成 Job ID，后续上传与执行都绑定该任务</p>
                  </div>
                </div>
                <StatusBadge label={stepLabel(createStepState)} tone={toStepTone(createStepState)} />
              </div>
              <div className="mt-3 flex flex-wrap items-center gap-2">
                <ActionButton onClick={handleCreateJob} disabled={!canCreate} variant="primary">
                  {jobId ? "任务已创建" : "创建任务"}
                </ActionButton>
              </div>
              <InlineNotice tone={createNotice.tone} message={createNotice.message} className="mt-3" />
            </article>

            <article className="timeline-item">
              <div className="timeline-item-header">
                <div className="timeline-item-title">
                  <span className="timeline-item-index">2</span>
                  <div>
                    <h3 className="m-0 text-sm font-semibold">上传文件</h3>
                    <p className="m-0 mt-1 text-xs muted-text">支持逐文件上传和 ZIP 上传，后端会进行安全校验</p>
                  </div>
                </div>
                <StatusBadge label={stepLabel(uploadStepState)} tone={toStepTone(uploadStepState)} />
              </div>

              <div className="mt-3 grid gap-3 lg:grid-cols-2">
                <div className="panel-soft p-3">
                  <p className="m-0 text-xs font-semibold">上传单文件</p>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/20 bg-black/35 p-2 text-sm"
                    type="file"
                    accept=".pdf,.docx,.xlsx"
                    multiple
                    onChange={(event) => setFiles(Array.from(event.target.files ?? []))}
                  />
                  <p className="mt-2 text-xs muted-text">
                    已选 {files.length} 个文件，{(totalBytes / 1024 / 1024).toFixed(2)} MB
                  </p>
                  <ActionButton
                    onClick={handleUploadFiles}
                    disabled={!canUpload || files.length === 0}
                    variant="secondary"
                    className="mt-2"
                  >
                    上传所选文件
                  </ActionButton>
                </div>

                <div className="panel-soft p-3">
                  <p className="m-0 text-xs font-semibold">上传 ZIP</p>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/20 bg-black/35 p-2 text-sm"
                    type="file"
                    accept=".zip"
                    onChange={(event) => setArchive(event.target.files?.[0] ?? null)}
                  />
                  <p className="mt-2 text-xs muted-text">压缩包将进行路径安全、扩展名和大小限制检查</p>
                  <ActionButton
                    onClick={handleUploadArchive}
                    disabled={!canUpload || !archive}
                    variant="secondary"
                    className="mt-2"
                  >
                    上传并解压 ZIP
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
                    <h3 className="m-0 text-sm font-semibold">启动任务</h3>
                    <p className="m-0 mt-1 text-xs muted-text">任务启动后进入 SSE 实时监控页，查看步骤进度和结果</p>
                  </div>
                </div>
                <StatusBadge label={stepLabel(runStepState)} tone={toStepTone(runStepState)} />
              </div>

              <div className="mt-3 flex flex-wrap items-center gap-2">
                <ActionButton onClick={handleStartJob} disabled={!canStart} variant="success">
                  启动匹配任务
                </ActionButton>
              </div>
              <InlineNotice tone={runNotice.tone} message={runNotice.message} className="mt-3" />
              {modelSettings?.has_api_key === false ? (
                <InlineNotice
                  tone="warning"
                  message="原因：OpenAI API Key 未配置。下一步：先在后端配置 OPENAI_API_KEY 后再启动任务。"
                  className="mt-3"
                />
              ) : null}
            </article>
          </div>
        </div>

        <aside className="panel p-5 md:p-6">
          <SectionHeader title="操作提示" subtitle="帮助你更快定位问题" />
          <ul className="mt-3 space-y-2 text-sm text-slate-200">
            <li>1. 若上传失败，先看错误里“原因 + 下一步”提示。</li>
            <li>2. ZIP 场景建议先本地检查扩展名，避免无效重试。</li>
            <li>3. 任务启动后可在历史列表中随时回看同一 Job。</li>
          </ul>
          {globalNotice ? <InlineNotice tone={globalNotice.tone} message={globalNotice.message} className="mt-4" /> : null}
        </aside>
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader
          title="历史任务"
          subtitle="服务端筛选 + 分页加载，默认按最近更新时间排序"
          right={
            <ActionButton onClick={() => void refreshHistoryJobs()} disabled={historyLoading} variant="ghost">
              {historyLoading ? "刷新中..." : "刷新列表"}
            </ActionButton>
          }
        />

        <div className="mt-4 grid gap-2 lg:grid-cols-[12rem_1fr_10rem_10rem_auto]">
          <label className="text-xs text-slate-200">
            状态
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
            关键词（Job ID）
            <input
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={keywordFilter}
              onChange={(event) => setKeywordFilter(event.target.value)}
              placeholder="例如：f1bf27ec"
            />
          </label>

          <label className="text-xs text-slate-200">
            更新起始日
            <input
              type="date"
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={updatedFrom}
              onChange={(event) => setUpdatedFrom(event.target.value)}
            />
          </label>

          <label className="text-xs text-slate-200">
            更新截止日
            <input
              type="date"
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={updatedTo}
              onChange={(event) => setUpdatedTo(event.target.value)}
            />
          </label>

          <div className="flex items-end">
            <ActionButton onClick={clearHistoryFilters} variant="secondary" className="w-full">
              清空筛选
            </ActionButton>
          </div>
        </div>

        <div className="mt-4 overflow-auto rounded-xl border border-white/10">
          <table className="data-table min-w-[760px] table-fixed">
            <thead>
              <tr>
                <th className="w-[18rem]">Job ID</th>
                <th className="w-[8rem]">状态</th>
                <th className="w-[5.5rem]">文件数</th>
                <th className="w-[5.5rem]">步骤数</th>
                <th className="w-[9rem]">更新时间</th>
                <th className="w-[12rem]">操作</th>
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
                        复制 ID
                      </ActionButton>
                      <ActionButton variant="secondary" onClick={() => router.push(`/jobs/${row.id}`)}>
                        查看任务
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
            <EmptyState title="暂无符合条件的任务" description="可以先创建一个新任务，上传文件后启动匹配流程。" />
          </div>
        ) : null}

        {!historyEmpty ? (
          <div className="mt-4 flex flex-wrap items-center justify-between gap-2">
            <p className="text-xs muted-text">已加载 {historyRows.length} 条记录</p>
            {historyHasMore ? (
              <ActionButton onClick={() => void loadMoreHistoryJobs()} disabled={historyLoadingMore} variant="secondary">
                {historyLoadingMore ? "加载中..." : "加载更多"}
              </ActionButton>
            ) : (
              <span className="text-xs muted-text">已到末页</span>
            )}
          </div>
        ) : null}
      </section>
    </div>
  );
}

export default function HomePage() {
  return (
    <Suspense fallback={<div className="page-wrap py-8 text-sm text-slate-200">正在加载任务控制台...</div>}>
      <HomePageContent />
    </Suspense>
  );
}
