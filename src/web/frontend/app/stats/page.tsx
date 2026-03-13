"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import { getStatsDashboard, type StatsDashboardResponse, type StatsDashboardQuery } from "@/lib/api";
import {
  buildExtractedProductsOption,
  buildFieldHeatmapOption,
  buildJobDurationOption,
  buildStepDurationOption,
} from "@/lib/stats-charts";
import { formatDateTime, formatDuration, toGuidedError } from "@/lib/view-models";
import { ActionButton, EmptyState, InlineNotice, SectionHeader, StatusBadge } from "@/components/ui";

const ReactECharts = dynamic(() => import("echarts-for-react"), {
  ssr: false,
  loading: () => <div className="stats-chart-loading">图表加载中...</div>,
}) as typeof import("echarts-for-react").default;

type NoticeTone = "info" | "success" | "warning" | "error";

interface NoticeState {
  tone: NoticeTone;
  message: string;
}

const DAY_OPTIONS: Array<{ label: string; value: 7 | 30 | 90 }> = [
  { label: "7 天", value: 7 },
  { label: "30 天", value: 30 },
  { label: "90 天", value: 90 },
];

const TOP_N_OPTIONS: Array<{ label: string; value: 20 | 40 | 60 }> = [
  { label: "Top 20", value: 20 },
  { label: "Top 40", value: 40 },
  { label: "Top 60", value: 60 },
];

function formatNumber(input: number | null | undefined, digits = 1): string {
  if (input == null || Number.isNaN(input)) return "-";
  return input.toFixed(digits);
}

function calcHeatmapHeight(cellCount: number): number {
  const columns = 8;
  const rowCount = Math.max(1, Math.ceil(cellCount / columns));
  return Math.min(560, Math.max(260, rowCount * 56 + 84));
}

export default function StatsPage() {
  const [days, setDays] = useState<7 | 30 | 90>(30);
  const [includeFailed, setIncludeFailed] = useState(true);
  const [topN, setTopN] = useState<20 | 40 | 60>(40);
  const [refreshToken, setRefreshToken] = useState(0);

  const [loading, setLoading] = useState(false);
  const [dashboard, setDashboard] = useState<StatsDashboardResponse | null>(null);
  const [notice, setNotice] = useState<NoticeState>({
    tone: "info",
    message: "设置筛选条件后会自动刷新统计数据。",
  });
  const [lastUpdatedAt, setLastUpdatedAt] = useState<string | null>(null);

  const query = useMemo<StatsDashboardQuery>(
    () => ({
      days,
      include_failed: includeFailed,
      top_n: topN,
    }),
    [days, includeFailed, topN]
  );

  useEffect(() => {
    let active = true;
    async function run() {
      setLoading(true);
      setNotice({ tone: "info", message: "正在加载统计数据..." });
      try {
        const payload = await getStatsDashboard(query);
        if (!active) return;
        setDashboard(payload);
        setLastUpdatedAt(new Date().toISOString());
        setNotice({ tone: "success", message: "统计数据已更新。" });
      } catch (error) {
        if (!active) return;
        setNotice({
          tone: "error",
          message: toGuidedError(error, "检查 backend 服务和数据库连接后重试"),
        });
      } finally {
        if (active) setLoading(false);
      }
    }
    void run();
    return () => {
      active = false;
    };
  }, [query, refreshToken]);

  const allJobs = dashboard?.job_durations ?? [];
  const durationJobs = useMemo(() => allJobs.filter((row) => row.duration_ms != null), [allJobs]);
  const extractedJobs = useMemo(() => allJobs.filter((row) => row.extracted_products != null), [allJobs]);
  const stepRows = dashboard?.step_durations ?? [];
  const fieldRows = dashboard?.field_frequency ?? [];

  const durationOption = useMemo(() => buildJobDurationOption(durationJobs), [durationJobs]);
  const stepOption = useMemo(() => buildStepDurationOption(stepRows), [stepRows]);
  const productsOption = useMemo(() => buildExtractedProductsOption(extractedJobs), [extractedJobs]);
  const heatmapOption = useMemo(() => buildFieldHeatmapOption(fieldRows), [fieldRows]);
  const heatmapHeight = useMemo(() => calcHeatmapHeight(fieldRows.length), [fieldRows.length]);

  return (
    <div className="page-wrap grid gap-5">
      <section className="panel p-5 md:p-6">
        <SectionHeader
          title="统计分析面板"
          subtitle="图表优先展示任务耗时、步骤分布、抽取规模与字段频率。"
          right={
            <>
              <StatusBadge label={loading ? "loading" : "ready"} tone={loading ? "running" : "done"} />
              <StatusBadge label={`最近更新: ${formatDateTime(lastUpdatedAt)}`} tone="active" />
            </>
          }
        />

        <div className="mt-4 grid gap-2 lg:grid-cols-[9rem_9rem_9rem_auto]">
          <label className="text-xs text-slate-200">
            时间窗口
            <select
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={days}
              onChange={(event) => setDays(Number(event.target.value) as 7 | 30 | 90)}
            >
              {DAY_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="text-xs text-slate-200">
            字段热力 Top N
            <select
              className="mt-1 w-full rounded-lg border border-white/20 bg-black/30 px-2 py-1.5 text-xs"
              value={topN}
              onChange={(event) => setTopN(Number(event.target.value) as 20 | 40 | 60)}
            >
              {TOP_N_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>

          <label className="text-xs text-slate-200">
            任务范围
            <div className="mt-1 flex h-[34px] items-center rounded-lg border border-white/20 bg-black/30 px-3">
              <input
                id="include-failed"
                type="checkbox"
                checked={includeFailed}
                onChange={(event) => setIncludeFailed(event.target.checked)}
              />
              <label htmlFor="include-failed" className="ml-2 text-xs text-slate-200">
                计入 failed
              </label>
            </div>
          </label>

          <div className="flex items-end">
            <ActionButton onClick={() => setRefreshToken((prev) => prev + 1)} disabled={loading} variant="secondary" className="w-full">
              {loading ? "刷新中..." : "刷新统计"}
            </ActionButton>
          </div>
        </div>

        <InlineNotice tone={notice.tone} message={notice.message} className="mt-3" />
      </section>

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <article className="info-card">
          <div className="info-card-top">
            <span className="info-card-title">任务总数</span>
            <StatusBadge label={`${dashboard?.overview.job_count ?? 0}`} tone="active" />
          </div>
          <div className="info-card-value">{dashboard?.overview.job_count ?? 0}</div>
          <p className="info-card-subtitle">
            成功 {dashboard?.overview.succeeded_count ?? 0} / 失败 {dashboard?.overview.failed_count ?? 0}
          </p>
        </article>
        <article className="info-card">
          <div className="info-card-top">
            <span className="info-card-title">平均任务耗时</span>
            <StatusBadge label="avg" tone="running" />
          </div>
          <div className="info-card-value">
            {formatDuration(
              dashboard?.overview.avg_job_duration_ms == null ? null : Math.round(dashboard.overview.avg_job_duration_ms)
            )}
          </div>
          <p className="info-card-subtitle">
            P50 {formatDuration(dashboard?.overview.p50_job_duration_ms ?? null)} / P90{" "}
            {formatDuration(dashboard?.overview.p90_job_duration_ms ?? null)}
          </p>
        </article>
        <article className="info-card">
          <div className="info-card-top">
            <span className="info-card-title">平均抽取产品数</span>
            <StatusBadge label="step2" tone="active" />
          </div>
          <div className="info-card-value">{formatNumber(dashboard?.overview.avg_extracted_products, 2)}</div>
          <p className="info-card-subtitle">统计窗口内每个 job 的 step2 抽取数量</p>
        </article>
        <article className="info-card">
          <div className="info-card-top">
            <span className="info-card-title">窗口范围</span>
            <StatusBadge label={`${days}d`} tone="done" />
          </div>
          <div className="info-card-value text-base">{formatDateTime(dashboard?.overview.window_from ?? null)}</div>
          <p className="info-card-subtitle">至 {formatDateTime(dashboard?.overview.window_to ?? null)}</p>
        </article>
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader title="Job 总耗时分布" subtitle="横向柱状图（按更新时间排序，支持 dataZoom）。" />
        {durationJobs.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="暂无任务样本" description="当前筛选条件下没有可用于总耗时统计的任务。" />
          </div>
        ) : (
          <div className="stats-chart-shell mt-3">
            <ReactECharts option={durationOption} notMerge lazyUpdate style={{ height: 440, width: "100%" }} opts={{ renderer: "canvas" }} />
          </div>
        )}
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader title="Step 耗时分布对比" subtitle="分组柱状图：avg / p50 / p90。" />
        {stepRows.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="暂无步骤耗时数据" description="当前样本不足，尚无法展示步骤耗时分布。" />
          </div>
        ) : (
          <div className="stats-chart-shell mt-3">
            <ReactECharts option={stepOption} notMerge lazyUpdate style={{ height: 420, width: "100%" }} opts={{ renderer: "canvas" }} />
          </div>
        )}
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader title="抽取产品数量分布" subtitle="柱状图（逐 job 的 step2 抽取数量）。" />
        {extractedJobs.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="暂无抽取样本" description="当前筛选条件下没有 step2 抽取数据。" />
          </div>
        ) : (
          <div className="stats-chart-shell mt-3">
            <ReactECharts option={productsOption} notMerge lazyUpdate style={{ height: 440, width: "100%" }} opts={{ renderer: "canvas" }} />
          </div>
        )}
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader title="数据库参数频率热力图" subtitle={`按字段出现次数排序，当前展示 Top ${topN}。`} />
        {fieldRows.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="暂无字段频率数据" description="请先执行包含 step2 参数抽取的任务。" />
          </div>
        ) : (
          <div className="stats-chart-shell mt-3">
            <ReactECharts
              option={heatmapOption}
              notMerge
              lazyUpdate
              style={{ height: heatmapHeight, width: "100%" }}
              opts={{ renderer: "canvas" }}
            />
          </div>
        )}
      </section>
    </div>
  );
}
