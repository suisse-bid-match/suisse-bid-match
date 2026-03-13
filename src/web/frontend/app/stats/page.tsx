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
  loading: () => <div className="stats-chart-loading">Loading chart...</div>,
}) as typeof import("echarts-for-react").default;

type NoticeTone = "info" | "success" | "warning" | "error";

interface NoticeState {
  tone: NoticeTone;
  message: string;
}

const DAY_OPTIONS: Array<{ label: string; value: 7 | 30 | 90 }> = [
  { label: "7 days", value: 7 },
  { label: "30 days", value: 30 },
  { label: "90 days", value: 90 },
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
    message: "Statistics refresh automatically when filters change.",
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
      setNotice({ tone: "info", message: "Loading statistics..." });
      try {
        const payload = await getStatsDashboard(query);
        if (!active) return;
        setDashboard(payload);
        setLastUpdatedAt(new Date().toISOString());
        setNotice({ tone: "success", message: "Statistics updated." });
      } catch (error) {
        if (!active) return;
        setNotice({
          tone: "error",
          message: toGuidedError(error, "Check backend service and database connection, then retry"),
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
          title="Analytics Dashboard"
          subtitle="Chart-first view of job duration, step distribution, extraction scale, and field frequency."
          right={
            <>
              <StatusBadge label={loading ? "loading" : "ready"} tone={loading ? "running" : "done"} />
              <StatusBadge label={`Last updated: ${formatDateTime(lastUpdatedAt)}`} tone="active" />
            </>
          }
        />

        <div className="mt-4 grid gap-2 lg:grid-cols-[9rem_9rem_9rem_auto]">
          <label className="text-xs text-slate-200">
            Time window
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
            Field heatmap Top N
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
            Job scope
            <div className="mt-1 flex h-[34px] items-center rounded-lg border border-white/20 bg-black/30 px-3">
              <input
                id="include-failed"
                type="checkbox"
                checked={includeFailed}
                onChange={(event) => setIncludeFailed(event.target.checked)}
              />
              <label htmlFor="include-failed" className="ml-2 text-xs text-slate-200">
                Include failed
              </label>
            </div>
          </label>

          <div className="flex items-end">
            <ActionButton onClick={() => setRefreshToken((prev) => prev + 1)} disabled={loading} variant="secondary" className="w-full">
              {loading ? "Refreshing..." : "Refresh stats"}
            </ActionButton>
          </div>
        </div>

        <InlineNotice tone={notice.tone} message={notice.message} className="mt-3" />
      </section>

      <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <article className="info-card">
          <div className="info-card-top">
            <span className="info-card-title">Total Jobs</span>
            <StatusBadge label={`${dashboard?.overview.job_count ?? 0}`} tone="active" />
          </div>
          <div className="info-card-value">{dashboard?.overview.job_count ?? 0}</div>
          <p className="info-card-subtitle">
            Succeeded {dashboard?.overview.succeeded_count ?? 0} / Failed {dashboard?.overview.failed_count ?? 0}
          </p>
        </article>
        <article className="info-card">
          <div className="info-card-top">
            <span className="info-card-title">Average Job Duration</span>
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
            <span className="info-card-title">Average Extracted Products</span>
            <StatusBadge label="step2" tone="active" />
          </div>
          <div className="info-card-value">{formatNumber(dashboard?.overview.avg_extracted_products, 2)}</div>
          <p className="info-card-subtitle">Step2 extracted product count per job in this time window</p>
        </article>
        <article className="info-card">
          <div className="info-card-top">
            <span className="info-card-title">Window Range</span>
            <StatusBadge label={`${days}d`} tone="done" />
          </div>
          <div className="info-card-value text-base">{formatDateTime(dashboard?.overview.window_from ?? null)}</div>
          <p className="info-card-subtitle">to {formatDateTime(dashboard?.overview.window_to ?? null)}</p>
        </article>
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader title="Job Total Duration Distribution" subtitle="Horizontal bar chart (sorted by update time, supports dataZoom)." />
        {durationJobs.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="No Job Samples" description="No jobs with total duration are available for the current filters." />
          </div>
        ) : (
          <div className="stats-chart-shell mt-3">
            <ReactECharts option={durationOption} notMerge lazyUpdate style={{ height: 440, width: "100%" }} opts={{ renderer: "canvas" }} />
          </div>
        )}
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader title="Step Duration Distribution" subtitle="Grouped bar chart: avg / p50 / p90." />
        {stepRows.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="No Step Duration Data" description="The current sample set is too small to render step duration distribution." />
          </div>
        ) : (
          <div className="stats-chart-shell mt-3">
            <ReactECharts option={stepOption} notMerge lazyUpdate style={{ height: 420, width: "100%" }} opts={{ renderer: "canvas" }} />
          </div>
        )}
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader title="Extracted Product Count Distribution" subtitle="Bar chart of Step2 extracted product counts by job." />
        {extractedJobs.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="No Extraction Samples" description="No Step2 extraction data is available for the current filters." />
          </div>
        ) : (
          <div className="stats-chart-shell mt-3">
            <ReactECharts option={productsOption} notMerge lazyUpdate style={{ height: 440, width: "100%" }} opts={{ renderer: "canvas" }} />
          </div>
        )}
      </section>

      <section className="panel p-5 md:p-6">
        <SectionHeader title="Database Field Frequency Heatmap" subtitle={`Sorted by field occurrence count, currently showing Top ${topN}.`} />
        {fieldRows.length === 0 ? (
          <div className="mt-3">
            <EmptyState title="No Field Frequency Data" description="Run jobs with Step2 parameter extraction first." />
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
