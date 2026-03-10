"use client";

import { useMemo, useState } from "react";

type IngestStartResponse = {
  job_id: string;
  status: string;
  uploaded_files: number;
  uploaded_bytes: number;
};

type IngestJobResponse = {
  job_id: string;
  status: string;
  package_id: string | null;
  source_name: string | null;
  document_count: number | null;
  field_count: number | null;
  processed_files: number;
  total_files: number;
  current_file: string | null;
  error: string | null;
};

type MatchJobStartResponse = {
  job_id: string;
  status: string;
};

type MatchJobStep = {
  step: string;
  status: string;
  percent: number;
  started_at: number | null;
  finished_at: number | null;
  message: string | null;
  error: string | null;
};

type MatchJobStatusResponse = {
  job_id: string;
  status: string;
  package_id: string;
  domain: string;
  top_k: number;
  strict_hard_constraints: boolean;
  run_id: string | null;
  error: string | null;
  overall_percent: number;
  current_step: string | null;
  steps: MatchJobStep[];
};

type Requirement = {
  requirement_id: string;
  param_key: string;
  operator: string;
  value: unknown;
  unit: string | null;
  is_hard: boolean;
  product_key?: string | null;
  product_name?: string | null;
  evidence_refs: string[];
  confidence: number;
};

type Mapping = {
  requirement_id: string;
  param_key: string;
  mapped_field: string | null;
  status: string;
  reason: string;
  confidence: number;
  is_hard: boolean;
};

type Candidate = {
  product_id: string;
  product_name: string;
  score: number;
  hard_passed: boolean;
  soft_score: number;
  matched_requirements: string[];
  unmet_requirements: string[];
  request_product_key?: string | null;
  request_product_name?: string | null;
};

type ProductResult = {
  product_key: string;
  product_name: string | null;
  quantity: number | null;
  blocked: boolean;
  requirements: Requirement[];
  candidates: Candidate[];
  unmet_constraints: string[];
};

type AuditEvent = {
  step: string;
  status: string;
  summary: string;
  error: string | null;
};

type MatchDetailsResponse = {
  run_id: string;
  package_id: string;
  domain: string;
  blocked: boolean;
  requirements: {
    requirements: Requirement[];
  };
  mapped_conditions: Mapping[];
  sql_plan: {
    validated: boolean;
    validation_errors: string[];
    block_reason: string | null;
  };
  sql_executed: string;
  candidates: Candidate[];
  product_results?: ProductResult[];
  unmet_constraints: string[];
  audit_trail: AuditEvent[];
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8000";

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "-";
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export default function HomePage() {
  const apiBase = useMemo(() => API_BASE.replace(/\/$/, ""), []);
  const [ingestStatus, setIngestStatus] = useState("No package ingested.");
  const [matchStatus, setMatchStatus] = useState("No match run yet.");
  const [packageId, setPackageId] = useState("");
  const [runId, setRunId] = useState("");
  const [matchJobId, setMatchJobId] = useState("");
  const [busyIngest, setBusyIngest] = useState(false);
  const [busyMatch, setBusyMatch] = useState(false);
  const [domain, setDomain] = useState("lighting");
  const [topK, setTopK] = useState(5);
  const [strictHard, setStrictHard] = useState(true);
  const [details, setDetails] = useState<MatchDetailsResponse | null>(null);
  const [matchJob, setMatchJob] = useState<MatchJobStatusResponse | null>(null);

  const ingestFiles = async (files: FileList | null) => {
    if (!files || files.length === 0) {
      setIngestStatus("Please select files first.");
      return;
    }
    setBusyIngest(true);
    setDetails(null);
    setRunId("");

    try {
      const form = new FormData();
      for (const file of Array.from(files)) {
        form.append("files", file, file.name);
      }

      const startResp = await fetch(`${apiBase}/api/packages/ingest/start`, {
        method: "POST",
        body: form,
      });
      const startPayload = (await startResp.json()) as IngestStartResponse | { detail?: string };
      if (!startResp.ok) {
        setIngestStatus((startPayload as { detail?: string }).detail || "Ingest start failed.");
        return;
      }

      const jobId = (startPayload as IngestStartResponse).job_id;
      const startedAt = Date.now();
      while (Date.now() - startedAt < 20 * 60 * 1000) {
        const pollResp = await fetch(`${apiBase}/api/packages/ingest/${jobId}?ts=${Date.now()}`);
        const pollPayload = (await pollResp.json()) as IngestJobResponse | { detail?: string };
        if (!pollResp.ok) {
          setIngestStatus((pollPayload as { detail?: string }).detail || "Ingest polling failed.");
          return;
        }

        const job = pollPayload as IngestJobResponse;
        if (job.status === "failed") {
          setIngestStatus(job.error || "Ingest failed.");
          return;
        }
        if (job.status === "completed" && job.package_id) {
          setPackageId(job.package_id);
          setIngestStatus(
            `Package ready: ${job.package_id}, docs=${job.document_count ?? 0}, fields=${job.field_count ?? 0}`,
          );
          return;
        }

        const progress =
          job.total_files > 0
            ? `${job.processed_files}/${job.total_files}`
            : `${job.processed_files}`;
        setIngestStatus(`Ingesting... ${progress} ${job.current_file || ""}`.trim());
        await new Promise((resolve) => window.setTimeout(resolve, 900));
      }

      setIngestStatus("Ingest timeout.");
    } catch (error) {
      setIngestStatus(`Ingest request failed: ${String(error)}`);
    } finally {
      setBusyIngest(false);
    }
  };

  const loadMatchDetails = async (targetRunId: string) => {
    const resp = await fetch(`${apiBase}/api/match/${targetRunId}`);
    const payload = (await resp.json()) as MatchDetailsResponse | { detail?: string };
    if (!resp.ok) {
      setMatchStatus((payload as { detail?: string }).detail || "Failed to load match run.");
      return;
    }
    setDetails(payload as MatchDetailsResponse);
  };

  const runMatch = async () => {
    if (!packageId) {
      setMatchStatus("Please ingest a package first.");
      return;
    }

    setBusyMatch(true);
    setMatchJob(null);
    let timeout: number | undefined;
    try {
      const controller = new AbortController();
      timeout = window.setTimeout(() => controller.abort(), 120000);
      const resp = await fetch(`${apiBase}/api/match/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        signal: controller.signal,
        body: JSON.stringify({
          package_id: packageId,
          domain,
          top_k: topK,
          strict_hard_constraints: strictHard,
        }),
      });
      const payload = (await resp.json()) as MatchJobStartResponse | { detail?: string };
      if (!resp.ok) {
        setMatchStatus((payload as { detail?: string }).detail || "Match run failed.");
        return;
      }
      const job = payload as MatchJobStartResponse;
      setMatchJobId(job.job_id);
      setMatchStatus(`Match job queued: ${job.job_id}`);

      const startedAt = Date.now();
      while (Date.now() - startedAt < 60 * 60 * 1000) {
        const pollResp = await fetch(`${apiBase}/api/match/run/${job.job_id}?ts=${Date.now()}`);
        const pollPayload = (await pollResp.json()) as MatchJobStatusResponse | { detail?: string };
        if (!pollResp.ok) {
          setMatchStatus((pollPayload as { detail?: string }).detail || "Match polling failed.");
          return;
        }
        const jobStatus = pollPayload as MatchJobStatusResponse;
        setMatchJob(jobStatus);
        if (jobStatus.status === "failed") {
          setMatchStatus(jobStatus.error || "Match job failed.");
          return;
        }
        if (jobStatus.status === "completed") {
          if (jobStatus.run_id) {
            setRunId(jobStatus.run_id);
            setMatchStatus(`Match run completed: ${jobStatus.run_id}`);
            await loadMatchDetails(jobStatus.run_id);
          } else {
            setMatchStatus("Match job completed but run_id missing.");
          }
          return;
        }
        const step = jobStatus.current_step ? ` (${jobStatus.current_step})` : "";
        setMatchStatus(`Match running${step}... ${jobStatus.overall_percent || 0}%`);
        await new Promise((resolve) => window.setTimeout(resolve, 1200));
      }
      setMatchStatus("Match job timeout.");
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        setMatchStatus("Match request timed out after 120s. Try smaller scope or check backend logs.");
      } else {
        setMatchStatus(`Match request failed: ${String(error)}`);
      }
    } finally {
      if (timeout !== undefined) {
        window.clearTimeout(timeout);
      }
      setBusyMatch(false);
    }
  };

  return (
    <main className="mx-auto min-h-screen w-full max-w-6xl px-4 py-8 sm:px-6 lg:px-8">
      <header className="mb-8 rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
        <h1 className="text-2xl font-semibold text-slate-900">Tender-to-SQL Match Workbench</h1>
        <p className="mt-2 text-sm text-slate-600">
          Upload tender documents, extract requirements, map to schema, generate validated SQL, and review Top-K
          matched products with audit trail.
        </p>
      </header>

      <section className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <h2 className="text-lg font-medium text-slate-900">1. Ingest Tender Package</h2>
          <input
            className="mt-4 block w-full rounded-md border border-slate-300 p-2 text-sm"
            type="file"
            multiple
            onChange={(e) => void ingestFiles(e.target.files)}
            disabled={busyIngest}
          />
          <p className="mt-3 text-xs text-slate-500">Supports zip/doc/docx/docm/xlsx/pdf uploads.</p>
          <p className="mt-4 rounded-md bg-slate-50 p-3 text-sm text-slate-700">{ingestStatus}</p>
        </div>

        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <h2 className="text-lg font-medium text-slate-900">2. Run Match</h2>
          <div className="mt-4 grid gap-3 sm:grid-cols-2">
            <label className="text-sm text-slate-700">
              Domain
              <input
                className="mt-1 w-full rounded-md border border-slate-300 p-2"
                value={domain}
                onChange={(e) => setDomain(e.target.value)}
              />
            </label>
            <label className="text-sm text-slate-700">
              Top K
              <input
                className="mt-1 w-full rounded-md border border-slate-300 p-2"
                type="number"
                min={1}
                max={50}
                value={topK}
                onChange={(e) => setTopK(Number(e.target.value) || 5)}
              />
            </label>
          </div>
          <label className="mt-3 flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={strictHard}
              onChange={(e) => setStrictHard(e.target.checked)}
            />
            Strict hard constraints
          </label>
          <button
            className="mt-4 rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:opacity-60"
            onClick={() => void runMatch()}
            disabled={busyMatch || !packageId}
          >
            {busyMatch ? "Running..." : "Run Match"}
          </button>
          <p className="mt-4 rounded-md bg-slate-50 p-3 text-sm text-slate-700">{matchStatus}</p>
          <p className="mt-2 text-xs text-slate-500">Package: {packageId || "-"} | Run: {runId || "-"}</p>
          <p className="mt-1 text-xs text-slate-500">Job: {matchJobId || "-"}</p>
        </div>
      </section>

      <section className="mt-6 grid gap-6">
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <h3 className="text-base font-medium text-slate-900">Match Progress</h3>
          <div className="mt-3">
            <div className="flex items-center justify-between text-xs text-slate-500">
              <span>Overall</span>
              <span>{matchJob?.overall_percent ?? 0}%</span>
            </div>
            <div className="mt-2 h-2 w-full rounded-full bg-slate-100">
              <div
                className="h-2 rounded-full bg-gradient-to-r from-emerald-400 via-sky-400 to-indigo-400 transition-all"
                style={{ width: `${matchJob?.overall_percent ?? 0}%` }}
              />
            </div>
          </div>
          <div className="mt-4 rounded-xl border border-slate-200 bg-slate-950 px-4 py-3 font-mono text-xs text-slate-200">
            {(matchJob?.steps || []).length === 0 && <div>No match job activity yet.</div>}
            {(matchJob?.steps || []).map((step) => {
              const statusColor =
                step.status === "completed"
                  ? "text-emerald-300"
                  : step.status === "running"
                    ? "text-sky-300"
                    : step.status === "failed"
                      ? "text-rose-300"
                      : "text-slate-400";
              return (
                <div key={step.step} className="flex flex-wrap items-start gap-2 py-1">
                  <span className={`min-w-[140px] ${statusColor}`}>{step.step}</span>
                  <span className="text-slate-400">{step.status}</span>
                  <span className="text-slate-500">{step.percent}%</span>
                  {step.message && <span className="text-slate-300">{step.message}</span>}
                  {step.error && <span className="text-rose-300">{step.error}</span>}
                </div>
              );
            })}
          </div>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <h3 className="text-base font-medium text-slate-900">Extracted Requirements</h3>
          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="text-left text-slate-500">
                <tr>
                  <th className="px-2 py-2">ID</th>
                  <th className="px-2 py-2">Param</th>
                  <th className="px-2 py-2">Operator</th>
                  <th className="px-2 py-2">Value</th>
                  <th className="px-2 py-2">Unit</th>
                  <th className="px-2 py-2">Hard</th>
                </tr>
              </thead>
              <tbody>
                {(details?.requirements.requirements || []).map((item) => (
                  <tr key={item.requirement_id} className="border-t border-slate-100">
                    <td className="px-2 py-2 font-mono text-xs">{item.requirement_id}</td>
                    <td className="px-2 py-2">{item.param_key}</td>
                    <td className="px-2 py-2">{item.operator}</td>
                    <td className="px-2 py-2">{formatValue(item.value)}</td>
                    <td className="px-2 py-2">{item.unit || "-"}</td>
                    <td className="px-2 py-2">{item.is_hard ? "Yes" : "No"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <h3 className="text-base font-medium text-slate-900">Schema Mapping + SQL</h3>
          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="text-left text-slate-500">
                <tr>
                  <th className="px-2 py-2">Requirement</th>
                  <th className="px-2 py-2">Mapped Field</th>
                  <th className="px-2 py-2">Status</th>
                  <th className="px-2 py-2">Confidence</th>
                  <th className="px-2 py-2">Reason</th>
                </tr>
              </thead>
              <tbody>
                {(details?.mapped_conditions || []).map((item) => (
                  <tr key={item.requirement_id} className="border-t border-slate-100">
                    <td className="px-2 py-2 font-mono text-xs">{item.requirement_id}</td>
                    <td className="px-2 py-2">{item.mapped_field || "-"}</td>
                    <td className="px-2 py-2">{item.status}</td>
                    <td className="px-2 py-2">{item.confidence.toFixed(2)}</td>
                    <td className="px-2 py-2">{item.reason}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <pre className="mt-4 overflow-auto rounded-md bg-slate-900 p-3 text-xs text-slate-100">
            {details?.sql_executed || "SQL not generated yet."}
          </pre>
          {details?.sql_plan.validation_errors?.length ? (
            <div className="mt-3 rounded-md bg-rose-50 p-3 text-sm text-rose-700">
              {details.sql_plan.validation_errors.join("; ")}
            </div>
          ) : null}
        </div>

        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <h3 className="text-base font-medium text-slate-900">Top Matched Products</h3>
          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="text-left text-slate-500">
                <tr>
                  <th className="px-2 py-2">Product</th>
                  <th className="px-2 py-2">Score</th>
                  <th className="px-2 py-2">Hard Pass</th>
                  <th className="px-2 py-2">Matched</th>
                  <th className="px-2 py-2">Unmet</th>
                </tr>
              </thead>
              <tbody>
                {(details?.candidates || []).map((item) => (
                  <tr key={`${item.request_product_key || "global"}:${item.product_id}`} className="border-t border-slate-100">
                    <td className="px-2 py-2">
                      {item.request_product_key ? (
                        <div className="text-xs text-slate-500">
                          Request: {item.request_product_name || item.request_product_key}
                        </div>
                      ) : null}
                      <div className="font-medium text-slate-900">{item.product_name}</div>
                      <div className="font-mono text-xs text-slate-500">{item.product_id}</div>
                    </td>
                    <td className="px-2 py-2">{item.score.toFixed(2)}</td>
                    <td className="px-2 py-2">{item.hard_passed ? "Yes" : "No"}</td>
                    <td className="px-2 py-2">{item.matched_requirements.join(", ") || "-"}</td>
                    <td className="px-2 py-2">{item.unmet_requirements.join(", ") || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {details?.unmet_constraints?.length ? (
            <div className="mt-3 rounded-md bg-amber-50 p-3 text-sm text-amber-800">
              Unmet constraints: {details.unmet_constraints.join("; ")}
            </div>
          ) : null}
        </div>

        {details?.product_results?.length ? (
          <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
            <h3 className="text-base font-medium text-slate-900">Per-Product Matching</h3>
            <div className="mt-3 space-y-3">
              {details.product_results.map((item) => (
                <div key={item.product_key} className="rounded-md border border-slate-200 p-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="font-medium text-slate-900">
                      {item.product_name || item.product_key}
                    </div>
                    <div className="text-xs text-slate-500">
                      key={item.product_key}
                      {item.quantity != null ? `, qty=${item.quantity}` : ""}
                    </div>
                  </div>
                  <div className="mt-1 text-sm text-slate-600">
                    requirements={item.requirements.length}, candidates={item.candidates.length}, blocked=
                    {item.blocked ? "yes" : "no"}
                  </div>
                  {item.unmet_constraints?.length ? (
                    <div className="mt-2 rounded bg-amber-50 px-2 py-1 text-xs text-amber-800">
                      {item.unmet_constraints.join("; ")}
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
          </div>
        ) : null}

        <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <h3 className="text-base font-medium text-slate-900">Audit Trail</h3>
          <ul className="mt-3 space-y-2 text-sm text-slate-700">
            {(details?.audit_trail || []).map((event, idx) => (
              <li key={`${event.step}-${idx}`} className="rounded-md border border-slate-200 p-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="font-medium text-slate-900">{event.step}</span>
                  <span className="rounded bg-slate-100 px-2 py-0.5 text-xs uppercase text-slate-600">
                    {event.status}
                  </span>
                </div>
                <p className="mt-1 text-slate-600">{event.summary}</p>
                {event.error ? <p className="mt-1 text-rose-700">{event.error}</p> : null}
              </li>
            ))}
          </ul>
        </div>
      </section>
    </main>
  );
}
