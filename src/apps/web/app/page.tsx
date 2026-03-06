"use client";

import { ReactNode, useMemo, useState } from "react";

type Citation = {
  title?: string | null;
  url?: string | null;
  doc_url?: string | null;
  snippet: string;
  score: number;
  notice_id: string;
  match_evidence?: {
    matched_terms: string[];
    matched_sentences: string[];
    score_breakdown: {
      dense_score: number;
      bm25_score: number;
      final_score: number;
    };
    llm_reason?: string | null;
    matching_points?: string[];
    confidence?: number | null;
  } | null;
};

type ChatResponse = {
  answer: string;
  citations: Citation[];
  citation_count_insufficient: boolean;
  debug?: unknown;
};

type NoticeResponse = Record<string, unknown>;

type ApiError = { status: number; body: unknown };

const DEFAULT_QUESTION =
  "Zurich IT services tenders deadline next 30 days, what are key requirements?";

function toIsoRange(date: string, endOfDay: boolean): string {
  const time = endOfDay ? "23:59:59" : "00:00:00";
  return `${date}T${time}`;
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function scoreLabel(score: number): string {
  return Number.isFinite(score) ? score.toFixed(3) : "-";
}

function escapeRegex(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function highlightByTerms(text: string, terms: string[]): ReactNode {
  if (!terms.length) {
    return text;
  }

  const escaped = terms.map((term) => term.trim()).filter(Boolean).map(escapeRegex);
  if (!escaped.length) {
    return text;
  }

  const pattern = new RegExp(`(${escaped.join("|")})`, "gi");
  const parts = text.split(pattern);

  return (
    <>
      {parts.map((part, index) => {
        const matched = terms.some((term) => term.toLowerCase() === part.toLowerCase());
        if (!matched) {
          return <span key={index}>{part}</span>;
        }
        return (
          <mark key={index} className="rounded bg-amber-200/80 px-1 text-slate-900">
            {part}
          </mark>
        );
      })}
    </>
  );
}

export default function Page() {
  const [mode, setMode] = useState<"chat" | "detail">("chat");

  const [question, setQuestion] = useState(DEFAULT_QUESTION);
  const [source, setSource] = useState("simap");
  const [buyer, setBuyer] = useState("");
  const [cpv, setCpv] = useState("");
  const [canton, setCanton] = useState("");
  const [language, setLanguage] = useState("");
  const [topK, setTopK] = useState(8);
  const [enableDeadline, setEnableDeadline] = useState(false);
  const [startDate, setStartDate] = useState(
    new Date(Date.now() - 7 * 86400000).toISOString().slice(0, 10)
  );
  const [endDate, setEndDate] = useState(
    new Date(Date.now() + 30 * 86400000).toISOString().slice(0, 10)
  );

  const [chatData, setChatData] = useState<ChatResponse | null>(null);
  const [chatError, setChatError] = useState<ApiError | null>(null);
  const [isChatLoading, setIsChatLoading] = useState(false);

  const [noticeId, setNoticeId] = useState("");
  const [noticeData, setNoticeData] = useState<NoticeResponse | null>(null);
  const [checklistData, setChecklistData] = useState<unknown>(null);
  const [changesData, setChangesData] = useState<unknown>(null);
  const [detailError, setDetailError] = useState<ApiError | null>(null);
  const [isDetailLoading, setIsDetailLoading] = useState(false);

  const cpvList = useMemo(
    () =>
      cpv
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
    [cpv]
  );

  const noticeTitle = readString(noticeData?.title) ?? "Notice payload";
  const noticeSourceId = readString(noticeData?.source_id);

  async function runChat() {
    setIsChatLoading(true);
    setChatError(null);
    setChatData(null);

    if (enableDeadline && startDate > endDate) {
      setChatError({
        status: 400,
        body: "Start date must be before end date.",
      });
      setIsChatLoading(false);
      return;
    }

    const filters: Record<string, unknown> = {
      source: source || undefined,
      cpv: cpvList.length ? cpvList : undefined,
      buyer: buyer || undefined,
      canton: canton || undefined,
      language: language || undefined,
    };

    if (enableDeadline) {
      filters.date_range = {
        start: toIsoRange(startDate, false),
        end: toIsoRange(endDate, true),
      };
    }

    const payload = {
      question,
      filters,
      top_k: topK,
      debug: true,
    };

    try {
      const resp = await fetch("/api/chat", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      const body = await resp.json();
      if (!resp.ok) {
        setChatError({ status: resp.status, body });
        return;
      }
      setChatData(body as ChatResponse);
    } catch (error) {
      setChatError({ status: 500, body: String(error) });
    } finally {
      setIsChatLoading(false);
    }
  }

  async function loadNotice() {
    if (!noticeId.trim()) {
      return;
    }

    setIsDetailLoading(true);
    setDetailError(null);
    setNoticeData(null);
    setChecklistData(null);
    setChangesData(null);

    try {
      const [noticeResp, checklistResp, changesResp] = await Promise.all([
        fetch(`/api/notices/${noticeId.trim()}`),
        fetch(`/api/notices/${noticeId.trim()}/checklist`),
        fetch(`/api/notices/${noticeId.trim()}/changes`),
      ]);

      if (!noticeResp.ok) {
        const body = await noticeResp.json();
        setDetailError({ status: noticeResp.status, body });
        return;
      }

      setNoticeData((await noticeResp.json()) as NoticeResponse);
      setChecklistData(await checklistResp.json());
      setChangesData(await changesResp.json());
    } catch (error) {
      setDetailError({ status: 500, body: String(error) });
    } finally {
      setIsDetailLoading(false);
    }
  }

  return (
    <main className="mx-auto min-h-screen max-w-7xl px-4 pb-10 pt-6 sm:px-6 lg:px-8">
      <header className="panel-hero reveal-up mb-6 overflow-hidden p-6 sm:p-8">
        <div className="grid gap-6 lg:grid-cols-[1.35fr_1fr]">
          <div>
            <p className="eyebrow">Suisse Bid Match / Operator Console</p>
            <h1 className="mt-3 text-3xl font-semibold leading-tight text-slate-900 sm:text-5xl">
              Procurement Intelligence Cockpit
            </h1>
            <p className="mt-4 max-w-2xl text-sm leading-6 text-slate-700 sm:text-base">
              Query SIMAP opportunities with hybrid retrieval, inspect evidence-backed
              answers, and drill into notice-level checklist and change analysis.
            </p>
            <div className="mt-6 flex flex-wrap gap-2">
              <span className="status-pill">SIMAP Focused</span>
              <span className="status-pill">FastAPI + Qdrant + Postgres</span>
              <span className="status-pill">Next.js Operations UI</span>
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-1">
            <div className="metric-card">
              <p className="metric-label">Retrieval</p>
              <p className="metric-value">Hybrid + GPT-5 mini match select</p>
              <p className="metric-note">Endpoint: /api/chat</p>
            </div>
            <div className="metric-card">
              <p className="metric-label">Notice Drilldown</p>
              <p className="metric-value">Detail + Checklist + Changes</p>
              <p className="metric-note">Endpoint: /api/notices/:id/*</p>
            </div>
            <div className="metric-card">
              <p className="metric-label">Proxy Route</p>
              <p className="metric-value">Next API passthrough</p>
              <p className="metric-note">/api/* -&gt; FastAPI</p>
            </div>
          </div>
        </div>
      </header>

      <div className="mb-6 flex flex-wrap items-center gap-3">
        <button
          type="button"
          onClick={() => setMode("chat")}
          className={`mode-switch ${mode === "chat" ? "mode-switch-active" : ""}`}
        >
          Search & Chat
        </button>
        <button
          type="button"
          onClick={() => setMode("detail")}
          className={`mode-switch ${mode === "detail" ? "mode-switch-active" : ""}`}
        >
          Tender Detail
        </button>
        <div className="ml-auto hidden rounded-full border border-white/70 bg-white/70 px-3 py-1 text-xs text-slate-600 backdrop-blur md:block">
          Live data path: browser -&gt; Next.js route -&gt; FastAPI
        </div>
      </div>

      {mode === "chat" ? (
        <section className="grid gap-5 lg:grid-cols-[1.05fr_1.45fr]">
          <article className="panel p-5 sm:p-6">
            <div className="mb-5 flex items-center justify-between">
              <h2 className="text-xl font-semibold text-slate-900">Prompt Builder</h2>
              <span className="mono rounded-lg bg-slate-900 px-2 py-1 text-[11px] text-white">
                TOP K {topK}
              </span>
            </div>

            <label className="field-label">Question</label>
            <textarea
              value={question}
              onChange={(event) => setQuestion(event.target.value)}
              className="text-area"
            />

            <div className="mt-4 grid gap-3 sm:grid-cols-2">
              <div>
                <label className="field-label">Source</label>
                <select
                  value={source}
                  onChange={(event) => setSource(event.target.value)}
                  className="text-input"
                >
                  <option value="">All sources</option>
                  <option value="simap">simap</option>
                </select>
              </div>

              <div>
                <label className="field-label">Buyer Contains</label>
                <input
                  value={buyer}
                  onChange={(event) => setBuyer(event.target.value)}
                  placeholder="e.g. Stadt Zurich"
                  className="text-input"
                />
              </div>

              <div>
                <label className="field-label">CPV Codes</label>
                <input
                  value={cpv}
                  onChange={(event) => setCpv(event.target.value)}
                  placeholder="72200000, 72000000"
                  className="text-input"
                />
              </div>

              <div>
                <label className="field-label">Canton / Region</label>
                <input
                  value={canton}
                  onChange={(event) => setCanton(event.target.value)}
                  placeholder="ZH"
                  className="text-input"
                />
              </div>

              <div>
                <label className="field-label">Language</label>
                <select
                  value={language}
                  onChange={(event) => setLanguage(event.target.value)}
                  className="text-input"
                >
                  <option value="">All languages</option>
                  <option value="en">en</option>
                  <option value="de">de</option>
                  <option value="fr">fr</option>
                  <option value="it">it</option>
                </select>
              </div>

              <div>
                <label className="field-label">Top K</label>
                <div className="rounded-2xl border border-slate-200 bg-white px-3 py-3">
                  <input
                    type="range"
                    min={3}
                    max={12}
                    value={topK}
                    onChange={(event) => setTopK(Number(event.target.value))}
                    className="w-full accent-teal-700"
                  />
                  <p className="mono mt-2 text-right text-xs text-slate-500">{topK}</p>
                </div>
              </div>
            </div>

            <div className="mt-4 rounded-2xl border border-slate-200/80 bg-slate-50/70 p-4">
              <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
                <input
                  type="checkbox"
                  checked={enableDeadline}
                  onChange={(event) => setEnableDeadline(event.target.checked)}
                />
                Enable deadline date filter
              </label>
              <div className="mt-3 grid gap-3 sm:grid-cols-2">
                <input
                  type="date"
                  value={startDate}
                  onChange={(event) => setStartDate(event.target.value)}
                  disabled={!enableDeadline}
                  className="text-input disabled:opacity-50"
                />
                <input
                  type="date"
                  value={endDate}
                  onChange={(event) => setEndDate(event.target.value)}
                  disabled={!enableDeadline}
                  className="text-input disabled:opacity-50"
                />
              </div>
            </div>

            <button
              type="button"
              onClick={runChat}
              disabled={isChatLoading || !question.trim()}
              className="btn-primary mt-5 w-full"
            >
              {isChatLoading ? "Running retrieval..." : "Run Grounded Query"}
            </button>
          </article>

          <article className="panel p-5 sm:p-6">
            <div className="mb-4 flex flex-wrap items-center gap-2">
              <h2 className="text-xl font-semibold text-slate-900">Grounded Answer</h2>
              {chatData ? (
                <span className="rounded-full border border-emerald-300 bg-emerald-50 px-2 py-1 text-xs font-medium text-emerald-700">
                  {chatData.citations.length} citations
                </span>
              ) : null}
            </div>

            {chatError ? (
              <pre className="max-h-[460px] overflow-auto rounded-2xl border border-rose-200 bg-rose-50 p-3 text-xs text-rose-700">
                {JSON.stringify(chatError, null, 2)}
              </pre>
            ) : isChatLoading ? (
              <div className="space-y-3 animate-pulse">
                <div className="h-20 rounded-2xl bg-slate-200/70" />
                <div className="h-20 rounded-2xl bg-slate-200/70" />
                <div className="h-20 rounded-2xl bg-slate-200/70" />
              </div>
            ) : chatData ? (
              <>
                <div className="mb-4 rounded-2xl border border-teal-200 bg-teal-50 p-4 text-sm leading-6 text-slate-800">
                  {chatData.answer}
                </div>

                {chatData.citation_count_insufficient ? (
                  <p className="mb-3 text-xs font-medium text-amber-700">
                    Less than 3 citations available for this query.
                  </p>
                ) : null}

                <div className="space-y-3">
                  {chatData.citations.map((citation, index) => (
                    <div
                      key={`${citation.notice_id}-${index}`}
                      className="rounded-2xl border border-slate-200 bg-white/90 p-4"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <p className="text-sm font-semibold text-slate-900">
                          {index + 1}. {citation.title ?? "Untitled"}
                        </p>
                        <span className="mono rounded-md bg-slate-900 px-2 py-1 text-[11px] text-white">
                          score {scoreLabel(citation.score)}
                        </span>
                      </div>

                      <p className="mono mt-1 text-[11px] text-slate-500">
                        notice {citation.notice_id}
                      </p>
                      <p className="mt-2 text-sm leading-6 text-slate-700">
                        {citation.snippet}
                      </p>

                      {citation.match_evidence ? (
                        <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50/80 p-3">
                          <p className="mono text-[11px] uppercase tracking-[0.14em] text-slate-500">
                            Similarity Evidence
                          </p>

                          {citation.match_evidence.llm_reason ? (
                            <p className="mt-2 rounded-lg border border-teal-200 bg-teal-50 px-3 py-2 text-xs leading-5 text-teal-900">
                              {citation.match_evidence.llm_reason}
                            </p>
                          ) : (
                            <p className="mt-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs leading-5 text-slate-600">
                              LLM rationale unavailable, showing lexical/semantic evidence only.
                            </p>
                          )}

                          {citation.match_evidence.matching_points?.length ? (
                            <div className="mt-2 flex flex-wrap gap-2">
                              {citation.match_evidence.matching_points.map((point, pointIndex) => (
                                <span
                                  key={`${citation.notice_id}-${index}-point-${pointIndex}`}
                                  className="rounded-full border border-teal-200 bg-white px-2 py-1 text-xs text-teal-800"
                                >
                                  {point}
                                </span>
                              ))}
                            </div>
                          ) : null}

                          <div className="mt-2 flex flex-wrap gap-2">
                            {citation.match_evidence.matched_terms.length ? (
                              citation.match_evidence.matched_terms.map((term) => (
                                <span
                                  key={`${citation.notice_id}-${index}-${term}`}
                                  className="rounded-full border border-amber-300 bg-amber-50 px-2 py-1 text-xs text-amber-800"
                                >
                                  {term}
                                </span>
                              ))
                            ) : (
                              <span className="text-xs text-slate-500">
                                No direct keyword overlap detected (semantic match still high).
                              </span>
                            )}
                          </div>

                          <div className="mt-3 grid gap-2 text-xs text-slate-700 sm:grid-cols-3">
                            <div className="rounded-lg border border-slate-200 bg-white px-2 py-1">
                              dense {scoreLabel(citation.match_evidence.score_breakdown.dense_score)}
                            </div>
                            <div className="rounded-lg border border-slate-200 bg-white px-2 py-1">
                              bm25 {scoreLabel(citation.match_evidence.score_breakdown.bm25_score)}
                            </div>
                            <div className="rounded-lg border border-slate-200 bg-white px-2 py-1">
                              final {scoreLabel(citation.match_evidence.score_breakdown.final_score)}
                            </div>
                          </div>

                          {typeof citation.match_evidence.confidence === "number" ? (
                            <p className="mt-2 text-xs text-slate-600">
                              LLM confidence {scoreLabel(citation.match_evidence.confidence)}
                            </p>
                          ) : null}

                          {citation.match_evidence.matched_sentences.length ? (
                            <div className="mt-3 space-y-2">
                              {citation.match_evidence.matched_sentences.map((sentence, sentenceIndex) => (
                                <p
                                  key={`${citation.notice_id}-${index}-sent-${sentenceIndex}`}
                                  className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs leading-5 text-slate-700"
                                >
                                  {highlightByTerms(
                                    sentence,
                                    citation.match_evidence?.matched_terms ?? []
                                  )}
                                </p>
                              ))}
                            </div>
                          ) : null}
                        </div>
                      ) : null}

                      <div className="mt-3 flex flex-wrap gap-2 text-xs">
                        {citation.url ? (
                          <a
                            href={citation.url}
                            target="_blank"
                            rel="noreferrer"
                            className="rounded-full border border-teal-200 bg-teal-50 px-3 py-1 text-teal-800 hover:bg-teal-100"
                          >
                            Notice Link
                          </a>
                        ) : null}
                        {citation.doc_url ? (
                          <a
                            href={citation.doc_url}
                            target="_blank"
                            rel="noreferrer"
                            className="rounded-full border border-sky-200 bg-sky-50 px-3 py-1 text-sky-800 hover:bg-sky-100"
                          >
                            Document Link
                          </a>
                        ) : null}
                      </div>
                    </div>
                  ))}
                </div>

                {chatData.debug ? (
                  <details className="mt-4 rounded-2xl border border-slate-200 bg-white p-3">
                    <summary className="cursor-pointer text-sm font-medium text-slate-700">
                      Debug payload
                    </summary>
                    <pre className="mt-2 max-h-72 overflow-auto text-xs">
                      {JSON.stringify(chatData.debug, null, 2)}
                    </pre>
                  </details>
                ) : null}
              </>
            ) : (
              <div className="rounded-2xl border border-dashed border-slate-300 bg-white/60 p-5 text-sm text-slate-600">
                Submit a prompt to retrieve candidate tenders and grounded citations.
              </div>
            )}
          </article>
        </section>
      ) : (
        <section className="grid gap-5 lg:grid-cols-[1fr_1.6fr]">
          <article className="panel p-5 sm:p-6">
            <h2 className="text-xl font-semibold text-slate-900">Load Notice</h2>
            <p className="mt-2 text-sm text-slate-600">
              Enter a notice id to fetch notice detail, generated checklist, and change impact.
            </p>

            <label className="field-label mt-4">Notice ID</label>
            <input
              value={noticeId}
              onChange={(event) => setNoticeId(event.target.value)}
              placeholder="e.g. 1234567"
              className="text-input"
            />

            <button
              type="button"
              onClick={loadNotice}
              disabled={isDetailLoading || !noticeId.trim()}
              className="btn-primary mt-4 w-full"
            >
              {isDetailLoading ? "Loading notice..." : "Load Tender Data"}
            </button>
          </article>

          <article className="panel p-5 sm:p-6">
            <h2 className="text-xl font-semibold text-slate-900">Tender Data Explorer</h2>

            {detailError ? (
              <pre className="mt-4 max-h-[520px] overflow-auto rounded-2xl border border-rose-200 bg-rose-50 p-3 text-xs text-rose-700">
                {JSON.stringify(detailError, null, 2)}
              </pre>
            ) : isDetailLoading ? (
              <div className="mt-4 space-y-3 animate-pulse">
                <div className="h-20 rounded-2xl bg-slate-200/70" />
                <div className="h-20 rounded-2xl bg-slate-200/70" />
                <div className="h-20 rounded-2xl bg-slate-200/70" />
              </div>
            ) : noticeData ? (
              <div className="mt-4 space-y-3">
                <div className="rounded-2xl border border-teal-200 bg-teal-50 p-4">
                  <p className="text-sm font-semibold text-slate-900">{noticeTitle}</p>
                  {noticeSourceId ? (
                    <p className="mono mt-1 text-xs text-slate-600">source_id {noticeSourceId}</p>
                  ) : null}
                </div>

                <details open className="rounded-2xl border border-slate-200 bg-white p-3">
                  <summary className="cursor-pointer text-sm font-medium text-slate-700">
                    Notice
                  </summary>
                  <pre className="mt-2 max-h-64 overflow-auto text-xs">
                    {JSON.stringify(noticeData, null, 2)}
                  </pre>
                </details>

                <details className="rounded-2xl border border-slate-200 bg-white p-3">
                  <summary className="cursor-pointer text-sm font-medium text-slate-700">
                    Checklist
                  </summary>
                  <pre className="mt-2 max-h-64 overflow-auto text-xs">
                    {JSON.stringify(checklistData, null, 2)}
                  </pre>
                </details>

                <details className="rounded-2xl border border-slate-200 bg-white p-3">
                  <summary className="cursor-pointer text-sm font-medium text-slate-700">
                    Changes
                  </summary>
                  <pre className="mt-2 max-h-64 overflow-auto text-xs">
                    {JSON.stringify(changesData, null, 2)}
                  </pre>
                </details>
              </div>
            ) : (
              <div className="mt-4 rounded-2xl border border-dashed border-slate-300 bg-white/60 p-5 text-sm text-slate-600">
                Enter a notice id to inspect detail, checklist, and changes.
              </div>
            )}
          </article>
        </section>
      )}
    </main>
  );
}
