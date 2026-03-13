import type { ReactNode } from "react";
import { InlineNotice, StatusBadge, cx } from "@/components/ui";
import { formatDuration, type StepProgressState } from "@/lib/view-models";

interface JobStepDetailPanelProps {
  step: StepProgressState;
  compactMode: boolean;
  resultLimit: number;
}

interface ParsedRequirement {
  requirementId: string;
  field: string;
  value: string;
  extractionConfidence: number | null;
  operator: string | null;
  isHard: boolean | null;
  sourceFile: string | null;
  sourceSnippet: string | null;
}

interface ParsedTenderProduct {
  productKey: string;
  productName: string;
  quantity: string;
  requirements: ParsedRequirement[];
}

interface ParsedRule {
  field: string;
  operator: string;
  isHard: boolean;
  operatorConfidence: number | null;
  hardnessConfidence: number | null;
  rationale: string;
}

interface ParsedHardConstraint {
  field: string;
  operator: string;
  value: string;
}

interface ParsedSqlQuery {
  queryId: string;
  productKey: string;
  sql: string;
  hardConstraints: ParsedHardConstraint[];
}

interface ParsedSqlResult {
  queryId: string;
  productKey: string;
  sql: string;
  rowCount: number;
  elapsedMs: number | null;
  rows: Record<string, unknown>[];
}

interface ParsedCandidate {
  rank: number | null;
  dbProductId: string;
  dbProductName: string;
  passesHard: boolean | null;
  softMatchScore: number | null;
  matchedSoftCount: number;
  unmetSoftCount: number;
  explanation: string;
}

interface ParsedMatchResult {
  productKey: string;
  candidates: ParsedCandidate[];
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

function asString(input: unknown): string | null {
  if (typeof input !== "string") {
    return null;
  }
  const text = input.trim();
  return text.length > 0 ? text : null;
}

function asNumber(input: unknown): number | null {
  if (typeof input !== "number" || Number.isNaN(input)) {
    return null;
  }
  return input;
}

function asBoolean(input: unknown): boolean | null {
  if (typeof input !== "boolean") {
    return null;
  }
  return input;
}

function asText(input: unknown): string {
  if (input == null) {
    return "NULL";
  }
  if (typeof input === "string") {
    return input;
  }
  if (typeof input === "number" || typeof input === "boolean") {
    return String(input);
  }
  try {
    return JSON.stringify(input);
  } catch {
    return String(input);
  }
}

function truncate(input: string, maxLength: number): string {
  if (input.length <= maxLength) {
    return input;
  }
  return `${input.slice(0, maxLength)}...`;
}

function parseRequirement(input: unknown): ParsedRequirement | null {
  const row = asRecord(input);
  if (!row) {
    return null;
  }
  const source = asRecord(row.source);
  return {
    requirementId: asString(row.requirement_id) ?? "-",
    field: asString(row.field) ?? "-",
    value: asText(row.value),
    extractionConfidence: asNumber(row.extraction_confidence),
    operator: asString(row.operator),
    isHard: asBoolean(row.is_hard),
    sourceFile: source ? asString(source.file_name) : null,
    sourceSnippet: source ? asString(source.snippet) : null
  };
}

function parseTenderProducts(input: unknown): ParsedTenderProduct[] {
  return asArray(input)
    .map((item) => {
      const row = asRecord(item);
      if (!row) {
        return null;
      }
      const requirements = asArray(row.requirements).map(parseRequirement).filter((value): value is ParsedRequirement => value != null);
      return {
        productKey: asString(row.product_key) ?? "-",
        productName: asString(row.product_name) ?? "-",
        quantity: asText(row.quantity),
        requirements
      };
    })
    .filter((value): value is ParsedTenderProduct => value != null);
}

function parseRules(input: unknown): ParsedRule[] {
  return asArray(input)
    .map((item) => {
      const row = asRecord(item);
      if (!row) {
        return null;
      }
      const field = asString(row.field);
      const operator = asString(row.operator);
      const isHard = asBoolean(row.is_hard);
      if (!field || !operator || isHard == null) {
        return null;
      }
      return {
        field,
        operator,
        isHard,
        operatorConfidence: asNumber(row.operator_confidence),
        hardnessConfidence: asNumber(row.hardness_confidence),
        rationale: asString(row.rationale) ?? ""
      };
    })
    .filter((value): value is ParsedRule => value != null);
}

function parseHardConstraints(input: unknown): ParsedHardConstraint[] {
  return asArray(input)
    .map((item) => {
      const row = asRecord(item);
      if (!row) {
        return null;
      }
      const field = asString(row.field);
      const operator = asString(row.operator);
      if (!field || !operator) {
        return null;
      }
      return {
        field,
        operator,
        value: asText(row.value)
      };
    })
    .filter((value): value is ParsedHardConstraint => value != null);
}

function parseSqlQueries(input: unknown): ParsedSqlQuery[] {
  return asArray(input)
    .map((item) => {
      const row = asRecord(item);
      if (!row) {
        return null;
      }
      const queryId = asString(row.query_id);
      const productKey = asString(row.product_key);
      const sql = asString(row.sql);
      if (!queryId || !productKey || !sql) {
        return null;
      }
      return {
        queryId,
        productKey,
        sql,
        hardConstraints: parseHardConstraints(row.hard_constraints_used)
      };
    })
    .filter((value): value is ParsedSqlQuery => value != null);
}

function parseSqlResults(input: unknown): ParsedSqlResult[] {
  return asArray(input)
    .map((item) => {
      const row = asRecord(item);
      if (!row) {
        return null;
      }
      const queryId = asString(row.query_id);
      const productKey = asString(row.product_key);
      if (!queryId || !productKey) {
        return null;
      }
      const rows = asArray(row.rows)
        .map((rawRow) => asRecord(rawRow))
        .filter((value): value is Record<string, unknown> => value != null);
      return {
        queryId,
        productKey,
        sql: asString(row.sql) ?? "",
        rowCount: asNumber(row.row_count) ?? rows.length,
        elapsedMs: asNumber(row.elapsed_ms),
        rows
      };
    })
    .filter((value): value is ParsedSqlResult => value != null);
}

function parseMatchResults(input: unknown): ParsedMatchResult[] {
  return asArray(input)
    .map((item) => {
      const row = asRecord(item);
      if (!row) {
        return null;
      }
      const productKey = asString(row.product_key);
      if (!productKey) {
        return null;
      }
      const candidates = asArray(row.candidates)
        .map((candidate) => {
          const data = asRecord(candidate);
          if (!data) {
            return null;
          }
          const name = asString(data.db_product_name);
          const dbProductId = asText(data.db_product_id);
          if (!name || !dbProductId) {
            return null;
          }
          return {
            rank: asNumber(data.rank),
            dbProductId,
            dbProductName: name,
            passesHard: asBoolean(data.passes_hard),
            softMatchScore: asNumber(data.soft_match_score),
            matchedSoftCount: asArray(data.matched_soft_constraints).length,
            unmetSoftCount: asArray(data.unmet_soft_constraints).length,
            explanation: asString(data.explanation) ?? ""
          };
        })
        .filter((value): value is ParsedCandidate => value != null);
      return {
        productKey,
        candidates
      };
    })
    .filter((value): value is ParsedMatchResult => value != null);
}

function kpiCard(label: string, value: string | number, hint: string): ReactNode {
  return (
    <article className="step-detail-kpi-card" key={label}>
      <p className="step-detail-kpi-label">{label}</p>
      <p className="step-detail-kpi-value">{value}</p>
      <p className="step-detail-kpi-hint">{hint}</p>
    </article>
  );
}

function renderRequirementChips(requirements: ParsedRequirement[], compactMode: boolean): ReactNode {
  const previewLimit = compactMode ? 4 : 8;
  const previewRows = requirements.slice(0, previewLimit);
  return (
    <div className="step-detail-chip-row">
      {previewRows.map((row) => (
        <span key={row.requirementId} className="step-detail-chip">
          {truncate(`${row.field}: ${row.value}`, compactMode ? 40 : 72)}
        </span>
      ))}
      {requirements.length > previewRows.length ? <span className="step-detail-chip-muted">+{requirements.length - previewRows.length} more</span> : null}
    </div>
  );
}

function renderStructuredFallback(stepName: string): ReactNode {
  return (
    <InlineNotice
      tone="warning"
      message={`This step (${stepName}) payload does not match the structured template. It has automatically fallen back to raw JSON.`}
      className="mt-3"
    />
  );
}

function renderStep2(step: StepProgressState, compactMode: boolean): ReactNode {
  const data = asRecord(step.payload?.data);
  if (!data) {
    return renderStructuredFallback(step.stepName);
  }
  const products = parseTenderProducts(data.tender_products);
  if (products.length === 0) {
    return renderStructuredFallback(step.stepName);
  }

  const totalRequirements = products.reduce((sum, row) => sum + row.requirements.length, 0);
  const ruleReadyCount = products.reduce(
    (sum, row) => sum + row.requirements.filter((requirement) => Boolean(requirement.operator)).length,
    0
  );
  const sourceCount = products.reduce((sum, row) => sum + row.requirements.filter((requirement) => Boolean(requirement.sourceFile)).length, 0);

  const productPreview = compactMode ? products.slice(0, 8) : products;
  return (
    <div className="step-detail-wrap">
      <div className="step-detail-kpi-grid">
        {kpiCard("Tender Items", products.length, "Number of products extracted in step2")}
        {kpiCard("Requirements", totalRequirements, "Total requirement entries")}
        {kpiCard("Operator Matched", ruleReadyCount, "Requirement entries with operator")}
        {kpiCard("Source Snippets", sourceCount, "Entries with source evidence")}
      </div>
      <div className="step-detail-grid">
        {productPreview.map((product) => (
          <article key={product.productKey} className="step-detail-card">
            <header className="step-detail-card-head">
              <div>
                <p className="step-detail-card-title">{product.productName}</p>
                <p className="step-detail-card-subtitle">
                  {product.productKey} · Qty {product.quantity}
                </p>
              </div>
              <StatusBadge label={`${product.requirements.length} req`} tone="active" />
            </header>
            {renderRequirementChips(product.requirements, compactMode)}
          </article>
        ))}
      </div>
      {products.length > productPreview.length ? (
        <p className="step-detail-muted">Compact mode shows only the first {productPreview.length} tender items. Disable compact mode to view all.</p>
      ) : null}
    </div>
  );
}

function renderStep3(step: StepProgressState, compactMode: boolean): ReactNode {
  const data = asRecord(step.payload?.data);
  if (!data) {
    return renderStructuredFallback(step.stepName);
  }
  const rules = parseRules(data.field_rules);
  if (rules.length === 0) {
    return renderStructuredFallback(step.stepName);
  }

  const hardCount = rules.filter((row) => row.isHard).length;
  const softCount = rules.length - hardCount;
  const avgConfidence =
    rules.reduce((sum, row) => sum + ((row.operatorConfidence ?? 0) + (row.hardnessConfidence ?? 0)) / 2, 0) / rules.length;

  return (
    <div className="step-detail-wrap">
      <div className="step-detail-kpi-grid">
        {kpiCard("Rules", rules.length, "field_rules entries")}
        {kpiCard("Hard Constraints", hardCount, "is_hard=true")}
        {kpiCard("Soft Constraints", softCount, "is_hard=false")}
        {kpiCard("Avg Confidence", avgConfidence.toFixed(2), "Average operator/hardness confidence")}
      </div>

      <div className="overflow-auto rounded-xl border border-white/10">
        <table className="data-table step-detail-table min-w-[680px]">
          <thead>
            <tr>
              <th>field</th>
              <th>operator</th>
              <th>hardness</th>
              <th>confidence</th>
              {!compactMode ? <th>rationale</th> : null}
            </tr>
          </thead>
          <tbody>
            {rules.map((row) => (
              <tr key={`${row.field}-${row.operator}`}>
                <td className="font-mono text-[12px]">{row.field}</td>
                <td>{row.operator}</td>
                <td>
                  <StatusBadge label={row.isHard ? "hard" : "soft"} tone={row.isHard ? "error" : "running"} />
                </td>
                <td>
                  {(row.operatorConfidence ?? 0).toFixed(2)} / {(row.hardnessConfidence ?? 0).toFixed(2)}
                </td>
                {!compactMode ? <td className="text-[12px]">{row.rationale || "-"}</td> : null}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function renderStep4(step: StepProgressState, compactMode: boolean): ReactNode {
  const data = asRecord(step.payload?.data);
  if (!data) {
    return renderStructuredFallback(step.stepName);
  }
  const products = parseTenderProducts(data.tender_products);
  if (products.length === 0) {
    return renderStructuredFallback(step.stepName);
  }

  const allRequirements = products.flatMap((product) => product.requirements);
  const mergedCount = allRequirements.filter((row) => Boolean(row.operator)).length;
  const hardCount = allRequirements.filter((row) => row.isHard === true).length;
  const softCount = allRequirements.filter((row) => row.isHard === false).length;
  const unknownCount = allRequirements.length - hardCount - softCount;

  const previewProducts = compactMode ? products.slice(0, 8) : products;
  return (
    <div className="step-detail-wrap">
      <div className="step-detail-kpi-grid">
        {kpiCard("Total Requirements", allRequirements.length, "All requirements before and after merge")}
        {kpiCard("Merged Operators", mergedCount, "Entries with operator merged")}
        {kpiCard("Hard/Soft", `${hardCount}/${softCount}`, "Distribution of hard vs soft")}
        {kpiCard("Unknown Hardness", unknownCount, "is_hard undefined")}
      </div>

      <div className="step-detail-grid">
        {previewProducts.map((product) => {
          const constraints = product.requirements.filter((row) => Boolean(row.operator));
          return (
            <article key={product.productKey} className="step-detail-card">
              <header className="step-detail-card-head">
                <div>
                  <p className="step-detail-card-title">{product.productName}</p>
                  <p className="step-detail-card-subtitle">{product.productKey}</p>
                </div>
                <StatusBadge label={`${constraints.length} merged`} tone="active" />
              </header>
              <div className="step-detail-chip-row">
                {constraints.slice(0, compactMode ? 4 : 8).map((row) => (
                  <span key={row.requirementId} className={cx("step-detail-chip", row.isHard ? "step-detail-chip-hard" : "step-detail-chip-soft")}>
                    {truncate(`${row.field} ${row.operator ?? "?"} ${row.value}`, compactMode ? 42 : 72)}
                  </span>
                ))}
                {constraints.length === 0 ? <span className="step-detail-chip-muted">No mergeable constraints detected</span> : null}
              </div>
            </article>
          );
        })}
      </div>
      {products.length > previewProducts.length ? (
        <p className="step-detail-muted">Compact mode shows only the first {previewProducts.length} tender items. Disable compact mode to view all.</p>
      ) : null}
    </div>
  );
}

function renderStep5(step: StepProgressState, compactMode: boolean, resultLimit: number): ReactNode {
  const data = asRecord(step.payload?.data);
  if (!data) {
    return renderStructuredFallback(step.stepName);
  }
  const queries = parseSqlQueries(data.queries);
  if (queries.length === 0) {
    return renderStructuredFallback(step.stepName);
  }

  const previewRows = queries.slice(0, resultLimit);
  const hardConstraintTotal = queries.reduce((sum, row) => sum + row.hardConstraints.length, 0);

  return (
    <div className="step-detail-wrap">
      <div className="step-detail-kpi-grid">
        {kpiCard("SQL Queries", queries.length, "Query count from step5 output")}
        {kpiCard("Hard Constraints", hardConstraintTotal, "Total hard_constraints_used")}
        {kpiCard("Showing", previewRows.length, `Default Top ${resultLimit}`)}
        {kpiCard("Display Mode", compactMode ? "Compact" : "Full", "Can be toggled anytime")}
      </div>
      <div className="step-detail-grid">
        {previewRows.map((row) => (
          <article key={row.queryId} className="step-detail-card">
            <header className="step-detail-card-head">
              <div>
                <p className="step-detail-card-title">{row.queryId}</p>
                <p className="step-detail-card-subtitle">product_key: {row.productKey}</p>
              </div>
              <StatusBadge label={`${row.hardConstraints.length} hard`} tone="error" />
            </header>
            <div className="step-detail-chip-row">
              {row.hardConstraints.map((constraint) => (
                <span key={`${constraint.field}-${constraint.operator}-${constraint.value}`} className="step-detail-chip step-detail-chip-hard">
                  {truncate(`${constraint.field} ${constraint.operator} ${constraint.value}`, compactMode ? 44 : 80)}
                </span>
              ))}
              {row.hardConstraints.length === 0 ? <span className="step-detail-chip-muted">No hard constraints written</span> : null}
            </div>
            <pre className="step-detail-sql-preview">{compactMode ? truncate(row.sql, 360) : row.sql}</pre>
          </article>
        ))}
      </div>
      {queries.length > previewRows.length ? <p className="step-detail-muted">Only the first {previewRows.length} SQL statements are shown.</p> : null}
    </div>
  );
}

function renderStep6(step: StepProgressState, compactMode: boolean, resultLimit: number): ReactNode {
  const data = asRecord(step.payload?.data);
  if (!data) {
    return renderStructuredFallback(step.stepName);
  }
  const results = parseSqlResults(data.results);
  if (results.length === 0) {
    return renderStructuredFallback(step.stepName);
  }

  const previewRows = results.slice(0, resultLimit);
  const totalRows = results.reduce((sum, row) => sum + row.rowCount, 0);
  const avgLatency =
    results.filter((row) => row.elapsedMs != null).reduce((sum, row) => sum + (row.elapsedMs ?? 0), 0) /
    Math.max(
      1,
      results.reduce((sum, row) => sum + (row.elapsedMs != null ? 1 : 0), 0)
    );

  return (
    <div className="step-detail-wrap">
      <div className="step-detail-kpi-grid">
        {kpiCard("Executed Queries", results.length, "SQL executions in step6")}
        {kpiCard("Total Rows", totalRows, "Total row_count")}
        {kpiCard("Avg Latency", Number.isFinite(avgLatency) ? `${Math.round(avgLatency)}ms` : "-", "Average per SQL")}
        {kpiCard("Showing", previewRows.length, `Default Top ${resultLimit}`)}
      </div>

      {compactMode ? (
        <div className="overflow-auto rounded-xl border border-white/10">
          <table className="data-table step-detail-table min-w-[760px]">
            <thead>
              <tr>
                <th>query_id</th>
                <th>product_key</th>
                <th>row_count</th>
                <th>elapsed</th>
                <th>sample</th>
              </tr>
            </thead>
            <tbody>
              {previewRows.map((row) => {
                const first = row.rows[0] ?? {};
                const name = asText(first.product_name ?? first.db_product_name ?? "-");
                return (
                  <tr key={row.queryId}>
                    <td className="font-mono text-[12px]">{row.queryId}</td>
                    <td>{row.productKey}</td>
                    <td>{row.rowCount}</td>
                    <td>{formatDuration(row.elapsedMs)}</td>
                    <td>{truncate(name, 70)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="step-detail-grid">
          {previewRows.map((row) => (
            <article key={row.queryId} className="step-detail-card">
              <header className="step-detail-card-head">
                <div>
                  <p className="step-detail-card-title">{row.queryId}</p>
                  <p className="step-detail-card-subtitle">product_key: {row.productKey}</p>
                </div>
                <StatusBadge label={`${row.rowCount} rows`} tone="active" />
              </header>
              <p className="step-detail-muted">Duration: {formatDuration(row.elapsedMs)}</p>
              {row.rows.slice(0, 2).map((sample, index) => (
                <div key={`${row.queryId}-sample-${index}`} className="step-detail-sample">
                  <p className="step-detail-sample-title">sample #{index + 1}</p>
                  <div className="step-detail-chip-row">
                    {Object.entries(sample)
                      .slice(0, 8)
                      .map(([key, value]) => (
                        <span key={key} className="step-detail-chip">
                          {truncate(`${key}: ${asText(value)}`, 52)}
                        </span>
                      ))}
                  </div>
                </div>
              ))}
            </article>
          ))}
        </div>
      )}

      {results.length > previewRows.length ? <p className="step-detail-muted">Only the first {previewRows.length} execution results are shown.</p> : null}
    </div>
  );
}

function renderStep7(step: StepProgressState, compactMode: boolean, resultLimit: number): ReactNode {
  const data = asRecord(step.payload?.data);
  if (!data) {
    return renderStructuredFallback(step.stepName);
  }
  const matchResults = parseMatchResults(data.match_results);
  if (matchResults.length === 0) {
    return renderStructuredFallback(step.stepName);
  }

  const totalCandidates = matchResults.reduce((sum, row) => sum + row.candidates.length, 0);
  const hardPassCount = matchResults.reduce((sum, row) => sum + row.candidates.filter((candidate) => candidate.passesHard === true).length, 0);
  const avgSoftScore =
    matchResults.reduce((sum, row) => sum + row.candidates.reduce((inner, candidate) => inner + (candidate.softMatchScore ?? 0), 0), 0) /
    Math.max(1, totalCandidates);

  return (
    <div className="step-detail-wrap">
      <div className="step-detail-kpi-grid">
        {kpiCard("Tender Result Items", matchResults.length, "match_results item count")}
        {kpiCard("Total Candidates", totalCandidates, "All candidates combined")}
        {kpiCard("Hard Constraint Pass", hardPassCount, "passes_hard=true")}
        {kpiCard("Avg Soft Score", avgSoftScore.toFixed(3), "Average soft_match_score")}
      </div>

      <div className="step-detail-grid">
        {matchResults.map((row) => {
          const previewCandidates = row.candidates.slice(0, resultLimit);
          return (
            <article key={row.productKey} className="step-detail-card">
              <header className="step-detail-card-head">
                <div>
                  <p className="step-detail-card-title">{row.productKey}</p>
                  <p className="step-detail-card-subtitle">Candidates: {row.candidates.length}</p>
                </div>
                <StatusBadge label={`Top ${previewCandidates.length}`} tone="active" />
              </header>
              <div className="overflow-auto rounded-xl border border-white/10">
                <table className="data-table step-detail-table min-w-[780px]">
                  <thead>
                    <tr>
                      <th>rank</th>
                      <th>product</th>
                      <th>soft</th>
                      <th>hard</th>
                      <th>soft constraints</th>
                      {!compactMode ? <th>explanation</th> : null}
                    </tr>
                  </thead>
                  <tbody>
                    {previewCandidates.map((candidate) => (
                      <tr key={`${row.productKey}-${candidate.rank ?? "n"}-${candidate.dbProductId}`}>
                        <td>{candidate.rank ?? "-"}</td>
                        <td className="text-[12px]">
                          <div className="font-mono">{candidate.dbProductId}</div>
                          <div>{truncate(candidate.dbProductName, compactMode ? 56 : 88)}</div>
                        </td>
                        <td>{candidate.softMatchScore != null ? candidate.softMatchScore.toFixed(3) : "-"}</td>
                        <td>
                          <StatusBadge
                            label={candidate.passesHard == null ? "unknown" : candidate.passesHard ? "pass" : "fail"}
                            tone={candidate.passesHard == null ? "idle" : candidate.passesHard ? "done" : "error"}
                          />
                        </td>
                        <td>
                          {candidate.matchedSoftCount} / {candidate.matchedSoftCount + candidate.unmetSoftCount}
                        </td>
                        {!compactMode ? <td className="text-[12px]">{truncate(candidate.explanation, 150)}</td> : null}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {row.candidates.length > previewCandidates.length ? (
                <p className="step-detail-muted">Only the first {previewCandidates.length} candidates are shown for this tender item.</p>
              ) : null}
            </article>
          );
        })}
      </div>
    </div>
  );
}

export function JobStepDetailPanel({ step, compactMode, resultLimit }: JobStepDetailPanelProps) {
  if (!step.payload) {
    return null;
  }

  switch (step.stepName) {
    case "step2_extract_requirements":
      return renderStep2(step, compactMode);
    case "step3_external_field_rules":
      return renderStep3(step, compactMode);
    case "step4_merge_requirements_hardness":
      return renderStep4(step, compactMode);
    case "step5_build_sql":
      return renderStep5(step, compactMode, resultLimit);
    case "step6_execute_sql":
      return renderStep6(step, compactMode, resultLimit);
    case "step7_rank_candidates":
      return renderStep7(step, compactMode, resultLimit);
    default:
      return null;
  }
}
