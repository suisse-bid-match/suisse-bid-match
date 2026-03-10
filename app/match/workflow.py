from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from app.core.models import (
    AuditEvent,
    DocumentClassification,
    DoclingLine,
    MatchRun,
    ProductMatchResult,
    RequirementSet,
    SQLPlan,
    utcnow,
)
from app.core.settings import settings
from app.core.storage import load_package_index

from .context import ContextLine, build_context_lines
from .doc_classifier import LLMDocumentClassifier
from .matcher import execute_and_rank
from .metadata import DomainMetadata, load_domain_metadata
from .requirement_extractor import LLMRequirementExtractor
from .schema_introspector import fetch_schema_metadata
from .schema_mapper import map_requirements_to_schema
from .sql_builder import build_sql_plan
from .sql_generator import LLMSQLGenerator
from .sql_validator import validate_readonly_select


@dataclass
class WorkflowState:
    package_id: str
    domain: str
    top_k: int
    strict_hard_constraints: bool
    package_index: Any = None
    metadata: DomainMetadata | None = None
    context_lines: list[ContextLine] | None = None
    doc_classifications: list[DocumentClassification] | None = None
    requirements: RequirementSet | None = None
    mapped_conditions: list[Any] | None = None
    schema_metadata: dict[str, Any] | None = None
    sql_plan: SQLPlan | None = None
    candidates: list[Any] | None = None
    product_results: list[ProductMatchResult] | None = None
    unmet_constraints: list[str] | None = None
    audit_trail: list[AuditEvent] | None = None
    step_web: dict[str, Any] | None = None
    blocked: bool = False


class MatchWorkflow:
    def __init__(self, progress_callback=None) -> None:
        self._progress_callback = progress_callback

    @staticmethod
    def _coerce_state(value: Any) -> WorkflowState:
        if isinstance(value, WorkflowState):
            return value
        if isinstance(value, dict):
            return WorkflowState(
                package_id=str(value.get("package_id", "")),
                domain=str(value.get("domain", "")),
                top_k=int(value.get("top_k", 0)),
                strict_hard_constraints=bool(value.get("strict_hard_constraints", True)),
                package_index=value.get("package_index"),
                metadata=value.get("metadata"),
                context_lines=value.get("context_lines"),
                doc_classifications=value.get("doc_classifications"),
                requirements=value.get("requirements"),
                mapped_conditions=value.get("mapped_conditions"),
                schema_metadata=value.get("schema_metadata"),
                sql_plan=value.get("sql_plan"),
                candidates=value.get("candidates"),
                product_results=value.get("product_results"),
                unmet_constraints=value.get("unmet_constraints"),
                audit_trail=value.get("audit_trail"),
                step_web=value.get("step_web"),
                blocked=bool(value.get("blocked", False)),
            )
        raise TypeError(f"unsupported workflow state type: {type(value)!r}")

    def run(
        self,
        *,
        package_id: str,
        domain: str,
        top_k: int,
        strict_hard_constraints: bool,
        progress_callback=None,
    ) -> MatchRun:
        if progress_callback is not None:
            self._progress_callback = progress_callback
        state = WorkflowState(
            package_id=package_id,
            domain=domain,
            top_k=top_k,
            strict_hard_constraints=strict_hard_constraints,
            context_lines=[],
            doc_classifications=[],
            mapped_conditions=[],
            candidates=[],
            product_results=[],
            unmet_constraints=[],
            audit_trail=[],
            step_web={},
        )

        if self._langgraph_available():
            state = self._run_with_langgraph(state)
        else:
            state = self._run_sequential(state)

        requirements = state.requirements or RequirementSet(
            package_id=package_id,
            domain=domain,
            requirements=[],
            generated_at=utcnow(),
        )
        sql_plan = state.sql_plan or SQLPlan(
            domain=domain,
            sql="",
            blocked=True,
            block_reason="sql plan unavailable",
            validated=False,
        )

        return MatchRun(
            run_id=str(uuid.uuid4()),
            package_id=package_id,
            domain=domain,
            created_at=utcnow(),
            top_k=top_k,
            strict_hard_constraints=strict_hard_constraints,
            blocked=state.blocked or sql_plan.blocked,
            doc_classifications=state.doc_classifications or [],
            requirements=requirements,
            mapped_conditions=state.mapped_conditions or [],
            sql_plan=sql_plan,
            candidates=state.candidates or [],
            product_results=state.product_results or [],
            unmet_constraints=state.unmet_constraints or [],
            audit_trail=state.audit_trail or [],
        )

    def _emit_progress(self, event: str, payload: dict) -> None:
        if self._progress_callback is None:
            return
        try:
            self._progress_callback(event, payload)
        except Exception:
            return

    def _run_step(self, state: WorkflowState | dict[str, Any], step: str, func) -> WorkflowState:
        state = self._coerce_state(state)
        started = utcnow()
        self._emit_progress(
            "step_started",
            {
                "step": step,
                "started_at": started.timestamp(),
                "message": f"{step} started",
            },
        )
        input_snapshot = {
            "blocked": state.blocked,
            "unmet_constraints": len(state.unmet_constraints or []),
        }
        try:
            state = func(state)
            state = self._coerce_state(state)
            status = "blocked" if state.blocked else "ok"
            summary = f"{step} completed"
            output_snapshot = {
                "blocked": state.blocked,
                "requirements": len(state.requirements.requirements) if state.requirements else 0,
                "mapped_conditions": len(state.mapped_conditions or []),
                "candidates": len(state.candidates or []),
                "product_results": len(state.product_results or []),
            }
            error = None
        except Exception as exc:
            state.blocked = True
            status = "failed"
            summary = f"{step} failed"
            output_snapshot = {}
            error = str(exc)

        if state.step_web and step in state.step_web:
            output_snapshot["web_search"] = state.step_web.pop(step)

        finished = utcnow()
        self._emit_progress(
            "step_finished",
            {
                "step": step,
                "status": "failed" if error else "completed",
                "finished_at": finished.timestamp(),
                "message": summary,
                "error": error,
            },
        )
        event = AuditEvent(
            step=step,
            status=status,  # type: ignore[arg-type]
            started_at=started,
            finished_at=finished,
            summary=summary,
            input_snapshot=input_snapshot,
            output_snapshot=output_snapshot,
            error=error,
        )
        if state.audit_trail is None:
            state.audit_trail = []
        state.audit_trail.append(event)
        return state

    def _run_sequential(self, state: WorkflowState) -> WorkflowState:
        steps = [
            ("parse_package", self._step_parse_package),
            ("classify_documents", self._step_classify_documents),
            ("extract_requirements", self._step_extract_requirements),
            ("fetch_schema_metadata", self._step_fetch_schema_metadata),
            ("map_to_schema", self._step_map_to_schema),
            ("generate_sql", self._step_generate_sql),
            ("validate_sql", self._step_validate_sql),
            ("execute_query", self._step_execute_query),
            ("rank_and_explain", self._step_rank_and_explain),
        ]
        for name, func in steps:
            state = self._run_step(state, name, func)
            if state.blocked:
                break
        state = self._run_step(state, "build_audit", self._step_build_audit)
        return state

    def _run_with_langgraph(self, state: WorkflowState) -> WorkflowState:
        try:
            from langgraph.graph import END, StateGraph
        except Exception:
            return self._run_sequential(state)

        graph = StateGraph(WorkflowState)

        graph.add_node("parse_package", lambda s: self._run_step(s, "parse_package", self._step_parse_package))
        graph.add_node(
            "classify_documents",
            lambda s: self._run_step(s, "classify_documents", self._step_classify_documents),
        )
        graph.add_node(
            "extract_requirements",
            lambda s: self._run_step(s, "extract_requirements", self._step_extract_requirements),
        )
        graph.add_node(
            "fetch_schema_metadata",
            lambda s: self._run_step(s, "fetch_schema_metadata", self._step_fetch_schema_metadata),
        )
        graph.add_node("map_to_schema", lambda s: self._run_step(s, "map_to_schema", self._step_map_to_schema))
        graph.add_node("generate_sql", lambda s: self._run_step(s, "generate_sql", self._step_generate_sql))
        graph.add_node("validate_sql", lambda s: self._run_step(s, "validate_sql", self._step_validate_sql))
        graph.add_node("execute_query", lambda s: self._run_step(s, "execute_query", self._step_execute_query))
        graph.add_node(
            "rank_and_explain",
            lambda s: self._run_step(s, "rank_and_explain", self._step_rank_and_explain),
        )
        graph.add_node("build_audit", lambda s: self._run_step(s, "build_audit", self._step_build_audit))

        graph.set_entry_point("parse_package")
        graph.add_edge("parse_package", "classify_documents")
        graph.add_edge("classify_documents", "extract_requirements")
        graph.add_edge("extract_requirements", "fetch_schema_metadata")
        graph.add_edge("fetch_schema_metadata", "map_to_schema")
        graph.add_edge("map_to_schema", "generate_sql")
        graph.add_edge("generate_sql", "validate_sql")
        graph.add_edge("validate_sql", "execute_query")
        graph.add_edge("execute_query", "rank_and_explain")
        graph.add_edge("rank_and_explain", "build_audit")
        graph.add_edge("build_audit", END)

        app = graph.compile()
        result = app.invoke(state)
        return self._coerce_state(result)

    @staticmethod
    def _langgraph_available() -> bool:
        try:
            import langgraph  # noqa: F401

            return True
        except Exception:
            return False

    def _step_parse_package(self, state: WorkflowState) -> WorkflowState:
        index = load_package_index(state.package_id)
        metadata = load_domain_metadata(state.domain)

        state.package_index = index
        state.metadata = metadata
        state.context_lines = []
        if not index.docling_documents:
            state.blocked = True
            if state.unmet_constraints is None:
                state.unmet_constraints = []
            state.unmet_constraints.append("no docling parse results found in package index")
        return state

    def _step_classify_documents(self, state: WorkflowState) -> WorkflowState:
        if state.package_index is None:
            state.blocked = True
            return state

        classifier = LLMDocumentClassifier()
        docling_by_id = {item.doc_id: item for item in state.package_index.docling_documents}
        classifications: list[Any] = []
        web_calls: list[dict[str, Any]] = []

        web_context = classifier.fetch_web_context()
        if classifier.last_web_search:
            web_calls.append({"package_id": state.package_id, **classifier.last_web_search})

        documents = list(state.package_index.documents)
        results: list[DocumentClassification | None] = [None] * len(documents)

        def _classify_one(idx: int, doc: Any) -> tuple[int, DocumentClassification]:
            docling = docling_by_id.get(doc.doc_id)
            docling_lines = docling.lines if docling and docling.lines else []
            # For speed: do not parse PDFs during classification; rely on filename/path + any cached lines.
            worker = LLMDocumentClassifier()
            try:
                result = worker.classify(
                    doc=doc,
                    docling_lines=docling_lines,
                    web_context=web_context,
                    enable_web_search=False,
                )
            except Exception as exc:
                result = DocumentClassification(
                    doc_id=doc.doc_id,
                    doc_name=doc.name,
                    is_application_form=False,
                    confidence=0.0,
                    reason=f"LLM classification failed: {exc}",
                    evidence_refs=[],
                    parse_failed=False,
                )
            return idx, result

        max_workers = min(6, max(1, len(documents)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_classify_one, idx, doc): idx
                for idx, doc in enumerate(documents)
            }
            for future in as_completed(futures):
                idx, result = future.result()
                results[idx] = result

        classifications = [item for item in results if item is not None]

        state.doc_classifications = classifications
        if web_calls:
            if state.step_web is None:
                state.step_web = {}
            state.step_web["classify_documents"] = {"calls": web_calls}

        if not any(getattr(item, "is_application_form", False) for item in classifications):
            keyword_hints = (
                "leistungsverzeichnis",
                " lv ",
                "angebot",
                "offerte",
                "preisblatt",
                "报价",
                "数量清单",
                "mengen",
            )
            best_idx = None
            best_score = -1.0
            for idx, item in enumerate(classifications):
                doc = next((d for d in state.package_index.documents if d.doc_id == item.doc_id), None)
                name = (doc.name if doc else item.doc_name or "").lower()
                score = float(getattr(item, "confidence", 0.0) or 0.0)
                if doc and doc.kind == "xlsx":
                    score += 0.25
                if any(hint in name for hint in keyword_hints):
                    score += 0.4
                docling = docling_by_id.get(item.doc_id)
                if docling and docling.lines:
                    score += min(len(docling.lines) / 500.0, 0.2)
                if score > best_score:
                    best_score = score
                    best_idx = idx
            if best_idx is not None:
                chosen = classifications[best_idx]
                chosen.is_application_form = True
                chosen.reason = (chosen.reason or "LLM classification").strip() + " | fallback: forced candidate"
            else:
                state.blocked = True
                if state.unmet_constraints is None:
                    state.unmet_constraints = []
                state.unmet_constraints.append("no application form documents identified")
        return state

    def _step_extract_requirements(self, state: WorkflowState) -> WorkflowState:
        if state.metadata is None or state.package_index is None or state.doc_classifications is None:
            state.blocked = True
            return state

        doc_ids = {
            item.doc_id
            for item in state.doc_classifications
            if getattr(item, "is_application_form", False)
        }
        lines = build_context_lines(
            state.package_index,
            max_lines=None,
            doc_ids=doc_ids,
            dedupe=False,
        )
        state.context_lines = lines
        if not lines:
            state.blocked = True
            if state.unmet_constraints is None:
                state.unmet_constraints = []
            state.unmet_constraints.append("no docling context lines from application documents")
            return state

        extractor = LLMRequirementExtractor()
        state.requirements = extractor.extract(
            package_id=state.package_id,
            domain=state.domain,
            meta=state.metadata,
            context_lines=lines,
        )
        if extractor.last_web_search:
            if state.step_web is None:
                state.step_web = {}
            state.step_web["extract_requirements"] = extractor.last_web_search
        if state.requirements is None or not state.requirements.requirements:
            state.blocked = True
            if state.unmet_constraints is None:
                state.unmet_constraints = []
            state.unmet_constraints.append("no requirements extracted from tender context")
        return state

    def _step_fetch_schema_metadata(self, state: WorkflowState) -> WorkflowState:
        if state.metadata is None:
            state.blocked = True
            return state
        try:
            state.schema_metadata = fetch_schema_metadata(
                table_whitelist=state.metadata.table_whitelist,
                field_whitelist=state.metadata.field_whitelist,
            )
        except Exception as exc:
            state.blocked = True
            if state.unmet_constraints is None:
                state.unmet_constraints = []
            state.unmet_constraints.append(f"schema metadata fetch failed: {exc}")
        return state

    def _step_map_to_schema(self, state: WorkflowState) -> WorkflowState:
        if state.metadata is None or state.requirements is None:
            state.blocked = True
            return state

        if state.unmet_constraints is None:
            state.unmet_constraints = []

        global_requirements = [
            item for item in state.requirements.requirements if not item.product_key
        ]

        if state.requirements.product_scopes:
            all_mapped = []
            product_results: list[ProductMatchResult] = []
            prefixed_unmet: list[str] = []
            for scope in state.requirements.product_scopes:
                scoped_reqs = scope.requirements + global_requirements
                mapped, unmet = map_requirements_to_schema(scoped_reqs, state.metadata)
                result = ProductMatchResult(
                    product_key=scope.product_key,
                    product_name=scope.product_name,
                    quantity=scope.quantity,
                    requirements=scoped_reqs,
                    mapped_conditions=mapped,
                    unmet_constraints=list(unmet),
                    blocked=bool(state.strict_hard_constraints and unmet),
                )
                product_results.append(result)
                all_mapped.extend(mapped)
                for item in unmet:
                    prefixed_unmet.append(f"{scope.product_key}: {item}")
            state.product_results = product_results
            state.mapped_conditions = all_mapped
            state.unmet_constraints.extend(prefixed_unmet)
            if state.strict_hard_constraints and product_results and all(item.blocked for item in product_results):
                state.blocked = True
            return state

        mapped, unmet = map_requirements_to_schema(state.requirements.requirements, state.metadata)
        state.mapped_conditions = mapped
        state.unmet_constraints.extend(unmet)
        if state.strict_hard_constraints and unmet:
            state.blocked = True
        return state

    def _step_generate_sql(self, state: WorkflowState) -> WorkflowState:
        if state.metadata is None or state.schema_metadata is None:
            state.blocked = True
            return state

        if state.unmet_constraints is None:
            state.unmet_constraints = []

        generator = LLMSQLGenerator()
        web_calls: list[dict[str, Any]] = []

        if state.product_results:
            first_plan: SQLPlan | None = None
            any_unblocked = False
            prefixed_unmet: list[str] = []
            for result in state.product_results:
                required_fields = self._required_select_fields(
                    result.mapped_conditions,
                    state.metadata,
                )
                sql_plan = generator.generate(
                    domain=state.domain,
                    requirements=result.requirements,
                    schema_metadata=state.schema_metadata,
                    required_fields=required_fields,
                    top_k=state.top_k,
                )
                if sql_plan.blocked and not sql_plan.sql.strip():
                    fallback = build_sql_plan(
                        domain=state.domain,
                        mappings=result.mapped_conditions,
                        meta=state.metadata,
                        top_k=state.top_k,
                        strict_hard_constraints=state.strict_hard_constraints,
                    )
                    if fallback.unmet_constraints:
                        result.unmet_constraints.extend(fallback.unmet_constraints)
                        prefixed_unmet.extend(
                            f"{result.product_key}: {item}" for item in fallback.unmet_constraints
                        )
                    sql_plan = fallback.plan
                result.sql_plan = sql_plan
                result.blocked = result.blocked or sql_plan.blocked
                if generator.last_web_search:
                    web_calls.append(
                        {
                            "product_key": result.product_key,
                            "product_name": result.product_name,
                            **generator.last_web_search,
                        }
                    )
                if first_plan is None:
                    first_plan = sql_plan
                if not result.blocked:
                    any_unblocked = True

            state.sql_plan = first_plan or SQLPlan(
                domain=state.domain,
                sql="",
                blocked=True,
                block_reason="sql plan unavailable",
                validated=False,
            )
            state.unmet_constraints.extend(prefixed_unmet)
            if state.strict_hard_constraints and not any_unblocked:
                state.blocked = True
            if web_calls:
                if state.step_web is None:
                    state.step_web = {}
                state.step_web["generate_sql"] = {"calls": web_calls}
            return state

        required_fields = self._required_select_fields(state.mapped_conditions or [], state.metadata)
        sql_plan = generator.generate(
            domain=state.domain,
            requirements=state.requirements.requirements if state.requirements else [],
            schema_metadata=state.schema_metadata,
            required_fields=required_fields,
            top_k=state.top_k,
        )
        if sql_plan.blocked and not sql_plan.sql.strip():
            fallback = build_sql_plan(
                domain=state.domain,
                mappings=state.mapped_conditions or [],
                meta=state.metadata,
                top_k=state.top_k,
                strict_hard_constraints=state.strict_hard_constraints,
            )
            if fallback.unmet_constraints:
                state.unmet_constraints.extend(fallback.unmet_constraints)
            sql_plan = fallback.plan
        state.sql_plan = sql_plan
        if generator.last_web_search:
            if state.step_web is None:
                state.step_web = {}
            state.step_web["generate_sql"] = generator.last_web_search
        if sql_plan.blocked:
            state.blocked = True
        return state

    def _step_validate_sql(self, state: WorkflowState) -> WorkflowState:
        if state.sql_plan is None or state.schema_metadata is None or state.metadata is None:
            state.blocked = True
            return state
        max_repairs = max(0, settings.match_sql_repair_max_rounds)
        generator = LLMSQLGenerator()

        def _validate_and_repair(
            plan: SQLPlan,
            requirements: list[Any],
            required_fields: list[str],
        ) -> SQLPlan:
            errors = validate_readonly_select(
                sql=plan.sql,
                table_whitelist=set(state.schema_metadata.get("allowed_tables", [])),
                field_whitelist=set(state.schema_metadata.get("allowed_fields", [])),
            )
            attempts = 0
            while errors and attempts < max_repairs:
                repaired = generator.repair(
                    domain=state.domain,
                    previous_sql=plan.sql,
                    validation_errors=errors,
                    requirements=requirements,
                    schema_metadata=state.schema_metadata,
                    required_fields=required_fields,
                    top_k=state.top_k,
                )
                if repaired is None or repaired.sql == plan.sql:
                    break
                plan = repaired
                errors = validate_readonly_select(
                    sql=plan.sql,
                    table_whitelist=set(state.schema_metadata.get("allowed_tables", [])),
                    field_whitelist=set(state.schema_metadata.get("allowed_fields", [])),
                )
                attempts += 1

            plan.validation_errors = errors
            plan.validated = len(errors) == 0
            if errors:
                plan.blocked = True
                plan.block_reason = f"SQL validation failed ({len(errors)} errors)"
            return plan

        if state.product_results:
            any_unblocked = False
            for result in state.product_results:
                if result.sql_plan is None:
                    result.blocked = True
                    continue
                required_fields = self._required_select_fields(
                    result.mapped_conditions,
                    state.metadata,
                )
                result.sql_plan = _validate_and_repair(
                    result.sql_plan,
                    result.requirements,
                    required_fields,
                )
                result.blocked = result.blocked or result.sql_plan.blocked
                if not result.blocked:
                    any_unblocked = True
            if state.sql_plan is not None:
                state.sql_plan = state.product_results[0].sql_plan if state.product_results else state.sql_plan
            if state.strict_hard_constraints and not any_unblocked:
                state.blocked = True
            return state

        required_fields = self._required_select_fields(state.mapped_conditions or [], state.metadata)
        state.sql_plan = _validate_and_repair(
            state.sql_plan,
            state.requirements.requirements if state.requirements else [],
            required_fields,
        )
        if not state.sql_plan.validated:
            state.blocked = True
        return state

    def _step_execute_query(self, state: WorkflowState) -> WorkflowState:
        if state.metadata is None or state.sql_plan is None:
            state.blocked = True
            return state

        if state.unmet_constraints is None:
            state.unmet_constraints = []

        if state.product_results:
            combined_candidates = []
            prefixed_unmet: list[str] = []
            any_candidates = False
            first_stats: dict[str, Any] | None = None
            for result in state.product_results:
                if result.sql_plan is None:
                    result.blocked = True
                    continue
                candidates, unmet, stats = execute_and_rank(
                    sql_plan=result.sql_plan,
                    mappings=result.mapped_conditions,
                    meta=state.metadata,
                    top_k=state.top_k,
                    strict_hard_constraints=state.strict_hard_constraints,
                )
                if first_stats is None:
                    first_stats = stats
                for candidate in candidates:
                    candidate.request_product_key = result.product_key
                    candidate.request_product_name = result.product_name
                result.candidates = candidates
                result.unmet_constraints.extend(unmet)
                if state.strict_hard_constraints and not candidates:
                    result.blocked = True
                if candidates:
                    any_candidates = True
                combined_candidates.extend(candidates[: state.top_k])
                for item in unmet:
                    prefixed_unmet.append(f"{result.product_key}: {item}")

            state.candidates = combined_candidates
            state.unmet_constraints.extend(prefixed_unmet)
            if state.sql_plan is not None and first_stats is not None:
                state.sql_plan.params["_query_stats"] = first_stats
            if state.strict_hard_constraints and not any_candidates:
                state.blocked = True
            elif state.product_results and all(item.blocked for item in state.product_results):
                state.blocked = True
            return state

        candidates, unmet, stats = execute_and_rank(
            sql_plan=state.sql_plan,
            mappings=state.mapped_conditions or [],
            meta=state.metadata,
            top_k=state.top_k,
            strict_hard_constraints=state.strict_hard_constraints,
        )
        state.candidates = candidates
        state.unmet_constraints.extend(unmet)
        state.sql_plan.params["_query_stats"] = stats
        if state.strict_hard_constraints and not candidates:
            state.blocked = True
        return state

    def _step_rank_and_explain(self, state: WorkflowState) -> WorkflowState:
        # Ranking and explanation are produced during query execution.
        return state

    def _step_build_audit(self, state: WorkflowState) -> WorkflowState:
        # Audit events are already appended by _run_step.
        return state

    @staticmethod
    def _required_select_fields(
        mappings: list[Any],
        meta: DomainMetadata,
    ) -> list[str]:
        required: set[str] = {
            "match_products.product_id",
            "match_products.product_name",
        }
        for item in mappings:
            field = getattr(item, "mapped_field", None)
            if field:
                required.add(field)
        for field in meta.field_whitelist:
            if field.endswith(("_path", "_url", "_languages")):
                required.add(field)
        return sorted(required)
