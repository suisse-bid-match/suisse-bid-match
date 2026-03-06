from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from typing import Any

from apps.api.core.config import get_settings
from apps.api.models.schemas import (
    ChatDebug,
    ChatRequest,
    ChatResponse,
    Citation,
    MatchEvidence,
    MatchScoreBreakdown,
)
from apps.api.services.agent.planner import build_plan
from apps.api.services.retrieval.hybrid import RetrievalCandidate, retrieve_hybrid
from apps.api.services.retrieval.rerank import rerank_candidates

logger = logging.getLogger(__name__)


EXPLAIN_STOPWORDS = {
    "the",
    "is",
    "are",
    "a",
    "an",
    "what",
    "which",
    "for",
    "in",
    "of",
    "to",
    "and",
    "on",
    "with",
    "next",
    "day",
    "days",
    "please",
    "tender",
    "tenders",
    "service",
    "services",
    "simap",
    "key",
}


def _fold_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _has_word(text: str, term: str) -> bool:
    folded_text = _fold_text(text)
    folded_term = _fold_text(term)
    if not folded_term:
        return False
    return re.search(rf"\b{re.escape(folded_term)}\b", folded_text) is not None


def _extract_question_terms(question: str, max_terms: int = 12) -> list[str]:
    tokens = re.findall(r"[A-Za-zÀ-ÿ0-9_-]+", question.lower())
    out: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        if len(token) < 3:
            continue
        if token in EXPLAIN_STOPWORDS:
            continue
        seen.add(token)
        out.append(token)
        if len(out) >= max_terms:
            break
    return out


def _candidate_combined_text(candidate: RetrievalCandidate) -> str:
    metadata = candidate.metadata or {}
    cpv_codes = metadata.get("cpv_codes") or []
    cpv_text = " ".join(str(code) for code in cpv_codes if code)
    parts = [
        candidate.title or "",
        candidate.text or "",
        str(metadata.get("buyer_name") or ""),
        str(metadata.get("region") or ""),
        str(metadata.get("procedure_type") or ""),
        cpv_text,
    ]
    return " ".join(parts)


def _extract_matched_sentences(text: str, matched_terms: list[str], limit: int = 3) -> list[str]:
    if not text:
        return []

    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+|\n+", text) if s.strip()]
    if not matched_terms:
        return sentences[:1]

    hits: list[str] = []
    for sentence in sentences:
        if any(_has_word(sentence, term) for term in matched_terms):
            clipped = sentence[:260] + ("..." if len(sentence) > 260 else "")
            hits.append(clipped)
        if len(hits) >= limit:
            break
    return hits or sentences[:1]


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    raw = (raw or "").strip()
    if not raw:
        return None

    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        return None

    try:
        data = json.loads(raw[start : end + 1])
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _to_float_or_none(value: Any) -> float | None:
    try:
        v = float(value)
    except Exception:
        return None
    if v < 0 or v > 1:
        return None
    return v


def _supports_temperature(model: str) -> bool:
    return not str(model or "").lower().startswith("gpt-5")


def _chat_completion_with_compat(
    client: Any,
    *,
    model: str,
    messages: list[dict[str, str]],
    temperature: float | None = None,
    response_format: dict[str, Any] | None = None,
):
    kwargs: dict[str, Any] = {"model": model, "messages": messages}
    if response_format is not None:
        kwargs["response_format"] = response_format
    if temperature is not None and _supports_temperature(model):
        kwargs["temperature"] = temperature

    try:
        return client.chat.completions.create(**kwargs)
    except Exception as exc:
        # Some models (e.g. GPT-5 family) reject explicit temperature.
        if "temperature" in kwargs and "temperature" in str(exc).lower() and "unsupported" in str(exc).lower():
            kwargs.pop("temperature", None)
            return client.chat.completions.create(**kwargs)
        raise


def _llm_select_matches(
    question: str,
    candidates: list[RetrievalCandidate],
    top_k: int,
) -> tuple[list[RetrievalCandidate], dict[str, dict[str, Any]]]:
    settings = get_settings()
    chat_api_key = settings.resolved_openai_chat_api_key
    if not chat_api_key or not settings.llm_matching_enabled:
        return candidates[:top_k], {}

    pool = candidates[: max(top_k, settings.llm_match_pool_size)]
    if not pool:
        return [], {}

    try:
        from openai import OpenAI

        client = OpenAI(api_key=chat_api_key)
        candidate_lines: list[str] = []
        for idx, candidate in enumerate(pool, start=1):
            metadata = candidate.metadata or {}
            buyer = str(metadata.get("buyer_name") or "-")
            region = str(metadata.get("region") or "-")
            language = str(metadata.get("language") or "-")
            excerpt = candidate.text.replace("\n", " ").strip()[:700]
            candidate_lines.append(
                f"[{idx}] title={candidate.title or '-'} buyer={buyer} region={region} "
                f"language={language} source_notice_id={candidate.notice_id}\n"
                f"snippet={excerpt}\n"
                f"scores dense={candidate.dense_score:.4f} bm25={candidate.bm25_score:.4f} final={candidate.final_score:.4f}"
            )

        prompt = (
            "Select the most relevant SIMAP records for the user query. "
            "Use evidence only from provided candidates.\n\n"
            f"User query:\n{question}\n\n"
            f"Return exactly top {top_k} items or fewer if evidence is clearly weak.\n"
            "Output JSON with this schema:\n"
            "{"
            "\"matches\":["
            "{\"index\":1,\"reason\":\"one sentence reason\","
            "\"matching_points\":[\"short point 1\",\"short point 2\"],"
            "\"confidence\":0.0}"
            "]"
            "}\n\n"
            "Rules:\n"
            "- index must refer to the candidate index.\n"
            "- reason should be concrete, not generic.\n"
            "- matching_points should mention what matches the query intent.\n"
            "- confidence must be between 0 and 1.\n\n"
            "Candidates:\n"
            + "\n\n".join(candidate_lines)
        )

        messages = [
            {"role": "system", "content": "You are a strict relevance ranker for public tenders."},
            {"role": "user", "content": prompt},
        ]
        try:
            resp = _chat_completion_with_compat(
                client,
                model=settings.openai_chat_model,
                temperature=0.0,
                messages=messages,
                response_format={"type": "json_object"},
            )
        except Exception:
            resp = _chat_completion_with_compat(
                client,
                model=settings.openai_chat_model,
                temperature=0.0,
                messages=messages,
            )
        raw = (resp.choices[0].message.content or "").strip()
        parsed = _extract_json_object(raw) or {}
        matches = parsed.get("matches") or []
        if not isinstance(matches, list):
            matches = []

        selected: list[RetrievalCandidate] = []
        seen_indices: set[int] = set()
        meta_by_chunk_id: dict[str, dict[str, Any]] = {}
        for item in matches:
            if not isinstance(item, dict):
                continue
            idx_raw = item.get("index")
            try:
                idx = int(idx_raw) - 1
            except Exception:
                continue
            if idx < 0 or idx >= len(pool) or idx in seen_indices:
                continue

            seen_indices.add(idx)
            candidate = pool[idx]
            selected.append(candidate)
            meta_by_chunk_id[candidate.chunk_id] = {
                "llm_reason": str(item.get("reason") or "").strip() or None,
                "matching_points": [str(x).strip() for x in (item.get("matching_points") or []) if str(x).strip()],
                "confidence": _to_float_or_none(item.get("confidence")),
            }
            if len(selected) >= top_k:
                break

        if len(selected) < top_k:
            for candidate in pool:
                if candidate in selected:
                    continue
                selected.append(candidate)
                if len(selected) >= top_k:
                    break

        return selected[:top_k], meta_by_chunk_id
    except Exception as exc:
        logger.warning("LLM match selection failed; fallback to heuristic ranking. err=%s", exc)
        return pool[:top_k], {}


def _build_match_evidence(
    question: str,
    candidate: RetrievalCandidate,
    llm_meta: dict[str, Any] | None = None,
) -> MatchEvidence:
    terms = _extract_question_terms(question)
    combined_text = _candidate_combined_text(candidate)
    matched_terms = [term for term in terms if _has_word(combined_text, term)][:8]
    matched_sentences = _extract_matched_sentences(candidate.text, matched_terms, limit=3)

    llm_reason = None
    matching_points: list[str] = []
    confidence = None
    if llm_meta:
        llm_reason = str(llm_meta.get("llm_reason") or "").strip() or None
        matching_points = [str(x).strip() for x in (llm_meta.get("matching_points") or []) if str(x).strip()][:4]
        confidence = _to_float_or_none(llm_meta.get("confidence"))

    return MatchEvidence(
        matched_terms=matched_terms,
        matched_sentences=matched_sentences,
        score_breakdown=MatchScoreBreakdown(
            dense_score=round(candidate.dense_score, 4),
            bm25_score=round(candidate.bm25_score, 4),
            final_score=round(candidate.final_score, 4),
        ),
        llm_reason=llm_reason,
        matching_points=matching_points,
        confidence=confidence,
    )


def _build_citations(
    question: str,
    candidates: list[RetrievalCandidate],
    llm_meta_by_chunk_id: dict[str, dict[str, Any]] | None = None,
) -> list[Citation]:
    citations: list[Citation] = []
    for c in candidates:
        snippet = c.text[:300] + ("..." if len(c.text) > 300 else "")
        llm_meta = (llm_meta_by_chunk_id or {}).get(c.chunk_id)
        citations.append(
            Citation(
                title=c.title,
                url=c.url,
                doc_url=c.doc_url,
                snippet=snippet,
                score=round(c.final_score, 4),
                notice_id=c.notice_id,
                match_evidence=_build_match_evidence(question, c, llm_meta=llm_meta),
            )
        )
    return citations


def _extractive_summary(question: str, candidates: list[RetrievalCandidate]) -> str:
    if not candidates:
        return "No relevant tender evidence was found for your query and filters."

    lead = [
        "Based on retrieved tender evidence, here are the key points:",
    ]

    for i, c in enumerate(candidates[:5], start=1):
        line = c.text.replace("\n", " ").strip()
        if len(line) > 220:
            line = line[:220] + "..."
        lead.append(f"{i}. {line}")

    lead.append("Use the citations to validate details in the original notice/documents.")
    return "\n".join(lead)


def _llm_answer(question: str, candidates: list[RetrievalCandidate]) -> str | None:
    settings = get_settings()
    chat_api_key = settings.resolved_openai_chat_api_key
    if not chat_api_key:
        return None

    try:
        from openai import OpenAI

        client = OpenAI(api_key=chat_api_key)
        context_blocks = []
        for i, c in enumerate(candidates[:8], start=1):
            context_blocks.append(
                f"[C{i}] title={c.title or '-'} url={c.url or '-'} doc_url={c.doc_url or '-'}\n{c.text[:1200]}"
            )

        prompt = (
            "You are Suisse Bid Match, a bidder-side tender matching copilot. Answer using only the provided evidence. "
            "If data is insufficient, say what is missing. Keep the answer concise and actionable.\n\n"
            f"Question:\n{question}\n\nEvidence:\n" + "\n\n".join(context_blocks)
        )

        resp = _chat_completion_with_compat(
            client,
            model=settings.openai_chat_model,
            messages=[
                {"role": "system", "content": "Return grounded answers from evidence only."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.warning("LLM answer generation failed; fallback to extractive summary. err=%s", exc)
        return None


def run_chat(db, request: ChatRequest) -> ChatResponse:
    settings = get_settings()
    timings: dict[str, float] = {}

    t0 = time.perf_counter()
    plan = build_plan(request.question, request.filters.model_dump() if request.filters else None)
    timings["plan_ms"] = (time.perf_counter() - t0) * 1000

    planned_queries = plan.get("retrieval_queries", [request.question])
    deduped_queries: list[str] = []
    seen_queries: set[str] = set()
    for query in planned_queries:
        q = str(query).strip()
        if not q or q in seen_queries:
            continue
        seen_queries.add(q)
        deduped_queries.append(q)

    if not deduped_queries:
        deduped_queries = [request.question]

    queries = deduped_queries[: settings.chat_max_retrieval_rounds]
    retrieved_map: dict[str, RetrievalCandidate] = {}
    retrieval_stats = []

    t1 = time.perf_counter()
    for q in queries:
        round_candidates, stats = retrieve_hybrid(db, q, filters=request.filters)
        retrieval_stats.append(stats)
        for c in round_candidates:
            prev = retrieved_map.get(c.chunk_id)
            if prev is None or c.final_score > prev.final_score:
                retrieved_map[c.chunk_id] = c
    timings["retrieve_ms"] = (time.perf_counter() - t1) * 1000

    t2 = time.perf_counter()
    top_k = request.top_k or settings.default_top_k
    reranked = rerank_candidates(
        question=request.question,
        candidates=list(retrieved_map.values()),
        filters=request.filters,
        top_k=max(top_k, settings.llm_match_pool_size),
    )
    timings["rerank_ms"] = (time.perf_counter() - t2) * 1000

    candidate_pool = reranked[: max(top_k, settings.llm_match_pool_size, 8)]

    t_llm_match = time.perf_counter()
    llm_meta_by_chunk_id: dict[str, dict[str, Any]] = {}
    final_context, llm_meta_by_chunk_id = _llm_select_matches(
        question=request.question,
        candidates=candidate_pool,
        top_k=top_k,
    )
    timings["llm_match_ms"] = (time.perf_counter() - t_llm_match) * 1000

    answer_context: list[RetrievalCandidate] = list(final_context)
    if len(answer_context) < max(top_k, 8):
        for candidate in candidate_pool:
            if candidate in answer_context:
                continue
            answer_context.append(candidate)
            if len(answer_context) >= max(top_k, 8):
                break

    t3 = time.perf_counter()
    answer = _llm_answer(request.question, answer_context)
    if not answer:
        answer = _extractive_summary(request.question, answer_context)
    timings["answer_ms"] = (time.perf_counter() - t3) * 1000

    citations = _build_citations(
        request.question,
        final_context,
        llm_meta_by_chunk_id=llm_meta_by_chunk_id,
    )
    insufficient = len(citations) < 3

    debug_obj = None
    if request.debug or settings.enable_debug_chat:
        debug_obj = ChatDebug(
            plan=plan,
            queries=queries,
            timings={k: round(v, 2) for k, v in timings.items()},
            retrieval_stats={
                "rounds": retrieval_stats,
                "merged_candidates": len(retrieved_map),
                "llm_matching": {
                    "enabled": settings.llm_matching_enabled,
                    "pool_size": len(candidate_pool),
                    "selected": len(final_context),
                    "with_reason_count": len(llm_meta_by_chunk_id),
                },
            },
        )

    logger.info(
        "chat_complete question_len=%s citations=%s merged_candidates=%s timings_ms=%s",
        len(request.question),
        len(citations),
        len(retrieved_map),
        {k: round(v, 2) for k, v in timings.items()},
    )

    return ChatResponse(
        answer=answer,
        citations=citations,
        used_filters=request.filters.model_dump(exclude_none=True) if request.filters else {},
        citation_count_insufficient=insufficient,
        debug=debug_obj,
    )
