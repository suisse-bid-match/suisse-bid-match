from __future__ import annotations

import logging
import re
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.api.core.config import get_settings
from apps.api.models.db import Chunk, DocumentRef, TenderNotice
from apps.api.models.schemas import ChecklistResponse, ChecklistStructured, Citation

logger = logging.getLogger(__name__)


KEYWORDS = {
    "eligibility": ["must", "required", "eligibility", "qualification", "experience"],
    "required_documents": ["submit", "annex", "certificate", "financial statement", "declaration", "document"],
    "key_dates": ["deadline", "opening", "question period", "publication", "submission"],
    "scoring_criteria": ["evaluation", "weight", "criteria", "award", "points"],
}


def _sentences(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text or "") if s.strip()]


def _extract_by_keywords(sentences: Iterable[str], keywords: list[str], limit: int = 8) -> list[str]:
    out = []
    for s in sentences:
        sl = s.lower()
        if any(k in sl for k in keywords):
            out.append(s)
        if len(out) >= limit:
            break
    return out


def _supports_temperature(model: str) -> bool:
    return not str(model or "").lower().startswith("gpt-5")


def _chat_completion_with_compat(client, *, model: str, messages: list[dict[str, str]], temperature: float | None = None):
    kwargs: dict[str, object] = {"model": model, "messages": messages}
    if temperature is not None and _supports_temperature(model):
        kwargs["temperature"] = temperature

    try:
        return client.chat.completions.create(**kwargs)
    except Exception as exc:
        if "temperature" in kwargs and "temperature" in str(exc).lower() and "unsupported" in str(exc).lower():
            kwargs.pop("temperature", None)
            return client.chat.completions.create(**kwargs)
        raise


def _llm_refine(structured: ChecklistStructured, summary: str) -> tuple[ChecklistStructured, str]:
    settings = get_settings()
    chat_api_key = settings.resolved_openai_chat_api_key
    if not chat_api_key:
        return structured, summary

    try:
        from openai import OpenAI

        client = OpenAI(api_key=chat_api_key)
        payload = {
            "eligibility": structured.eligibility,
            "required_documents": structured.required_documents,
            "key_dates": structured.key_dates,
            "scoring_criteria": structured.scoring_criteria,
            "risks": structured.risks,
        }
        prompt = (
            "Clean and deduplicate this tender checklist JSON. "
            "Keep it factual, avoid inventing missing values, and return concise bullet-style strings.\n\n"
            f"Input JSON:\n{payload}\n\nDraft summary:\n{summary}"
        )
        resp = _chat_completion_with_compat(
            client,
            model=settings.openai_chat_model,
            messages=[
                {"role": "system", "content": "Output plain text only."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
        )
        refined_summary = (resp.choices[0].message.content or "").strip() or summary
        return structured, refined_summary
    except Exception as exc:
        logger.warning("Checklist LLM refinement failed: %s", exc)
        return structured, summary


def generate_checklist(db: Session, notice_id: str) -> ChecklistResponse:
    notice = db.scalar(select(TenderNotice).where(TenderNotice.id == notice_id))
    if not notice:
        raise ValueError("Notice not found")

    docs = list(db.scalars(select(DocumentRef).where(DocumentRef.notice_id == notice_id)).all())
    chunks = list(db.scalars(select(Chunk).where(Chunk.notice_id == notice_id).order_by(Chunk.chunk_index)).all())

    corpus = []
    if notice.description:
        corpus.append(notice.description)
    corpus.extend([d.text for d in docs if d.text])
    corpus.extend([c.text for c in chunks if c.text])

    merged_text = "\n".join(corpus)
    sentence_pool = _sentences(merged_text)

    structured = ChecklistStructured(
        eligibility=_extract_by_keywords(sentence_pool, KEYWORDS["eligibility"]),
        required_documents=_extract_by_keywords(sentence_pool, KEYWORDS["required_documents"]),
        key_dates=_extract_by_keywords(sentence_pool, KEYWORDS["key_dates"]),
        scoring_criteria=_extract_by_keywords(sentence_pool, KEYWORDS["scoring_criteria"]),
        risks=[],
    )

    if notice.deadline_date:
        structured.risks.append(f"Submission deadline: {notice.deadline_date.isoformat()}")
    if not structured.required_documents:
        structured.risks.append("Required submission documents were not clearly detected in available text.")
    if not structured.scoring_criteria:
        structured.risks.append("Evaluation criteria were not explicitly found.")

    summary = (
        f"Notice '{notice.title or notice.source_id}' checklist extracted. "
        f"Eligibility items: {len(structured.eligibility)}, required documents: {len(structured.required_documents)}, "
        f"key dates: {len(structured.key_dates)}, scoring criteria: {len(structured.scoring_criteria)}."
    )

    structured, summary = _llm_refine(structured, summary)

    evidence: list[Citation] = []
    for c in chunks[:5]:
        evidence.append(
            Citation(
                title=notice.title,
                url=notice.url,
                doc_url=(c.metadata_json or {}).get("doc_url"),
                snippet=(c.text[:300] + "...") if len(c.text) > 300 else c.text,
                score=1.0,
                notice_id=notice.id,
            )
        )

    return ChecklistResponse(
        notice_id=notice.id,
        structured=structured,
        summary=summary,
        evidence_citations=evidence,
    )
