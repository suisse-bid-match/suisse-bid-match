from __future__ import annotations

import re
import unicodedata
from collections import defaultdict

from apps.api.models.schemas import ChatFilters
from apps.api.services.retrieval.hybrid import RetrievalCandidate


KEY_TERMS = {
    "deadline": ["deadline", "due date", "submission", "closing"],
    "requirements": ["requirement", "must", "mandatory", "eligibility", "qualification"],
    "documents": ["document", "annex", "certificate", "statement", "submit"],
    "evaluation": ["evaluation", "criteria", "weight", "award"],
}

QUERY_STOPWORDS = {
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
    "days",
    "day",
    "please",
    "tender",
    "tenders",
    "service",
    "services",
    "deadline",
    "key",
    "requirements",
    "requirement",
}


def _contains_any(text: str, terms: list[str]) -> bool:
    txt = text.lower()
    return any(term in txt for term in terms)


def _fold_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _has_word(text: str, term: str) -> bool:
    folded_text = _fold_text(text)
    folded_term = _fold_text(term)
    return re.search(rf"\b{re.escape(folded_term)}\b", folded_text, flags=re.IGNORECASE) is not None


def _candidate_combined_text(candidate: RetrievalCandidate) -> str:
    text = candidate.text.lower()
    title = (candidate.title or "").lower()
    buyer = str((candidate.metadata or {}).get("buyer_name") or "").lower()
    region = str((candidate.metadata or {}).get("region") or "").lower()
    return " ".join([text, title, buyer, region])


def rerank_candidates(
    question: str,
    candidates: list[RetrievalCandidate],
    filters: ChatFilters | None,
    top_k: int,
) -> list[RetrievalCandidate]:
    q = question.lower()
    query_tokens = [t for t in re.findall(r"[a-zA-ZÀ-ÿ0-9_-]+", q) if len(t) >= 3]
    salient_tokens = [t for t in query_tokens if t not in QUERY_STOPWORDS]
    proper_nouns = [t.lower() for t in re.findall(r"\b[A-Z][A-Za-zÀ-ÿ-]{2,}\b", question)]
    proper_nouns = [t for t in proper_nouns if t not in QUERY_STOPWORDS]

    for c in candidates:
        boost = 0.0
        text = c.text.lower()
        combined = _candidate_combined_text(c)

        for term_group in KEY_TERMS.values():
            if _contains_any(q, term_group) and _contains_any(text, term_group):
                boost += 0.08

        if salient_tokens:
            token_hits = sum(1 for t in salient_tokens if _has_word(combined, t))
            boost += min(0.02 * token_hits, 0.12)

        if proper_nouns:
            for term in proper_nouns:
                if _has_word(combined, term):
                    boost += 0.2
                else:
                    boost -= 0.08

        if filters:
            if filters.buyer and c.metadata and c.metadata.get("buyer_name"):
                if filters.buyer.lower() in str(c.metadata.get("buyer_name", "")).lower():
                    boost += 0.05
            if filters.language and c.metadata and c.metadata.get("language"):
                if filters.language.lower() == str(c.metadata.get("language")).lower():
                    boost += 0.04
            if filters.canton and c.metadata and c.metadata.get("region"):
                if filters.canton.lower() in str(c.metadata.get("region")).lower():
                    boost += 0.04

        # Slight penalty for very short chunks.
        text_len = len(re.findall(r"\w+", c.text))
        if text_len < 40:
            boost -= 0.03

        c.final_score += boost

    candidates.sort(key=lambda x: x.final_score, reverse=True)

    # If the query includes explicit proper nouns (e.g. Zurich), prioritize candidates
    # that contain those terms in text/title/buyer/region before general matches.
    if proper_nouns:
        matched: list[RetrievalCandidate] = []
        non_matched: list[RetrievalCandidate] = []
        for c in candidates:
            combined = _candidate_combined_text(c)
            if any(_has_word(combined, term) for term in proper_nouns):
                matched.append(c)
            else:
                non_matched.append(c)
        if matched:
            candidates = matched + non_matched

    # Keep best chunk per notice first, then fill remaining slots with next best chunks.
    by_notice: dict[str, list[RetrievalCandidate]] = defaultdict(list)
    for c in candidates:
        by_notice[c.notice_id].append(c)

    selected: list[RetrievalCandidate] = []
    for notice_id in by_notice:
        selected.append(by_notice[notice_id][0])
        if len(selected) >= top_k:
            break

    if len(selected) < top_k:
        for c in candidates:
            if c not in selected:
                selected.append(c)
            if len(selected) >= top_k:
                break

    return selected
