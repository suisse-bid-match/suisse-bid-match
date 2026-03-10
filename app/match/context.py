from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.models import PackageIndex


@dataclass
class ContextLine:
    text: str
    evidence_ref: str
    doc_id: str

def build_context_lines(
    index: PackageIndex,
    max_lines: int | None,
    doc_ids: set[str] | None = None,
    *,
    dedupe: bool = True,
) -> list[ContextLine]:
    lines: list[ContextLine] = []
    seen: set[str] = set()

    def _push(text: str, evidence_ref: str, doc_id: str) -> bool:
        cleaned = text.strip()
        if not cleaned:
            return False
        if dedupe:
            key = re.sub(r"\s+", " ", cleaned).lower()
            if key in seen:
                return False
            seen.add(key)
        lines.append(
            ContextLine(
                text=cleaned,
                evidence_ref=evidence_ref,
                doc_id=doc_id,
            )
        )
        return True

    for docling_doc in index.docling_documents:
        if doc_ids is not None and docling_doc.doc_id not in doc_ids:
            continue
        for line in docling_doc.lines:
            _push(line.text, line.evidence_ref, docling_doc.doc_id)
            if max_lines is not None and len(lines) >= max_lines:
                return lines

    return lines
