from __future__ import annotations

from app.core.settings import settings
from app.match.context import ContextLine
from app.match.metadata import load_domain_metadata
from app.match.requirement_extractor import extract_requirements


def test_requirement_extraction_requires_llm(monkeypatch) -> None:
    monkeypatch.setattr(settings, "openai_api_key", "")
    meta = load_domain_metadata("lighting")
    lines = [
        ContextLine(text="功率至少 0.2 kW，必须满足", evidence_ref="chunk:1", doc_id="doc_1"),
        ContextLine(text="建议 UGR <= 19", evidence_ref="chunk:2", doc_id="doc_1"),
        ContextLine(text="CE certified is required", evidence_ref="chunk:3", doc_id="doc_2"),
    ]

    result = extract_requirements(
        package_id="pkg_1",
        domain="lighting",
        meta=meta,
        context_lines=lines,
    )

    assert result is None
