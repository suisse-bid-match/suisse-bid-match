from __future__ import annotations

from app.core.models import TenderRequirement
from app.match.metadata import load_domain_metadata
from app.match.schema_mapper import map_requirements_to_schema


def test_schema_mapper_blocks_out_of_range_hard_requirement() -> None:
    meta = load_domain_metadata("lighting")
    requirements = [
        TenderRequirement(
            requirement_id="req_001",
            param_key="cri",
            operator="gte",
            value=500,
            is_hard=True,
            evidence_refs=["chunk:1"],
            confidence=0.9,
        )
    ]

    mapped, unmet = map_requirements_to_schema(requirements, meta)

    assert mapped[0].status == "unmapped"
    assert "guardrail" in mapped[0].reason.lower()
    assert len(unmet) == 1


def test_schema_mapper_blocks_low_confidence_hard_requirement() -> None:
    meta = load_domain_metadata("lighting")
    requirements = [
        TenderRequirement(
            requirement_id="req_001",
            param_key="lumen",
            operator="gte",
            value=9000,
            is_hard=True,
            evidence_refs=["chunk:1"],
            confidence=0.2,
        )
    ]

    mapped, unmet = map_requirements_to_schema(requirements, meta)

    assert mapped[0].status == "unmapped"
    assert "confidence" in mapped[0].reason.lower()
    assert len(unmet) == 1
