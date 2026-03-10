from __future__ import annotations

from app.core.models import TenderRequirement
from app.match.metadata import load_domain_metadata
from app.match.schema_mapper import map_requirements_to_schema


def test_mapping_maps_canonical_key() -> None:
    meta = load_domain_metadata("lighting")
    mapped, unmet = map_requirements_to_schema(
        [
            TenderRequirement(
                requirement_id="req_001",
                param_key="lumen",
                operator="gte",
                value=9000,
                is_hard=True,
                confidence=0.95,
                evidence_refs=["chunk:1"],
                raw_text="Leuchtenlichtstrom mindestens 9000 lm",
            )
        ],
        meta,
    )

    assert unmet == []
    assert mapped[0].status == "mapped"
    assert mapped[0].mapped_field == "match_specs.lumen_output_max"


def test_mapping_blocks_unknown_canonical_key() -> None:
    meta = load_domain_metadata("lighting")
    mapped, unmet = map_requirements_to_schema(
        [
            TenderRequirement(
                requirement_id="req_001",
                param_key="unknown_param",
                operator="eq",
                value="foo",
                is_hard=True,
                confidence=0.95,
                evidence_refs=["chunk:1"],
            )
        ],
        meta,
    )

    assert mapped[0].status == "unmapped"
    assert "canonical key" in mapped[0].reason.lower()
    assert len(unmet) == 1
