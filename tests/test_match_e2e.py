from __future__ import annotations

import time
from pathlib import Path

from sqlalchemy import text

from app.core.blob_storage import get_blob_storage
from app.core.database import get_engine, get_pim_engine, get_session_factory
from app.core.models import (
    DoclingDocument,
    DoclingLine,
    DocumentClassification,
    DocumentInfo,
    PackageIndex,
    ReferenceChunk,
    RequirementSet,
    SQLPlan,
    TenderRequirement,
    utcnow,
)
from app.core.settings import settings
from app.core.storage import ensure_runtime_layout, save_package_index
from app.main import MatchRunRequest, api_get_match_audit, api_get_match_run, api_get_match_job, api_run_match
from app.match.doc_classifier import LLMDocumentClassifier
from app.match.metadata import load_domain_metadata
from app.match.orchestrator import run_match
from app.match.requirement_extractor import LLMRequirementExtractor
from app.match.sql_generator import LLMSQLGenerator


def _configure_isolated_runtime(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    data_dir = tmp_path / "data"
    packages_dir = runtime_dir / "packages"
    runs_dir = runtime_dir / "runs"
    matches_dir = runtime_dir / "matches"
    blob_dir = runtime_dir / "blob"
    data_dir.mkdir(parents=True, exist_ok=True)

    settings.runtime_dir = runtime_dir
    settings.data_dir = data_dir
    settings.packages_dir = packages_dir
    settings.runs_dir = runs_dir
    settings.matches_dir = matches_dir
    settings.blob_dir = blob_dir
    settings.profile_path = data_dir / "profile.json"
    settings.seed_profile_path = Path("/tmp/non-existent-seed-profile.json")
    settings.database_url = f"sqlite:///{(runtime_dir / 'app.db').as_posix()}"
    settings.pim_database_url = f"sqlite:///{(runtime_dir / 'pim.db').as_posix()}"
    settings.pim_assets_root = "/pim-assets"
    settings.match_data_dir = Path(__file__).resolve().parents[1] / "data" / "match"

    get_engine.cache_clear()
    get_pim_engine.cache_clear()
    get_session_factory.cache_clear()
    get_blob_storage.cache_clear()
    load_domain_metadata.cache_clear()
    ensure_runtime_layout()


def _prepare_product_db() -> None:
    engine = get_pim_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS match_products (
                    product_id TEXT PRIMARY KEY,
                    product_name TEXT NOT NULL,
                    article_number TEXT,
                    manufacturer_name TEXT,
                    retailer_name TEXT,
                    light_category_de TEXT,
                    light_family_de TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS match_specs (
                    product_id TEXT PRIMARY KEY,
                    electrical_power_w REAL,
                    lumen_output_max REAL,
                    ip_rating REAL,
                    ik_rating REAL,
                    ugr REAL,
                    cri REAL,
                    color_temp_k_max REAL,
                    controls_dali BOOLEAN,
                    controls_bluetooth BOOLEAN,
                    controls_matter BOOLEAN,
                    emergency_light BOOLEAN,
                    runtime_hours REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS match_certs (
                    product_id TEXT PRIMARY KEY,
                    ce BOOLEAN,
                    enec BOOLEAN,
                    icon_tags TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS match_assets (
                    product_id TEXT PRIMARY KEY,
                    datasheet_path TEXT,
                    datasheet_url TEXT,
                    mounting_instruction_path TEXT,
                    image_path TEXT,
                    asset_languages TEXT
                )
                """
            )
        )

        conn.execute(text("DELETE FROM match_products"))
        conn.execute(text("DELETE FROM match_specs"))
        conn.execute(text("DELETE FROM match_certs"))
        conn.execute(text("DELETE FROM match_assets"))

        conn.execute(
            text(
                """
                INSERT INTO match_products
                  (product_id, product_name, article_number, manufacturer_name, retailer_name, light_category_de, light_family_de)
                VALUES
                  ('P-001', 'Alpha Pro', 'A-001', 'Maker A', 'Retailer A', 'Anbau-Downlight', 'Family X'),
                  ('P-002', 'Beta Eco', 'B-001', 'Maker B', 'Retailer B', 'Anbau-Downlight', 'Family Y')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO match_specs
                  (product_id, electrical_power_w, lumen_output_max, ip_rating, ik_rating, ugr, cri, color_temp_k_max,
                   controls_dali, controls_bluetooth, controls_matter, emergency_light, runtime_hours)
                VALUES
                  ('P-001', 220, 12000, 65, 8, 18, 90, 4000, 1, 0, 0, 1, 50000),
                  ('P-002', 180, 9800, 54, 6, 22, 80, 3000, 0, 1, 0, 0, 42000)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO match_certs (product_id, ce, enec, icon_tags)
                VALUES
                  ('P-001', 1, 1, 'CE | ENEC'),
                  ('P-002', 1, 0, 'CE')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO match_assets
                  (product_id, datasheet_path, datasheet_url, mounting_instruction_path, image_path, asset_languages)
                VALUES
                  ('P-001', 'datasheets/p1_en.pdf', NULL, 'mounting-instructions/p1.pdf', 'images/p1.jpg', 'en,de'),
                  ('P-002', 'datasheets/p2_en.pdf', NULL, 'mounting-instructions/p2.pdf', 'images/p2.jpg', 'en')
                """
            )
        )


def _prepare_package_with_requirements() -> str:
    from app.core.storage import new_package_dir

    package_id, package_dir = new_package_dir()
    source_file = package_dir / "source" / "requirements.pdf"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_bytes(b"fake")

    index = PackageIndex(
        package_id=package_id,
        created_at=utcnow(),
        source_name="test_pkg",
        root_dir=str(package_dir),
        documents=[
            DocumentInfo(
                doc_id="doc_01",
                name="requirements.pdf",
                relative_path=str(source_file.relative_to(package_dir)),
                kind="pdf",
                role="REFERENCE_ONLY",
            )
        ],
        fields=[],
        pdf_insight=None,
        reference_chunks=[
            ReferenceChunk(
                chunk_id="doc_01:chunk:0001",
                doc_id="doc_01",
                doc_name="requirements.pdf",
                section_path="page:1",
                page_or_anchor="1",
                text="Lumens must be at least 11000 lm.",
                tokens=7,
            ),
            ReferenceChunk(
                chunk_id="doc_01:chunk:0002",
                doc_id="doc_01",
                doc_name="requirements.pdf",
                section_path="page:1",
                page_or_anchor="1",
                text="IP rating must be at least IP65.",
                tokens=6,
            ),
            ReferenceChunk(
                chunk_id="doc_01:chunk:0003",
                doc_id="doc_01",
                doc_name="requirements.pdf",
                section_path="page:1",
                page_or_anchor="1",
                text="CE certified is required.",
                tokens=4,
            ),
        ],
        docling_documents=[
            DoclingDocument(
                doc_id="doc_01",
                used=True,
                error=None,
                lines=[
                    DoclingLine(
                        evidence_ref="doc_01:docling:0001",
                        text="Lumens must be at least 11000 lm.",
                    ),
                    DoclingLine(
                        evidence_ref="doc_01:docling:0002",
                        text="IP rating must be at least IP65.",
                    ),
                    DoclingLine(
                        evidence_ref="doc_01:docling:0003",
                        text="CE certified is required.",
                    ),
                ],
            )
        ],
    )
    save_package_index(index)
    return package_id


def _stub_classify(self, *, doc, docling_lines):
    return DocumentClassification(
        doc_id=doc.doc_id,
        doc_name=doc.name,
        is_application_form=True,
        confidence=0.9,
        reason="stub",
        evidence_refs=[docling_lines[0].evidence_ref],
        parse_failed=False,
    )


def _stub_extract(self, *, package_id, domain, meta, context_lines):
    requirements = [
        TenderRequirement(
            requirement_id="req_001",
            param_key="lumen",
            operator="gte",
            value=11000,
            unit="lm",
            is_hard=True,
            evidence_refs=["doc_01:docling:0001"],
            confidence=0.9,
        ),
        TenderRequirement(
            requirement_id="req_002",
            param_key="ip_grade",
            operator="gte",
            value=65,
            unit=None,
            is_hard=True,
            evidence_refs=["doc_01:docling:0002"],
            confidence=0.9,
        ),
        TenderRequirement(
            requirement_id="req_003",
            param_key="ce",
            operator="bool_true",
            value=True,
            unit=None,
            is_hard=True,
            evidence_refs=["doc_01:docling:0003"],
            confidence=0.9,
        ),
    ]
    return RequirementSet(
        package_id=package_id,
        domain=domain,
        requirements=requirements,
        generated_at=utcnow(),
    )


def _stub_generate(self, *, domain, requirements, schema_metadata, required_fields, top_k):
    meta = load_domain_metadata(domain)
    sql = meta.sql_template.format(hard_filters="", soft_filters="")
    hard_count = sum(1 for item in requirements if item.is_hard)
    soft_count = sum(1 for item in requirements if not item.is_hard)
    return SQLPlan(
        domain=domain,
        sql=sql,
        params={"limit": top_k},
        hard_clause_count=hard_count,
        soft_clause_count=soft_count,
        limit=top_k,
        validated=False,
        validation_errors=[],
        blocked=False,
    )


def test_match_pipeline_end_to_end(tmp_path: Path, monkeypatch) -> None:
    _configure_isolated_runtime(tmp_path)
    _prepare_product_db()
    package_id = _prepare_package_with_requirements()

    monkeypatch.setattr(LLMDocumentClassifier, "classify", _stub_classify)
    monkeypatch.setattr(LLMRequirementExtractor, "extract", _stub_extract)
    monkeypatch.setattr(LLMSQLGenerator, "generate", _stub_generate)

    run = run_match(package_id=package_id, domain="lighting", top_k=5, strict_hard_constraints=True)

    assert run.blocked is False
    assert run.sql_plan.validated is True
    assert run.candidates
    assert run.candidates[0].product_id == "P-001"
    assert run.candidates[0].hard_passed is True
    assert run.candidates[0].row["datasheet_path"] == "/pim-assets/datasheets/p1_en.pdf"
    assert all(item.step for item in run.audit_trail)


def test_match_api_route_functions(tmp_path: Path, monkeypatch) -> None:
    _configure_isolated_runtime(tmp_path)
    _prepare_product_db()
    package_id = _prepare_package_with_requirements()

    monkeypatch.setattr(LLMDocumentClassifier, "classify", _stub_classify)
    monkeypatch.setattr(LLMRequirementExtractor, "extract", _stub_extract)
    monkeypatch.setattr(LLMSQLGenerator, "generate", _stub_generate)

    payload = MatchRunRequest(
        package_id=package_id,
        domain="lighting",
        top_k=5,
        strict_hard_constraints=True,
    )
    run_response = api_run_match(payload)
    run_payload = run_response.model_dump(mode="json")
    job_id = run_payload["job_id"]

    final = None
    for _ in range(60):
        job = api_get_match_job(job_id)
        if job["status"] in {"completed", "failed"}:
            final = job
            break
        time.sleep(0.05)
    assert final is not None
    assert final["status"] == "completed"

    run_id = final["run_id"]
    details = api_get_match_run(run_id)
    assert "requirements" in details
    assert "sql_executed" in details
    assert len(details["candidates"]) >= 1

    audit = api_get_match_audit(run_id)
    assert isinstance(audit["audit_trail"], list)
    assert len(audit["audit_trail"]) > 0
