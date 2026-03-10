from __future__ import annotations

from pathlib import Path

import pytest

from app.ingestion.package_ingest import ingest_from_source


def _uster_source() -> Path:
    return Path(
        os.getenv(
            "USTER_SOURCE_PATH",
            "/home/daz/all_things_for_genai_hackathon/real_tenders/20260220_Upload_simap_BKP_233-20260306T200217Z-3-001/20260220_Upload_simap_BKP_233",
        )
    )


@pytest.mark.skipif(not _uster_source().exists(), reason="Uster source package not found")
def test_uster_ingestion_extracts_docs_fields_pdf() -> None:
    index = ingest_from_source(_uster_source())

    assert len(index.documents) == 7
    assert sum(1 for d in index.documents if d.kind == "docx") == 5
    assert sum(1 for d in index.documents if d.kind == "xlsx") == 1
    assert sum(1 for d in index.documents if d.kind == "pdf") == 1

    assert len(index.fields) == 0

    assert index.pdf_insight is not None
    assert len(index.pdf_insight.sections) > 0
    assert len(index.pdf_insight.criteria_lines) > 0

    assert index.reference_chunks is not None
