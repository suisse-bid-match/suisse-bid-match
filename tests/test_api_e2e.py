from __future__ import annotations

import mimetypes
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app


def _uster_source() -> Path:
    return Path(
        os.getenv(
            "USTER_SOURCE_PATH",
            "/home/daz/all_things_for_genai_hackathon/real_tenders/20260220_Upload_simap_BKP_233-20260306T200217Z-3-001/20260220_Upload_simap_BKP_233",
        )
    )


@pytest.mark.skipif(not _uster_source().exists(), reason="Uster source package not found")
def test_api_end_to_end_via_route_functions() -> None:
    client = TestClient(app)
    files_payload: list[tuple[str, tuple[str, object, str]]] = []
    handles = []

    for source_file in sorted(_uster_source().iterdir()):
        if not source_file.is_file():
            continue
        mime = mimetypes.guess_type(source_file.name)[0] or "application/octet-stream"
        fh = source_file.open("rb")
        handles.append(fh)
        files_payload.append(("files", (source_file.name, fh, mime)))

    try:
        ingest_resp = client.post("/api/packages/ingest", files=files_payload)
    finally:
        for fh in handles:
            fh.close()

    assert ingest_resp.status_code == 200
    ingest = ingest_resp.json()
    assert ingest["document_count"] == 7

    fields_resp = client.get(f"/api/packages/{ingest['package_id']}/fields")
    assert fields_resp.status_code == 200
    fields = fields_resp.json()
    assert fields["fields"] == []
