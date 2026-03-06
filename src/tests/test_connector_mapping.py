from apps.api.services.connectors.simap_api import SimapApiConnector


def test_normalize_publication_maps_core_fields():
    raw = {
        "id": "abc-123",
        "title": "IT Services Framework",
        "description": "Submission deadline is 2026-04-01.",
        "buyer": {"name": "City of Zurich", "location": "Zurich"},
        "cpvCodes": ["72000000", "72200000"],
        "publicationDate": "2026-03-01T10:00:00Z",
        "deadlineDate": "2026-04-01T12:00:00Z",
        "language": "en",
        "documents": [{"url": "https://example.com/a.pdf", "filename": "spec.pdf", "mimeType": "application/pdf"}],
    }
    normalized = SimapApiConnector.normalize_publication(raw)
    assert normalized["source"] == "simap"
    assert normalized["source_id"] == "abc-123"
    assert normalized["buyer_name"] == "City of Zurich"
    assert "72000000" in normalized["cpv_codes"]
    assert normalized["documents"][0]["url"] == "https://example.com/a.pdf"


def test_normalize_publication_handles_localized_payload_fields():
    raw = {
        "id": "project-1",
        "publicationId": "publication-1",
        "language": "en",
        "title": {"de": None, "en": "Network Cockpit", "fr": "Cockpit reseau"},
        "procurement": {"orderDescription": {"en": "<p>Detailed scope</p>"}},
        "project-info": {"procOfficeAddress": {"name": {"en": "SBB AG"}}},
    }
    normalized = SimapApiConnector.normalize_publication(raw)
    assert normalized["source_id"] == "publication-1"
    assert normalized["project_id"] == "project-1"
    assert normalized["title"] == "Network Cockpit"
    assert normalized["description"] == "<p>Detailed scope</p>"
    assert normalized["buyer_name"] == "SBB AG"


def test_list_publications_uses_quick_filter_and_rolls_pagination(monkeypatch):
    connector = SimapApiConnector()
    calls = []
    payloads = [
        {"projects": [{"id": "project-1", "publicationId": "publication-1"}], "pagination": {"lastItem": "20260201|100"}},
        {"projects": [{"id": "project-2", "publicationId": "publication-2"}], "pagination": {"lastItem": ""}},
    ]

    def fake_request(method, path, params=None):
        calls.append((method, path, dict(params or {})))
        return payloads[len(calls) - 1]

    monkeypatch.setattr(connector, "_request", fake_request)
    out = connector.list_publications(updated_since="2026-02-03T21:10:52.803707+00:00", limit=2)

    assert [x["publicationId"] for x in out] == ["publication-1", "publication-2"]
    assert calls[0][2]["newestPublicationFrom"] == "2026-02-03"
    assert "lastItem" not in calls[0][2]
    assert calls[1][2]["lastItem"] == "20260201|100"
    assert "limit" not in calls[0][2]
    assert "size" not in calls[0][2]
    assert "updatedSince" not in calls[0][2]
    assert "updated_since" not in calls[0][2]
    assert "lang" not in calls[0][2]
    assert "language" not in calls[0][2]


def test_list_publications_without_date_still_sends_required_quick_filter(monkeypatch):
    connector = SimapApiConnector()
    call_params = []

    def fake_request(method, path, params=None):
        call_params.append(dict(params or {}))
        return {"projects": [], "pagination": {"lastItem": ""}}

    monkeypatch.setattr(connector, "_request", fake_request)
    connector.list_publications(updated_since=None, limit=5)

    assert call_params
    assert "newestPublicationFrom" in call_params[0]
    assert len(call_params[0]["newestPublicationFrom"]) == 10
