from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_autofill_routes_are_removed() -> None:
    client = TestClient(app)

    resp = client.post("/api/autofill/run", json={"package_id": "dummy"})
    assert resp.status_code == 404
