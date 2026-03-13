from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Generator

from fastapi import FastAPI
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.api.stats import router as stats_router
from app.db import Base, get_db
from app.models import Job, JobStatus

pytest.importorskip("httpx")
from fastapi.testclient import TestClient


def _build_client() -> tuple[TestClient, sessionmaker]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    testing_session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    app = FastAPI()
    app.include_router(stats_router, prefix="/api/v1")

    def _override_get_db() -> Generator[Session, None, None]:
        db = testing_session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = _override_get_db
    return TestClient(app), testing_session


def test_stats_dashboard_query_validation() -> None:
    client, _ = _build_client()
    assert client.get("/api/v1/stats/dashboard?days=0").status_code == 422
    assert client.get("/api/v1/stats/dashboard?top_n=0").status_code == 422
    assert client.get("/api/v1/stats/dashboard?days=366").status_code == 422


def test_stats_dashboard_returns_payload_shape() -> None:
    client, testing_session = _build_client()
    with testing_session() as db:
        base = datetime.now(timezone.utc) - timedelta(hours=1)
        db.add(
            Job(
                id="job-200",
                status=JobStatus.succeeded,
                created_at=base,
                updated_at=base + timedelta(minutes=20),
                started_at=base + timedelta(minutes=1),
                finished_at=base + timedelta(minutes=3),
            )
        )
        db.commit()

    response = client.get("/api/v1/stats/dashboard")
    assert response.status_code == 200
    payload = response.json()
    assert "overview" in payload
    assert "job_durations" in payload
    assert "step_durations" in payload
    assert "field_frequency" in payload
    assert payload["overview"]["job_count"] == 1
