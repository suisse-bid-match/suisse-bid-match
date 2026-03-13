from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Job, JobStatus, RuleSource, RuleStatus, RuleVersion
from app.repositories.jobs import JobRepository
from app.repositories.rules import RuleRepository


def _session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    testing_session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return testing_session()


def test_list_jobs_filters_and_sorts_by_updated_desc() -> None:
    db = _session()
    base_time = datetime(2026, 3, 13, 10, 0, tzinfo=timezone.utc)
    db.add_all(
        [
            Job(id="job-alpha-001", status=JobStatus.ready, created_at=base_time, updated_at=base_time),
            Job(
                id="job-beta-002",
                status=JobStatus.failed,
                created_at=base_time + timedelta(minutes=1),
                updated_at=base_time + timedelta(minutes=9),
            ),
            Job(
                id="job-gamma-003",
                status=JobStatus.ready,
                created_at=base_time + timedelta(minutes=2),
                updated_at=base_time + timedelta(minutes=5),
            ),
        ]
    )
    db.commit()

    repo = JobRepository(db)
    rows = repo.list_jobs(limit=10, offset=0)
    assert [row.id for row in rows] == ["job-beta-002", "job-gamma-003", "job-alpha-001"]

    ready_rows = repo.list_jobs(status=JobStatus.ready, limit=10, offset=0)
    assert [row.id for row in ready_rows] == ["job-gamma-003", "job-alpha-001"]

    query_rows = repo.list_jobs(query="beta", limit=10, offset=0)
    assert [row.id for row in query_rows] == ["job-beta-002"]

    ranged_rows = repo.list_jobs(
        updated_from=base_time + timedelta(minutes=4),
        updated_to=base_time + timedelta(minutes=6),
        limit=10,
        offset=0,
    )
    assert [row.id for row in ranged_rows] == ["job-gamma-003"]


def test_list_rule_versions_filters_and_paginates() -> None:
    db = _session()
    payload = {"field_rules": []}
    report = {"valid": True}

    db.add_all(
        [
            RuleVersion(
                id="rule-001",
                version_number=1,
                status=RuleStatus.archived,
                source=RuleSource.seed,
                payload=payload,
                validation_report=report,
                note="legacy seed",
            ),
            RuleVersion(
                id="rule-002",
                version_number=2,
                status=RuleStatus.published,
                source=RuleSource.manual,
                payload=payload,
                validation_report=report,
                note="manual publish",
            ),
            RuleVersion(
                id="rule-003",
                version_number=3,
                status=RuleStatus.draft,
                source=RuleSource.llm,
                payload=payload,
                validation_report=report,
                note="llm candidate",
            ),
        ]
    )
    db.commit()

    repo = RuleRepository(db)
    paged = repo.list_versions(limit=2, offset=0)
    assert [row.id for row in paged] == ["rule-003", "rule-002"]

    status_filtered = repo.list_versions(status=RuleStatus.published, limit=10, offset=0)
    assert [row.id for row in status_filtered] == ["rule-002"]

    source_filtered = repo.list_versions(source=RuleSource.llm, limit=10, offset=0)
    assert [row.id for row in source_filtered] == ["rule-003"]

    query_by_note = repo.list_versions(query="manual", limit=10, offset=0)
    assert [row.id for row in query_by_note] == ["rule-002"]

    query_by_id = repo.list_versions(query="rule-001", limit=10, offset=0)
    assert [row.id for row in query_by_id] == ["rule-001"]
