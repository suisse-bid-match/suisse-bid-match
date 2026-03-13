from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Job, JobStatus, JobStep
from app.services.stats import build_stats_dashboard


def _session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    testing_session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return testing_session()


def _payload_with_fields(*fields: str) -> dict:
    return {
        "data": {
            "tender_products": [
                {
                    "product_key": "item_001",
                    "requirements": [{"field": field} for field in fields],
                }
            ]
        }
    }


def test_stats_dashboard_aggregates_duration_counts_and_field_frequency() -> None:
    db = _session()
    base = datetime.now(timezone.utc) - timedelta(days=1)

    job = Job(
        id="job-001",
        status=JobStatus.succeeded,
        created_at=base,
        updated_at=base + timedelta(minutes=10),
        started_at=base + timedelta(minutes=1),
        finished_at=base + timedelta(minutes=3),
    )
    db.add(job)
    db.flush()

    db.add_all(
        [
            JobStep(
                id=1,
                job_id=job.id,
                step_name="step2_extract_requirements",
                step_status="ok",
                payload=_payload_with_fields(
                    "vw_bid_specs.ip_rating",
                    "vw_bid_specs.ip_rating",
                    "vw_bid_specs.ik_rating",
                ),
                updated_at=base + timedelta(minutes=2),
            ),
            JobStep(
                id=2,
                job_id=job.id,
                step_name="step3_external_field_rules",
                step_status="ok",
                payload={"elapsed_ms": 120},
                updated_at=base + timedelta(minutes=2, seconds=10),
            ),
            JobStep(
                id=3,
                job_id=job.id,
                step_name="step4_merge_requirements_hardness",
                step_status="ok",
                payload={"data": {"elapsed_ms": 230}},
                updated_at=base + timedelta(minutes=2, seconds=20),
            ),
            JobStep(
                id=4,
                job_id=job.id,
                step_name="step5_build_sql",
                step_status="ok",
                payload={"data": {}},
                updated_at=base + timedelta(minutes=2, seconds=50),
            ),
            JobStep(
                id=5,
                job_id=job.id,
                step_name="step6_execute_sql",
                step_status="ok",
                payload={
                    "data": {
                        "results": [
                            {"elapsed_ms": 11},
                            {"elapsed_ms": 19},
                        ]
                    }
                },
                updated_at=base + timedelta(minutes=2, seconds=55),
            ),
        ]
    )
    db.commit()

    stats = build_stats_dashboard(db, days=30, include_failed=True, top_n=40)

    assert stats.overview.job_count == 1
    assert stats.overview.succeeded_count == 1
    assert stats.overview.failed_count == 0
    assert stats.overview.avg_job_duration_ms == 120000.0
    assert stats.overview.p50_job_duration_ms == 120000
    assert stats.overview.p90_job_duration_ms == 120000
    assert stats.overview.avg_extracted_products == 1.0

    assert len(stats.job_durations) == 1
    assert stats.job_durations[0].job_id == "job-001"
    assert stats.job_durations[0].duration_ms == 120000
    assert stats.job_durations[0].extracted_products == 1

    step_rows = {row.step_name: row for row in stats.step_durations}
    assert step_rows["step3_external_field_rules"].avg_duration_ms == 120.0
    assert step_rows["step4_merge_requirements_hardness"].avg_duration_ms == 230.0
    assert step_rows["step5_build_sql"].avg_duration_ms == 30000.0
    assert step_rows["step6_execute_sql"].avg_duration_ms == 30.0

    assert stats.field_frequency[0].field == "vw_bid_specs.ip_rating"
    assert stats.field_frequency[0].count == 2
    assert stats.field_frequency[1].field == "vw_bid_specs.ik_rating"
    assert stats.field_frequency[1].count == 1


def test_stats_dashboard_filters_failed_jobs_when_requested() -> None:
    db = _session()
    base = datetime.now(timezone.utc) - timedelta(days=2)

    db.add_all(
        [
            Job(
                id="job-ok",
                status=JobStatus.succeeded,
                created_at=base,
                updated_at=base + timedelta(minutes=5),
                started_at=base + timedelta(minutes=1),
                finished_at=base + timedelta(minutes=3),
            ),
            Job(
                id="job-failed",
                status=JobStatus.failed,
                created_at=base,
                updated_at=base + timedelta(minutes=6),
                started_at=base + timedelta(minutes=2),
                finished_at=base + timedelta(minutes=4),
            ),
        ]
    )
    db.commit()

    stats_with_failed = build_stats_dashboard(db, days=30, include_failed=True, top_n=40)
    stats_without_failed = build_stats_dashboard(db, days=30, include_failed=False, top_n=40)

    assert stats_with_failed.overview.job_count == 2
    assert stats_with_failed.overview.failed_count == 1
    assert stats_without_failed.overview.job_count == 1
    assert stats_without_failed.overview.failed_count == 0
    assert [row.job_id for row in stats_without_failed.job_durations] == ["job-ok"]
