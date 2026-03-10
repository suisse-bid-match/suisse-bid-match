from __future__ import annotations

from app.main import (
    INGEST_JOB_QUEUE_TIMEOUT_SEC,
    INGEST_JOB_STALL_TIMEOUT_SEC,
    _mark_stale_ingest_job_failed,
)


def test_mark_stale_ingest_job_failed_for_queued_timeout() -> None:
    now = 10_000.0
    job = {
        "status": "queued",
        "created_at": now - INGEST_JOB_QUEUE_TIMEOUT_SEC - 1,
        "updated_at": now - INGEST_JOB_QUEUE_TIMEOUT_SEC - 1,
        "started_at": None,
        "finished_at": None,
        "error": None,
    }

    _mark_stale_ingest_job_failed(job, now)

    assert job["status"] == "failed"
    assert "queue timeout" in str(job["error"])
    assert job["finished_at"] == now


def test_mark_stale_ingest_job_failed_for_processing_stall() -> None:
    now = 20_000.0
    job = {
        "status": "processing",
        "created_at": now - 300,
        "updated_at": now - INGEST_JOB_STALL_TIMEOUT_SEC - 1,
        "started_at": now - 300,
        "finished_at": None,
        "error": None,
    }

    _mark_stale_ingest_job_failed(job, now)

    assert job["status"] == "failed"
    assert "stalled" in str(job["error"])
    assert job["finished_at"] == now
