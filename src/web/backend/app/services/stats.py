from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from math import ceil, floor
from statistics import mean
from typing import Any

from sqlalchemy.orm import Session

from ..models import JobStatus
from ..repositories.jobs import JobRepository
from ..schemas import (
    FieldFrequencyStatRow,
    JobDurationStatRow,
    StatsDashboardResponse,
    StatsOverviewResponse,
    StepDurationStatRow,
)


STEP_ORDER = [
    "schema_snapshot",
    "step1_kb_bootstrap",
    "step2_extract_requirements",
    "step3_external_field_rules",
    "step4_merge_requirements_hardness",
    "step5_build_sql",
    "step6_execute_sql",
    "step7_rank_candidates",
]
STEP_INDEX = {name: index for index, name in enumerate(STEP_ORDER)}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc(input_value: datetime | None) -> datetime | None:
    if input_value is None:
        return None
    if input_value.tzinfo is None:
        return input_value.replace(tzinfo=timezone.utc)
    return input_value.astimezone(timezone.utc)


def _as_record(input_value: Any) -> dict[str, Any] | None:
    if isinstance(input_value, dict):
        return input_value
    return None


def _as_list(input_value: Any) -> list[Any]:
    if isinstance(input_value, list):
        return input_value
    return []


def _as_number(input_value: Any) -> float | None:
    if isinstance(input_value, bool):
        return None
    if isinstance(input_value, (int, float)):
        return float(input_value)
    return None


def _as_non_empty_string(input_value: Any) -> str | None:
    if not isinstance(input_value, str):
        return None
    text = input_value.strip()
    return text if text else None


def _duration_ms(started_at: datetime | None, finished_at: datetime | None) -> int | None:
    normalized_started_at = _to_utc(started_at)
    normalized_finished_at = _to_utc(finished_at)
    if normalized_started_at is None or normalized_finished_at is None:
        return None
    duration = int((normalized_finished_at - normalized_started_at).total_seconds() * 1000)
    return max(duration, 0)


def _percentile(values: list[int], ratio: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * ratio
    lower_index = floor(index)
    upper_index = ceil(index)
    lower = ordered[lower_index]
    upper = ordered[upper_index]
    if lower_index == upper_index:
        return lower
    interpolated = lower + (upper - lower) * (index - lower_index)
    return int(round(interpolated))


def _extract_step_duration_ms(
    *,
    step_name: str,
    payload: dict[str, Any],
    updated_at: datetime | None,
    previous_checkpoint: datetime | None,
    job_started_at: datetime | None,
) -> int | None:
    direct_elapsed = _as_number(payload.get("elapsed_ms"))
    if direct_elapsed is not None:
        return max(int(round(direct_elapsed)), 0)

    data = _as_record(payload.get("data")) or {}
    nested_elapsed = _as_number(data.get("elapsed_ms"))
    if nested_elapsed is not None:
        return max(int(round(nested_elapsed)), 0)

    if step_name == "step6_execute_sql":
        total_elapsed = 0.0
        has_value = False
        for row in _as_list(data.get("results")):
            record = _as_record(row) or {}
            value = _as_number(record.get("elapsed_ms"))
            if value is None:
                continue
            total_elapsed += value
            has_value = True
        if has_value:
            return max(int(round(total_elapsed)), 0)

    normalized_updated_at = _to_utc(updated_at)
    normalized_checkpoint = _to_utc(previous_checkpoint) or _to_utc(job_started_at)
    if normalized_updated_at is None or normalized_checkpoint is None:
        return None
    fallback_elapsed = int((normalized_updated_at - normalized_checkpoint).total_seconds() * 1000)
    return max(fallback_elapsed, 0)


def _extract_step2_metrics(payload: dict[str, Any]) -> tuple[int | None, Counter[str]]:
    data = _as_record(payload.get("data")) or {}
    tender_products = _as_list(data.get("tender_products"))
    if not tender_products:
        return 0, Counter()

    field_counter: Counter[str] = Counter()
    for product in tender_products:
        product_record = _as_record(product) or {}
        requirements = _as_list(product_record.get("requirements"))
        for requirement in requirements:
            requirement_record = _as_record(requirement) or {}
            field_name = _as_non_empty_string(requirement_record.get("field"))
            if field_name:
                field_counter[field_name] += 1

    return len(tender_products), field_counter


def build_stats_dashboard(
    db: Session,
    *,
    days: int,
    include_failed: bool,
    top_n: int,
) -> StatsDashboardResponse:
    now = utc_now()
    window_from = now - timedelta(days=days)
    repo = JobRepository(db)
    jobs = repo.list_jobs_for_stats(updated_from=window_from, updated_to=now, include_failed=include_failed)
    job_ids = [row.id for row in jobs]
    steps_by_job = repo.list_job_steps_bulk(job_ids)

    job_duration_values: list[int] = []
    extracted_values: list[int] = []
    step_durations: dict[str, list[int]] = {}
    field_counter: Counter[str] = Counter()
    job_rows: list[JobDurationStatRow] = []

    for job in jobs:
        job_duration = _duration_ms(job.started_at, job.finished_at)
        if job_duration is not None:
            job_duration_values.append(job_duration)

        extracted_products: int | None = None
        previous_checkpoint = job.started_at
        for step in steps_by_job.get(job.id, []):
            payload = _as_record(step.payload) or {}
            step_elapsed = _extract_step_duration_ms(
                step_name=step.step_name,
                payload=payload,
                updated_at=step.updated_at,
                previous_checkpoint=previous_checkpoint,
                job_started_at=job.started_at,
            )
            if step_elapsed is not None:
                step_durations.setdefault(step.step_name, []).append(step_elapsed)

            if step.step_name == "step2_extract_requirements":
                step2_products, step2_counter = _extract_step2_metrics(payload)
                extracted_products = step2_products
                field_counter.update(step2_counter)

            if step.updated_at is not None:
                previous_checkpoint = step.updated_at

        if extracted_products is not None:
            extracted_values.append(extracted_products)

        job_rows.append(
            JobDurationStatRow(
                job_id=job.id,
                status=job.status,
                updated_at=job.updated_at,
                started_at=job.started_at,
                finished_at=job.finished_at,
                duration_ms=job_duration,
                extracted_products=extracted_products,
            )
        )

    def _step_sort_key(item: tuple[str, list[int]]) -> tuple[int, str]:
        step_name = item[0]
        return (STEP_INDEX.get(step_name, 10_000), step_name)

    step_rows = [
        StepDurationStatRow(
            step_name=step_name,
            sample_count=len(values),
            avg_duration_ms=float(mean(values)) if values else None,
            p50_duration_ms=_percentile(values, 0.5),
            p90_duration_ms=_percentile(values, 0.9),
        )
        for step_name, values in sorted(step_durations.items(), key=_step_sort_key)
        if values
    ]

    field_rows = [
        FieldFrequencyStatRow(field=field, count=count)
        for field, count in sorted(field_counter.items(), key=lambda row: (-row[1], row[0]))[:top_n]
    ]

    overview = StatsOverviewResponse(
        window_from=window_from,
        window_to=now,
        job_count=len(jobs),
        succeeded_count=sum(1 for row in jobs if row.status == JobStatus.succeeded),
        failed_count=sum(1 for row in jobs if row.status == JobStatus.failed),
        avg_job_duration_ms=float(mean(job_duration_values)) if job_duration_values else None,
        p50_job_duration_ms=_percentile(job_duration_values, 0.5),
        p90_job_duration_ms=_percentile(job_duration_values, 0.9),
        avg_extracted_products=float(mean(extracted_values)) if extracted_values else None,
    )

    return StatsDashboardResponse(
        overview=overview,
        job_durations=job_rows,
        step_durations=step_rows,
        field_frequency=field_rows,
    )
