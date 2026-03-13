from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import Job, JobEvent, JobFile, JobStatus, JobStep



def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobRepository:
    def __init__(self, db: Session):
        self.db = db

    def create_job(self) -> Job:
        job = Job(status=JobStatus.created)
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        return job

    def get_job(self, job_id: str) -> Job | None:
        return self.db.get(Job, job_id)

    def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        query: str | None = None,
        updated_from: datetime | None = None,
        updated_to: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        stmt = select(Job)
        if status is not None:
            stmt = stmt.where(Job.status == status)
        if query:
            normalized = query.strip()
            if normalized:
                stmt = stmt.where(Job.id.ilike(f"%{normalized}%"))
        if updated_from is not None:
            stmt = stmt.where(Job.updated_at >= updated_from)
        if updated_to is not None:
            stmt = stmt.where(Job.updated_at <= updated_to)
        stmt = stmt.order_by(Job.updated_at.desc()).offset(max(offset, 0)).limit(max(limit, 1))
        return list(self.db.scalars(stmt).all())

    def list_jobs_for_stats(
        self,
        *,
        updated_from: datetime,
        updated_to: datetime,
        include_failed: bool,
    ) -> list[Job]:
        stmt = select(Job).where(Job.updated_at >= updated_from, Job.updated_at <= updated_to)
        if not include_failed:
            stmt = stmt.where(Job.status != JobStatus.failed)
        stmt = stmt.order_by(Job.updated_at.desc())
        return list(self.db.scalars(stmt).all())

    def list_job_files(self, job_id: str) -> list[JobFile]:
        stmt = select(JobFile).where(JobFile.job_id == job_id).order_by(JobFile.created_at.asc())
        return list(self.db.scalars(stmt).all())

    def list_job_steps(self, job_id: str) -> list[JobStep]:
        stmt = select(JobStep).where(JobStep.job_id == job_id).order_by(JobStep.updated_at.asc())
        return list(self.db.scalars(stmt).all())

    def list_job_steps_bulk(self, job_ids: list[str]) -> dict[str, list[JobStep]]:
        if not job_ids:
            return {}
        stmt = (
            select(JobStep)
            .where(JobStep.job_id.in_(job_ids))
            .order_by(JobStep.job_id.asc(), JobStep.updated_at.asc())
        )
        rows = list(self.db.scalars(stmt).all())
        grouped: dict[str, list[JobStep]] = {}
        for row in rows:
            grouped.setdefault(row.job_id, []).append(row)
        return grouped

    def count_job_files(self, job_id: str) -> int:
        return len(self.list_job_files(job_id))

    def count_job_files_bulk(self, job_ids: list[str]) -> dict[str, int]:
        if not job_ids:
            return {}
        stmt = (
            select(JobFile.job_id, func.count(JobFile.id))
            .where(JobFile.job_id.in_(job_ids))
            .group_by(JobFile.job_id)
        )
        return {job_id: int(count) for job_id, count in self.db.execute(stmt).all()}

    def count_job_steps_bulk(self, job_ids: list[str]) -> dict[str, int]:
        if not job_ids:
            return {}
        stmt = (
            select(JobStep.job_id, func.count(JobStep.id))
            .where(JobStep.job_id.in_(job_ids))
            .group_by(JobStep.job_id)
        )
        return {job_id: int(count) for job_id, count in self.db.execute(stmt).all()}

    def add_job_file(self, *, job_id: str, relative_path: str, stored_path: str, size_bytes: int, extension: str) -> JobFile:
        stmt = select(JobFile).where(JobFile.job_id == job_id, JobFile.relative_path == relative_path)
        row = self.db.scalars(stmt).first()
        if row is None:
            row = JobFile(
                job_id=job_id,
                relative_path=relative_path,
                stored_path=stored_path,
                size_bytes=size_bytes,
                extension=extension,
            )
        else:
            row.stored_path = stored_path
            row.size_bytes = size_bytes
            row.extension = extension
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    def set_job_status(
        self,
        job: Job,
        status: JobStatus,
        *,
        error_message: str | None = None,
        runtime_dir: str | None = None,
        final_output_path: str | None = None,
        rule_version_id: str | None = None,
        started: bool = False,
        finished: bool = False,
    ) -> Job:
        job.status = status
        job.updated_at = utc_now()
        if error_message is not None:
            job.error_message = error_message
        if runtime_dir is not None:
            job.runtime_dir = runtime_dir
        if final_output_path is not None:
            job.final_output_path = final_output_path
        if rule_version_id is not None:
            job.rule_version_id = rule_version_id
        if started:
            job.started_at = utc_now()
        if finished:
            job.finished_at = utc_now()
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        return job

    def append_event(self, *, job_id: str, event_type: str, payload: dict) -> JobEvent:
        event = JobEvent(job_id=job_id, event_type=event_type, payload=payload)
        self.db.add(event)
        self.db.commit()
        self.db.refresh(event)
        return event

    def events_after(self, *, job_id: str, last_event_id: int, limit: int = 100) -> list[JobEvent]:
        stmt = (
            select(JobEvent)
            .where(JobEvent.job_id == job_id, JobEvent.id > last_event_id)
            .order_by(JobEvent.id.asc())
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def upsert_step(self, *, job_id: str, step_name: str, step_status: str, payload: dict) -> JobStep:
        stmt = select(JobStep).where(JobStep.job_id == job_id, JobStep.step_name == step_name)
        row = self.db.scalars(stmt).first()
        if row is None:
            row = JobStep(job_id=job_id, step_name=step_name, step_status=step_status, payload=payload)
        else:
            row.step_status = step_status
            row.payload = payload
            row.updated_at = utc_now()
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row
