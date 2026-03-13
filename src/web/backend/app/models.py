from __future__ import annotations

from datetime import datetime, timezone
import enum
import uuid

from sqlalchemy import JSON, DateTime, Enum, ForeignKey, Index, Integer, BigInteger, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base



def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobStatus(str, enum.Enum):
    created = "created"
    uploading = "uploading"
    ready = "ready"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"


class RuleStatus(str, enum.Enum):
    draft = "draft"
    published = "published"
    archived = "archived"


class RuleSource(str, enum.Enum):
    manual = "manual"
    llm = "llm"
    seed = "seed"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.created, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    runtime_dir: Mapped[str | None] = mapped_column(Text, nullable=True)
    final_output_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_version_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("rule_versions.id"), nullable=True)

    files: Mapped[list["JobFile"]] = relationship("JobFile", back_populates="job", cascade="all, delete-orphan")
    steps: Mapped[list["JobStep"]] = relationship("JobStep", back_populates="job", cascade="all, delete-orphan")
    events: Mapped[list["JobEvent"]] = relationship("JobEvent", back_populates="job", cascade="all, delete-orphan")


class JobFile(Base):
    __tablename__ = "job_files"
    __table_args__ = (UniqueConstraint("job_id", "relative_path", name="uq_job_file_path"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    relative_path: Mapped[str] = mapped_column(Text, nullable=False)
    stored_path: Mapped[str] = mapped_column(Text, nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    extension: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)

    job: Mapped["Job"] = relationship("Job", back_populates="files")


class JobStep(Base):
    __tablename__ = "job_steps"
    __table_args__ = (UniqueConstraint("job_id", "step_name", name="uq_job_step_name"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    step_name: Mapped[str] = mapped_column(String(128), nullable=False)
    step_status: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)

    job: Mapped["Job"] = relationship("Job", back_populates="steps")


class JobEvent(Base):
    __tablename__ = "job_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)

    job: Mapped["Job"] = relationship("Job", back_populates="events")


class RuleVersion(Base):
    __tablename__ = "rule_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    version_number: Mapped[int] = mapped_column(Integer, nullable=False, unique=True, index=True)
    status: Mapped[RuleStatus] = mapped_column(Enum(RuleStatus), default=RuleStatus.draft, nullable=False)
    source: Mapped[RuleSource] = mapped_column(Enum(RuleSource), default=RuleSource.manual, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    validation_report: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    copilot_log: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


Index(
    "ix_rule_versions_single_published",
    RuleVersion.status,
    unique=True,
    postgresql_where=text("status = 'published'"),
)


class AppSetting(Base):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[dict | str | int | float | bool | list | None] = mapped_column(JSON, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)
