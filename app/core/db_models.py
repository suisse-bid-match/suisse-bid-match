from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class PackageRecord(Base):
    __tablename__ = "packages"

    package_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_name: Mapped[str] = mapped_column(String(512), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    document_count: Mapped[int] = mapped_column(Integer, nullable=False)
    field_count: Mapped[int] = mapped_column(Integer, nullable=False)

    files: Mapped[list["PackageFileRecord"]] = relationship(
        back_populates="package", cascade="all, delete-orphan"
    )
    fields: Mapped[list["ExtractedFieldRecord"]] = relationship(
        back_populates="package", cascade="all, delete-orphan"
    )


class PackageFileRecord(Base):
    __tablename__ = "package_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    package_id: Mapped[str] = mapped_column(ForeignKey("packages.package_id"), index=True)
    doc_id: Mapped[str | None] = mapped_column(String(1024))
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    blob_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    package: Mapped["PackageRecord"] = relationship(back_populates="files")


class ExtractedFieldRecord(Base):
    __tablename__ = "extracted_fields"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    package_id: Mapped[str] = mapped_column(ForeignKey("packages.package_id"), index=True)
    field_id: Mapped[str] = mapped_column(String(1024), nullable=False)
    doc_id: Mapped[str] = mapped_column(String(1024), nullable=False)
    semantic_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    location: Mapped[str] = mapped_column(String(1024), nullable=False)
    critical: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    amount_related: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    package: Mapped["PackageRecord"] = relationship(back_populates="fields")


class MatchRunRecord(Base):
    __tablename__ = "match_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    package_id: Mapped[str] = mapped_column(ForeignKey("packages.package_id"), index=True)
    domain: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    blocked: Mapped[bool] = mapped_column(Boolean, nullable=False)
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False)

    package: Mapped["PackageRecord"] = relationship()
    candidates: Mapped[list["MatchCandidateRecord"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    audits: Mapped[list["MatchAuditRecord"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class MatchCandidateRecord(Base):
    __tablename__ = "match_candidates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("match_runs.run_id"), index=True)
    product_id: Mapped[str] = mapped_column(String(256), nullable=False)
    product_name: Mapped[str] = mapped_column(Text, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    hard_passed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    run: Mapped["MatchRunRecord"] = relationship(back_populates="candidates")


class MatchAuditRecord(Base):
    __tablename__ = "match_audits"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("match_runs.run_id"), index=True)
    step: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    run: Mapped["MatchRunRecord"] = relationship(back_populates="audits")
