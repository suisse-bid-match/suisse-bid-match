from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import delete

from app.core.blob_storage import get_blob_storage
from app.core.database import init_db, session_scope
from app.core.db_models import (
    ExtractedFieldRecord,
    MatchAuditRecord,
    MatchCandidateRecord,
    MatchRunRecord,
    PackageFileRecord,
    PackageRecord,
)
from app.core.models import DocumentInfo, utcnow

from .models import CompanyProfile, MatchRun, PackageIndex
from .settings import settings


def ensure_runtime_layout() -> None:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.match_data_dir.mkdir(parents=True, exist_ok=True)
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    settings.packages_dir.mkdir(parents=True, exist_ok=True)
    settings.runs_dir.mkdir(parents=True, exist_ok=True)
    settings.matches_dir.mkdir(parents=True, exist_ok=True)
    settings.blob_dir.mkdir(parents=True, exist_ok=True)
    if not settings.profile_path.exists() and settings.seed_profile_path.exists():
        shutil.copy2(settings.seed_profile_path, settings.profile_path)
    init_db()
    get_blob_storage().ensure_ready()


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def package_index_blob_key(package_id: str) -> str:
    return f"packages/{package_id}/index.json"


def package_doc_blob_key(package_id: str, file_name: str) -> str:
    return f"packages/{package_id}/source/{file_name}"


def match_blob_key(run_id: str) -> str:
    return f"matches/{run_id}/run.json"


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def load_profile() -> CompanyProfile:
    ensure_runtime_layout()
    data = _load_json(settings.profile_path)
    return CompanyProfile.model_validate(data)


def save_profile(profile: CompanyProfile) -> CompanyProfile:
    ensure_runtime_layout()
    _save_json(settings.profile_path, profile.model_dump(mode="json"))
    return profile


def new_package_dir() -> tuple[str, Path]:
    package_id = str(uuid.uuid4())
    package_dir = settings.packages_dir / package_id
    package_dir.mkdir(parents=True, exist_ok=True)
    return package_id, package_dir


def package_index_path(package_id: str) -> Path:
    return settings.packages_dir / package_id / "index.json"


def save_package_index(index: PackageIndex) -> PackageIndex:
    ensure_runtime_layout()
    index_path = package_index_path(index.package_id)
    _save_json(index_path, index.model_dump(mode="json"))

    blob = get_blob_storage()
    blob.put_file(index_path, package_index_blob_key(index.package_id))

    package_root = Path(index.root_dir)
    created_at = utcnow()
    with session_scope() as session:
        session.merge(
            PackageRecord(
                package_id=index.package_id,
                source_name=index.source_name,
                created_at=index.created_at,
                document_count=len(index.documents),
                field_count=len(index.fields),
            )
        )
        session.execute(delete(PackageFileRecord).where(PackageFileRecord.package_id == index.package_id))
        session.execute(delete(ExtractedFieldRecord).where(ExtractedFieldRecord.package_id == index.package_id))

        for doc in index.documents:
            local_file = package_root / doc.relative_path
            blob_key = package_doc_blob_key(index.package_id, doc.name)
            blob.put_file(local_file, blob_key)
            session.add(
                PackageFileRecord(
                    package_id=index.package_id,
                    doc_id=doc.doc_id,
                    name=doc.name,
                    kind=doc.kind,
                    size_bytes=local_file.stat().st_size,
                    sha256=_sha256(local_file),
                    blob_key=blob_key,
                    created_at=created_at,
                )
            )

        for field in index.fields:
            session.add(
                ExtractedFieldRecord(
                    package_id=index.package_id,
                    field_id=field.field_id,
                    doc_id=field.doc_id,
                    semantic_key=field.semantic_key,
                    label=field.label,
                    location=field.location,
                    critical=field.critical,
                    required=field.required,
                    amount_related=field.amount_related,
                    source_kind=field.source_kind,
                    created_at=created_at,
                )
            )

    return index


def load_package_index(package_id: str) -> PackageIndex:
    ensure_runtime_layout()
    index_path = package_index_path(package_id)
    if not index_path.exists():
        get_blob_storage().get_file(package_index_blob_key(package_id), index_path)
    data = _load_json(index_path)
    return PackageIndex.model_validate(data)


def new_match_dir(run_id: str) -> Path:
    match_dir = settings.matches_dir / run_id
    match_dir.mkdir(parents=True, exist_ok=True)
    return match_dir


def match_json_path(run_id: str) -> Path:
    return settings.matches_dir / run_id / "run.json"


def save_match_run(run: MatchRun) -> MatchRun:
    ensure_runtime_layout()
    run_json = match_json_path(run.run_id)
    _save_json(run_json, run.model_dump(mode="json"))
    blob = get_blob_storage()
    blob.put_file(run_json, match_blob_key(run.run_id))

    with session_scope() as session:
        session.merge(
            MatchRunRecord(
                run_id=run.run_id,
                package_id=run.package_id,
                domain=run.domain,
                created_at=run.created_at,
                blocked=run.blocked,
                candidate_count=len(run.candidates),
            )
        )
        session.execute(delete(MatchCandidateRecord).where(MatchCandidateRecord.run_id == run.run_id))
        session.execute(delete(MatchAuditRecord).where(MatchAuditRecord.run_id == run.run_id))

        for item in run.candidates:
            session.add(
                MatchCandidateRecord(
                    run_id=run.run_id,
                    product_id=item.product_id,
                    product_name=item.product_name,
                    score=item.score,
                    hard_passed=item.hard_passed,
                )
            )
        for event in run.audit_trail:
            session.add(
                MatchAuditRecord(
                    run_id=run.run_id,
                    step=event.step,
                    status=event.status,
                    summary=event.summary,
                    created_at=event.finished_at,
                )
            )

    return run


def load_match_run(run_id: str) -> MatchRun:
    ensure_runtime_layout()
    json_path = match_json_path(run_id)
    if not json_path.exists():
        get_blob_storage().get_file(match_blob_key(run_id), json_path)
    data = _load_json(json_path)
    return MatchRun.model_validate(data)


def ensure_local_package_document(index: PackageIndex, doc: DocumentInfo) -> Path:
    local_path = Path(index.root_dir) / doc.relative_path
    if local_path.exists():
        return local_path
    get_blob_storage().get_file(package_doc_blob_key(index.package_id, doc.name), local_path)
    return local_path

