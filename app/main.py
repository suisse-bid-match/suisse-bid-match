from __future__ import annotations

import shutil
import tempfile
import threading
import time
import uuid
from pathlib import Path, PurePosixPath

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.core.models import APIIngestResponse, CompanyProfile
from app.core.settings import settings
from app.core.storage import (
    ensure_runtime_layout,
    load_match_run,
    load_package_index,
    load_profile,
    save_profile,
)
from app.match.metadata import load_domain_metadata, validate_all_domain_metadata
from app.match.orchestrator import run_match
from app.ingestion.package_ingest import IngestError, ingest_from_source

UPLOAD_CHUNK_BYTES = 4 * 1024 * 1024
INGEST_JOB_RETENTION_SEC = 60 * 60
INGEST_JOB_QUEUE_TIMEOUT_SEC = 2 * 60
INGEST_JOB_STALL_TIMEOUT_SEC = 15 * 60
INGEST_JOB_MAX_RUNTIME_SEC = 45 * 60
MATCH_JOB_RETENTION_SEC = 60 * 60
MATCH_JOB_QUEUE_TIMEOUT_SEC = 2 * 60
MATCH_JOB_STALL_TIMEOUT_SEC = 15 * 60
MATCH_JOB_MAX_RUNTIME_SEC = 45 * 60


class APIIngestStartResponse(BaseModel):
    job_id: str
    status: str
    uploaded_files: int
    uploaded_bytes: int


class MatchRunRequest(BaseModel):
    package_id: str
    domain: str = Field(default_factory=lambda: settings.match_default_domain)
    top_k: int = Field(default_factory=lambda: settings.match_default_top_k)
    strict_hard_constraints: bool = True


ingest_jobs: dict[str, dict] = {}
ingest_jobs_lock = threading.Lock()
match_jobs: dict[str, dict] = {}
match_jobs_lock = threading.Lock()


class APIMatchJobStartResponse(BaseModel):
    job_id: str
    status: str


app = FastAPI(title="SwissTender Copilot MVP", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _persist_upload(upload: UploadFile, target_dir: Path) -> Path:
    raw_name = upload.filename or "upload.bin"
    relative = _safe_upload_relative_path(raw_name)
    out_path = (target_dir / relative).resolve()
    target_root = target_dir.resolve()
    if target_root not in out_path.parents and out_path != target_root:
        raise IngestError(f"invalid upload path: {raw_name}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        stem = out_path.stem
        suffix = out_path.suffix
        idx = 1
        while True:
            candidate = out_path.with_name(f"{stem}_{idx}{suffix}")
            if not candidate.exists():
                out_path = candidate
                break
            idx += 1
    with out_path.open("wb") as out:
        while True:
            chunk = await upload.read(UPLOAD_CHUNK_BYTES)
            if not chunk:
                break
            out.write(chunk)
    await upload.close()
    return out_path


def _safe_upload_relative_path(raw_name: str) -> Path:
    normalized = raw_name.replace("\\", "/")
    candidate = PurePosixPath(normalized)
    if candidate.is_absolute():
        raise IngestError(f"invalid upload path: {raw_name}")

    safe_parts: list[str] = []
    for part in candidate.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            raise IngestError(f"invalid upload path: {raw_name}")
        if ":" in part:
            raise IngestError(f"invalid upload path: {raw_name}")
        safe_parts.append(part)

    if not safe_parts:
        return Path("upload.bin")
    return Path(*safe_parts)


def _prune_old_ingest_jobs() -> None:
    now = time.time()
    with ingest_jobs_lock:
        expired = [
            job_id
            for job_id, job in ingest_jobs.items()
            if now - float(job.get("updated_at", now)) > INGEST_JOB_RETENTION_SEC
        ]
        for job_id in expired:
            ingest_jobs.pop(job_id, None)


def _set_ingest_job(job_id: str, **updates) -> None:
    with ingest_jobs_lock:
        job = ingest_jobs.get(job_id)
        if job is None:
            return
        job.update(updates)
        job["updated_at"] = time.time()


def _create_ingest_job(*, uploaded_files: int, uploaded_bytes: int) -> str:
    _prune_old_ingest_jobs()
    job_id = str(uuid.uuid4())
    now = time.time()
    with ingest_jobs_lock:
        ingest_jobs[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "uploaded_files": uploaded_files,
            "uploaded_bytes": uploaded_bytes,
            "total_files": 0,
            "processed_files": 0,
            "current_file": None,
            "current_relative_path": None,
            "current_kind": None,
            "package_id": None,
            "source_name": None,
            "document_count": None,
            "field_count": None,
            "error": None,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "finished_at": None,
        }
    return job_id


MATCH_STEP_ORDER = [
    "parse_package",
    "classify_documents",
    "extract_requirements",
    "fetch_schema_metadata",
    "map_to_schema",
    "generate_sql",
    "validate_sql",
    "execute_query",
    "rank_and_explain",
    "build_audit",
]


def _prune_old_match_jobs() -> None:
    now = time.time()
    with match_jobs_lock:
        expired = [
            job_id
            for job_id, job in match_jobs.items()
            if now - float(job.get("updated_at", now)) > MATCH_JOB_RETENTION_SEC
        ]
        for job_id in expired:
            match_jobs.pop(job_id, None)


def _set_match_job(job_id: str, **updates) -> None:
    with match_jobs_lock:
        job = match_jobs.get(job_id)
        if job is None:
            return
        job.update(updates)
        job["updated_at"] = time.time()


def _create_match_job(*, payload: MatchRunRequest) -> str:
    _prune_old_match_jobs()
    job_id = str(uuid.uuid4())
    now = time.time()
    steps = [
        {
            "step": step,
            "status": "queued",
            "percent": 0,
            "started_at": None,
            "finished_at": None,
            "message": None,
            "error": None,
        }
        for step in MATCH_STEP_ORDER
    ]
    with match_jobs_lock:
        match_jobs[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "package_id": payload.package_id,
            "domain": payload.domain,
            "top_k": payload.top_k,
            "strict_hard_constraints": payload.strict_hard_constraints,
            "run_id": None,
            "error": None,
            "overall_percent": 0,
            "current_step": None,
            "steps": steps,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "finished_at": None,
        }
    return job_id


def _update_match_job_step(
    job_id: str,
    *,
    step: str,
    status: str,
    percent: int,
    message: str | None = None,
    error: str | None = None,
    started_at: float | None = None,
    finished_at: float | None = None,
) -> None:
    with match_jobs_lock:
        job = match_jobs.get(job_id)
        if job is None:
            return
        steps = job.get("steps") or []
        for row in steps:
            if row.get("step") == step:
                row["status"] = status
                row["percent"] = percent
                if message is not None:
                    row["message"] = message
                if error is not None:
                    row["error"] = error
                if started_at is not None:
                    row["started_at"] = started_at
                if finished_at is not None:
                    row["finished_at"] = finished_at
                break
        total = len(steps) or 1
        overall = round(sum(int(item.get("percent", 0)) for item in steps) / total)
        job["overall_percent"] = overall
        job["current_step"] = next(
            (item.get("step") for item in steps if item.get("status") == "running"),
            None,
        )
        job["updated_at"] = time.time()


def _mark_stale_match_job_failed(job: dict, now: float) -> None:
    status = str(job.get("status") or "")
    if status in {"completed", "failed"}:
        return

    created_at = float(job.get("created_at") or now)
    updated_at = float(job.get("updated_at") or created_at)
    started_at_raw = job.get("started_at")
    started_at = float(started_at_raw) if started_at_raw else None

    if status == "queued" and now - created_at > MATCH_JOB_QUEUE_TIMEOUT_SEC:
        job.update(
            status="failed",
            error="match queue timeout: job did not start in time",
            finished_at=now,
            updated_at=now,
        )
        return

    if status == "processing":
        if started_at is not None and now - started_at > MATCH_JOB_MAX_RUNTIME_SEC:
            job.update(
                status="failed",
                error="match timeout: processing exceeded maximum runtime",
                finished_at=now,
                updated_at=now,
            )
            return
        if now - updated_at > MATCH_JOB_STALL_TIMEOUT_SEC:
            job.update(
                status="failed",
                error="match stalled: no progress updates received",
                finished_at=now,
                updated_at=now,
            )


def _match_progress_callback_factory(job_id: str):
    def _callback(event: str, payload: dict) -> None:
        if event == "step_started":
            _set_match_job(
                job_id,
                status="processing",
                started_at=payload.get("started_at"),
            )
            _update_match_job_step(
                job_id,
                step=payload.get("step", ""),
                status="running",
                percent=0,
                message=payload.get("message"),
                started_at=payload.get("started_at"),
            )
            return
        if event == "step_finished":
            step_status = payload.get("status") or "completed"
            _update_match_job_step(
                job_id,
                step=payload.get("step", ""),
                status=step_status,
                percent=100,
                message=payload.get("message"),
                error=payload.get("error"),
                finished_at=payload.get("finished_at"),
            )
            if step_status == "failed":
                _set_match_job(
                    job_id,
                    status="failed",
                    error=payload.get("error") or "match step failed",
                    finished_at=payload.get("finished_at"),
                )

    return _callback


def _mark_stale_ingest_job_failed(job: dict, now: float) -> None:
    status = str(job.get("status") or "")
    if status in {"completed", "failed"}:
        return

    created_at = float(job.get("created_at") or now)
    updated_at = float(job.get("updated_at") or created_at)
    started_at_raw = job.get("started_at")
    started_at = float(started_at_raw) if started_at_raw else None

    if status == "queued" and now - created_at > INGEST_JOB_QUEUE_TIMEOUT_SEC:
        job.update(
            status="failed",
            error="ingest queue timeout: job did not start in time",
            finished_at=now,
            updated_at=now,
        )
        return

    if status == "processing":
        if started_at is not None and now - started_at > INGEST_JOB_MAX_RUNTIME_SEC:
            job.update(
                status="failed",
                error="ingest timeout: processing exceeded maximum runtime",
                finished_at=now,
                updated_at=now,
            )
            return
        if now - updated_at > INGEST_JOB_STALL_TIMEOUT_SEC:
            job.update(
                status="failed",
                error="ingest stalled: no progress updates received",
                finished_at=now,
                updated_at=now,
            )


def _ingest_progress_callback_factory(job_id: str):
    def _callback(event: str, payload: dict) -> None:
        if event == "prepared":
            _set_ingest_job(
                job_id,
                status="processing",
                total_files=int(payload.get("total_files") or 0),
                processed_files=0,
            )
            return
        if event == "file_started":
            _set_ingest_job(
                job_id,
                status="processing",
                current_file=payload.get("file_name"),
                current_relative_path=payload.get("relative_path"),
                current_kind=payload.get("kind"),
                total_files=int(payload.get("total_files") or 0),
            )
            return
        if event == "file_done":
            _set_ingest_job(
                job_id,
                status="processing",
                processed_files=int(payload.get("index") or 0),
                total_files=int(payload.get("total_files") or 0),
                current_file=payload.get("file_name"),
                current_relative_path=payload.get("relative_path"),
                current_kind=payload.get("kind"),
            )
            return
        if event == "completed":
            _set_ingest_job(
                job_id,
                total_files=int(payload.get("total_files") or 0),
                package_id=payload.get("package_id"),
                document_count=payload.get("document_count"),
                field_count=payload.get("field_count"),
            )

    return _callback


def _run_ingest_job(job_id: str, source_path: Path, staging_root: Path) -> None:
    _set_ingest_job(job_id, status="processing", started_at=time.time())
    try:
        callback = _ingest_progress_callback_factory(job_id)
        index = ingest_from_source(source_path, progress_callback=callback)
        _set_ingest_job(
            job_id,
            status="completed",
            package_id=index.package_id,
            source_name=index.source_name,
            document_count=len(index.documents),
            field_count=len(index.fields),
            processed_files=len(index.documents),
            total_files=len(index.documents),
            finished_at=time.time(),
            current_file=None,
            current_relative_path=None,
            current_kind=None,
        )
    except Exception as exc:
        _set_ingest_job(
            job_id,
            status="failed",
            error=str(exc),
            finished_at=time.time(),
        )
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def _run_match_job(job_id: str, payload: MatchRunRequest) -> None:
    _set_match_job(job_id, status="processing", started_at=time.time())
    try:
        callback = _match_progress_callback_factory(job_id)
        run = run_match(
            package_id=payload.package_id,
            domain=payload.domain,
            top_k=payload.top_k,
            strict_hard_constraints=payload.strict_hard_constraints,
            progress_callback=callback,
        )
        with match_jobs_lock:
            job = match_jobs.get(job_id)
            if job is None:
                return
            if job.get("status") != "failed":
                for step in job.get("steps", []):
                    if step.get("status") == "queued":
                        step.update(
                            status="completed",
                            percent=100,
                            message="skipped",
                            finished_at=time.time(),
                        )
                job.update(
                    status="completed",
                    run_id=run.run_id,
                    overall_percent=100,
                    finished_at=time.time(),
                    current_step=None,
                )
            else:
                job.update(
                    run_id=run.run_id,
                    finished_at=time.time(),
                    current_step=None,
                )
            job["updated_at"] = time.time()
    except Exception as exc:
        _set_match_job(
            job_id,
            status="failed",
            error=str(exc),
            finished_at=time.time(),
        )


@app.on_event("startup")
def _startup() -> None:
    ensure_runtime_layout()
    validation = validate_all_domain_metadata()
    invalid = {name: errs for name, errs in validation.items() if errs}
    if invalid:
        details = "; ".join([f"{name}: {', '.join(errs)}" for name, errs in invalid.items()])
        raise RuntimeError(f"domain metadata validation failed: {details}")
    load_domain_metadata(settings.match_default_domain)


@app.get("/")
def home() -> dict[str, str]:
    return {
        "service": "SwissTender Copilot API",
        "docs": "/docs",
    }


@app.post("/api/packages/ingest", response_model=APIIngestResponse)
async def api_ingest_package(
    files: list[UploadFile] | None = File(default=None),
) -> APIIngestResponse:
    uploaded: list[UploadFile] = []
    if isinstance(files, list):
        uploaded = [f for f in files if f.filename]
    if not uploaded:
        raise HTTPException(
            status_code=400,
            detail="upload files are required",
        )

    try:
        ensure_runtime_layout()
        with tempfile.TemporaryDirectory(prefix="ingest_upload_", dir=settings.runtime_dir) as temp_dir:
            temp_root = Path(temp_dir)
            persisted_files = [await _persist_upload(upload, temp_root) for upload in uploaded]
            if len(persisted_files) == 1 and persisted_files[0].suffix.lower() == ".zip":
                index = ingest_from_source(persisted_files[0])
            else:
                index = ingest_from_source(temp_root)

        return APIIngestResponse(
            package_id=index.package_id,
            source_name=index.source_name,
            document_count=len(index.documents),
            field_count=len(index.fields),
        )
    except IngestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/packages/ingest/start", response_model=APIIngestStartResponse)
async def api_ingest_package_start(
    files: list[UploadFile] | None = File(default=None),
) -> APIIngestStartResponse:
    uploaded: list[UploadFile] = []
    if isinstance(files, list):
        uploaded = [f for f in files if f.filename]
    if not uploaded:
        raise HTTPException(
            status_code=400,
            detail="upload files are required",
        )

    staging_root: Path | None = None
    try:
        ensure_runtime_layout()
        staging_root = Path(tempfile.mkdtemp(prefix="ingest_staging_", dir=settings.runtime_dir))
        upload_root = staging_root / "upload"
        upload_root.mkdir(parents=True, exist_ok=True)

        persisted_files = [await _persist_upload(upload, upload_root) for upload in uploaded]
        uploaded_bytes = sum(path.stat().st_size for path in persisted_files if path.exists())
        source_path = (
            persisted_files[0]
            if len(persisted_files) == 1 and persisted_files[0].suffix.lower() == ".zip"
            else upload_root
        )
        job_id = _create_ingest_job(uploaded_files=len(persisted_files), uploaded_bytes=uploaded_bytes)
        worker = threading.Thread(
            target=_run_ingest_job,
            args=(job_id, source_path, staging_root),
            daemon=True,
            name=f"ingest-job-{job_id[:8]}",
        )
        worker.start()
        return APIIngestStartResponse(
            job_id=job_id,
            status="queued",
            uploaded_files=len(persisted_files),
            uploaded_bytes=uploaded_bytes,
        )
    except IngestError as exc:
        if staging_root is not None:
            shutil.rmtree(staging_root, ignore_errors=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        if staging_root is not None:
            shutil.rmtree(staging_root, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"failed to start ingest job: {exc}") from exc


@app.get("/api/packages/ingest/{job_id}")
async def api_get_ingest_job(job_id: str) -> dict:
    with ingest_jobs_lock:
        job = ingest_jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="ingest job not found")
        _mark_stale_ingest_job_failed(job, time.time())
        return dict(job)


@app.get("/api/packages/{package_id}/fields")
async def api_package_fields(package_id: str) -> dict:
    try:
        index = load_package_index(package_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="package not found") from exc

    role_summary: dict[str, int] = {}
    reference_index_stats: dict[str, int] = {}
    for doc in index.documents:
        role_summary[doc.role] = role_summary.get(doc.role, 0) + 1
    for chunk in index.reference_chunks:
        reference_index_stats[chunk.doc_id] = reference_index_stats.get(chunk.doc_id, 0) + 1

    return {
        "package_id": index.package_id,
        "documents": [d.model_dump(mode="json") for d in index.documents],
        "fields": [f.model_dump(mode="json") for f in index.fields],
        "pdf_insight": index.pdf_insight.model_dump(mode="json") if index.pdf_insight else None,
        "doc_role_summary": role_summary,
        "reference_index_stats": reference_index_stats,
    }


@app.get("/api/profile", response_model=CompanyProfile)
async def api_get_profile() -> CompanyProfile:
    return load_profile()


@app.put("/api/profile", response_model=CompanyProfile)
async def api_put_profile(profile: CompanyProfile) -> CompanyProfile:
    return save_profile(profile)


@app.post("/api/match/run", response_model=APIMatchJobStartResponse)
def api_run_match(payload: MatchRunRequest) -> APIMatchJobStartResponse:
    try:
        job_id = _create_match_job(payload=payload)
        worker = threading.Thread(
            target=_run_match_job,
            args=(job_id, payload),
            daemon=True,
            name=f"match-job-{job_id[:8]}",
        )
        worker.start()
        return APIMatchJobStartResponse(
            job_id=job_id,
            status="queued",
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="package not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"match job start failed: {exc}") from exc


@app.get("/api/match/run/{job_id}")
def api_get_match_job(job_id: str) -> dict:
    with match_jobs_lock:
        job = match_jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="match job not found")
        _mark_stale_match_job_failed(job, time.time())
        return dict(job)


@app.get("/api/match/{run_id}")
def api_get_match_run(run_id: str) -> dict:
    try:
        run = load_match_run(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="match run not found") from exc

    return {
        "run_id": run.run_id,
        "package_id": run.package_id,
        "domain": run.domain,
        "created_at": run.created_at,
        "blocked": run.blocked,
        "top_k": run.top_k,
        "strict_hard_constraints": run.strict_hard_constraints,
        "doc_classifications": [item.model_dump(mode="json") for item in run.doc_classifications],
        "requirements": run.requirements.model_dump(mode="json"),
        "mapped_conditions": [item.model_dump(mode="json") for item in run.mapped_conditions],
        "sql_plan": run.sql_plan.model_dump(mode="json"),
        "sql_executed": run.sql_plan.sql,
        "candidates": [item.model_dump(mode="json") for item in run.candidates],
        "product_results": [item.model_dump(mode="json") for item in run.product_results],
        "unmet_constraints": run.unmet_constraints,
        "audit_trail": [item.model_dump(mode="json") for item in run.audit_trail],
    }


@app.get("/api/match/{run_id}/audit")
def api_get_match_audit(run_id: str) -> dict:
    try:
        run = load_match_run(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="match run not found") from exc
    return {
        "run_id": run.run_id,
        "package_id": run.package_id,
        "domain": run.domain,
        "audit_trail": [item.model_dump(mode="json") for item in run.audit_trail],
    }


@app.post("/api/dev/reset-runtime")
async def api_reset_runtime() -> dict:
    # Helper endpoint for local demo/testing.
    if settings.runtime_dir.exists():
        shutil.rmtree(settings.runtime_dir)
    ensure_runtime_layout()
    return {"ok": True}
