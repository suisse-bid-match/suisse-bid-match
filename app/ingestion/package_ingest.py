from __future__ import annotations

import shutil
from pathlib import Path
from typing import Callable
from zipfile import ZipFile

from app.core.models import (
    DoclingDocument,
    DoclingLine,
    DocumentInfo,
    PackageIndex,
    PdfInsight,
    ReferenceChunk,
    utcnow,
)
from app.core.settings import settings
from app.core.storage import new_package_dir, save_package_index
from app.ingestion.docling_bridge import ensure_docling_ready, extract_docling_cues_result

SUPPORTED_SUFFIXES = {".doc", ".docx", ".docm", ".xlsx", ".pdf"}


class IngestError(RuntimeError):
    pass


def _is_staged_runtime_upload_dir(source: Path) -> bool:
    try:
        src = source.resolve()
        runtime_root = settings.runtime_dir.resolve()
    except FileNotFoundError:
        return False
    return source.is_dir() and src.parent == runtime_root and source.name.startswith("ingest_upload_")


def _safe_copy_source(source: Path, target_root: Path) -> Path:
    if not source.exists():
        raise IngestError(f"source path does not exist: {source}")

    target = target_root / "source"
    if source.is_dir():
        if _is_staged_runtime_upload_dir(source):
            # Fast path for browser uploads: move staged files/directories instead of copying.
            target.mkdir(parents=True, exist_ok=True)
            for child in source.iterdir():
                shutil.move(str(child), target / child.name)
        else:
            shutil.copytree(source, target)
    elif source.suffix.lower() == ".zip":
        target.mkdir(parents=True, exist_ok=True)
        with ZipFile(source) as zf:
            zf.extractall(target)
    else:
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target / source.name)
    return target


def _collect_supported_files(source_root: Path) -> list[Path]:
    files: list[Path] = []
    for file_path in source_root.rglob("*"):
        if not file_path.is_file():
            continue
        if file_path.name.startswith("~$"):
            continue
        if "__MACOSX" in file_path.parts:
            continue
        if file_path.name.endswith("Zone.Identifier"):
            continue
        if file_path.suffix.lower() in SUPPORTED_SUFFIXES:
            files.append(file_path)

    if not files:
        raise IngestError("no supported files found (expect .doc/.docx/.docm/.xlsx/.pdf)")

    ordered = sorted(files, key=lambda p: str(p.relative_to(source_root)).lower())
    return ordered


def _doc_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".doc":
        return "doc"
    if suffix in {".docx", ".docm"}:
        return suffix.lstrip(".")
    if suffix == ".xlsx":
        return "xlsx"
    if suffix == ".pdf":
        return "pdf"
    raise IngestError(f"unsupported file kind: {path}")


def _merge_pdf_insights(pdf_map: dict[str, PdfInsight]) -> PdfInsight | None:
    if not pdf_map:
        return None
    first = next(iter(pdf_map.values()))
    sections: list[dict[str, str]] = []
    deadline_lines: list[str] = []
    required_document_lines: list[str] = []
    criteria_lines: list[str] = []

    for insight in pdf_map.values():
        sections.extend(insight.sections)
        deadline_lines.extend(insight.deadline_lines)
        required_document_lines.extend(insight.required_document_lines)
        criteria_lines.extend(insight.criteria_lines)

    return PdfInsight(
        title=first.title,
        sections=sections[:200],
        deadline_lines=deadline_lines[:80],
        required_document_lines=required_document_lines[:120],
        criteria_lines=criteria_lines[:120],
    )


def _normalize_doc_id(stem: str) -> str:
    return stem.lower().replace(" ", "_")


def _emit_progress(
    callback: Callable[[str, dict], None] | None,
    event: str,
    payload: dict,
) -> None:
    if callback is None:
        return
    try:
        callback(event, payload)
    except Exception:
        # Progress callback should never break ingestion flow.
        return


def ingest_from_source(
    source_path: Path,
    progress_callback: Callable[[str, dict], None] | None = None,
) -> PackageIndex:
    package_id, package_dir = new_package_dir()
    source_root = _safe_copy_source(source_path, package_dir)
    selected_files = _collect_supported_files(source_root)
    _emit_progress(
        progress_callback,
        "prepared",
        {
            "total_files": len(selected_files),
        },
    )

    documents: list[DocumentInfo] = []
    pdf_by_doc_id: dict[str, PdfInsight] = {}
    reference_chunks: list[ReferenceChunk] = []
    docling_documents: list[DoclingDocument] = []

    docling_ready, docling_error = ensure_docling_ready()

    total_files = len(selected_files)
    for idx, file_path in enumerate(selected_files, start=1):
        kind = _doc_kind(file_path)
        _emit_progress(
            progress_callback,
            "file_started",
            {
                "index": idx,
                "total_files": total_files,
                "file_name": file_path.name,
                "relative_path": str(file_path.relative_to(source_root)),
                "kind": kind,
            },
        )
        doc_id = f"doc_{idx:02d}_{_normalize_doc_id(file_path.stem)}"
        relative_path = str(file_path.relative_to(package_dir))
        doc = DocumentInfo(
            doc_id=doc_id,
            name=file_path.name,
            relative_path=relative_path,
            kind=kind,  # type: ignore[arg-type]
        )
        doc.role = "REFERENCE_ONLY"
        doc.field_understanding_status = "skipped"
        doc.field_understanding_reason = "autofill_disabled"
        documents.append(doc)


        docling_parse = DoclingDocument(doc_id=doc_id)
        if docling_ready:
            if kind == "pdf":
                docling_parse.used = False
                docling_parse.error = "PDF_DEFERRED"
                docling_documents.append(docling_parse)
                _emit_progress(
                    progress_callback,
                    "file_done",
                    {
                        "index": idx,
                        "total_files": total_files,
                        "file_name": file_path.name,
                        "relative_path": str(file_path.relative_to(source_root)),
                        "kind": kind,
                        "note": "pdf docling deferred",
                    },
                )
                continue
            doc_limit = settings.docling_max_lines
            dedupe = True
            if kind == "xlsx":
                doc_limit = settings.docling_full_xlsx_max_lines
                dedupe = False
            result = extract_docling_cues_result(
                file_path,
                limit=doc_limit,
                dedupe=dedupe,
                response_format=settings.docling_response_format,
            )
            docling_parse.used = result.used
            docling_parse.error = result.error
            docling_parse.lines = [
                DoclingLine(
                    evidence_ref=f"{doc_id}:docling:{idx:04d}",
                    text=line,
                )
                for idx, line in enumerate(result.lines, start=1)
            ]
        else:
            docling_parse.used = False
            docling_parse.error = docling_error or "DOCLING_UNAVAILABLE"
        docling_documents.append(docling_parse)

        _emit_progress(
            progress_callback,
            "file_done",
            {
                "index": idx,
                "total_files": total_files,
                "file_name": file_path.name,
                "relative_path": str(file_path.relative_to(source_root)),
                "kind": kind,
            },
        )

    for doc in documents:
        doc.role_confidence = 0.0
        doc.role_reasons = []
        doc.submit_required = None
        doc.anchor_candidates_count = 0
    all_fields: list = []

    pdf_insight = _merge_pdf_insights(pdf_by_doc_id)

    index = PackageIndex(
        package_id=package_id,
        created_at=utcnow(),
        source_name=source_path.name,
        root_dir=str(package_dir),
        documents=documents,
        fields=all_fields,
        pdf_insight=pdf_insight,
        reference_chunks=reference_chunks,
        docling_documents=docling_documents,
    )
    save_package_index(index)
    _emit_progress(
        progress_callback,
        "completed",
        {
            "total_files": total_files,
            "package_id": index.package_id,
            "document_count": len(index.documents),
            "field_count": len(index.fields),
        },
    )
    return index
