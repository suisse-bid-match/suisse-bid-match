from __future__ import annotations

import os
import re
import tempfile
import threading
from pathlib import Path
from typing import Any

import anyio
from fastapi import FastAPI, File, HTTPException, Query, UploadFile

app = FastAPI(title="SwissTender Docling Service", version="0.1.0")

_converter_lock = threading.Lock()
_converter: object | None = None


def _http_error(status_code: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status_code, detail={"code": code, "message": message})


def _normalize_line(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _lines_from_text(text: str, limit: int) -> list[str]:
    out: list[str] = []
    for raw in text.splitlines():
        line = _normalize_line(raw)
        if len(line) < 2:
            continue
        out.append(line)
        if len(out) >= limit:
            break
    return out


def _extract_strings(value: Any, out: list[str], limit: int, depth: int = 0) -> None:
    if len(out) >= limit or depth > 8:
        return
    if isinstance(value, str):
        line = _normalize_line(value)
        if len(line) >= 2:
            out.append(line)
        return
    if isinstance(value, dict):
        for item in value.values():
            _extract_strings(item, out, limit, depth + 1)
            if len(out) >= limit:
                break
        return
    if isinstance(value, list):
        for item in value:
            _extract_strings(item, out, limit, depth + 1)
            if len(out) >= limit:
                break


def _unique_lines(lines: list[str], limit: int) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for line in lines:
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(line)
        if len(unique) >= limit:
            break
    return unique


_IMAGE_HINT_RE = re.compile(
    r"(\!\[[^\]]*\]\([^\)]*\)|<img\b|\bfigure\b|\babbildung\b|\bimage\b|\bphoto\b)",
    re.IGNORECASE,
)
_IMAGE_FILE_RE = re.compile(r"\.(png|jpe?g|gif|bmp|tiff?|svg|webp)\b", re.IGNORECASE)
_TABLE_SEP_RE = re.compile(r"^\s*\|?\s*:?-{2,}:?(?:\s*\|\s*:?-{2,}:?)+\s*\|?\s*$")
_PIPES_ONLY_RE = re.compile(r"^\s*\|+\s*$")
_JUNK_LINE_RE = re.compile(r"^\s*[-|_ ]{2,}\s*$")
_NONE_LINE_RE = re.compile(r"^\s*none\s*$", re.IGNORECASE)


def _drop_image_lines(lines: list[str]) -> list[str]:
    filtered: list[str] = []
    for line in lines:
        if _IMAGE_FILE_RE.search(line) or _IMAGE_HINT_RE.search(line):
            continue
        filtered.append(line)
    return filtered


def _drop_noise_lines(lines: list[str]) -> list[str]:
    filtered: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if _TABLE_SEP_RE.match(stripped) or _PIPES_ONLY_RE.match(stripped):
            continue
        if _JUNK_LINE_RE.match(stripped):
            continue
        if _NONE_LINE_RE.match(stripped):
            continue
        filtered.append(line)
    return filtered


def _get_converter() -> object:
    global _converter
    with _converter_lock:
        if _converter is not None:
            return _converter
        try:
            from docling.datamodel.base_models import InputFormat
            from docling.datamodel.pipeline_options import PdfPipelineOptions
            from docling.document_converter import DocumentConverter, PdfFormatOption
        except Exception as exc:
            raise _http_error(500, "DOCLING_IMPORT_FAILED", str(exc)) from exc
        try:
            artifacts_path = os.getenv("DOCLING_ARTIFACTS_PATH", "/srv/docling-artifacts")
            Path(artifacts_path).mkdir(parents=True, exist_ok=True)
            pdf_options = PdfPipelineOptions(
                do_ocr=False,
                do_table_structure=True,
                do_code_enrichment=False,
                do_formula_enrichment=False,
                do_picture_classification=False,
                do_picture_description=False,
                generate_page_images=False,
                generate_picture_images=False,
                generate_table_images=False,
                artifacts_path=artifacts_path,
            )
            format_options = {
                InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_options),
            }
            _converter = DocumentConverter(format_options=format_options)
        except Exception as exc:
            raise _http_error(500, "DOCLING_INIT_FAILED", str(exc)) from exc
        return _converter


def _extract_cues_from_document(
    document: object,
    limit: int,
    *,
    dedupe: bool,
    response_format: str = "plaintext",
) -> list[str]:
    lines: list[str] = []
    if response_format != "plaintext":
        response_format = "plaintext"
    method_names = ("export_to_text", "to_text")
    for method_name in method_names:
        method = getattr(document, method_name, None)
        if not callable(method):
            continue
        try:
            text = method()
        except Exception:
            continue
        if isinstance(text, str):
            lines.extend(_lines_from_text(text, limit=limit))
            if len(lines) >= limit:
                break

    lines = _drop_image_lines(lines)
    lines = _drop_noise_lines(lines)
    if dedupe:
        return _unique_lines(lines, limit=limit)
    return lines[:limit]


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"ok": "true", "ready": "true" if _converter is not None else "false"}


@app.post("/warmup")
def warmup() -> dict[str, str]:
    _get_converter()
    return {"ok": "true", "ready": "true"}


@app.post("/extract-cues")
async def extract_cues(
    limit: int = Query(240, ge=1, le=50000),
    dedupe: bool = Query(True),
    response_format: str = Query("plaintext", pattern="^(plaintext)$"),
    file: UploadFile = File(...),
) -> dict[str, object]:
    suffix = Path(file.filename or "input.bin").suffix.lower()
    if suffix not in {".docx", ".docm", ".xlsx", ".pdf"}:
        raise _http_error(415, "DOCLING_UNSUPPORTED_FORMAT", f"unsupported extension: {suffix or '(none)'}")

    with tempfile.TemporaryDirectory(prefix="docling_") as td:
        input_path = Path(td) / Path(file.filename or f"input{suffix}").name
        written = 0
        with input_path.open("wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                written += len(chunk)
        await file.close()
        if written == 0:
            raise _http_error(400, "DOCLING_EMPTY_FILE", "uploaded file is empty")

        converter = _get_converter()
        try:
            result = await anyio.to_thread.run_sync(converter.convert, str(input_path))
        except Exception as exc:
            raise _http_error(422, "DOCLING_CONVERT_FAILED", str(exc)) from exc
        document = getattr(result, "document", result)
        lines = _extract_cues_from_document(
            document,
            limit=limit,
            dedupe=dedupe,
            response_format=response_format,
        )
        return {"ok": True, "lines": lines, "response_format": response_format}
