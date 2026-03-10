from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path

from app.core.docling_client import DoclingClientError, extract_cues, healthz, warmup
from app.core.settings import settings

_HEALTH_TTL_SEC = 10.0
_WARMUP_TTL_SEC = 120.0

_state_lock = threading.Lock()
_last_health_check_at = 0.0
_last_health_ok = False
_last_warmup_at = 0.0
_last_warmup_ok = False


@dataclass
class DoclingCueResult:
    lines: list[str]
    used: bool
    error: str | None = None


def _now() -> float:
    return time.monotonic()


def docling_available() -> bool:
    global _last_health_check_at, _last_health_ok
    with _state_lock:
        age = _now() - _last_health_check_at
        if age < _HEALTH_TTL_SEC:
            return _last_health_ok
    ok = healthz()
    with _state_lock:
        _last_health_ok = ok
        _last_health_check_at = _now()
    return ok


def ensure_docling_ready() -> tuple[bool, str | None]:
    global _last_warmup_at, _last_warmup_ok
    with _state_lock:
        warmup_age = _now() - _last_warmup_at
        if _last_warmup_ok and warmup_age < _WARMUP_TTL_SEC:
            return True, None

    if not docling_available():
        with _state_lock:
            _last_warmup_ok = False
            _last_warmup_at = _now()
        return False, "DOCLING_HEALTHCHECK_FAILED"

    try:
        warmup()
    except DoclingClientError as exc:
        with _state_lock:
            _last_warmup_ok = False
            _last_warmup_at = _now()
        return False, f"{exc.code}: {exc.message}"

    with _state_lock:
        _last_warmup_ok = True
        _last_warmup_at = _now()
    return True, None


def extract_docling_cues(
    path: Path,
    limit: int = 240,
    *,
    dedupe: bool = True,
    response_format: str = "markdown",
) -> list[str]:
    return extract_docling_cues_result(
        path,
        limit=limit,
        dedupe=dedupe,
        response_format=response_format,
    ).lines


def extract_docling_cues_result(
    path: Path,
    limit: int = 240,
    *,
    dedupe: bool = True,
    response_format: str = "markdown",
) -> DoclingCueResult:
    if path.suffix.lower() == ".pdf" and settings.docling_pdf_split_enabled:
        return _extract_docling_cues_pdf_split(
            path,
            limit=limit,
            dedupe=dedupe,
            response_format=response_format,
        )
    try:
        lines = extract_cues(path, limit=limit, dedupe=dedupe, response_format=response_format)
    except DoclingClientError as exc:
        return DoclingCueResult(lines=[], used=False, error=f"{exc.code}: {exc.message}")

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
    return DoclingCueResult(lines=unique, used=bool(unique), error=None)


def _extract_docling_cues_pdf_split(
    path: Path,
    limit: int = 240,
    *,
    dedupe: bool = True,
    response_format: str = "markdown",
) -> DoclingCueResult:
    try:
        from pypdf import PdfReader, PdfWriter
    except Exception as exc:
        return DoclingCueResult(lines=[], used=False, error=f"PDF_SPLIT_UNAVAILABLE: {exc}")

    try:
        reader = PdfReader(str(path))
    except Exception as exc:
        return DoclingCueResult(lines=[], used=False, error=f"PDF_READ_FAILED: {exc}")

    total_pages = len(reader.pages)
    if total_pages <= 0:
        return DoclingCueResult(lines=[], used=False, error="PDF_EMPTY")

    chunk_size = max(1, settings.docling_pdf_chunk_pages)
    max_chunks = max(1, settings.docling_pdf_max_chunks)
    lines: list[str] = []
    error: str | None = None

    for chunk_index, start in enumerate(range(0, total_pages, chunk_size), start=1):
        if chunk_index > max_chunks:
            break
        end = min(start + chunk_size, total_pages)
        writer = PdfWriter()
        for page_index in range(start, end):
            writer.add_page(reader.pages[page_index])

        temp_path = path.with_suffix(f".docling_part_{start+1:04d}_{end:04d}.pdf")
        try:
            with temp_path.open("wb") as handle:
                writer.write(handle)
        except Exception as exc:
            error = f"PDF_SPLIT_WRITE_FAILED: {exc}"
            break

        try:
            chunk_lines = extract_cues(
                temp_path,
                limit=max(1, limit - len(lines)),
                dedupe=dedupe,
                response_format=response_format,
            )
        except DoclingClientError as exc:
            error = f"{exc.code}: {exc.message}"
            break
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass

        lines.extend(chunk_lines)
        if len(lines) >= limit:
            break

    if not lines:
        return DoclingCueResult(lines=[], used=False, error=error or "DOCLING_EMPTY_RESULT")

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
    return DoclingCueResult(lines=unique, used=True, error=None)
