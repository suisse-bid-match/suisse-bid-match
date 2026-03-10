from __future__ import annotations

import mimetypes
from dataclasses import dataclass
from pathlib import Path

import httpx

from app.core.settings import settings


@dataclass
class DoclingClientError(RuntimeError):
    code: str
    message: str

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


def _timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=settings.docling_connect_timeout_sec,
        read=settings.docling_timeout_sec,
        write=settings.docling_timeout_sec,
        pool=settings.docling_timeout_sec,
    )


def _extract_error(response: httpx.Response, fallback_code: str) -> DoclingClientError:
    message = response.text
    code = fallback_code
    try:
        payload = response.json()
        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, dict):
                code = str(detail.get("code") or fallback_code)
                message = str(detail.get("message") or detail)
            elif isinstance(detail, str):
                message = detail
    except Exception:
        pass
    return DoclingClientError(code=code, message=message)


def healthz() -> bool:
    endpoint = f"{settings.docling_url.rstrip('/')}/healthz"
    try:
        with httpx.Client(timeout=_timeout()) as client:
            response = client.get(endpoint)
    except httpx.RequestError:
        return False
    if response.status_code != 200:
        return False
    try:
        payload = response.json()
    except Exception:
        return False
    return str(payload.get("ok", "")).lower() == "true"


def warmup() -> None:
    endpoint = f"{settings.docling_url.rstrip('/')}/warmup"
    try:
        with httpx.Client(timeout=_timeout()) as client:
            response = client.post(endpoint)
    except httpx.RequestError as exc:
        raise DoclingClientError(code="DOCLING_UNAVAILABLE", message=str(exc)) from exc

    if response.status_code != 200:
        raise _extract_error(response, fallback_code=f"DOCLING_HTTP_{response.status_code}")

    try:
        payload = response.json()
    except Exception as exc:
        raise DoclingClientError(code="DOCLING_INVALID_JSON", message=str(exc)) from exc

    ok_value = str(payload.get("ok", "")).lower()
    if ok_value != "true":
        raise DoclingClientError(
            code="DOCLING_WARMUP_FAILED",
            message=f"unexpected warmup payload: {payload}",
        )


def extract_cues(
    file_path: Path,
    limit: int = 240,
    *,
    dedupe: bool = True,
    response_format: str = "markdown",
) -> list[str]:
    endpoint = f"{settings.docling_url.rstrip('/')}/extract-cues"
    mime = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    params = {
        "limit": str(limit),
        "dedupe": "true" if dedupe else "false",
        "response_format": response_format,
    }
    try:
        with file_path.open("rb") as fh:
            with httpx.Client(timeout=_timeout()) as client:
                response = client.post(
                    endpoint,
                    params=params,
                    files={"file": (file_path.name, fh, mime)},
                )
    except httpx.RequestError as exc:
        raise DoclingClientError(code="DOCLING_UNAVAILABLE", message=str(exc)) from exc

    if response.status_code != 200:
        raise _extract_error(response, fallback_code=f"DOCLING_HTTP_{response.status_code}")

    try:
        payload = response.json()
    except Exception as exc:
        raise DoclingClientError(code="DOCLING_INVALID_JSON", message=str(exc)) from exc

    lines = payload.get("lines", [])
    if not isinstance(lines, list):
        raise DoclingClientError(code="DOCLING_INVALID_PAYLOAD", message="lines must be a list")

    out: list[str] = []
    for line in lines:
        if not isinstance(line, str):
            continue
        stripped = line.strip()
        if not stripped:
            continue
        out.append(stripped)
    return out
