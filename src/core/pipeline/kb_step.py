from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .io_utils import collect_files
from .openai_client import (
    create_vector_store,
    create_vector_store_file_batch,
    list_vector_stores,
    upload_file,
    wait_vector_store_file_batch,
)


def _is_empty_file_error(exc: Exception) -> bool:
    return "file is empty" in str(exc).strip().lower()


def _compute_fingerprint(files: list[Path], root_dir: Path) -> str:
    hasher = hashlib.sha256()
    for path in sorted(files):
        rel = str(path.relative_to(root_dir))
        stat = path.stat()
        chunk = f"{rel}|{stat.st_size}|{stat.st_mtime_ns}\n".encode("utf-8")
        hasher.update(chunk)
    return hasher.hexdigest()


def _find_existing_store(stores: list[dict], *, kb_key: str, fingerprint: str) -> dict | None:
    candidates: list[tuple[int, int, dict]] = []
    for store in stores:
        status = str(store.get("status") or "")
        if status == "expired":
            continue
        metadata = store.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if metadata.get("kb_key") != kb_key:
            continue
        if metadata.get("kb_fingerprint") != fingerprint:
            continue
        created_at = store.get("created_at")
        created_at_ts = int(created_at) if isinstance(created_at, int) else 0
        candidates.append((1, created_at_ts, store))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return candidates[0][2]


def ensure_vector_store(
    *,
    base_url: str,
    api_key: str,
    kb_key: str,
    vector_store_name: str,
    source_dir: Path,
    file_purpose: str,
    description: str | None = None,
) -> dict[str, Any]:
    source_dir = source_dir.resolve()
    files = collect_files(source_dir)
    non_empty_files = [path for path in files if path.stat().st_size > 0]
    skipped_empty = len(files) - len(non_empty_files)
    if not non_empty_files:
        raise RuntimeError(f"KB source dir contains no supported files: {source_dir}")

    fingerprint = _compute_fingerprint(non_empty_files, source_dir)
    stores = list_vector_stores(base_url, api_key)
    existing = _find_existing_store(stores, kb_key=kb_key, fingerprint=fingerprint)
    if existing is not None:
        return {
            "kb_key": kb_key,
            "source_dir": str(source_dir),
            "source_file_count": len(files),
            "vector_store": {
                "id": existing.get("id"),
                "name": existing.get("name"),
                "reused": True,
                "status": existing.get("status"),
            },
            "upload_summary": {
                "uploaded_files": 0,
                "skipped_files": skipped_empty,
            },
        }

    created = create_vector_store(
        base_url,
        api_key,
        name=vector_store_name,
        metadata={"kb_key": kb_key, "kb_fingerprint": fingerprint},
        description=description,
    )
    vector_store_id = created.get("id")
    if not isinstance(vector_store_id, str) or not vector_store_id.strip():
        raise RuntimeError("create vector store returned no id")

    file_ids: list[str] = []
    skipped_api_empty = 0
    for path in non_empty_files:
        try:
            file_id = upload_file(base_url, api_key, path, file_purpose)
        except Exception as exc:
            if _is_empty_file_error(exc):
                skipped_api_empty += 1
                continue
            raise
        file_ids.append(file_id)

    if not file_ids:
        raise RuntimeError("KB upload produced zero valid files after skipping empty files.")

    batch = create_vector_store_file_batch(
        base_url,
        api_key,
        vector_store_id=vector_store_id,
        file_ids=file_ids,
    )
    batch_id = batch.get("id")
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise RuntimeError("vector store file batch returned no id")
    waited = wait_vector_store_file_batch(
        base_url,
        api_key,
        vector_store_id=vector_store_id,
        batch_id=batch_id,
        timeout_sec=3600,
        poll_interval_sec=5,
    )
    waited_status = str(waited.get("status") or "")
    if waited_status != "completed":
        raise RuntimeError(f"vector store batch did not complete successfully: {waited_status}")

    return {
        "kb_key": kb_key,
        "source_dir": str(source_dir),
        "source_file_count": len(files),
        "vector_store": {
            "id": vector_store_id,
            "name": created.get("name"),
            "reused": False,
            "status": waited_status,
        },
        "upload_summary": {
            "uploaded_files": len(file_ids),
            "skipped_files": skipped_empty + skipped_api_empty,
        },
    }
