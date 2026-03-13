from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests


def _should_disable_json_format(resp: requests.Response) -> bool:
    if resp.status_code not in {400, 422}:
        return False
    text = (resp.text or "").lower()
    return any(marker in text for marker in ("text.format", "response_format", "json_object", "json_schema"))


def _request_with_retries(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    timeout: int,
    max_retries: int = 4,
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504),
    **kwargs: Any,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(2**attempt)
            continue
        if response.status_code in retry_statuses and attempt + 1 < max_retries:
            time.sleep(2**attempt)
            continue
        return response
    if last_error is not None:
        raise RuntimeError(f"request failed after retries: {last_error}") from last_error
    raise RuntimeError("request failed after retries")


def upload_file(base_url: str, api_key: str, path: Path, purpose: str, upload_name: str | None = None) -> str:
    url = f"{base_url.rstrip('/')}/files"
    headers = {"Authorization": f"Bearer {api_key}"}
    with path.open("rb") as handle:
        files = {"file": (upload_name or path.name, handle)}
        data = {"purpose": purpose}
        resp = _request_with_retries(
            "POST",
            url,
            headers=headers,
            files=files,
            data=data,
            timeout=300,
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"upload failed for {path.name}: {resp.status_code} {resp.text}")
    payload = resp.json()
    file_id = payload.get("id")
    if not isinstance(file_id, str) or not file_id.strip():
        raise RuntimeError(f"upload failed for {path.name}: missing file id")
    return file_id


def list_vector_stores(base_url: str, api_key: str) -> list[dict]:
    url = f"{base_url.rstrip('/')}/vector_stores"
    headers = {"Authorization": f"Bearer {api_key}"}
    stores: list[dict] = []
    after: str | None = None
    while True:
        params: dict[str, Any] = {"limit": 100}
        if after:
            params["after"] = after
        resp = _request_with_retries("GET", url, headers=headers, params=params, timeout=60)
        if resp.status_code >= 400:
            raise RuntimeError(f"failed to list vector stores: {resp.status_code} {resp.text}")
        payload = resp.json()
        data = payload.get("data") or []
        stores.extend(data)
        if payload.get("has_more") and data:
            next_after = data[-1].get("id")
            after = next_after if isinstance(next_after, str) else None
            if not after:
                break
        else:
            break
    return stores


def create_vector_store(
    base_url: str,
    api_key: str,
    *,
    name: str,
    metadata: dict[str, str],
    description: str | None = None,
) -> dict:
    url = f"{base_url.rstrip('/')}/vector_stores"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload: dict[str, Any] = {"name": name, "metadata": metadata}
    if description:
        payload["description"] = description
    resp = _request_with_retries("POST", url, headers=headers, json=payload, timeout=60)
    if resp.status_code >= 400:
        raise RuntimeError(f"failed to create vector store: {resp.status_code} {resp.text}")
    return resp.json()


def create_vector_store_file_batch(
    base_url: str,
    api_key: str,
    *,
    vector_store_id: str,
    file_ids: list[str],
) -> dict:
    url = f"{base_url.rstrip('/')}/vector_stores/{vector_store_id}/file_batches"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"file_ids": file_ids}
    resp = _request_with_retries("POST", url, headers=headers, json=payload, timeout=120)
    if resp.status_code >= 400:
        # fallback for APIs expecting "files" objects
        payload_alt = {"files": [{"file_id": fid} for fid in file_ids]}
        resp = _request_with_retries("POST", url, headers=headers, json=payload_alt, timeout=120)
        if resp.status_code >= 400:
            raise RuntimeError(f"failed to create file batch: {resp.status_code} {resp.text}")
    return resp.json()


def get_vector_store_file_batch(
    base_url: str,
    api_key: str,
    *,
    vector_store_id: str,
    batch_id: str,
) -> dict:
    url = f"{base_url.rstrip('/')}/vector_stores/{vector_store_id}/file_batches/{batch_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    resp = _request_with_retries("GET", url, headers=headers, timeout=60)
    if resp.status_code >= 400:
        raise RuntimeError(f"failed to get file batch: {resp.status_code} {resp.text}")
    return resp.json()


def wait_vector_store_file_batch(
    base_url: str,
    api_key: str,
    *,
    vector_store_id: str,
    batch_id: str,
    timeout_sec: int = 3600,
    poll_interval_sec: int = 5,
) -> dict:
    start = time.time()
    while True:
        payload = get_vector_store_file_batch(
            base_url,
            api_key,
            vector_store_id=vector_store_id,
            batch_id=batch_id,
        )
        status = str(payload.get("status") or "")
        if status in {"completed", "failed", "cancelled"}:
            return payload
        if time.time() - start > timeout_sec:
            raise TimeoutError(f"timed out waiting for file batch {batch_id}")
        time.sleep(poll_interval_sec)


def call_responses(
    base_url: str,
    api_key: str,
    model: str,
    *,
    system_prompt: str,
    user_text: str,
    file_ids: list[str] | None = None,
    tools: list[dict] | None = None,
    include: list[str] | None = None,
    json_mode: bool = True,
) -> dict:
    url = f"{base_url.rstrip('/')}/responses"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    file_items = [{"type": "input_file", "file_id": file_id} for file_id in (file_ids or [])]

    payload_base: dict[str, Any] = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": system_prompt}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": user_text}, *file_items],
            },
        ],
    }
    if tools:
        payload_base["tools"] = tools
    if include:
        payload_base["include"] = include

    use_json_format = bool(json_mode)
    last_error: Exception | None = None
    for attempt in range(4):
        payload = dict(payload_base)
        if use_json_format:
            payload["text"] = {"format": {"type": "json_object"}}
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=600)
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(2**attempt)
            continue
        if resp.status_code in {429, 500, 502, 503, 504}:
            time.sleep(2**attempt)
            continue
        if resp.status_code >= 400:
            if use_json_format and _should_disable_json_format(resp):
                use_json_format = False
                continue
            raise RuntimeError(f"responses call failed: {resp.status_code} {resp.text}")
        return resp.json()

    if last_error is not None:
        raise RuntimeError(f"responses call failed after retries: {last_error}") from last_error
    raise RuntimeError("responses call failed after retries")


def _iter_output_texts(response: dict):
    for item in response.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                yield content["text"]


def _unwrap_code_fence(raw: str) -> str:
    stripped = raw.strip()
    if not stripped.startswith("```"):
        return raw
    lines = stripped.splitlines()
    if len(lines) < 3:
        return raw
    if not lines[0].startswith("```") or not lines[-1].startswith("```"):
        return raw
    return "\n".join(lines[1:-1]).strip()


def load_json_with_repair(raw_text: str, payload_name: str) -> dict:
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        try:
            from json_repair import repair_json
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                f"{payload_name} is not valid JSON and json_repair is unavailable"
            ) from exc
        repaired_text = repair_json(raw_text)
        try:
            return json.loads(repaired_text)
        except json.JSONDecodeError as exc:
            snippet = raw_text[:500].replace("\n", "\\n")
            raise RuntimeError(
                f"{payload_name} is not valid JSON and auto-repair failed. Snippet: {snippet}"
            ) from exc


def extract_output_json(response: dict) -> dict:
    last_error: Exception | None = None
    for text in _iter_output_texts(response):
        candidates = [text, _unwrap_code_fence(text)]
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidates.append(text[start : end + 1])
        for idx, candidate in enumerate(candidates):
            candidate = candidate.strip()
            if not candidate:
                continue
            try:
                return load_json_with_repair(candidate, f"response_output_json[{idx}]")
            except Exception as exc:
                last_error = exc
    if last_error:
        raise RuntimeError(f"No valid output JSON found in response: {last_error}") from last_error
    raise RuntimeError("No output_text JSON found in response")

