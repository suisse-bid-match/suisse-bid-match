from __future__ import annotations

import json
import re
import uuid
from typing import Any

from app.core.models import DocumentClassification, DocumentInfo, DoclingLine, utcnow
from app.core.openai_web_search import build_web_search_kwargs, extract_web_search_info
from app.core.settings import settings


def _parse_json(text: str) -> dict[str, Any]:
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(0))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return False


def _coerce_confidence(value: object, fallback: float = 0.0) -> float:
    try:
        val = float(value)
    except Exception:
        val = fallback
    if val < 0:
        return 0.0
    if val > 1:
        return 1.0
    return round(val, 4)


def _dump_llm_output(payload: dict[str, Any]) -> None:
    try:
        out_dir = settings.runtime_dir / "llm_outputs"
        out_dir.mkdir(parents=True, exist_ok=True)
        file_id = str(uuid.uuid4())
        step = str(payload.get("step") or "llm")
        out_path = out_dir / f"{file_id}_{step}.json"
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
    except Exception:
        return


class LLMDocumentClassifier:
    def __init__(self) -> None:
        self.enabled = bool(settings.openai_api_key)
        self._client = None
        self.last_web_search: dict[str, Any] | None = None
        if self.enabled:
            try:
                from openai import OpenAI

                self._client = OpenAI(
                    api_key=settings.openai_api_key,
                    timeout=settings.doc_classifier_timeout_sec,
                    max_retries=settings.doc_classifier_max_retries,
                )
            except Exception:
                self.enabled = False
                self._client = None

    def fetch_web_context(self) -> str | None:
        if not self.enabled or self._client is None:
            return None
        try:
            response = self._client.responses.create(
                model=settings.doc_classifier_model,
                input=[
                    {
                        "role": "system",
                        "content": (
                            "You are researching Swiss lighting tender document formats. "
                            "Use web search to gather concise, high-signal indicators for LV/BOQ/price sheets. "
                            "Return a short bullet list (max 8 bullets) in plain text."
                        ),
                    }
                ],
                max_output_tokens=max(200, settings.doc_classifier_max_output_tokens),
                **build_web_search_kwargs(),
            )
        except Exception:
            self.last_web_search = None
            return None
        self.last_web_search = extract_web_search_info(response)
        summary = (response.output_text or "").strip()
        return summary or None

    def classify(
        self,
        *,
        doc: DocumentInfo,
        docling_lines: list[DoclingLine],
        web_context: str | None = None,
        enable_web_search: bool = False,
    ) -> DocumentClassification:
        self.last_web_search = None
        if not self.enabled or self._client is None:
            return DocumentClassification(
                doc_id=doc.doc_id,
                doc_name=doc.name,
                is_application_form=False,
                confidence=0.0,
                reason="LLM unavailable",
                evidence_refs=[],
                parse_failed=False,
            )

        allowed_evidence = {line.evidence_ref for line in docling_lines}

        max_lines = max(1, settings.doc_classifier_max_lines)
        max_chars = max(0, settings.doc_classifier_max_chars)
        excerpt_lines = docling_lines[:max_lines]
        if max_chars:
            trimmed: list[DoclingLine] = []
            size = 0
            for line in excerpt_lines:
                cost = len(line.text) + len(line.evidence_ref) + 4
                if trimmed and size + cost > max_chars:
                    break
                trimmed.append(line)
                size += cost
            excerpt_lines = trimmed
        excerpt_text = "\n".join(
            f"[{line.evidence_ref}] {line.text}" for line in excerpt_lines if line.text.strip()
        )
        if doc.name:
            excerpt_text = f"[filename] {doc.name}\n[filepath] {doc.relative_path}\n{excerpt_text}"
        if not excerpt_text.strip():
            excerpt_text = (
                f"[filename] {doc.name}\n[filepath] {doc.relative_path}\n"
                "[note] Docling produced no extractable text."
            )

        payload = {
            "task": (
                "Decide whether this file is a tender parameter list / bill of quantities / price schedule "
                "that defines product types, quantities, or technical requirements."
            ),
            "document": {
                "name": doc.name,
                "kind": doc.kind,
                "relative_path": doc.relative_path,
            },
            "docling_excerpt": excerpt_text,
            "web_context": web_context or "",
            "output_schema": {
                "is_application_form": "boolean",
                "confidence": "0..1 float",
                "reason": "short reason",
                "evidence_refs": ["list of evidence refs from docling_excerpt"],
            },
            "constraints": [
                "Return JSON only, no markdown",
                "If unsure, set is_application_form=false",
                "Only use evidence_refs that appear inside the excerpt brackets",
                "If no evidence_refs are available, return an empty list",
            ],
        }

        try:
            tools_kwargs = build_web_search_kwargs() if enable_web_search else {}
            create_kwargs: dict[str, Any] = {
                "model": settings.doc_classifier_model,
                "input": [
                    {
                        "role": "system",
                        "content": (
                            "You classify tender documents for lighting projects. "
                            "You understand English, German, and French. "
                            "Target documents are parameter lists / LV / price schedules. "
                            "Use the provided web context if available. "
                            "Do not browse the web in this step. "
                            "Assume the package contains at least one parameter list document. "
                            "If none are clearly identified, pick the most likely one. "
                            "Strong positive hints: Leistungsverzeichnis, LV, Preisblatt, Angebot, Offerte, "
                            "Stückzahl, Einheitspreis, Positionsliste, bordereau de prix, devis, cahier de soumission, "
                            "quantités, prix unitaire, bill of quantities, price schedule, quotation, unit price, "
                            "报价清单, 数量清单, 价格表, 参数清单. "
                            "Negative hints: contracts, terms/conditions, planning, drawings, photos."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(payload, ensure_ascii=False),
                    },
                ],
                "max_output_tokens": max(80, settings.doc_classifier_max_output_tokens),
            }
            if tools_kwargs:
                create_kwargs.update(tools_kwargs)
            else:
                create_kwargs["text"] = {"format": {"type": "json_object"}}
            response = self._client.responses.create(
                **create_kwargs,
            )
            self.last_web_search = extract_web_search_info(response)
            raw_text = (response.output_text or "").strip()
            parsed = _parse_json(raw_text)
            _dump_llm_output(
                {
                    "step": "classify_documents",
                    "created_at": utcnow().isoformat(),
                    "model": settings.doc_classifier_model,
                    "doc_id": doc.doc_id,
                    "doc_name": doc.name,
                    "doc_relative_path": doc.relative_path,
                    "parse_ok": bool(parsed),
                    "raw_text": raw_text,
                }
            )
        except Exception as exc:
            return DocumentClassification(
                doc_id=doc.doc_id,
                doc_name=doc.name,
                is_application_form=False,
                confidence=0.0,
                reason=f"LLM classification failed: {exc}",
                evidence_refs=[],
                parse_failed=False,
            )

        raw_refs = parsed.get("evidence_refs", [])
        evidence_refs: list[str] = []
        if isinstance(raw_refs, list):
            for ref in raw_refs:
                if not isinstance(ref, str):
                    continue
                ref = ref.strip()
                if ref and ref in allowed_evidence:
                    evidence_refs.append(ref)

        return DocumentClassification(
            doc_id=doc.doc_id,
            doc_name=doc.name,
            is_application_form=_coerce_bool(parsed.get("is_application_form")),
            confidence=_coerce_confidence(parsed.get("confidence"), fallback=0.0),
            reason=str(parsed.get("reason") or "LLM classification").strip(),
            evidence_refs=evidence_refs[:12],
            parse_failed=False,
        )
