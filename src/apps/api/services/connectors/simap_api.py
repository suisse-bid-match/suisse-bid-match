from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from apps.api.core.config import get_settings
from apps.api.services.utils import as_list, parse_datetime, safe_get

logger = logging.getLogger(__name__)


class SimpleRateLimiter:
    def __init__(self, requests_per_second: float = 1.0):
        self.min_interval = 1.0 / max(requests_per_second, 0.01)
        self._last_call_ts = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_call_ts
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call_ts = time.monotonic()


class SimapApiConnector:
    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.simap_base_url.rstrip("/")
        self.timeout = self.settings.simap_timeout_seconds
        self.limiter = SimpleRateLimiter(self.settings.simap_rps)

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.settings.simap_token:
            headers["Authorization"] = f"Bearer {self.settings.simap_token}"
        return headers

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3), reraise=True)
    def _request(self, method: str, path: str, params: dict[str, Any] | None = None) -> Any:
        self.limiter.wait()
        url = f"{self.base_url}{path}"
        with httpx.Client(timeout=self.timeout, headers=self._headers()) as client:
            response = client.request(method, url, params=params)
            response.raise_for_status()
            return response.json()

    @staticmethod
    def _extract_projects(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for key in ("projects", "items", "results", "data", "publications"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
        return []

    @staticmethod
    def _pick_localized_text(value: Any, preferred_lang: str = "en") -> str | None:
        if isinstance(value, str):
            text = value.strip()
            return text or None
        if isinstance(value, dict):
            candidates = [preferred_lang, "de", "fr", "it", "en"]
            for key in candidates:
                candidate = value.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
            for candidate in value.values():
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
        return None

    def list_publications(self, updated_since: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        # SIMAP public project search requires at least one "search input" or a quick-filter.
        # We use newestPublicationFrom as a stable quick-filter and roll pages via lastItem.
        try:
            target_limit = max(1, int(limit))
        except Exception:
            target_limit = 50

        params_base: dict[str, Any] = {}

        from_date: str | None = None
        if updated_since:
            parsed = parse_datetime(updated_since)
            if parsed:
                from_date = parsed.date().isoformat()
            else:
                logger.warning("SIMAP updated_since could not be parsed; fallback quick-filter will be used value=%s", updated_since)
        if not from_date:
            from_date = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
        params_base["newestPublicationFrom"] = from_date

        publications: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        last_item: str | None = None
        max_pages = 50

        for _ in range(max_pages):
            if len(publications) >= target_limit:
                break

            params = dict(params_base)
            if last_item:
                params["lastItem"] = last_item

            try:
                payload = self._request("GET", self.settings.simap_publications_path, params=params)
            except Exception as exc:
                logger.warning("SIMAP list_publications failed params=%s err=%s", params, exc)
                break

            batch = self._extract_projects(payload)
            if not batch:
                break

            for item in batch:
                key = str(safe_get(item, "publicationId") or safe_get(item, "id") or "")
                if key and key in seen_keys:
                    continue
                if key:
                    seen_keys.add(key)
                publications.append(item)
                if len(publications) >= target_limit:
                    break

            next_last_item = None
            if isinstance(payload, dict):
                next_last_item = safe_get(payload, "pagination.lastItem") or safe_get(payload, "lastItem")
            if not next_last_item:
                break

            next_last_item = str(next_last_item)
            if last_item == next_last_item:
                break
            last_item = next_last_item

        return publications[:target_limit]

    def get_publication(
        self,
        publication_id: str,
        project_id: str | None = None,
    ) -> dict[str, Any] | None:
        path = self.settings.simap_publication_detail_path
        replacements = {
            "projectId": project_id,
            "project_id": project_id,
            "publicationId": publication_id,
            "publication_id": publication_id,
        }
        for key, value in replacements.items():
            if value is not None:
                path = path.replace("{" + key + "}", str(value))

        if "{" in path and "}" in path:
            logger.warning(
                "SIMAP detail path has unresolved placeholders path=%s project_id=%s publication_id=%s",
                path,
                project_id,
                publication_id,
            )
            return None

        try:
            payload = self._request("GET", path, params=None)
            if isinstance(payload, dict):
                return payload
            return None
        except Exception as exc:
            logger.warning(
                "SIMAP get_publication failed project_id=%s publication_id=%s err=%s",
                project_id,
                publication_id,
                exc,
            )
            return None

    @staticmethod
    def normalize_publication(raw: dict[str, Any]) -> dict[str, Any]:
        preferred_lang = str(
            safe_get(raw, "base.creationLanguage")
            or safe_get(raw, "language")
            or "en"
        ).lower()

        title_value = (
            safe_get(raw, "title")
            or safe_get(raw, "name")
            or safe_get(raw, "subject")
            or safe_get(raw, "project-info.title")
            or safe_get(raw, "project-info.projectTitle")
            or safe_get(raw, "projectTitle")
            or safe_get(raw, "base.title")
        )
        description_value = (
            safe_get(raw, "description")
            or safe_get(raw, "summary")
            or safe_get(raw, "body")
            or safe_get(raw, "project-info.description")
            or safe_get(raw, "project-info.projectDescription")
            or safe_get(raw, "procurement.description")
            or safe_get(raw, "procurement.orderDescription")
        )

        # Publication ID: prefer explicit publicationId, fall back to id fields.
        publication_id = str(
            safe_get(raw, "publicationId")
            or safe_get(raw, "publication_id")
            or safe_get(raw, "noticeId")
            or safe_get(raw, "uuid")
            or safe_get(raw, "id")
            or safe_get(raw, "base.id")
            or ""
        ).strip()

        # Project ID: prefer explicit projectId/base.projectId. If list response contains
        # both id (project) + publicationId, use id as project_id.
        project_id = str(
            safe_get(raw, "projectId")
            or safe_get(raw, "project_id")
            or safe_get(raw, "project.id")
            or safe_get(raw, "project.projectId")
            or safe_get(raw, "project.idProject")
            or safe_get(raw, "base.projectId")
            or ""
        ).strip()
        if not project_id and safe_get(raw, "publicationId") and safe_get(raw, "id"):
            project_id = str(safe_get(raw, "id")).strip()

        buyer = safe_get(raw, "buyer") or safe_get(raw, "contractingAuthority") or {}
        if not isinstance(buyer, dict):
            buyer = {}

        docs = as_list(safe_get(raw, "documents") or safe_get(raw, "attachments") or [])
        normalized_docs = []
        for doc in docs:
            if isinstance(doc, dict):
                doc_url = safe_get(doc, "url") or safe_get(doc, "href")
                if not doc_url:
                    continue
                normalized_docs.append(
                    {
                        "url": doc_url,
                        "filename": safe_get(doc, "filename") or safe_get(doc, "name"),
                        "mime_type": safe_get(doc, "mime_type") or safe_get(doc, "mimeType") or safe_get(doc, "type"),
                        "raw": doc,
                    }
                )
            elif isinstance(doc, str) and doc.startswith("http"):
                normalized_docs.append({"url": doc, "filename": None, "mime_type": None, "raw": {"url": doc}})

        cpv_raw = as_list(
            safe_get(raw, "cpvCodes")
            or safe_get(raw, "cpv")
            or safe_get(raw, "cpv_codes")
            or safe_get(raw, "procurement.cpvCode")
            or safe_get(raw, "procurement.cpvCodes")
        )
        cpv_codes = []
        for cpv in cpv_raw:
            if isinstance(cpv, dict):
                code = cpv.get("code") or cpv.get("value")
                if code:
                    cpv_codes.append(str(code))
            else:
                cpv_codes.append(str(cpv))

        langs = as_list(
            safe_get(raw, "languages")
            or safe_get(raw, "language")
            or safe_get(raw, "project-info.publicationLanguages")
        )

        return {
            "source": "simap",
            "source_id": publication_id,
            "publication_id": publication_id or None,
            "project_id": project_id or None,
            "title": SimapApiConnector._pick_localized_text(title_value, preferred_lang=preferred_lang),
            "description": SimapApiConnector._pick_localized_text(description_value, preferred_lang=preferred_lang),
            "buyer_name": (
                SimapApiConnector._pick_localized_text(safe_get(buyer, "name"), preferred_lang=preferred_lang)
                or safe_get(raw, "buyerName")
                or safe_get(raw, "contractingAuthorityName")
                or safe_get(raw, "project-info.procOfficeName")
                or SimapApiConnector._pick_localized_text(
                    safe_get(raw, "project-info.procOfficeAddress.name"),
                    preferred_lang=preferred_lang,
                )
            ),
            "buyer_location": safe_get(buyer, "location") or safe_get(raw, "buyerLocation"),
            "cpv_codes": cpv_codes,
            "procedure_type": (
                safe_get(raw, "procedureType")
                or safe_get(raw, "procedure")
                or safe_get(raw, "type")
                or safe_get(raw, "processType")
                or safe_get(raw, "procurement.procedureType")
            ),
            "publication_date": parse_datetime(
                safe_get(raw, "publicationDate")
                or safe_get(raw, "publishedAt")
                or safe_get(raw, "datePublication")
                or safe_get(raw, "createdAt")
                or safe_get(raw, "dates.publicationDate")
                or safe_get(raw, "base.publicationDate")
            ),
            "deadline_date": parse_datetime(
                safe_get(raw, "deadlineDate")
                or safe_get(raw, "submissionDeadline")
                or safe_get(raw, "deadline")
                or safe_get(raw, "dates.offerDeadline")
                or safe_get(raw, "dates.submissionDeadline")
                or safe_get(raw, "dates.deadline")
            ),
            "languages": [str(x) for x in langs if x],
            "region": safe_get(raw, "region") or safe_get(raw, "canton") or safe_get(raw, "buyer.canton"),
            "url": safe_get(raw, "url") or safe_get(raw, "publicationUrl") or safe_get(raw, "link"),
            "documents": normalized_docs,
            "raw_json": raw,
        }
