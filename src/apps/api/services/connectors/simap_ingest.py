from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.api.models.db import DocumentRef, NoticeVersion, TenderNotice
from apps.api.services.connectors.simap_api import SimapApiConnector
from apps.api.services.utils import canonical_json_hash

logger = logging.getLogger(__name__)


def _snapshot_from_notice(normalized: dict[str, Any]) -> dict[str, Any]:
    doc_urls = [d.get("url") for d in normalized.get("documents", []) if d.get("url")]
    return {
        "source": normalized.get("source"),
        "source_id": normalized.get("source_id"),
        "project_id": normalized.get("project_id"),
        "publication_id": normalized.get("publication_id"),
        "title": normalized.get("title"),
        "description": normalized.get("description"),
        "buyer_name": normalized.get("buyer_name"),
        "buyer_location": normalized.get("buyer_location"),
        "cpv_codes": normalized.get("cpv_codes", []),
        "procedure_type": normalized.get("procedure_type"),
        "publication_date": normalized.get("publication_date").isoformat() if normalized.get("publication_date") else None,
        "deadline_date": normalized.get("deadline_date").isoformat() if normalized.get("deadline_date") else None,
        "languages": normalized.get("languages", []),
        "region": normalized.get("region"),
        "url": normalized.get("url"),
        "documents": doc_urls,
    }


def ingest_simap_publications(
    db: Session,
    updated_since: datetime | None = None,
    limit: int = 50,
) -> dict[str, int]:
    start = time.perf_counter()
    connector = SimapApiConnector()
    if updated_since is None:
        updated_since = datetime.now(timezone.utc) - timedelta(days=30)

    publications = connector.list_publications(
        updated_since=updated_since.isoformat(),
        limit=limit,
    )

    upserted = 0
    versions_created = 0
    docs_discovered = 0
    errors = 0

    for item in publications:
        try:
            normalized = connector.normalize_publication(item)
            source_id = normalized.get("source_id")
            publication_id = normalized.get("publication_id") or source_id
            project_id = normalized.get("project_id")
            if not source_id:
                errors += 1
                continue

            detail = connector.get_publication(
                publication_id=publication_id,
                project_id=project_id,
            )
            if detail:
                normalized = connector.normalize_publication(detail)
                normalized["source_id"] = normalized.get("source_id") or source_id
                normalized["publication_id"] = normalized.get("publication_id") or publication_id
                normalized["project_id"] = normalized.get("project_id") or project_id
                normalized["raw_json"] = detail

            notice = db.scalar(
                select(TenderNotice).where(TenderNotice.source == "simap", TenderNotice.source_id == normalized["source_id"])
            )
            created = False
            if notice is None:
                notice = TenderNotice(source="simap", source_id=normalized["source_id"])
                created = True

            notice.title = normalized.get("title")
            notice.description = normalized.get("description")
            notice.buyer_name = normalized.get("buyer_name")
            notice.buyer_location = normalized.get("buyer_location")
            notice.cpv_codes = normalized.get("cpv_codes", [])
            notice.procedure_type = normalized.get("procedure_type")
            notice.publication_date = normalized.get("publication_date")
            notice.deadline_date = normalized.get("deadline_date")
            notice.languages = normalized.get("languages", [])
            notice.region = normalized.get("region")
            notice.url = normalized.get("url")
            notice.documents = normalized.get("documents", [])
            notice.raw_json = normalized.get("raw_json")

            if created:
                db.add(notice)
                db.flush()

            existing_docs = {
                d.url: d for d in db.scalars(select(DocumentRef).where(DocumentRef.notice_id == notice.id)).all() if d.url
            }
            for doc in normalized.get("documents", []):
                doc_url = doc.get("url")
                if not doc_url:
                    continue
                if doc_url in existing_docs:
                    continue
                db.add(
                    DocumentRef(
                        notice_id=notice.id,
                        url=doc_url,
                        filename=doc.get("filename"),
                        mime_type=doc.get("mime_type"),
                        raw_json=doc.get("raw") if isinstance(doc.get("raw"), dict) else doc,
                    )
                )
                docs_discovered += 1

            snapshot = _snapshot_from_notice(normalized)
            content_hash = canonical_json_hash(snapshot)
            latest_version = db.scalar(
                select(NoticeVersion)
                .where(NoticeVersion.notice_id == notice.id)
                .order_by(NoticeVersion.version_ts.desc())
                .limit(1)
            )
            if latest_version is None or latest_version.content_hash != content_hash:
                db.add(
                    NoticeVersion(
                        notice_id=notice.id,
                        content_hash=content_hash,
                        raw_json_snapshot=snapshot,
                    )
                )
                versions_created += 1

            upserted += 1
        except Exception as exc:
            logger.exception("Failed to process publication: %s", exc)
            errors += 1

    db.commit()

    elapsed_ms = int((time.perf_counter() - start) * 1000)
    return {
        "fetched": len(publications),
        "upserted": upserted,
        "versions_created": versions_created,
        "docs_discovered": docs_discovered,
        "errors": errors,
        "elapsed_ms": elapsed_ms,
    }
