from __future__ import annotations

import logging

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from apps.api.models.db import get_db_session
from apps.api.models.schemas import IngestResponse, IngestSimapRequest, ReindexRequest, ReindexResponse
from apps.api.services.connectors.apify_dataset import raise_not_implemented as apify_not_implemented
from apps.api.services.connectors.simap_ingest import ingest_simap_publications
from apps.api.services.connectors.ted import raise_not_implemented as ted_not_implemented
from apps.api.services.indexing.upsert import reindex_notices

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/ingest/simap", response_model=IngestResponse)
def ingest_simap(payload: IngestSimapRequest, db: Session = Depends(get_db_session)):
    result = ingest_simap_publications(
        db,
        updated_since=payload.updated_since,
        limit=payload.limit,
    )
    logger.info("SIMAP ingest finished: %s", result)
    return IngestResponse(**result)


@router.post("/reindex", response_model=ReindexResponse)
def reindex(payload: ReindexRequest, db: Session = Depends(get_db_session)):
    stats = reindex_notices(db, notice_ids=payload.notice_ids, full=payload.full)
    logger.info("Reindex finished: %s", stats)
    return ReindexResponse(**stats)


@router.post("/ingest/ted")
def ingest_ted():
    ted_not_implemented()


@router.post("/ingest/apify")
def ingest_apify():
    apify_not_implemented()
