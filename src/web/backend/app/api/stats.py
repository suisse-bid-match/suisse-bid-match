from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas import StatsDashboardResponse
from ..services.stats import build_stats_dashboard


router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("/dashboard", response_model=StatsDashboardResponse)
def get_stats_dashboard(
    db: Annotated[Session, Depends(get_db)],
    days: Annotated[int, Query(ge=1, le=365)] = 30,
    include_failed: bool = True,
    top_n: Annotated[int, Query(ge=1, le=200)] = 40,
) -> StatsDashboardResponse:
    return build_stats_dashboard(
        db,
        days=days,
        include_failed=include_failed,
        top_n=top_n,
    )
