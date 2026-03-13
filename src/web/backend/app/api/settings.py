from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..config import Settings, get_settings
from ..db import get_db
from ..repositories.app_settings import AppSettingsRepository
from ..schemas import ModelSettingsResponse, SetModelRequest


router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("/model", response_model=ModelSettingsResponse)
def get_model_settings(
    db: Annotated[Session, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> ModelSettingsResponse:
    repo = AppSettingsRepository(db)
    model = repo.get_current_openai_model(
        default_model=settings.openai_model,
        allowed_models=settings.allowed_openai_models,
    )
    return ModelSettingsResponse(
        current_model=model,
        allowed_models=settings.allowed_openai_models,
        has_api_key=bool(settings.openai_api_key),
    )


@router.put("/model", response_model=ModelSettingsResponse)
def set_model_settings(
    request: SetModelRequest,
    db: Annotated[Session, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> ModelSettingsResponse:
    repo = AppSettingsRepository(db)
    try:
        model = repo.set_current_openai_model(request.model, allowed_models=settings.allowed_openai_models)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return ModelSettingsResponse(
        current_model=model,
        allowed_models=settings.allowed_openai_models,
        has_api_key=bool(settings.openai_api_key),
    )
