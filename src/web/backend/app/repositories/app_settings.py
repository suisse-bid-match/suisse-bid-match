from __future__ import annotations

from sqlalchemy.orm import Session

from ..models import AppSetting


CURRENT_OPENAI_MODEL_KEY = "current_openai_model"


class AppSettingsRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_setting(self, key: str) -> AppSetting | None:
        return self.db.get(AppSetting, key)

    def get_value(self, key: str):
        row = self.get_setting(key)
        if row is None:
            return None
        return row.value

    def set_value(self, key: str, value) -> AppSetting:
        row = self.get_setting(key)
        if row is None:
            row = AppSetting(key=key, value=value)
        else:
            row.value = value
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    def get_current_openai_model(self, *, default_model: str, allowed_models: list[str]) -> str:
        value = self.get_value(CURRENT_OPENAI_MODEL_KEY)
        if isinstance(value, str) and value in allowed_models:
            return value
        fallback = default_model if default_model in allowed_models else allowed_models[0]
        self.set_value(CURRENT_OPENAI_MODEL_KEY, fallback)
        return fallback

    def set_current_openai_model(self, model: str, *, allowed_models: list[str]) -> str:
        if model not in allowed_models:
            raise ValueError(f"unsupported model: {model}")
        self.set_value(CURRENT_OPENAI_MODEL_KEY, model)
        return model
