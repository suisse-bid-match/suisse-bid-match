from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.repositories.app_settings import AppSettingsRepository


def _session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    testing_session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return testing_session()


def test_get_current_openai_model_seeds_default() -> None:
    db = _session()
    repo = AppSettingsRepository(db)
    model = repo.get_current_openai_model(
        default_model="gpt-5-mini",
        allowed_models=["gpt-5.4", "gpt-5-mini"],
    )
    assert model == "gpt-5-mini"


def test_set_current_openai_model_persists() -> None:
    db = _session()
    repo = AppSettingsRepository(db)
    repo.set_current_openai_model("gpt-5.4", allowed_models=["gpt-5.4", "gpt-5-mini"])
    model = repo.get_current_openai_model(
        default_model="gpt-5-mini",
        allowed_models=["gpt-5.4", "gpt-5-mini"],
    )
    assert model == "gpt-5.4"


def test_set_current_openai_model_rejects_unsupported() -> None:
    db = _session()
    repo = AppSettingsRepository(db)
    try:
        repo.set_current_openai_model("gpt-4.1", allowed_models=["gpt-5.4", "gpt-5-mini"])
    except ValueError as exc:
        assert "unsupported model" in str(exc)
        return
    raise AssertionError("expected ValueError")
