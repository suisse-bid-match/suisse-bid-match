from __future__ import annotations

import sys
import types

from apps.api.core.config import Settings
from apps.api.services.agent.orchestrator import _llm_answer, _llm_select_matches
from apps.api.services.retrieval.embeddings import EmbeddingService
from apps.api.services.retrieval.hybrid import RetrievalCandidate


def test_settings_key_resolution_precedence():
    settings = Settings(
        _env_file=None,
        openai_api_key="legacy-key",
        openai_chat_api_key="chat-key",
        openai_embedding_api_key="embed-key",
    )
    assert settings.resolved_openai_chat_api_key == "chat-key"
    assert settings.resolved_openai_embedding_api_key == "embed-key"


def test_settings_key_resolution_fallback_to_legacy():
    settings = Settings(_env_file=None, openai_api_key="legacy-key")
    assert settings.resolved_openai_chat_api_key == "legacy-key"
    assert settings.resolved_openai_embedding_api_key == "legacy-key"


def test_embedding_service_stays_local_when_only_chat_key(monkeypatch):
    class FakeSettings:
        embedding_backend = "auto"
        resolved_openai_embedding_api_key = None
        openai_embedding_model = "text-embedding-3-small"
        local_embedding_model = "sentence-transformers/all-MiniLM-L6-v2"

    monkeypatch.setattr("apps.api.services.retrieval.embeddings.get_settings", lambda: FakeSettings())
    svc = EmbeddingService()
    assert svc.backend == "local"
    assert svc.dimension == 384


def test_embedding_service_openai_uses_embedding_key(monkeypatch):
    captured: dict[str, str] = {}

    class FakeOpenAI:
        def __init__(self, api_key):
            captured["api_key"] = api_key

    fake_module = types.ModuleType("openai")
    fake_module.OpenAI = FakeOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake_module)

    class FakeSettings:
        embedding_backend = "openai"
        resolved_openai_embedding_api_key = "embed-key"
        openai_embedding_model = "text-embedding-3-small"
        local_embedding_model = "sentence-transformers/all-MiniLM-L6-v2"

    monkeypatch.setattr("apps.api.services.retrieval.embeddings.get_settings", lambda: FakeSettings())
    svc = EmbeddingService()
    svc._get_openai_client()
    assert svc.backend == "openai"
    assert captured["api_key"] == "embed-key"


def test_llm_answer_uses_chat_key_and_returns_content(monkeypatch):
    captured: dict[str, str] = {}

    class FakeCompletions:
        @staticmethod
        def create(**kwargs):
            captured["model"] = kwargs["model"]
            message = types.SimpleNamespace(content="grounded answer")
            choice = types.SimpleNamespace(message=message)
            return types.SimpleNamespace(choices=[choice])

    class FakeChat:
        completions = FakeCompletions()

    class FakeOpenAI:
        def __init__(self, api_key):
            captured["api_key"] = api_key
            self.chat = FakeChat()

    fake_module = types.ModuleType("openai")
    fake_module.OpenAI = FakeOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake_module)

    fake_settings = types.SimpleNamespace(
        resolved_openai_chat_api_key="chat-key",
        openai_chat_model="gpt-4o-mini",
    )
    monkeypatch.setattr("apps.api.services.agent.orchestrator.get_settings", lambda: fake_settings)

    candidates = [
        RetrievalCandidate(
            chunk_id="c1",
            notice_id="n1",
            doc_id=None,
            title="t1",
            url=None,
            doc_url=None,
            text="evidence text",
            dense_score=1.0,
            final_score=1.0,
        )
    ]
    answer = _llm_answer("question", candidates)
    assert answer == "grounded answer"
    assert captured["api_key"] == "chat-key"
    assert captured["model"] == "gpt-4o-mini"


def test_llm_answer_returns_none_without_any_chat_key(monkeypatch):
    fake_settings = types.SimpleNamespace(
        resolved_openai_chat_api_key=None,
        openai_chat_model="gpt-4o-mini",
    )
    monkeypatch.setattr("apps.api.services.agent.orchestrator.get_settings", lambda: fake_settings)
    answer = _llm_answer("question", [])
    assert answer is None


def test_llm_answer_omits_temperature_for_gpt5_models(monkeypatch):
    captured: dict[str, object] = {}

    class FakeCompletions:
        @staticmethod
        def create(**kwargs):
            captured["kwargs"] = kwargs
            message = types.SimpleNamespace(content="grounded answer")
            choice = types.SimpleNamespace(message=message)
            return types.SimpleNamespace(choices=[choice])

    class FakeChat:
        completions = FakeCompletions()

    class FakeOpenAI:
        def __init__(self, api_key):
            captured["api_key"] = api_key
            self.chat = FakeChat()

    fake_module = types.ModuleType("openai")
    fake_module.OpenAI = FakeOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake_module)

    fake_settings = types.SimpleNamespace(
        resolved_openai_chat_api_key="chat-key",
        openai_chat_model="gpt-5-mini",
    )
    monkeypatch.setattr("apps.api.services.agent.orchestrator.get_settings", lambda: fake_settings)

    candidates = [
        RetrievalCandidate(
            chunk_id="c1",
            notice_id="n1",
            doc_id=None,
            title="t1",
            url=None,
            doc_url=None,
            text="evidence text",
            dense_score=1.0,
            final_score=1.0,
        )
    ]
    _llm_answer("question", candidates)
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert "temperature" not in kwargs


def test_llm_select_matches_omits_temperature_and_returns_reason_for_gpt5(monkeypatch):
    captured_calls: list[dict[str, object]] = []

    class FakeCompletions:
        @staticmethod
        def create(**kwargs):
            captured_calls.append(kwargs)
            message = types.SimpleNamespace(
                content='{"matches":[{"index":1,"reason":"Buyer and scope match","matching_points":["same buyer","IT scope"],"confidence":0.92}]}'
            )
            choice = types.SimpleNamespace(message=message)
            return types.SimpleNamespace(choices=[choice])

    class FakeChat:
        completions = FakeCompletions()

    class FakeOpenAI:
        def __init__(self, api_key):
            self.chat = FakeChat()

    fake_module = types.ModuleType("openai")
    fake_module.OpenAI = FakeOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake_module)

    fake_settings = types.SimpleNamespace(
        resolved_openai_chat_api_key="chat-key",
        openai_chat_model="gpt-5-mini",
        llm_matching_enabled=True,
        llm_match_pool_size=12,
    )
    monkeypatch.setattr("apps.api.services.agent.orchestrator.get_settings", lambda: fake_settings)

    candidates = [
        RetrievalCandidate(
            chunk_id="c1",
            notice_id="n1",
            doc_id=None,
            title="IT Services",
            url=None,
            doc_url=None,
            text="IT tender for Zurich municipality.",
            dense_score=0.8,
            bm25_score=0.2,
            final_score=0.75,
            metadata={"buyer_name": "Zurich", "region": "ZH", "language": "de"},
        )
    ]

    selected, meta_by_chunk_id = _llm_select_matches("Find Zurich IT tenders", candidates, top_k=1)
    assert len(selected) == 1
    assert selected[0].chunk_id == "c1"
    assert meta_by_chunk_id["c1"]["llm_reason"] == "Buyer and scope match"
    assert all("temperature" not in call for call in captured_calls)
