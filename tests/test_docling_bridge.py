from __future__ import annotations

from pathlib import Path

from app.core.docling_client import DoclingClientError
from app.ingestion import docling_bridge


def test_docling_bridge_uses_cooldown_after_failure(tmp_path: Path, monkeypatch) -> None:
    calls = {"count": 0}

    def _failing_extract(path: Path, limit: int) -> list[str]:
        calls["count"] += 1
        raise DoclingClientError(code="DOCLING_UNAVAILABLE", message="offline")

    monkeypatch.setattr(docling_bridge, "extract_cues", _failing_extract)
    docling_bridge._fail_cooldown_until = 0.0

    source = tmp_path / "sample.docx"
    source.write_text("placeholder", encoding="utf-8")

    assert docling_bridge.extract_docling_cues(source) == []
    assert calls["count"] == 1

    # Immediate second call should short-circuit from cooldown.
    assert docling_bridge.extract_docling_cues(source) == []
    assert calls["count"] == 1


def test_docling_bridge_health_result_is_cached(monkeypatch) -> None:
    calls = {"count": 0}

    def _health() -> bool:
        calls["count"] += 1
        return True

    monkeypatch.setattr(docling_bridge, "healthz", _health)
    docling_bridge._last_health_check_at = 0.0
    docling_bridge._last_health_ok = False

    assert docling_bridge.docling_available() is True
    assert docling_bridge.docling_available() is True
    assert calls["count"] == 1


def test_docling_bridge_warmup_is_cached(monkeypatch) -> None:
    calls = {"health": 0, "warmup": 0}

    def _health() -> bool:
        calls["health"] += 1
        return True

    def _warmup() -> None:
        calls["warmup"] += 1

    monkeypatch.setattr(docling_bridge, "healthz", _health)
    monkeypatch.setattr(docling_bridge, "warmup", _warmup)
    docling_bridge._last_health_check_at = 0.0
    docling_bridge._last_health_ok = False
    docling_bridge._last_warmup_at = 0.0
    docling_bridge._last_warmup_ok = False
    docling_bridge._fail_cooldown_until = 0.0

    ok, reason = docling_bridge.ensure_docling_ready()
    assert ok is True
    assert reason is None

    ok, reason = docling_bridge.ensure_docling_ready()
    assert ok is True
    assert reason is None
    assert calls["health"] == 1
    assert calls["warmup"] == 1
