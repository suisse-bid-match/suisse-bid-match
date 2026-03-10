from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _disable_live_openai(monkeypatch: pytest.MonkeyPatch) -> None:
    # Test suite should stay deterministic and not depend on outbound API calls.
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    from app.core.settings import settings

    monkeypatch.setattr(settings, "openai_api_key", "")
