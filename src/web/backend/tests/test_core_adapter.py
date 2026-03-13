from __future__ import annotations

from app.services.core_adapter import _parse_llm_progress_line


def test_parse_llm_progress_line_accepts_prefixed_json() -> None:
    line = 'LLM_PROGRESS::{"step_name":"step2_extract_requirements","kind":"status","status":"llm_request_started"}\n'
    payload = _parse_llm_progress_line(line)
    assert payload is not None
    assert payload["step_name"] == "step2_extract_requirements"
    assert payload["status"] == "llm_request_started"


def test_parse_llm_progress_line_ignores_non_prefixed_line() -> None:
    assert _parse_llm_progress_line("normal log line\n") is None
