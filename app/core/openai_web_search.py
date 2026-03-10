from __future__ import annotations

import json
from typing import Any

from app.core.settings import settings


def build_web_search_kwargs() -> dict[str, Any]:
    if not settings.openai_web_search_enabled:
        return {}
    tool = {"type": "web_search", "external_web_access": bool(settings.openai_web_search_external)}
    tool_choice = settings.openai_web_search_tool_choice or "auto"
    return {
        "tools": [tool],
        "tool_choice": tool_choice,
        "include": ["web_search_call.action.sources"],
    }


def _response_to_dict(response: Any) -> dict[str, Any]:
    if response is None:
        return {}
    if isinstance(response, dict):
        return response
    for attr in ("model_dump", "dict"):
        handler = getattr(response, attr, None)
        if callable(handler):
            try:
                return handler()
            except Exception:
                pass
    handler = getattr(response, "json", None)
    if callable(handler):
        try:
            return json.loads(handler())
        except Exception:
            return {}
    return {}


def _collect_web_search_sources(node: Any, out: list[dict[str, Any]]) -> None:
    if isinstance(node, dict):
        if node.get("type") == "web_search_call":
            action = node.get("action")
            if isinstance(action, dict):
                sources = action.get("sources")
                if isinstance(sources, list):
                    for item in sources:
                        out.append(_normalize_source(item))
        for value in node.values():
            _collect_web_search_sources(value, out)
        return
    if isinstance(node, list):
        for item in node:
            _collect_web_search_sources(item, out)


def _collect_web_search_queries(node: Any, out: list[str]) -> None:
    if isinstance(node, dict):
        if node.get("type") == "web_search_call":
            action = node.get("action")
            if isinstance(action, dict):
                queries = action.get("queries")
                if isinstance(queries, list):
                    for q in queries:
                        if isinstance(q, str):
                            out.append(q)
                query = action.get("query")
                if isinstance(query, str):
                    out.append(query)
        for value in node.values():
            _collect_web_search_queries(value, out)
        return
    if isinstance(node, list):
        for item in node:
            _collect_web_search_queries(item, out)


def _collect_citations(node: Any, out: list[dict[str, Any]]) -> None:
    if isinstance(node, dict):
        annotations = node.get("annotations")
        if isinstance(annotations, list):
            for item in annotations:
                if not isinstance(item, dict):
                    continue
                if item.get("type") != "url_citation":
                    continue
                out.append(_normalize_citation(item))
        for value in node.values():
            _collect_citations(value, out)
        return
    if isinstance(node, list):
        for item in node:
            _collect_citations(item, out)


def _normalize_source(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {"raw": raw}
    return {
        "url": raw.get("url"),
        "title": raw.get("title"),
        "snippet": raw.get("snippet"),
        "source": raw.get("source"),
    }


def _normalize_citation(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "url": raw.get("url"),
        "title": raw.get("title"),
        "start_index": raw.get("start_index"),
        "end_index": raw.get("end_index"),
    }


def _dedupe(items: list[dict[str, Any]], key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    unique: list[dict[str, Any]] = []
    for item in items:
        key = tuple(item.get(field) for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def extract_web_search_info(response: Any) -> dict[str, Any] | None:
    payload = _response_to_dict(response)
    if not payload:
        return None

    sources: list[dict[str, Any]] = []
    queries: list[str] = []
    citations: list[dict[str, Any]] = []

    _collect_web_search_sources(payload, sources)
    _collect_web_search_queries(payload, queries)
    _collect_citations(payload, citations)

    sources = _dedupe(sources, ("url", "title"))
    citations = _dedupe(citations, ("url", "title", "start_index", "end_index"))
    queries = list(dict.fromkeys([q.strip() for q in queries if q and q.strip()]))

    if not sources and not citations and not queries:
        return None

    return {
        "queries": queries[:10],
        "sources": sources[:20],
        "citations": citations[:20],
    }
