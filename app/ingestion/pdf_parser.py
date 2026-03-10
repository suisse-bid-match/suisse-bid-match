from __future__ import annotations

import re
from pathlib import Path

from pypdf import PdfReader

from app.core.models import PdfInsight
from app.core.semantic import looks_like_heading


DEADLINE_PATTERNS = (
    r"\b\d{1,2}\.\d{1,2}\.\d{4}\b",
    r"\b\d{1,2}\.\s*[A-Za-zäöüÄÖÜ]+\s*\d{4}\b",
)

REQUIRED_DOC_HINTS = (
    "formular",
    "einzureichen",
    "deckblatt",
    "zusatzformular",
    "selbstdeklaration",
    "nachweis",
)

CRITERIA_HINTS = (
    "eignungskriterien",
    "zuschlagskriterien",
    "beurteilung",
)


def parse_pdf_insight(pdf_path: Path) -> PdfInsight:
    reader = PdfReader(str(pdf_path))
    lines: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if line:
                lines.append(line)

    sections: list[dict[str, str]] = []
    current_title = "Document"
    current_body: list[str] = []

    for line in lines:
        if looks_like_heading(line):
            if current_body:
                sections.append(
                    {
                        "title": current_title,
                        "summary": " ".join(current_body[:4])[:400],
                    }
                )
            current_title = line
            current_body = []
        else:
            current_body.append(line)

    if current_body:
        sections.append({"title": current_title, "summary": " ".join(current_body[:4])[:400]})

    deadline_lines: list[str] = []
    for line in lines:
        lower = line.lower()
        if any(k in lower for k in ("eingabe", "frist", "termin", "fragen", "beantwortung")):
            if any(re.search(pattern, line) for pattern in DEADLINE_PATTERNS):
                deadline_lines.append(line)

    required_lines = [
        line for line in lines if any(h in line.lower() for h in REQUIRED_DOC_HINTS)
    ]

    criteria_lines = [line for line in lines if any(h in line.lower() for h in CRITERIA_HINTS)]

    title = lines[0] if lines else pdf_path.name

    return PdfInsight(
        title=title,
        deadline_lines=deadline_lines[:20],
        required_document_lines=required_lines[:50],
        criteria_lines=criteria_lines[:50],
        sections=sections[:120],
    )
