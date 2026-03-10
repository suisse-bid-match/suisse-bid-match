from __future__ import annotations

import re
from collections.abc import Iterable


LABEL_MAP: dict[str, str] = {
    "anbieter": "company_name",
    "firma": "company_name",
    "firmenbezeichnung": "company_name",
    "adresse plz ort": "company_address_full",
    "adresse": "company_address_full",
    "kontaktperson": "contact_name",
    "verantwortliche person": "contact_name",
    "telefon": "contact_phone",
    "e mail": "contact_email",
    "email": "contact_email",
    "mwst nr uid": "uid_vat",
    "uid": "uid_vat",
    "rechtsform": "legal_form",
    "geschaftszweck": "business_purpose",
    "haupttatigkeit": "business_activity",
    "zertifikate": "certifications",
    "angebotspreis": "offer_price_gross_excl_vat",
    "eingabesumme": "offer_price_gross_excl_vat",
    "rabatt": "discount_percent",
    "skonto": "skonto_percent",
    "allgemeine bauabzuge": "general_deduction_percent",
    "mwst": "vat_percent",
    "ort datum": "sign_place_date",
    "unterschrift": "signature",
    "objektbezeichnung": "reference_project_name",
    "bauherrschaft": "reference_client",
    "ausfuhrungszeit": "reference_year",
    "auftragssumme": "reference_amount_chf",
    "referenzperson": "reference_contact",
    "telefonnummer der referenzperson": "reference_contact_phone",
}

CRITICAL_KEYS: set[str] = {
    "company_name",
    "company_address_full",
    "contact_name",
    "contact_phone",
    "contact_email",
    "uid_vat",
}


AMOUNT_KEYS: set[str] = {
    "offer_price_gross_excl_vat",
    "reference_amount_chf",
    "discount_percent",
    "skonto_percent",
    "general_deduction_percent",
    "vat_percent",
}


def normalize_label(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("/", " ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9äöüß ]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def semantic_key_from_label(label: str) -> str:
    normalized = normalize_label(label)
    for key, semantic in LABEL_MAP.items():
        if key in normalized:
            return semantic
    if normalized:
        return f"custom_{normalized.replace(' ', '_')}"
    return "unknown"


def is_critical_key(semantic_key: str) -> bool:
    return semantic_key in CRITICAL_KEYS


def is_amount_key(semantic_key: str) -> bool:
    return semantic_key in AMOUNT_KEYS


def looks_like_heading(line: str) -> bool:
    return bool(re.match(r"^\s*\d+(\.\d+)*\s+", line))


def safe_text(parts: Iterable[str]) -> str:
    joined = " ".join(p for p in parts if p)
    joined = re.sub(r"\s+", " ", joined)
    return joined.strip()
