#!/usr/bin/env python
from __future__ import annotations

import pathlib
import sys
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.core.config import get_settings
from apps.api.core.logging import setup_logging
from apps.api.models.db import SessionLocal
from apps.api.services.connectors.simap_ingest import ingest_simap_publications
from apps.api.services.indexing.qdrant_client import ensure_collection


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    ensure_collection()

    with SessionLocal() as db:
        result = ingest_simap_publications(
            db,
            updated_since=datetime.now(timezone.utc) - timedelta(days=30),
            limit=30,
        )

    print("Seed result:")
    for k, v in result.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
