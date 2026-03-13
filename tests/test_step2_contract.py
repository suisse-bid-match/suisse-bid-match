from __future__ import annotations

import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "core"))

from pipeline.contracts import validate_step2_data  # noqa: E402


class TestStep2Contract(unittest.TestCase):
    def _schema(self) -> dict:
        return {
            "tables": [
                {
                    "name": "vw_bid_specs",
                    "columns": [{"name": "ugr", "type": "double"}],
                }
            ]
        }

    def test_duplicate_field_in_same_product_is_rejected(self):
        payload = {
            "schema_snapshot": self._schema(),
            "tender_products": [
                {
                    "product_key": "item_001",
                    "requirements": [
                        {"field": "vw_bid_specs.ugr", "value": 19, "unit": None, "source": None, "extraction_confidence": 0.9},
                        {"field": "vw_bid_specs.ugr", "value": 20, "unit": None, "source": None, "extraction_confidence": 0.8},
                    ],
                }
            ],
        }
        with self.assertRaises(Exception):
            validate_step2_data(payload)

    def test_operator_must_not_exist_in_step2_requirement(self):
        payload = {
            "schema_snapshot": self._schema(),
            "tender_products": [
                {
                    "product_key": "item_001",
                    "requirements": [
                        {
                            "field": "vw_bid_specs.ugr",
                            "value": 19,
                            "unit": None,
                            "source": None,
                            "extraction_confidence": 0.9,
                            "operator": "lte",
                        }
                    ],
                }
            ],
        }
        with self.assertRaises(Exception):
            validate_step2_data(payload)

    def test_accepts_llm_execution_summary(self):
        payload = {
            "schema_snapshot": self._schema(),
            "tender_products": [
                {
                    "product_key": "item_001",
                    "requirements": [
                        {"field": "vw_bid_specs.ugr", "value": 19, "unit": None, "source": None, "extraction_confidence": 0.9}
                    ],
                }
            ],
            "llm_execution": {
                "step_name": "step2_extract_requirements",
                "request_started_at": "2026-03-13T20:00:00Z",
                "request_finished_at": "2026-03-13T20:00:01Z",
                "duration_ms": 1000,
                "final_status": "succeeded",
                "response_received": True,
                "fallback_used": False,
                "failure_message": None,
                "reasoning_summary": "ok",
                "reasoning_chars": 2,
                "stream_event_counts": {"reasoning_summary_delta": 1},
                "status_events": ["llm_request_started", "llm_response_received"],
            },
        }
        normalized = validate_step2_data(payload)
        self.assertEqual(normalized["llm_execution"]["step_name"], "step2_extract_requirements")


if __name__ == "__main__":
    unittest.main()
