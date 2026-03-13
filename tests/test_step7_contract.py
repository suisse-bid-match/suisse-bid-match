from __future__ import annotations

import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "core"))

from pipeline.contracts import validate_step7_data  # noqa: E402


class TestStep7Contract(unittest.TestCase):
    def test_accepts_legacy_ranked_candidates_and_normalizes(self):
        payload = {
            "match_results": [
                {
                    "product_key": "item_001",
                    "ranked_candidates": [
                        {
                            "rank": 1,
                            "db_product_id": 123,
                            "db_product_name": "Lamp",
                            "passes_hard": True,
                            "soft_match_score": 1.0,
                            "matched_soft_constraints": [],
                            "unmet_soft_constraints": [],
                            "explanation": "ok",
                        }
                    ],
                }
            ]
        }

        normalized = validate_step7_data(payload)
        row = normalized["match_results"][0]
        self.assertIn("candidates", row)
        self.assertNotIn("ranked_candidates", row)
        self.assertEqual(row["candidates"][0]["db_product_id"], 123)

    def test_accepts_llm_execution_summary(self):
        payload = {
            "match_results": [],
            "llm_execution": {
                "step_name": "step7_rank_candidates",
                "request_started_at": "2026-03-13T20:00:00Z",
                "request_finished_at": "2026-03-13T20:00:03Z",
                "duration_ms": 3000,
                "final_status": "failed",
                "response_received": False,
                "fallback_used": True,
                "failure_message": "context_length_exceeded",
                "reasoning_summary": None,
                "reasoning_chars": 0,
                "stream_event_counts": {"status": 2},
                "status_events": ["llm_request_started", "llm_request_failed"],
            },
        }

        normalized = validate_step7_data(payload)
        self.assertEqual(normalized["llm_execution"]["step_name"], "step7_rank_candidates")
        self.assertTrue(normalized["llm_execution"]["fallback_used"])


if __name__ == "__main__":
    unittest.main()
