from __future__ import annotations

import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "core"))

from pipeline.matching import build_fallback_step7  # noqa: E402


class TestMatchingUnknownHandling(unittest.TestCase):
    def test_zero_is_treated_as_unknown_for_soft_score(self):
        step4_data = {
            "tender_products": [
                {
                    "product_key": "item_001",
                    "requirements": [
                        {
                            "field": "vw_bid_specs.ip_rating",
                            "operator": "gte",
                            "value": 44,
                            "is_hard": False,
                        }
                    ],
                }
            ]
        }
        step6_data = {
            "results": [
                {
                    "product_key": "item_001",
                    "rows": [
                        {"product_id": 1, "product_name": "Lamp A", "ip_rating": 0},
                        {"product_id": 2, "product_name": "Lamp B", "ip_rating": 54},
                    ],
                }
            ]
        }

        payload = build_fallback_step7(step4_data, step6_data)
        candidates = payload["match_results"][0]["candidates"]
        self.assertEqual(candidates[0]["db_product_id"], 2)
        self.assertEqual(candidates[0]["soft_match_score"], 1.0)
        self.assertEqual(candidates[1]["db_product_id"], 1)
        self.assertEqual(candidates[1]["unmet_soft_constraints"], [])


if __name__ == "__main__":
    unittest.main()
