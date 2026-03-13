from __future__ import annotations

import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "core"))

from pipeline.contracts import validate_step4_data  # noqa: E402
from pipeline.sql_builder import build_step4_merged  # noqa: E402


class TestStep4Merge(unittest.TestCase):
    def test_global_field_rule_applies_across_products_and_missing_is_skipped(self):
        step2_data = {
            "schema_snapshot": {"tables": []},
            "tender_products": [
                {
                    "product_key": "item_001",
                    "product_name": "A",
                    "quantity": 1,
                    "requirements": [
                        {
                            "requirement_id": "item_001.req_0001",
                            "field": "vw_bid_specs.ugr",
                            "value": 19,
                            "unit": None,
                            "source": None,
                            "extraction_confidence": 0.9,
                        },
                        {
                            "requirement_id": "item_001.req_0002",
                            "field": "vw_bid_specs.cri",
                            "value": 80,
                            "unit": None,
                            "source": None,
                            "extraction_confidence": 0.9,
                        },
                    ],
                },
                {
                    "product_key": "item_002",
                    "product_name": "B",
                    "quantity": 2,
                    "requirements": [
                        {
                            "requirement_id": "item_002.req_0001",
                            "field": "vw_bid_specs.ugr",
                            "value": 22,
                            "unit": None,
                            "source": None,
                            "extraction_confidence": 0.9,
                        }
                    ],
                },
            ],
        }
        step3_data = {
            "field_rules": [
                {
                    "field": "vw_bid_specs.ugr",
                    "operator": "lte",
                    "is_hard": True,
                    "operator_confidence": 0.95,
                    "hardness_confidence": 0.96,
                    "rationale": "rule",
                }
            ]
        }
        merged = build_step4_merged(step2_data, step3_data)
        validated = validate_step4_data(merged)

        req1 = validated["tender_products"][0]["requirements"][0]
        req2 = validated["tender_products"][1]["requirements"][0]
        self.assertEqual(req1["operator"], "lte")
        self.assertTrue(req1["is_hard"])
        self.assertEqual(req2["operator"], "lte")
        self.assertTrue(req2["is_hard"])

        skipped = validated["skipped_requirements"]
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["field"], "vw_bid_specs.cri")
        self.assertEqual(skipped[0]["reason"], "missing_step3_field_rule")


if __name__ == "__main__":
    unittest.main()
