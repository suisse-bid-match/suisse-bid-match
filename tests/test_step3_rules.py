from __future__ import annotations

import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "core"))

from pipeline.contracts import validate_step3_data  # noqa: E402


class TestStep3Rules(unittest.TestCase):
    def test_duplicate_field_rules_are_rejected(self):
        payload = {
            "field_rules": [
                {
                    "field": "vw_bid_specs.ugr",
                    "operator": "lte",
                    "is_hard": True,
                    "operator_confidence": 0.9,
                    "hardness_confidence": 0.9,
                    "rationale": "x",
                },
                {
                    "field": "vw_bid_specs.ugr",
                    "operator": "eq",
                    "is_hard": False,
                    "operator_confidence": 0.8,
                    "hardness_confidence": 0.8,
                    "rationale": "y",
                },
            ]
        }
        with self.assertRaises(Exception):
            validate_step3_data(payload)

    def test_invalid_operator_is_rejected(self):
        payload = {
            "field_rules": [
                {
                    "field": "vw_bid_specs.ugr",
                    "operator": "approx",
                    "is_hard": True,
                    "operator_confidence": 0.9,
                    "hardness_confidence": 0.9,
                    "rationale": "x",
                }
            ]
        }
        with self.assertRaises(Exception):
            validate_step3_data(payload)

    def test_non_schema_field_is_rejected(self):
        payload = {
            "field_rules": [
                {
                    "field": "vw_bid_specs.unknown",
                    "operator": "eq",
                    "is_hard": True,
                    "operator_confidence": 0.9,
                    "hardness_confidence": 0.9,
                    "rationale": "x",
                }
            ]
        }
        with self.assertRaises(Exception):
            validate_step3_data(payload, allowed_fields={"vw_bid_specs.ugr"})


if __name__ == "__main__":
    unittest.main()
