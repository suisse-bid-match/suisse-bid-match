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
                    "name": "match_specs",
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
                        {"field": "match_specs.ugr", "value": 19, "unit": None, "source": None, "extraction_confidence": 0.9},
                        {"field": "match_specs.ugr", "value": 20, "unit": None, "source": None, "extraction_confidence": 0.8},
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
                            "field": "match_specs.ugr",
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


if __name__ == "__main__":
    unittest.main()

