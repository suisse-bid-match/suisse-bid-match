from __future__ import annotations

import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "core"))

from pipeline.sql_builder import build_step5_sql  # noqa: E402


class TestStep5SQL(unittest.TestCase):
    def test_only_hard_with_operator_is_used_for_where(self):
        step4_data = {
            "tender_products": [
                {
                    "product_key": "item_001",
                    "requirements": [
                        {"field": "match_specs.ugr", "operator": "lte", "value": 19, "is_hard": True},
                        {"field": "match_specs.cri", "value": 80, "is_hard": True},  # no operator -> skip
                        {"field": "match_specs.ip_rating", "operator": "gte", "value": 44, "is_hard": False},  # soft -> skip in where
                    ],
                }
            ]
        }
        schema = {
            "tables": [
                {
                    "name": "match_products",
                    "columns": [
                        {"name": "product_id", "type": "bigint"},
                        {"name": "product_name", "type": "text"},
                        {"name": "article_number", "type": "varchar"},
                        {"name": "manufacturer_name", "type": "varchar"},
                        {"name": "is_current", "type": "tinyint"},
                    ],
                },
                {
                    "name": "match_specs",
                    "columns": [
                        {"name": "product_id", "type": "bigint"},
                        {"name": "ugr", "type": "double"},
                        {"name": "cri", "type": "bigint"},
                        {"name": "ip_rating", "type": "smallint"},
                        {"name": "is_current", "type": "tinyint"},
                    ],
                },
            ]
        }
        payload = build_step5_sql(step4_data, schema, join_key="product_id")
        self.assertEqual(len(payload["queries"]), 1)
        sql = payload["queries"][0]["sql"]
        self.assertIn("ms.ugr <= 19", sql)
        self.assertNotIn("ms.cri", sql.split("WHERE", 1)[1])  # no hard operator
        self.assertNotIn("ms.ip_rating >= 44", sql)


if __name__ == "__main__":
    unittest.main()

