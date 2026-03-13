from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "core"))

from pipeline.mysql_client import fetch_schema_metadata, run_mysql_query  # noqa: E402


class TestMySQLClient(unittest.TestCase):
    @patch("pipeline.mysql_client.pymysql.connect")
    def test_run_mysql_query_formats_tsv(self, mock_connect):
        cursor = MagicMock()
        cursor.description = [("product_id",), ("product_name",)]
        cursor.fetchall.return_value = [("1", "Lamp A"), ("2", "Lamp B")]

        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        connection.cursor.return_value.__exit__.return_value = False
        mock_connect.return_value = connection

        output, elapsed_ms = run_mysql_query("host", "user", "pwd", "db", "SELECT 1")
        self.assertGreaterEqual(elapsed_ms, 0)
        self.assertIn("product_id\tproduct_name", output)
        self.assertIn("1\tLamp A", output)

    @patch("pipeline.mysql_client.run_mysql_query")
    def test_fetch_schema_metadata_parses_columns(self, mock_run):
        mock_run.return_value = (
            "table_name\tcolumn_name\tdata_type\n"
            "vw_bid_specs\tugr\tdouble\n"
            "vw_bid_specs\tcri\tbigint\n",
            2,
        )
        payload = fetch_schema_metadata("host", "user", "pwd", "db", ["vw_bid_specs"])
        self.assertEqual(len(payload["tables"]), 1)
        self.assertEqual(payload["tables"][0]["name"], "vw_bid_specs")
        self.assertEqual(payload["tables"][0]["columns"][0]["name"], "ugr")


if __name__ == "__main__":
    unittest.main()
