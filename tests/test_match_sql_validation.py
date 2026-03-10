from __future__ import annotations

from app.match.sql_validator import validate_readonly_select


TABLES = {"match_products", "match_specs", "match_certs", "match_assets"}
FIELDS = {
    "match_products.product_id",
    "match_products.product_name",
    "match_specs.product_id",
    "match_specs.electrical_power_w",
    "match_specs.ugr",
    "match_certs.product_id",
    "match_certs.ce",
    "match_assets.product_id",
    "match_assets.datasheet_path",
}


def test_sql_validator_accepts_select_with_limit_and_whitelisted_fields() -> None:
    sql = (
        "SELECT p.product_id, p.product_name, s.electrical_power_w, s.ugr, c.ce, a.datasheet_path "
        "FROM match_products p "
        "LEFT JOIN match_specs s ON s.product_id = p.product_id "
        "LEFT JOIN match_certs c ON c.product_id = p.product_id "
        "LEFT JOIN match_assets a ON a.product_id = p.product_id "
        "WHERE s.electrical_power_w >= :req_1_gte LIMIT :limit"
    )
    errors = validate_readonly_select(sql=sql, table_whitelist=TABLES, field_whitelist=FIELDS)
    assert errors == []


def test_sql_validator_rejects_non_select_and_stacked_statements() -> None:
    sql = "SELECT product_id FROM match_products LIMIT 5; DROP TABLE match_products"
    errors = validate_readonly_select(sql=sql, table_whitelist=TABLES, field_whitelist=FIELDS)
    assert any("multiple statements" in item.lower() for item in errors)


def test_sql_validator_rejects_non_whitelisted_table() -> None:
    sql = "SELECT p.product_id FROM match_products p JOIN secret_table s ON s.id = p.product_id LIMIT 5"
    errors = validate_readonly_select(sql=sql, table_whitelist=TABLES, field_whitelist=FIELDS)
    assert any("table not allowed" in item for item in errors)


def test_sql_validator_rejects_missing_limit() -> None:
    sql = "SELECT p.product_id FROM match_products p WHERE p.product_id IS NOT NULL"
    errors = validate_readonly_select(sql=sql, table_whitelist=TABLES, field_whitelist=FIELDS)
    assert any("limit" in item.lower() for item in errors)
