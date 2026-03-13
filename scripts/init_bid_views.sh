#!/usr/bin/env sh
set -eu

MYSQL_HOST="${PIM_MYSQL_HOST:-mysql}"
MYSQL_PORT="${PIM_MYSQL_PORT:-3306}"
MYSQL_USER="${PIM_MYSQL_USER:-root}"
MYSQL_PASSWORD="${PIM_MYSQL_PASSWORD:-root}"
MYSQL_DB="${PIM_MYSQL_DB:-pim_raw}"

echo "[mysql-views-init] Waiting for MySQL at ${MYSQL_HOST}:${MYSQL_PORT} ..."
ATTEMPT=0
until mysqladmin ping -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" --silent >/dev/null 2>&1; do
  ATTEMPT=$((ATTEMPT + 1))
  if [ "$ATTEMPT" -ge 120 ]; then
    echo "[mysql-views-init] Timed out waiting for MySQL" >&2
    exit 1
  fi
  sleep 2
done
echo "[mysql-views-init] MySQL is ready"

mysql_exec() {
  mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$@"
}

for table_name in articles article_technical_profiles article_character_profiles; do
  exists="$(mysql_exec -N -B -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${MYSQL_DB}' AND table_name='${table_name}';")"
  if [ "$exists" = "0" ]; then
    echo "[mysql-views-init] Required table missing: ${MYSQL_DB}.${table_name}" >&2
    exit 1
  fi
done

mysql_exec "$MYSQL_DB" <<'SQL'
CREATE OR REPLACE VIEW vw_bid_products AS
SELECT
  a.id AS product_id,
  a.article_number,
  a.supplier_article_number,
  a.l_number,
  a.gtin_number,
  a.etim_class,
  a.etim_version,
  a.manufacturer_id,
  m.man_name AS manufacturer_name,
  COALESCE(
    NULLIF(a.short_description_de, ''),
    NULLIF(a.short_description_en, ''),
    NULLIF(a.very_short_description_de, ''),
    NULLIF(a.very_short_description_en, ''),
    a.article_number
  ) AS product_name,
  COALESCE(
    NULLIF(a.tender_description_de, ''),
    NULLIF(a.tender_description_en, ''),
    NULLIF(a.long_description_de, ''),
    NULLIF(a.long_description_en, '')
  ) AS tender_description,
  a.light_category_id,
  a.light_family_id,
  lc.name_de AS light_category_name,
  lf.name_de AS light_family_name,
  a.is_current
FROM articles a
LEFT JOIN manufacturers m ON m.id = a.manufacturer_id
LEFT JOIN light_categories lc ON lc.id = a.light_category_id
LEFT JOIN light_families lf ON lf.id = a.light_family_id;

CREATE OR REPLACE VIEW vw_bid_specs AS
SELECT
  a.id AS product_id,
  t.length,
  t.width,
  t.height,
  t.weight,
  t.diameter,
  t.cutout_width,
  t.cutout_length,
  t.cutout_depth,
  t.cutout_diameter,
  t.ip_rating,
  t.ip_rating_two,
  t.ik_rating,
  t.protection_class,
  t.min_temp,
  t.max_temp,
  t.electrical_power,
  t.electrical_current,
  t.electrical_c10,
  t.electrical_b10,
  t.electrical_c16,
  t.electrical_b16,
  t.electrical_power_factor,
  t.electrical_surge_protection,
  t.electrical_surge_protection_differential,
  c.cri,
  c.direct_ugr AS ugr,
  c.efficiency,
  c.hour AS lifetime_hours,
  c.mac_adam,
  c.light_output
FROM articles a
JOIN article_technical_profiles t ON t.id = a.article_technical_profile_id
JOIN article_character_profiles c ON c.id = a.article_character_profile_id;
SQL

view_count="$(mysql_exec -N -B -e "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='${MYSQL_DB}' AND table_name IN ('vw_bid_products','vw_bid_specs');")"
if [ "$view_count" != "2" ]; then
  echo "[mysql-views-init] View creation failed" >&2
  exit 1
fi
echo "[mysql-views-init] Views created/updated successfully"
