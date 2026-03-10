-- PIM match query layer for Agent SQL access.
-- Expected database: pim_raw (MySQL 8+)

DROP VIEW IF EXISTS `match_assets`;
DROP VIEW IF EXISTS `match_certs`;
DROP VIEW IF EXISTS `match_specs`;
DROP VIEW IF EXISTS `match_products`;

CREATE VIEW `match_products` AS
SELECT
  a.id AS product_id,
  a.article_number AS article_number,
  a.l_number AS l_number,
  a.version AS version,
  a.is_current AS is_current,
  COALESCE(
    NULLIF(a.short_description_en, ''),
    NULLIF(a.short_description_de, ''),
    NULLIF(a.short_description_fr, ''),
    NULLIF(a.short_description_it, ''),
    a.article_number
  ) AS product_name,
  a.short_description_de AS short_description_de,
  a.short_description_en AS short_description_en,
  a.short_description_fr AS short_description_fr,
  a.short_description_it AS short_description_it,
  a.tender_description_de AS tender_description_de,
  a.tender_description_en AS tender_description_en,
  a.tender_description_fr AS tender_description_fr,
  a.tender_description_it AS tender_description_it,
  m.man_name AS manufacturer_name,
  r.name AS retailer_name,
  lc.name_de AS light_category_de,
  lc.name_en AS light_category_en,
  lf.name_de AS light_family_de,
  lf.name_en AS light_family_en
FROM articles a
LEFT JOIN manufacturers m ON m.id = a.manufacturer_id
LEFT JOIN retailers r ON r.id = a.retailer_id
LEFT JOIN light_categories lc ON lc.id = a.light_category_id
LEFT JOIN light_families lf ON lf.id = a.light_family_id
WHERE a.is_current = 1;

CREATE VIEW `match_specs` AS
SELECT
  a.id AS product_id,
  atp.electrical_power AS electrical_power_w,
  atp.ip_rating AS ip_rating,
  atp.ip_rating_two AS ip_rating_two,
  atp.ik_rating AS ik_rating,
  atp.min_temp AS min_temp_c,
  atp.max_temp AS max_temp_c,
  atp.emergency_light AS emergency_light,
  atp.one_hour AS emergency_runtime_1h,
  atp.three_hour AS emergency_runtime_3h,
  acp.direct_ugr AS ugr,
  acp.cri AS cri,
  acp.hour AS runtime_hours,
  acp.efficiency AS luminous_efficacy,
  acp.controls_dali AS controls_dali,
  acp.controls_dali_two AS controls_dali_two,
  acp.controls_bluetooth AS controls_bluetooth,
  acp.controls_matter AS controls_matter,
  acp.controls_dmx AS controls_dmx,
  acp.light_color_tw AS controls_tw,
  acp.light_color_dtw AS controls_dtw,
  acp.light_color_rgb AS controls_rgb,
  acp.light_color_rgbw AS controls_rgbw,
  CAST(NULLIF(REGEXP_SUBSTR(acp.luminaire_fluxes, '[0-9]+(\\.[0-9]+)?'), '') AS DECIMAL(12,2)) AS lumen_output_max,
  CAST(NULLIF(REGEXP_SUBSTR(acp.light_color_colors, '[0-9]+(\\.[0-9]+)?'), '') AS DECIMAL(12,2)) AS color_temp_k_max
FROM articles a
LEFT JOIN article_technical_profiles atp ON atp.id = a.article_technical_profile_id
LEFT JOIN article_character_profiles acp ON acp.id = a.article_character_profile_id
WHERE a.is_current = 1;

CREATE VIEW `match_certs` AS
SELECT
  a.id AS product_id,
  MAX(CASE WHEN UPPER(i.name) = 'CE' THEN 1 ELSE 0 END) AS ce,
  MAX(CASE WHEN UPPER(i.name) = 'ENEC' THEN 1 ELSE 0 END) AS enec,
  MAX(CASE WHEN UPPER(i.name) LIKE '%DALI%' THEN 1 ELSE 0 END) AS dali,
  MAX(CASE WHEN UPPER(i.name) LIKE '%BLUETOOTH%' THEN 1 ELSE 0 END) AS bluetooth,
  MAX(CASE WHEN UPPER(i.name) LIKE '%MATTER%' THEN 1 ELSE 0 END) AS matter,
  GROUP_CONCAT(DISTINCT i.name ORDER BY i.name SEPARATOR ' | ') AS icon_tags
FROM articles a
LEFT JOIN icon_articles ia ON ia.article_id = a.id
LEFT JOIN icons i ON i.id = ia.icon_id
WHERE a.is_current = 1
GROUP BY a.id;

CREATE VIEW `match_assets` AS
SELECT
  a.id AS product_id,
  MAX(CASE WHEN am.media_type = 'MAN_DATA_SHEET_PDF' AND am.path NOT LIKE 'http%' THEN am.path END) AS datasheet_path,
  MAX(CASE WHEN am.media_type = 'MAN_DATA_SHEET_PDF' AND am.path LIKE 'http%' THEN am.path END) AS datasheet_url,
  MAX(CASE WHEN am.media_type = 'MAN_MOUNTING_INSTRUCTION_PDF' AND am.path NOT LIKE 'http%' THEN am.path END) AS mounting_instruction_path,
  MAX(CASE WHEN am.media_type IN ('MAN_NORMAL_IMAGE', 'MAN_AMBIENT_IMAGE') AND am.path NOT LIKE 'http%' THEN am.path END) AS image_path,
  GROUP_CONCAT(DISTINCT am.language ORDER BY am.language SEPARATOR ',') AS asset_languages
FROM articles a
LEFT JOIN article_media am
  ON am.article_id = a.id
  AND am.path NOT LIKE '%:Zone.Identifier'
WHERE a.is_current = 1
GROUP BY a.id;
