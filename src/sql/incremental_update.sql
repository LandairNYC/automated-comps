-- ============================================================
-- COMPSCOPE INCREMENTAL UPDATE SQL
-- ============================================================
-- Safe to run at any time. Never drops or modifies existing data.
-- Only inserts NEW sales that don't already exist in comps_dev_base_v2.
--
-- Parameter: :cutoff_date  (injected by pipeline_incremental.py)
-- Example:   '2026-02-01'
-- ============================================================


-- ============================================================
-- STEP 1: Find new sales from staging since cutoff date
-- Only BBLs not already in comps_dev_base_v2
-- ============================================================
CREATE TEMP TABLE new_sales_raw AS
SELECT
    CASE WHEN borough ~ '^[0-9\.]+$' THEN borough::numeric::integer END as borough,
    CASE WHEN block   ~ '^[0-9\.]+$' THEN block::numeric::integer   END as block,
    CASE WHEN lot     ~ '^[0-9\.]+$' THEN lot::numeric::integer     END as lot,
    CASE WHEN REPLACE(REPLACE(sale_price, ',', ''), ' ', '') ~ '^[0-9\.]+$'
         THEN REPLACE(REPLACE(sale_price, ',', ''), ' ', '')::numeric
    END as sale_price_clean,
    sale_date::timestamp as sale_date,
    address,
    neighborhood,
    zip_code,
    CASE WHEN year_built ~ '^[0-9\.]+$' THEN year_built::numeric::integer END as year_built,
    CASE WHEN residential_units ~ '^[0-9\.]+$' THEN residential_units::numeric::integer END as residential_units
FROM stg_sales_raw
WHERE sale_price IS NOT NULL
  AND sale_price != '0'
  AND sale_price != ''
  AND sale_date::timestamp >= :cutoff_date
  -- Only truly new BBL+date combos
  AND NOT EXISTS (
      SELECT 1 FROM comps_dev_base_v2 v
      WHERE CASE WHEN stg_sales_raw.borough ~ '^[0-9\.]+$' THEN stg_sales_raw.borough::numeric::integer END = v.borough
        AND CASE WHEN stg_sales_raw.block   ~ '^[0-9\.]+$' THEN stg_sales_raw.block::numeric::integer   END = v.block
        AND CASE WHEN stg_sales_raw.lot     ~ '^[0-9\.]+$' THEN stg_sales_raw.lot::numeric::integer     END = v.lot
        AND stg_sales_raw.sale_date::timestamp = v.sale_date
  );
--SPLIT--


-- ============================================================
-- STEP 2: Join new sales to PLUTO for property data
-- ============================================================
CREATE TEMP TABLE new_comps_base AS
SELECT
    s.borough, s.block, s.lot,
    s.sale_price_clean,
    s.sale_date,
    COALESCE(p.address, s.address)   as address,
    s.neighborhood,
    s.zip_code,
    p.zoning,
    p.building_class,
    p.landuse,
    p.resid_far  as pluto_resid_far,
    p.comm_far   as pluto_comm_far,
    p.lotarea,
    p.bldgarea,
    p.comarea,
    p.resarea,
    p.lot_frontage,
    p.lot_depth,
    p.num_buildings,
    p.unitsres,
    p.unitstotal,
    p.numfloors,
    COALESCE(p.yearbuilt, s.year_built) as year_built,
    p.ownername,
    p.latitude,
    p.longitude
FROM new_sales_raw s
INNER JOIN pluto_clean p
    ON s.borough = p.borough
    AND s.block  = p.block
    AND s.lot    = p.lot
WHERE s.sale_price_clean > 0
  AND s.sale_price_clean < 500000000;
--SPLIT--


-- ============================================================
-- STEP 3: Add buyers/sellers from ACRIS + FAR + zoning_base
-- ============================================================
CREATE TEMP TABLE new_comps_enriched AS
WITH deed_matches AS (
    SELECT DISTINCT ON (al.borough, al.block, al.lot, c.sale_date)
        al.borough, al.block, al.lot, al.document_id,
        m.document_date, m.doc_type, c.sale_date,
        ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) as days_diff
    FROM new_comps_base c
    JOIN acris_legals_clean al
        ON c.borough = al.borough AND c.block = al.block AND c.lot = al.lot
    JOIN acris_master_clean m ON al.document_id = m.document_id
    WHERE m.doc_type IN ('DEED', 'DEED, TS', 'DEED, LE', 'RPTT', 'RPTT&RET')
      AND ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) <= 90
    ORDER BY al.borough, al.block, al.lot, c.sale_date,
             ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) ASC
),
buyers_agg AS (
    SELECT document_id,
           STRING_AGG(DISTINCT name, '; ' ORDER BY name) as buyer_names
    FROM acris_parties_clean
    WHERE party_role = 'buyer'
       OR party_role = 'borrower'
       OR (party_role = 'other' AND party_type = '1')
    GROUP BY document_id
),
sellers_agg AS (
    SELECT document_id,
           STRING_AGG(DISTINCT name, '; ' ORDER BY name) as seller_names
    FROM acris_parties_clean
    WHERE party_role = 'seller'
       OR (party_role = 'other' AND party_type = '2')
    GROUP BY document_id
),
zoning_extract AS (
    SELECT c.*,
        CASE
            WHEN c.zoning ~ '/R[0-9]+'        THEN 'R' || SUBSTRING(c.zoning FROM '/R([0-9]+)')
            WHEN c.zoning ~ '^M[0-9]-[0-9]+A' THEN SUBSTRING(c.zoning FROM '^M[0-9]-[0-9]+')
            WHEN c.zoning ~ '^R[0-9]+-[0-9]'  THEN SUBSTRING(c.zoning FROM '^R[0-9]+-[0-9]')
            WHEN c.zoning ~ '^R[0-9]+[A-Z]'   THEN SUBSTRING(c.zoning FROM '^R[0-9]+[A-Z]')
            WHEN c.zoning ~ '^R[0-9]+'         THEN SUBSTRING(c.zoning FROM '^R[0-9]+')
            WHEN c.zoning ~ '^M[0-9]-[0-9]+'   THEN SUBSTRING(c.zoning FROM '^M[0-9]-[0-9]+')
            WHEN c.zoning ~ '^M[0-9]'          THEN SUBSTRING(c.zoning FROM '^M[0-9]')
            ELSE NULL
        END as zoning_base
    FROM new_comps_base c
)
SELECT
    c.*,
    d.document_id, d.document_date, d.doc_type, d.days_diff,
    b.buyer_names,
    s.seller_names,
    z.far_narrow as official_far_narrow,
    z.far_wide   as official_far_wide,
    CASE WHEN c.lotarea > 0 THEN c.sale_price_clean / c.lotarea     END as price_per_land_sf,
    CASE WHEN c.bldgarea > 0 THEN c.sale_price_clean / c.bldgarea   END as price_per_bldg_sf,
    CASE WHEN c.lotarea > 0 AND z.far_narrow > 0
         THEN c.lotarea * z.far_narrow END                           as buildable_sf_narrow,
    CASE WHEN c.lotarea > 0 AND z.far_wide > 0
         THEN c.lotarea * z.far_wide   END                           as buildable_sf_wide,
    CASE WHEN c.lotarea > 0 AND z.far_narrow > 0
         THEN c.sale_price_clean / (c.lotarea * z.far_narrow) END    as price_per_buildable_sf,
    CASE
        WHEN c.num_buildings = 0 OR c.bldgarea = 0                               THEN 'Vacant Land'
        WHEN c.zoning LIKE 'M%' AND c.building_class IN ('F1','F2','F4','F8','F9') THEN 'Industrial Building'
        WHEN c.zoning LIKE 'M%'                                                   THEN 'Industrial Development Site'
        WHEN c.zoning LIKE 'R%' AND c.lotarea >= c.bldgarea                      THEN 'Development Site'
        WHEN c.zoning LIKE 'R%'                                                   THEN 'Residential Property'
        WHEN c.zoning LIKE 'C%' AND c.lotarea >= c.bldgarea                      THEN 'Development Site'
        WHEN c.zoning LIKE 'C%'                                                   THEN 'Residential Property'
        ELSE 'Mixed-Use Site'
    END as asset_type
FROM zoning_extract c
LEFT JOIN deed_matches d
    ON c.borough = d.borough AND c.block = d.block AND c.lot = d.lot AND c.sale_date = d.sale_date
LEFT JOIN buyers_agg  b ON d.document_id = b.document_id
LEFT JOIN sellers_agg s ON d.document_id = s.document_id
LEFT JOIN zoning_far_official z ON c.zoning_base = z.zoning_base;
--SPLIT--


-- ============================================================
-- STEP 4: Apply all filters + insert into comps_dev_base_v2
-- Same filters as full rebuild — no existing records touched
-- ============================================================
INSERT INTO comps_dev_base_v2 (
    borough, block, lot,
    sale_price_clean, sale_date,
    address, neighborhood, zip_code,
    zoning, zoning_base, building_class, landuse,
    lotarea, bldgarea, comarea, resarea,
    lot_frontage, lot_depth,
    num_buildings, unitsres, unitstotal, numfloors,
    year_built, ownername,
    latitude, longitude,
    document_id, document_date, doc_type, days_diff,
    buyer_names, seller_names,
    pluto_resid_far, pluto_comm_far,
    buildable_sf,
    price_per_land_sf, price_per_bldg_sf,
    asset_type,
    development_potential_score,
    portfolio_flag, is_portfolio
)
WITH filtered AS (
    SELECT *,
        buildable_sf_narrow as buildable_sf,
        CASE
            WHEN lotarea >= bldgarea THEN 100
            WHEN bldgarea = 0        THEN 100
            ELSE ROUND(100.0 * (lotarea - bldgarea) / lotarea, 0)
        END as development_potential_score,
        NULL::text    as portfolio_flag,
        false         as is_portfolio,
        ROW_NUMBER() OVER (
            PARTITION BY address, sale_date
            ORDER BY sale_price_clean DESC, lotarea DESC
        ) as rn
    FROM new_comps_enriched
    WHERE
        sale_date >= '2022-01-01'
        AND sale_date < '2026-12-31'
        AND sale_price_clean >= 100000
        AND sale_price_clean <= 20000000
        AND (zoning LIKE 'R%' OR zoning LIKE 'M%')
        AND building_class NOT IN ('D0','D1','D2','D3','D4','D5','D6','D7','D8','D9')
        AND building_class NOT LIKE 'R%'
        AND building_class != 'C6'
        AND (
            (zoning ~ '^R[3-5]' AND lotarea >= 4975)
            OR (zoning ~ '^R([5-9]|10|11|12)' AND (lotarea >= bldgarea OR lotarea >= 2500))
            OR (zoning ~ '^M[1-3]')
        )
        AND (
            buyer_names IS NULL
            OR buyer_names LIKE '%LLC%'
            OR buyer_names LIKE '%CORP%'
            OR buyer_names LIKE '%INC%'
            OR buyer_names LIKE '%LP%'
            OR buyer_names LIKE '%LLP%'
            OR buyer_names LIKE '%TRUST%'
            OR buyer_names LIKE '%COMPANY%'
            OR buyer_names LIKE '%PARTNERS%'
        )
        AND (sale_price_clean / NULLIF(lotarea, 0)) <= 2000
        AND lotarea IS NOT NULL
        AND lotarea > 0
        AND pluto_resid_far IS NOT NULL
        AND pluto_resid_far > 0
)
SELECT
    borough, block, lot,
    sale_price_clean, sale_date,
    address, neighborhood, zip_code,
    zoning, zoning_base, building_class, landuse,
    lotarea, bldgarea, comarea, resarea,
    lot_frontage, lot_depth,
    num_buildings, unitsres, unitstotal, numfloors,
    year_built, ownername,
    latitude, longitude,
    document_id, document_date, doc_type, days_diff,
    buyer_names, seller_names,
    pluto_resid_far, pluto_comm_far,
    buildable_sf,
    price_per_land_sf, price_per_bldg_sf,
    CASE
        WHEN buildable_sf >= 2 * bldgarea AND bldgarea > 0 AND unitsres < 6 THEN 'Development Site'
        ELSE asset_type
    END,
    development_potential_score,
    portfolio_flag, is_portfolio
FROM filtered
WHERE rn = 1;
--SPLIT--


-- ============================================================
-- STEP 5: Clean up temp tables
-- ============================================================
DROP TABLE IF EXISTS new_sales_raw;
--SPLIT--
DROP TABLE IF EXISTS new_comps_base;
--SPLIT--
DROP TABLE IF EXISTS new_comps_enriched;