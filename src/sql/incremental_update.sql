-- ============================================================
-- COMPSCOPE INCREMENTAL UPDATE SQL
-- ============================================================
-- Safe to run at any time. NEVER drops or modifies existing data.
-- Only inserts NEW sales that don't already exist in comps_dev_base_v2.
--
-- Mirrors the exact pipeline from the full rebuild:
--   stg_sales_raw  → new_sales_clean  (temp)
--   + pluto_clean  → new_comps_base   (temp)
--   + acris_clean  → new_comps_enriched (temp, with mortgage fallback)
--   → INSERT INTO comps_dev_base_v2 (new records only, never touches existing)
--
-- Parameter: :cutoff_date  (injected by pipeline.py)
-- ============================================================


-- ============================================================
-- STEP 1: New sales from stg_sales_raw since cutoff
-- Mirrors sales_clean logic exactly
-- Skips BBL+date combos already in comps_dev_base_v2
-- ============================================================
CREATE TEMP TABLE new_sales_clean AS
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
    CASE WHEN year_built ~ '^[0-9\.]+$' THEN year_built::numeric::integer END as year_built
FROM stg_sales_raw
WHERE sale_price IS NOT NULL
  AND sale_price != '0'
  AND sale_price != ''
  AND sale_date::timestamp >= :cutoff_date
  AND NOT EXISTS (
      SELECT 1 FROM comps_dev_base_v2 v
      WHERE CASE WHEN stg_sales_raw.borough ~ '^[0-9\.]+$' THEN stg_sales_raw.borough::numeric::integer END = v.borough
        AND CASE WHEN stg_sales_raw.block   ~ '^[0-9\.]+$' THEN stg_sales_raw.block::numeric::integer   END = v.block
        AND CASE WHEN stg_sales_raw.lot     ~ '^[0-9\.]+$' THEN stg_sales_raw.lot::numeric::integer     END = v.lot
        AND stg_sales_raw.sale_date::timestamp = v.sale_date
  );
--SPLIT--


-- ============================================================
-- STEP 2: Join new sales to pluto_clean
-- Mirrors comps_base logic exactly
-- ============================================================
CREATE TEMP TABLE new_comps_base AS
SELECT
    s.borough, s.block, s.lot,
    s.sale_price_clean, s.sale_date,
    COALESCE(p.address, s.address) as address,
    s.neighborhood, s.zip_code,
    p.zoning, p.building_class, p.landuse,
    p.built_far  as pluto_built_far,
    p.resid_far  as pluto_resid_far,
    p.comm_far   as pluto_comm_far,
    p.facil_far  as pluto_facil_far,
    p.lotarea, p.bldgarea, p.comarea, p.resarea,
    p.lot_frontage, p.lot_depth,
    p.num_buildings, p.unitsres, p.unitstotal, p.numfloors,
    COALESCE(p.yearbuilt, s.year_built) as year_built,
    p.ownername, p.latitude, p.longitude
FROM new_sales_clean s
INNER JOIN pluto_clean p
    ON s.borough = p.borough
    AND s.block  = p.block
    AND s.lot    = p.lot
WHERE s.sale_price_clean > 0
  AND s.sale_price_clean < 500000000;
--SPLIT--


-- ============================================================
-- STEP 3: Enrich with ACRIS buyers/sellers + zoning_base + buildable SF
-- Mirrors comps_enriched exactly, including mortgage fallback for 95% coverage
-- ============================================================
CREATE TEMP TABLE new_comps_enriched AS
WITH deed_matches AS (
    SELECT DISTINCT ON (al.borough, al.block, al.lot, c.sale_date)
        al.borough, al.block, al.lot, al.document_id,
        m.document_date, m.doc_type, c.sale_date,
        ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) as days_diff,
        1 as priority
    FROM new_comps_base c
    JOIN acris_legals_clean al
        ON c.borough = al.borough AND c.block = al.block AND c.lot = al.lot
    JOIN acris_master_clean m ON al.document_id = m.document_id
    WHERE m.doc_type IN ('DEED', 'DEED, TS', 'DEED, LE', 'RPTT', 'RPTT&RET')
      AND ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) <= 90
    ORDER BY al.borough, al.block, al.lot, c.sale_date,
             ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) ASC
),
mortgage_fallback AS (
    SELECT DISTINCT ON (al.borough, al.block, al.lot, c.sale_date)
        al.borough, al.block, al.lot, al.document_id,
        m.document_date, m.doc_type, c.sale_date,
        ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) as days_diff,
        2 as priority
    FROM new_comps_base c
    JOIN acris_legals_clean al
        ON c.borough = al.borough AND c.block = al.block AND c.lot = al.lot
    JOIN acris_master_clean m ON al.document_id = m.document_id
    WHERE m.doc_type = 'MTGE'
      AND ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) <= 90
      AND NOT EXISTS (
          SELECT 1 FROM deed_matches dm
          WHERE dm.borough = c.borough AND dm.block = c.block
            AND dm.lot = c.lot AND dm.sale_date = c.sale_date
      )
    ORDER BY al.borough, al.block, al.lot, c.sale_date,
             ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) ASC
),
all_matches AS (
    SELECT * FROM deed_matches
    UNION ALL
    SELECT * FROM mortgage_fallback
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
base_zoning_extract AS (
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
    CASE
        WHEN c.lotarea > 0 AND c.pluto_resid_far > 0
        THEN ROUND(c.lotarea * c.pluto_resid_far, 0)
        ELSE NULL
    END as buildable_sf,
    CASE WHEN c.lotarea  > 0 THEN c.sale_price_clean / c.lotarea  END as price_per_land_sf,
    CASE WHEN c.bldgarea > 0 THEN c.sale_price_clean / c.bldgarea END as price_per_bldg_sf
FROM base_zoning_extract c
LEFT JOIN all_matches d
    ON c.borough = d.borough AND c.block = d.block
    AND c.lot = d.lot AND c.sale_date = d.sale_date
LEFT JOIN buyers_agg  b ON d.document_id = b.document_id
LEFT JOIN sellers_agg s ON d.document_id = s.document_id;
--SPLIT--


-- ============================================================
-- STEP 4: Filter and INSERT into comps_dev_base_v2
-- Same filters as full rebuild. Existing records never touched.
-- ============================================================
INSERT INTO comps_dev_base_v2 (
    borough, block, lot, bbl,
    address, neighborhood, zip_code,
    sale_price_clean, sale_date,
    document_id, document_date, doc_type, days_diff,
    zoning, zoning_base, building_class, building_class_name, landuse, asset_type,
    lotarea, bldgarea, comarea, resarea,
    lot_frontage, lot_depth,
    num_buildings, unitsres, unitstotal, numfloors, year_built,
    ownername, latitude, longitude,
    pluto_resid_far, pluto_comm_far,
    buildable_sf, ppbsf,
    price_per_land_sf, price_per_bldg_sf,
    buyer_names, seller_names,
    development_potential_score,
    is_portfolio, portfolio_parcel_count, portfolio_flag,
    outlier_flag
)
WITH
portfolio_docs AS (
    SELECT document_id, COUNT(*) as parcel_count
    FROM new_comps_enriched
    WHERE document_id IS NOT NULL
    GROUP BY document_id
    HAVING COUNT(*) >= 2
),
filtered AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY e.address, e.sale_date
            ORDER BY e.sale_price_clean DESC, e.lotarea DESC
        ) as rn
    FROM new_comps_enriched e
    WHERE
        e.sale_date >= '2022-01-01'
        AND e.sale_date < '2026-12-31'
        AND e.sale_price_clean >= 100000
        AND e.zoning NOT LIKE 'C8%'
        AND e.zoning NOT LIKE 'R3%'
        AND e.zoning NOT LIKE 'R4%'
        AND e.zoning NOT SIMILAR TO 'R5[^BD]%'
        AND e.zoning_base != 'R5'
        AND (
            e.zoning_base IN ('R5B', 'R5D')
            OR e.zoning_base ~ '^R([6-9]|10|11|12)'
            OR e.zoning_base ~ '^M[1-3]'
            OR e.zoning      ~ '^M[1-3]'
            OR (e.zoning LIKE 'C%' AND e.zoning_base ~ '^R([6-9]|10|11|12)')
        )
        AND e.building_class NOT IN ('D0','D1','D2','D3','D4','D5','D6','D7','D8','D9','C6')
        AND e.building_class NOT LIKE 'R%'
        AND (
            (e.zoning_base ~ '^R([6-9]|10|11|12)' AND e.lotarea >= 1500)
            OR (e.zoning_base IN ('R5B','R5D')     AND e.lotarea >= 2000)
            OR (e.zoning_base ~ '^M'               AND e.lotarea >= 2000)
            OR e.lotarea >= 2000
        )
        AND (
            e.unitsres <= 5
            OR (e.lotarea * e.pluto_resid_far) >= (2 * e.bldgarea)
        )
        AND (
            e.buyer_names IS NULL
            OR e.buyer_names LIKE '%LLC%'
            OR e.buyer_names LIKE '%CORP%'
            OR e.buyer_names LIKE '%INC%'
            OR e.buyer_names LIKE '%LP%'
            OR e.buyer_names LIKE '%LLP%'
            OR e.buyer_names LIKE '%TRUST%'
            OR e.buyer_names LIKE '%COMPANY%'
            OR e.buyer_names LIKE '%PARTNERS%'
        )
        AND (e.sale_price_clean / NULLIF(e.lotarea, 0)) <= 2000
        AND e.lotarea IS NOT NULL
        AND e.lotarea > 0
        AND (e.pluto_resid_far IS NULL OR e.pluto_resid_far > 0)
)
SELECT
    f.borough, f.block, f.lot,
    CONCAT(f.borough,'-',LPAD(f.block::text,5,'0'),'-',LPAD(f.lot::text,4,'0')) as bbl,
    f.address, f.neighborhood, f.zip_code,
    f.sale_price_clean, f.sale_date,
    f.document_id, f.document_date, f.doc_type, f.days_diff,
    f.zoning, f.zoning_base, f.building_class,
    bcl.building_class_name,
    f.landuse,
    CASE
        WHEN f.building_class LIKE 'V%' OR f.bldgarea = 0 OR f.num_buildings = 0
            THEN 'Vacant Land'
        WHEN f.building_class = 'K4'
            THEN 'Retail Building'
        WHEN (f.zoning ~ 'R' OR f.zoning_base ~ '^R')
            THEN CASE
                WHEN f.buildable_sf IS NOT NULL AND f.bldgarea > 0
                     AND f.buildable_sf >= (2 * f.bldgarea)
                    THEN 'Development Site'
                ELSE 'Residential Property'
            END
        WHEN f.zoning LIKE 'M%'
             AND f.building_class IN ('F1','F2','F4','F5','F8','F9','E1','E2','E7','E9')
            THEN 'Industrial Building'
        WHEN f.zoning LIKE 'M%'
            THEN 'Industrial Development Site'
        ELSE 'Residential Property'
    END as asset_type,
    f.lotarea, f.bldgarea, f.comarea, f.resarea,
    f.lot_frontage, f.lot_depth,
    f.num_buildings, f.unitsres, f.unitstotal, f.numfloors, f.year_built,
    f.ownername, f.latitude, f.longitude,
    f.pluto_resid_far, f.pluto_comm_far,
    f.buildable_sf,
    ROUND(f.sale_price_clean / NULLIF(f.buildable_sf, 0), 2) as ppbsf,
    ROUND(f.price_per_land_sf, 2)  as price_per_land_sf,
    ROUND(f.price_per_bldg_sf, 2)  as price_per_bldg_sf,
    f.buyer_names, f.seller_names,
    CASE
        WHEN f.lotarea >= f.bldgarea THEN 100
        WHEN f.bldgarea = 0          THEN 100
        ELSE ROUND(100.0 * (f.lotarea - f.bldgarea) / f.lotarea, 0)
    END as development_potential_score,
    CASE WHEN p.document_id IS NOT NULL THEN TRUE ELSE FALSE END as is_portfolio,
    COALESCE(p.parcel_count, 1) as portfolio_parcel_count,
    CASE
        WHEN p.document_id IS NOT NULL AND p.parcel_count >= 5 THEN 'Package Sale (5+ parcels) - Manual Review Required'
        WHEN p.document_id IS NOT NULL AND p.parcel_count >= 3 THEN 'Package Sale (3-4 parcels) - Verify Before Using'
        WHEN p.document_id IS NOT NULL                         THEN 'Package Sale (2 parcels)'
        ELSE NULL
    END as portfolio_flag,
    CASE
        WHEN f.lot_frontage < 25
             AND (
                 f.zoning_base ~ '^R(7|8|9|10|11|12)'
                 OR f.zoning   ~ '^R(7|8|9|10|11|12)'
                 OR f.zoning   ~ '/R(7|8|9|10|11|12)'
             )
            THEN 'Sliver Lot - Review PPBSF'
        ELSE NULL
    END as outlier_flag
FROM filtered f
LEFT JOIN portfolio_docs p ON f.document_id = p.document_id
LEFT JOIN building_class_lookup bcl ON f.building_class = bcl.building_class
WHERE f.rn = 1;

-- Tag sliver lot outliers on newly inserted records
UPDATE comps_dev_base_v2
SET outlier_flag = 'Sliver Lot - Review PPBSF'
WHERE outlier_flag IS NULL
  AND lot_frontage < 25
  AND (
      zoning_base ~ '^R(7|8|9|10|11|12)'
      OR zoning   ~ '^R(7|8|9|10|11|12)'
      OR zoning   ~ '/R(7|8|9|10|11|12)'
  );

-- Tag portfolio sales on newly inserted records
UPDATE comps_dev_base_v2 v
SET is_portfolio = true,
    portfolio_flag = CASE
        WHEN p.parcel_count >= 5 THEN 'Package Sale (5+ parcels) - Manual Review Required'
        WHEN p.parcel_count >= 3 THEN 'Package Sale (3-4 parcels) - Verify Before Using'
        ELSE 'Package Sale (2 parcels)'
    END
FROM (
    SELECT document_id, COUNT(*) as parcel_count
    FROM comps_dev_base_v2
    WHERE document_id IS NOT NULL
    GROUP BY document_id
    HAVING COUNT(*) >= 2
) p
WHERE v.document_id = p.document_id
  AND v.is_portfolio = false;
--SPLIT--


-- ============================================================
-- STEP 5: Clean up temp tables
-- ============================================================
DROP TABLE IF EXISTS new_sales_clean;
--SPLIT--
DROP TABLE IF EXISTS new_comps_base;
--SPLIT--
DROP TABLE IF EXISTS new_comps_enriched;