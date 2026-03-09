-- ============================================================
-- COMPSCOPE PIPELINE REBUILD SQL
-- Order: pluto_clean → sales_clean → acris_clean →
--        comps_base → comps_enriched → comps_dev_base_v2
-- ============================================================

-- ============================================================
-- STEP 1: Rebuild pluto_clean
-- ============================================================
DROP TABLE IF EXISTS pluto_clean CASCADE;
CREATE TABLE pluto_clean AS
SELECT
    CASE borough
        WHEN 'MN' THEN 1
        WHEN 'BX' THEN 2
        WHEN 'BK' THEN 3
        WHEN 'QN' THEN 4
        WHEN 'SI' THEN 5
    END as borough,
    CASE WHEN block ~ '^[0-9]+$' THEN block::integer END as block,
    CASE WHEN lot ~ '^[0-9]+$' THEN lot::integer END as lot,
    address,
    bbl,
    zipcode,
    zonedist1 as zoning,
    bldgclass as building_class,
    landuse,
    CASE WHEN builtfar ~ '^[0-9\.]+$' THEN builtfar::numeric END as built_far,
    CASE WHEN residfar ~ '^[0-9\.]+$' THEN residfar::numeric END as resid_far,
    CASE WHEN commfar ~ '^[0-9\.]+$' THEN commfar::numeric END as comm_far,
    CASE WHEN facilfar ~ '^[0-9\.]+$' THEN facilfar::numeric END as facil_far,
    CASE WHEN lotarea ~ '^[0-9\.]+$' THEN lotarea::numeric END as lotarea,
    CASE WHEN bldgarea ~ '^[0-9\.]+$' THEN bldgarea::numeric END as bldgarea,
    CASE WHEN comarea ~ '^[0-9\.]+$' THEN comarea::numeric END as comarea,
    CASE WHEN resarea ~ '^[0-9\.]+$' THEN resarea::numeric END as resarea,
    CASE WHEN lotfront ~ '^[0-9\.]+$' THEN lotfront::numeric END as lot_frontage,
    CASE WHEN lotdepth ~ '^[0-9\.]+$' THEN lotdepth::numeric END as lot_depth,
    CASE WHEN yearbuilt ~ '^[0-9]+$' THEN yearbuilt::integer END as yearbuilt,
    CASE WHEN numfloors ~ '^[0-9\.]+$' THEN numfloors::numeric END as numfloors,
    CASE WHEN numbldgs ~ '^[0-9]+$' THEN numbldgs::integer END as num_buildings,
    CASE WHEN unitsres ~ '^[0-9]+$' THEN unitsres::integer END as unitsres,
    CASE WHEN unitstotal ~ '^[0-9]+$' THEN unitstotal::integer END as unitstotal,
    ownername,
    CASE WHEN latitude ~ '^[0-9\.\-]+$' THEN latitude::numeric END as latitude,
    CASE WHEN longitude ~ '^[0-9\.\-]+$' THEN longitude::numeric END as longitude
FROM stg_pluto_raw
WHERE borough IS NOT NULL;

-- ============================================================
-- STEP 2: Rebuild sales_clean
-- ============================================================
DROP TABLE IF EXISTS sales_clean CASCADE;
CREATE TABLE sales_clean AS
SELECT
    CASE WHEN borough ~ '^[0-9\.]+$' THEN borough::numeric::integer END as borough,
    CASE WHEN block ~ '^[0-9\.]+$' THEN block::numeric::integer END as block,
    CASE WHEN lot ~ '^[0-9\.]+$' THEN lot::numeric::integer END as lot,
    CASE WHEN REPLACE(REPLACE(sale_price, ',', ''), ' ', '') ~ '^[0-9\.]+$'
         THEN REPLACE(REPLACE(sale_price, ',', ''), ' ', '')::numeric
    END as sale_price_clean,
    sale_date::timestamp as sale_date,
    address,
    neighborhood,
    building_class_at_present,
    building_class_category,
    tax_class_at_present,
    zip_code,
    CASE WHEN REPLACE(land_square_feet, ',', '') ~ '^[0-9\.]+$'
         THEN REPLACE(land_square_feet, ',', '')::numeric
    END as land_square_feet,
    CASE WHEN REPLACE(gross_square_feet, ',', '') ~ '^[0-9\.]+$'
         THEN REPLACE(gross_square_feet, ',', '')::numeric
    END as gross_square_feet,
    CASE WHEN year_built ~ '^[0-9\.]+$' THEN year_built::numeric::integer END as year_built,
    CASE WHEN residential_units ~ '^[0-9\.]+$' THEN residential_units::numeric::integer END as residential_units,
    CASE WHEN commercial_units ~ '^[0-9\.]+$' THEN commercial_units::numeric::integer END as commercial_units,
    CASE WHEN total_units ~ '^[0-9\.]+$' THEN total_units::numeric::integer END as total_units
FROM stg_sales_raw
WHERE sale_price IS NOT NULL
  AND sale_price != '0'
  AND sale_price != '';

-- ============================================================
-- STEP 3: Rebuild ACRIS clean tables
-- ============================================================
DROP TABLE IF EXISTS acris_master_clean CASCADE;
CREATE TABLE acris_master_clean AS
SELECT
    document_id,
    record_type,
    crfn,
    CASE WHEN borough ~ '^[0-9]+$' THEN borough::integer END as borough,
    doc_type,
    document_date::timestamp as document_date,
    CASE WHEN doc_amount ~ '^[0-9\.]+$' THEN doc_amount::numeric END as doc_amount,
    recorded_datetime::timestamp as recorded_datetime,
    modified_date::timestamp as modified_date,
    reel_yr,
    reel_nbr,
    reel_pg,
    pct_transferred
FROM stg_acris_master
WHERE document_id IS NOT NULL;

DROP TABLE IF EXISTS acris_parties_clean CASCADE;
CREATE TABLE acris_parties_clean AS
SELECT
    document_id,
    party_type,
    CASE party_type
        WHEN '1' THEN 'buyer'
        WHEN '2' THEN 'seller'
        WHEN '3' THEN 'other'
        ELSE 'unknown'
    END as party_role,
    UPPER(TRIM(name)) as name,
    address_1,
    address_2,
    city,
    state,
    zip,
    country,
    addr_unit
FROM stg_acris_parties
WHERE document_id IS NOT NULL
  AND name IS NOT NULL
  AND TRIM(name) != '';

DROP TABLE IF EXISTS acris_legals_clean CASCADE;
CREATE TABLE acris_legals_clean AS
SELECT
    document_id,
    record_type,
    CASE WHEN borough ~ '^[0-9]+$' THEN borough::integer END as borough,
    CASE WHEN block ~ '^[0-9]+$' THEN block::integer END as block,
    CASE WHEN lot ~ '^[0-9]+$' THEN lot::integer END as lot,
    easement,
    partial_lot,
    air_rights,
    subterranean_rights,
    property_type,
    street_name,
    unit
FROM stg_acris_legals
WHERE document_id IS NOT NULL;

-- ============================================================
-- STEP 4: Rebuild zoning_far_official (idempotent)
-- ============================================================
DROP TABLE IF EXISTS zoning_far_official CASCADE;
CREATE TABLE zoning_far_official (
    zoning_base TEXT PRIMARY KEY,
    far_narrow NUMERIC NOT NULL,
    far_wide NUMERIC NOT NULL,
    notes TEXT
);
INSERT INTO zoning_far_official (zoning_base, far_narrow, far_wide, notes) VALUES
('R5',   1.25, 1.25,  'R5 standard'),
('R5B',  1.35, 1.35,  'R5B contextual'),
('R5D',  2.0,  2.0,   'R5D contextual'),
('R6',   2.2,  3.0,   'R6: NS=2.2, WS=3.0'),
('R6A',  2.7,  3.6,   'R6A: Base=2.7, Incl=3.6'),
('R6B',  2.0,  2.2,   'R6B: Base=2.0, Incl=2.2'),
('R6D',  3.0,  4.0,   'R6D contextual'),
('R7',   3.44, 4.0,   'R7: NS=3.44, WS=4.0'),
('R7-1', 2.0,  3.0,   'R7-1 contextual'),
('R7-2', 2.7,  4.6,   'R7-2: Base NS=2.7, Incl WS=4.6'),
('R7A',  3.45, 4.6,   'R7A: Base=3.45, Incl=4.6'),
('R7B',  3.0,  4.0,   'R7B contextual'),
('R7D',  4.2,  5.6,   'R7D: Base=4.2, Incl=5.6'),
('R7X',  3.75, 6.0,   'R7X: Base=3.75, Incl=6.0'),
('R8',   5.4,  7.2,   'R8: Base=5.4, Incl=7.2'),
('R8A',  5.4,  7.2,   'R8A: Base=5.4, Incl=7.2'),
('R8B',  5.4,  7.2,   'R8B: Same as R8A'),
('R8X',  6.0,  7.2,   'R8X: Higher density'),
('R9',   7.52, 8.0,   'R9: Standard=7.52, Incl=8.0'),
('R9-1', 7.52, 8.0,   'R9-1 variant'),
('R9A',  7.52, 8.5,   'R9A: Standard=7.52, Incl=8.5'),
('R9D',  7.52, 8.0,   'R9D'),
('R9X',  7.52, 8.0,   'R9X'),
('R10',  10.0, 12.0,  'R10: Base=10.0, Incl=12.0'),
('R10A', 10.0, 12.0,  'R10A contextual'),
('R10H', 10.0, 12.0,  'R10H: High density'),
('R10X', 10.0, 12.0,  'R10X contextual'),
('R11',  12.0, 15.0,  'R11: City of Yes 2025'),
('R12',  15.0, 18.0,  'R12: City of Yes 2025'),
('M1-1', 1.0,  1.0,   'M1-1 light mfg'),
('M1-2', 2.0,  2.0,   'M1-2 light mfg'),
('M1-3', 5.0,  5.0,   'M1-3 light mfg'),
('M1-4', 2.0,  2.0,   'M1-4 light mfg'),
('M1-5', 5.0,  5.0,   'M1-5 light mfg'),
('M1-6', 10.0, 10.0,  'M1-6 light mfg'),
('M1-8', 2.0,  2.0,   'M1-8 variant'),
('M1-9', 2.0,  2.0,   'M1-9 variant'),
('M2-1', 2.0,  2.0,   'M2-1 heavy mfg'),
('M2-2', 5.0,  5.0,   'M2-2 heavy mfg'),
('M2-3', 2.0,  2.0,   'M2-3 heavy mfg'),
('M2-4', 5.0,  5.0,   'M2-4 heavy mfg'),
('M3-1', 2.0,  2.0,   'M3-1 heavy mfg'),
('M3-2', 2.0,  2.0,   'M3-2 heavy mfg');

CREATE INDEX idx_zoning_far ON zoning_far_official(zoning_base);

-- ============================================================
-- STEP 5: Rebuild comps_base (Sales + PLUTO join)
-- ============================================================
DROP TABLE IF EXISTS comps_base CASCADE;
CREATE TABLE comps_base AS
SELECT
    s.borough, s.block, s.lot,
    s.sale_price_clean,
    s.sale_date,
    COALESCE(p.address, s.address) as address,
    s.neighborhood,
    s.zip_code,
    p.zoning,
    p.building_class,
    p.landuse,
    p.built_far as pluto_built_far,
    p.resid_far as pluto_resid_far,
    p.comm_far as pluto_comm_far,
    p.facil_far as pluto_facil_far,
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
FROM sales_clean s
INNER JOIN pluto_clean p
    ON s.borough = p.borough
    AND s.block = p.block
    AND s.lot = p.lot
WHERE s.sale_price_clean > 0
  AND s.sale_price_clean < 500000000;

CREATE INDEX idx_comps_base_bbl ON comps_base(borough, block, lot);
CREATE INDEX idx_comps_base_date ON comps_base(sale_date);
CREATE INDEX idx_comps_base_zoning ON comps_base(zoning);

-- ============================================================
-- STEP 6: Rebuild comps_enriched (add buyers, sellers, FAR, buildable SF)
-- ============================================================
DROP TABLE IF EXISTS comps_enriched CASCADE;
CREATE TABLE comps_enriched AS
WITH deed_matches AS (
    SELECT DISTINCT ON (al.borough, al.block, al.lot, c.sale_date)
        al.borough, al.block, al.lot, al.document_id,
        m.document_date, m.doc_type, c.sale_date,
        ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) as days_diff
    FROM comps_base c
    JOIN acris_legals_clean al
        ON c.borough = al.borough AND c.block = al.block AND c.lot = al.lot
    JOIN acris_master_clean m ON al.document_id = m.document_id
    WHERE m.doc_type IN ('DEED', 'DEED, TS', 'DEED, LE', 'RPTT', 'RPTT&RET')
      AND ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) <= 90
    ORDER BY al.borough, al.block, al.lot, c.sale_date,
             ABS(EXTRACT(EPOCH FROM (m.document_date - c.sale_date)) / 86400) ASC
),
buyers_agg AS (
    SELECT
        document_id,
        STRING_AGG(DISTINCT name, '; ' ORDER BY name) as buyer_names
    FROM acris_parties_clean
    WHERE party_role = 'buyer'
       OR party_role = 'borrower'
       OR (party_role = 'other' AND party_type = '1')
    GROUP BY document_id
),
sellers_agg AS (
    SELECT
        document_id,
        STRING_AGG(DISTINCT name, '; ' ORDER BY name) as seller_names
    FROM acris_parties_clean
    WHERE party_role = 'seller'
       OR (party_role = 'other' AND party_type = '2')
    GROUP BY document_id
),
base_zoning_extract AS (
    SELECT
        c.*,
        CASE
            WHEN c.zoning ~ '/R[0-9]+'   THEN 'R' || SUBSTRING(c.zoning FROM '/R([0-9]+)')
            WHEN c.zoning ~ '^M[0-9]-[0-9]+A' THEN SUBSTRING(c.zoning FROM '^M[0-9]-[0-9]+')
            WHEN c.zoning ~ '^R[0-9]+-[0-9]'  THEN SUBSTRING(c.zoning FROM '^R[0-9]+-[0-9]')
            WHEN c.zoning ~ '^R[0-9]+[A-Z]'   THEN SUBSTRING(c.zoning FROM '^R[0-9]+[A-Z]')
            WHEN c.zoning ~ '^R[0-9]+'         THEN SUBSTRING(c.zoning FROM '^R[0-9]+')
            WHEN c.zoning ~ '^M[0-9]-[0-9]+'   THEN SUBSTRING(c.zoning FROM '^M[0-9]-[0-9]+')
            WHEN c.zoning ~ '^M[0-9]'          THEN SUBSTRING(c.zoning FROM '^M[0-9]')
            ELSE NULL
        END as zoning_base
    FROM comps_base c
)
SELECT
    c.*,
    d.document_id,
    d.document_date,
    d.doc_type,
    d.days_diff,
    b.buyer_names,
    s.seller_names,
    c.zoning_base,
    z.far_narrow as official_far_narrow,
    z.far_wide as official_far_wide,
    CASE WHEN c.lotarea > 0 THEN c.sale_price_clean / c.lotarea END as price_per_land_sf,
    CASE WHEN c.bldgarea > 0 THEN c.sale_price_clean / c.bldgarea END as price_per_bldg_sf,
    -- Buildable SF (narrow = conservative, used as primary)
    CASE WHEN c.lotarea > 0 AND z.far_narrow > 0 THEN c.lotarea * z.far_narrow END as buildable_sf_narrow,
    CASE WHEN c.lotarea > 0 AND z.far_wide > 0   THEN c.lotarea * z.far_wide   END as buildable_sf_wide,
    -- PPBSF using narrow FAR
    CASE WHEN c.lotarea > 0 AND z.far_narrow > 0
         THEN c.sale_price_clean / (c.lotarea * z.far_narrow) END as price_per_buildable_sf,
    -- Asset type (pre-classification, will be overridden in dev_base step)
    CASE
        WHEN c.num_buildings = 0 OR c.bldgarea = 0                          THEN 'Vacant Land'
        WHEN c.zoning LIKE 'M%' AND c.building_class IN ('F1','F2','F4','F8','F9') THEN 'Industrial Building'
        WHEN c.zoning LIKE 'M%'                                              THEN 'Industrial Development Site'
        WHEN c.zoning LIKE 'R%' AND c.lotarea >= c.bldgarea                 THEN 'Residential Development Site'
        WHEN c.zoning LIKE 'R%'                                              THEN 'Residential Property'
        WHEN c.zoning LIKE 'C%' AND c.lotarea >= c.bldgarea                 THEN 'Residential Development Site'
        WHEN c.zoning LIKE 'C%'                                              THEN 'Residential Property'
        ELSE 'Mixed-Use Site'
    END as asset_type
FROM base_zoning_extract c
LEFT JOIN deed_matches d
    ON c.borough = d.borough AND c.block = d.block AND c.lot = d.lot AND c.sale_date = d.sale_date
LEFT JOIN buyers_agg b ON d.document_id = b.document_id
LEFT JOIN sellers_agg s ON d.document_id = s.document_id
LEFT JOIN zoning_far_official z ON c.zoning_base = z.zoning_base;

CREATE INDEX idx_comps_enriched_bbl ON comps_enriched(borough, block, lot);
CREATE INDEX idx_comps_enriched_date ON comps_enriched(sale_date);
CREATE INDEX idx_comps_enriched_zoning ON comps_enriched(zoning);
CREATE INDEX idx_comps_enriched_building_class ON comps_enriched(building_class);
CREATE INDEX idx_comps_enriched_price ON comps_enriched(sale_price_clean);
CREATE INDEX idx_comps_enriched_asset_type ON comps_enriched(asset_type);

-- ============================================================
-- STEP 7: Rebuild comps_dev_base_v2 (final filtered dev sites)
-- ============================================================
DROP TABLE IF EXISTS comps_dev_base_v2 CASCADE;
CREATE TABLE comps_dev_base_v2 AS
WITH filtered_comps AS (
    SELECT
        *,
        -- Use narrow buildable SF as primary
        buildable_sf_narrow as buildable_sf,
        pluto_resid_far,
        pluto_comm_far,
        -- Development potential score
        CASE
            WHEN lotarea >= bldgarea THEN 100
            WHEN bldgarea = 0        THEN 100
            ELSE ROUND(100.0 * (lotarea - bldgarea) / lotarea, 0)
        END as development_potential_score,
        -- Portfolio detection
        CASE
            WHEN seller_names IS NOT NULL AND EXISTS (
                SELECT 1 FROM comps_enriched c2
                WHERE c2.seller_names = comps_enriched.seller_names
                  AND c2.sale_date::date = comps_enriched.sale_date::date
                  AND c2.document_id = comps_enriched.document_id
                GROUP BY c2.seller_names, c2.sale_date::date, c2.document_id
                HAVING COUNT(*) >= 3
            ) THEN 'Package Sale (' ||
                (SELECT COUNT(*)::text FROM comps_enriched c2
                 WHERE c2.seller_names = comps_enriched.seller_names
                   AND c2.sale_date::date = comps_enriched.sale_date::date
                   AND c2.document_id = comps_enriched.document_id) || ' parcels) - Verify Before Using'
            ELSE NULL
        END as portfolio_flag,
        CASE
            WHEN seller_names IS NOT NULL AND EXISTS (
                SELECT 1 FROM comps_enriched c2
                WHERE c2.seller_names = comps_enriched.seller_names
                  AND c2.sale_date::date = comps_enriched.sale_date::date
                  AND c2.document_id = comps_enriched.document_id
                GROUP BY c2.seller_names, c2.sale_date::date, c2.document_id
                HAVING COUNT(*) >= 3
            ) THEN true ELSE false
        END as is_portfolio,
        -- Deduplication
        ROW_NUMBER() OVER (
            PARTITION BY address, sale_date
            ORDER BY sale_price_clean DESC, lotarea DESC
        ) as rn
    FROM comps_enriched
    WHERE
        sale_date >= '2022-01-01'
        AND sale_date < '2026-01-01'
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
    buildable_sf_narrow, buildable_sf_wide,
    official_far_narrow, official_far_wide,
    price_per_land_sf, price_per_bldg_sf, price_per_buildable_sf,
    asset_type,
    development_potential_score,
    portfolio_flag, is_portfolio
FROM filtered_comps
WHERE rn = 1;

-- Apply Dev Site override rule
UPDATE comps_dev_base_v2
SET asset_type = 'Development Site'
WHERE buildable_sf >= 2 * bldgarea
  AND bldgarea > 0
  AND unitsres < 6
  AND asset_type != 'Development Site';

-- Rename legacy asset type
UPDATE comps_dev_base_v2
SET asset_type = 'Development Site'
WHERE asset_type = 'Residential Development Site';

CREATE INDEX idx_devv2_bbl ON comps_dev_base_v2(borough, block, lot);
CREATE INDEX idx_devv2_date ON comps_dev_base_v2(sale_date);
CREATE INDEX idx_devv2_zoning ON comps_dev_base_v2(zoning);
CREATE INDEX idx_devv2_price ON comps_dev_base_v2(sale_price_clean);
CREATE INDEX idx_devv2_asset ON comps_dev_base_v2(asset_type);
CREATE INDEX idx_devv2_neighborhood ON comps_dev_base_v2(neighborhood);
