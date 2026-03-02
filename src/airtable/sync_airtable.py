import os
import time
import argparse
from difflib import SequenceMatcher
from typing import Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pyairtable import Api


load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "Property Comps")
AIRTABLE_AREAS_TABLE = os.getenv("AIRTABLE_AREAS_TABLE", "Areas")

RATE_SLEEP = 0.20


MANUAL_NEIGHBORHOOD_MAP = {
    "HELL'S KITCHEN": "Hells Kitchen",
    "MIDTOWN CBD": "Midtown",
    "MADISON": "Madison",
    "SCHUYLERVILLE/PELHAM BAY": "Pelham Bay",
}


ASSET_TYPE_MAP = {
    "Residential Development Site": "Residential Development Site",
    "Residential Property": "Residential Development Site",
    "Vacant Land": "Residential Development Site",
    "Industrial Development Site": "Industrial Land",
    "Industrial Building": "Industrial Building",
    "Industrial + Office Building": "Industrial + Office Building",
    "Mixed Use": "Mixed Use",
    "Commercial": "Commercial",
}


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)


def get_airtable_tables():
    if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
        raise RuntimeError("Missing AIRTABLE_PAT or AIRTABLE_BASE_ID")
    api = Api(AIRTABLE_PAT)
    comps = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    areas = api.table(AIRTABLE_BASE_ID, AIRTABLE_AREAS_TABLE)
    return comps, areas


def safe_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def safe_int(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None


def fuzzy_match(name: str, candidates: List[str], threshold: float = 0.65) -> Optional[str]:
    name_l = name.lower().strip()
    best = None
    best_score = 0.0
    for c in candidates:
        score = SequenceMatcher(None, name_l, c.lower().strip()).ratio()
        if score > best_score:
            best_score = score
            best = c
    return best if best and best_score >= threshold else None


def build_area_cache(areas_table):
    area_lookup: Dict[str, str] = {}
    area_names: List[str] = []
    for rec in areas_table.all():
        nm = rec.get("fields", {}).get("Name")
        if nm:
            area_lookup[nm.lower().strip()] = rec["id"]
            area_names.append(nm)
    return area_lookup, area_names


def resolve_area_id(db_neighborhood: Optional[str], area_lookup: Dict[str, str], area_names: List[str]):
    if not db_neighborhood:
        return None

    raw = str(db_neighborhood).strip()
    upper = raw.upper()

    mapped = MANUAL_NEIGHBORHOOD_MAP.get(upper)
    if mapped:
        rec_id = area_lookup.get(mapped.lower().strip())
        if rec_id:
            return [rec_id]

    rec_id = area_lookup.get(raw.lower().strip())
    if rec_id:
        return [rec_id]

    m = fuzzy_match(raw, area_names, threshold=0.65)
    if m:
        rec_id = area_lookup.get(m.lower().strip())
        if rec_id:
            return [rec_id]

    return None


def format_block_lot(borough, block, lot):
    if borough is None or block is None or lot is None:
        return None
    try:
        return f"{str(borough).strip()}-{str(int(block)).zfill(5)}-{str(int(lot)).zfill(4)}"
    except Exception:
        return None


def map_asset_type(db_asset_type: Optional[str]):
    if not db_asset_type:
        return None
    raw = str(db_asset_type).strip()
    mapped = ASSET_TYPE_MAP.get(raw)
    return [mapped] if mapped else [raw]


def fetch_properties(limit: Optional[int] = None) -> List[dict]:
    query = """
        SELECT
            address,
            borough,
            block,
            lot,
            neighborhood,
            sale_price_clean,
            sale_date,
            zoning,
            asset_type,
            building_class,
            lotarea,
            bldgarea,
            lot_frontage,
            lot_depth,
            buildable_sf_narrow,
            buildable_sf_wide,
            official_far_narrow,
            unitsres,
            buyer_names,
            seller_names
        FROM comps_dev_base_v2
        ORDER BY sale_date DESC
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return [dict(r) for r in cur.fetchall()]


def map_row_to_airtable_fields(row: dict, area_lookup: Dict[str, str], area_names: List[str], area_misses: List[str]):
    closing_date = row.get("sale_date")
    if closing_date and hasattr(closing_date, "strftime"):
        closing_date = closing_date.strftime("%Y-%m-%d")
    elif closing_date:
        closing_date = str(closing_date)[:10]

    fields = {
        "Address": row.get("address"),
        "Closing Price": safe_float(row.get("sale_price_clean")),
        "Closing Date": closing_date,
        "Buyer Name Text": row.get("buyer_names"),
        "Seller Name Text": row.get("seller_names"),
        "Zones (Manual)": row.get("zoning"),
        "Lot Sqft": safe_float(row.get("lotarea")),
        "Building Sq. Ft.": safe_float(row.get("bldgarea")),
        "Lot Width (Feet)": safe_float(row.get("lot_frontage")),
        "Lot Depth (Feet)": safe_float(row.get("lot_depth")),
        "Buildable Sq. Ft.": safe_float(row.get("buildable_sf_narrow")),
        "Buildable SF Wide": safe_float(row.get("buildable_sf_wide")),  # Airtable column typo
        "Res FAR": safe_float(row.get("official_far_narrow")),
        "Residential Units": safe_int(row.get("unitsres")),
        "Building Class RAW": row.get("building_class"),
        "BSF Type": "Market Rate",
        "Data Source": "Auto-Sync",
    }

    asset = map_asset_type(row.get("asset_type"))
    if asset:
        fields["Asset Type"] = asset

    key = format_block_lot(row.get("borough"), row.get("block"), row.get("lot"))
    if key:
        fields["Block & Lot"] = key

    area_ids = resolve_area_id(row.get("neighborhood"), area_lookup, area_names)
    if area_ids:
        fields["Area"] = area_ids
    else:
        if row.get("neighborhood"):
            area_misses.append(str(row.get("neighborhood")))

    return {k: v for k, v in fields.items() if v is not None}


def build_existing_map(comps_table) -> Dict[str, str]:
    existing = comps_table.all(formula="{Data Source} = 'Auto-Sync'")
    m = {}
    for rec in existing:
        key = rec.get("fields", {}).get("Block & Lot")
        if key:
            m[str(key)] = rec["id"]
    return m


def sync(limit: Optional[int] = None, dry_run: bool = False):
    comps_table, areas_table = get_airtable_tables()

    print("=" * 70)
    print("AIRTABLE SYNC (comps_dev_base_v2 -> Property Comps)")
    print("=" * 70)
    print(f"Base:  {AIRTABLE_BASE_ID}")
    print(f"Table: {AIRTABLE_TABLE_NAME}")

    print("Loading Areas...")
    area_lookup, area_names = build_area_cache(areas_table)
    print(f"Areas loaded: {len(area_lookup)}")

    print("Loading existing Auto-Sync records...")
    existing_map = build_existing_map(comps_table)
    print(f"Existing Auto-Sync keys: {len(existing_map)}")

    print("Fetching from DB...")
    rows = fetch_properties(limit=limit)
    print(f"Fetched: {len(rows)}")

    area_misses: List[str] = []
    payloads: List[tuple] = []

    for r in rows:
        fields = map_row_to_airtable_fields(r, area_lookup, area_names, area_misses)
        key = fields.get("Block & Lot")
        if key:
            payloads.append((key, fields))

    print(f"Prepared: {len(payloads)} records (skipped no key: {len(rows) - len(payloads)})")

    if dry_run:
        print("\n[DRY RUN] First 5 mapped records:")
        for key, f in payloads[:5]:
            print(f"  Key={key} | Address={f.get('Address')} | Area={f.get('Area', 'NO MATCH')} | Asset={f.get('Asset Type')}")
        if area_misses:
            uniq = sorted(set(area_misses))
            print(f"\n[DRY RUN] Area misses ({len(uniq)} unique). First 20:")
            for x in uniq[:20]:
                print(f"  - {x}")
        return

    created = 0
    updated = 0
    errors = 0

    print("\nUpserting to Airtable...")
    for key, fields in payloads:
        try:
            rec_id = existing_map.get(key)
            if rec_id:
                comps_table.update(rec_id, fields)
                updated += 1
            else:
                rec = comps_table.create(fields)
                existing_map[key] = rec["id"]
                created += 1
        except Exception as e:
            errors += 1
            print("Error:", key, e)

        time.sleep(RATE_SLEEP)

    print("\n" + "=" * 70)
    print("Created:", created)
    print("Updated:", updated)
    print("Errors:", errors)

    if area_misses:
        uniq = sorted(set(area_misses))
        print("\nArea misses (unique):", len(uniq))
        for x in uniq[:30]:
            print(" -", x)
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    sync(limit=args.limit, dry_run=args.dry_run)