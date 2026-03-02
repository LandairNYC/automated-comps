"""
Supabase → Airtable Sync Script
KPI #2: Development Site Comps
Fixes: linked record IDs for Area field, multi-select for Asset Type
"""

import os
import time
import argparse
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from difflib import SequenceMatcher

from pathlib import Path
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# ── Config ────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID  = os.getenv("AIRTABLE_BASE_ID", "appUxgIpmSUYBZxJ8")
COMPS_TABLE       = "Property Comps"
AREAS_TABLE       = "Areas"

SUPABASE_HOST     = os.getenv("SUPABASE_HOST")
SUPABASE_DB       = os.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER     = os.getenv("SUPABASE_USER", "postgres")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT     = int(os.getenv("SUPABASE_PORT", "5432"))

BATCH_SIZE = 10
RATE_LIMIT_PAUSE = 0.25  # seconds between batches (5 req/sec limit)

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}

# ── Airtable helpers ───────────────────────────────────────────────────────────

def airtable_url(table: str) -> str:
    import urllib.parse
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{urllib.parse.quote(table)}"


def get_all_records(table: str) -> list[dict]:
    """Fetch all records from an Airtable table (handles pagination)."""
    records = []
    params = {}
    while True:
        resp = requests.get(airtable_url(table), headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
        time.sleep(RATE_LIMIT_PAUSE)
    return records


def upsert_records(table: str, records: list[dict], field_to_match: str = None) -> dict:
    """
    Create or update records in Airtable.
    If field_to_match is set, uses PATCH upsert (Airtable upsert API).
    Otherwise plain POST create.
    Returns summary dict.
    """
    url = airtable_url(table)
    created = updated = errors = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]

        if field_to_match:
            # Upsert via PATCH
            payload = {
                "performUpsert": {"fieldsToMergeOn": [field_to_match]},
                "records": [{"fields": r} for r in batch],
            }
            resp = requests.patch(url, headers=HEADERS, json=payload)
        else:
            payload = {"records": [{"fields": r} for r in batch]}
            resp = requests.post(url, headers=HEADERS, json=payload)

        if resp.ok:
            result = resp.json()
            created += len(result.get("createdRecords", []))
            updated += len(result.get("updatedRecords", []))
        else:
            errors += len(batch)
            print(f"  ✗ Batch {i//BATCH_SIZE + 1} error: {resp.status_code} – {resp.text[:300]}")

        time.sleep(RATE_LIMIT_PAUSE)
        if (i // BATCH_SIZE) % 50 == 0 and i > 0:
            print(f"  … {i + len(batch)}/{len(records)} processed")

    return {"created": created, "updated": updated, "errors": errors}


# ── Neighborhood name mapping ──────────────────────────────────────────────────

# Manual overrides for known mismatches between DB format and Airtable
# DB uses ALL-CAPS with dashes/parens; Airtable uses Title Case
MANUAL_NEIGHBORHOOD_MAP = {
    # Williamsburg variants
    "WILLIAMSBURG-SOUTH":        "South Williamsburg",
    "WILLIAMSBURG-NORTH":        "North Williamsburg",
    "WILLIAMSBURG":              "North Williamsburg",
    "EAST WILLIAMSBURG":         "East Williamsburg",
    # Harlem variants
    "CENTRAL HARLEM":            "Central Harlem",
    "CENTRAL HARLEM-NORTH":      "Central Harlem",
    "EAST HARLEM":               "East Harlem",
    "WEST HARLEM":               "West Harlem",
    "SOUTH HARLEM":              "South Harlem",
    # Manhattan
    "UPPER WEST SIDE (59-79)":   "Upper West Side",
    "UPPER WEST SIDE (79-96)":   "Upper West Side",
    "UPPER EAST SIDE (59-79)":   "Upper East Side",
    "UPPER EAST SIDE (79-96)":   "Upper East Side",
    "UPPER EAST SIDE (96-110)":  "Upper East Side",
    "LOWER EAST SIDE":           "Lower East Side",
    "MIDTOWN-MIDEAST":           "Midtown",
    "MIDTOWN-MIDWEST":           "Midtown",
    "MIDTOWN CBD":               "Midtown",
    "HELL'S KITCHEN":            "Hells Kitchen",
    "HELLS KITCHEN":             "Hells Kitchen",
    "SOHO":                      "SOHO",
    "NOHO":                      "SOHO",
    "NOMAD":                     "NOMAD",
    "CHELSEA":                   "Chelsea",
    "GRAMERCY":                  "Gramercy Park",
    "GREENWICH VILLAGE-CENTRAL": "Greenwich Village",
    "GREENWICH VILLAGE-WEST":    "Greenwich Village",
    "TRIBECA":                   "Tribeca",
    "FINANCIAL":                 "Financial District",
    "BATTERY PARK":              "Financial District",
    "INWOOD":                    "Inwood",
    "WASHINGTON HEIGHTS LOWER":  "Washington Heights",
    "WASHINGTON HEIGHTS UPPER":  "Washington Heights",
    "MORNINGSIDE HEIGHTS":       "Morningside Heights",
    "MANHATTANVILLE":            "Manhattanville",
    "MARBLE HILL":               "Marble Hill",
    "HUDSON YARDS":              "Hudson Yards",
    "FLATIRON":                  "Flatiron District",
    "KIPS BAY":                  "Kips Bay",
    "MURRAY HILL":               "Murray Hill",
    "SUTTON PLACE":              "Sutton Place",
    # Brooklyn
    "BEDFORD STUYVESANT":        "Bedford-Stuyvesant",
    "BEDFORD-STUYVESANT":        "Bedford-Stuyvesant",
    "CROWN HEIGHTS NORTH":       "Crown Heights",
    "CROWN HEIGHTS SOUTH":       "Crown Heights",
    "CROWN HEIGHTS":             "Crown Heights",
    "EAST CROWN HEIGHTS":        "East Crown Heights",
    "BUSHWICK NORTH":            "Bushwick",
    "BUSHWICK SOUTH":            "Bushwick",
    "BUSHWICK":                  "Bushwick",
    "EAST FLATBUSH":             "East Flatbush",
    "FLATBUSH":                  "Flatbush",
    "BOROUGH PARK":              "Borough Park",
    "BENSONHURST":               "Bensonhurst",
    "BAY RIDGE":                 "Bay Ridge",
    "SUNSET PARK EAST":          "Sunset Park",
    "SUNSET PARK WEST":          "Sunset Park",
    "SUNSET PARK":               "Sunset Park",
    "RED HOOK":                  "Red Hook",
    "GOWANUS":                   "Gowanus",
    "PARK SLOPE":                "Park Slope",
    "SOUTH SLOPE":               "South Slope",
    "COBBLE HILL":               "Cobble Hill",
    "BOERUM HILL":               "Boerum Hill",
    "CARROLL GARDENS":           "Carroll Gardens",
    "CLINTON HILL":              "Clinton Hill",
    "PROSPECT HEIGHTS":          "Prospect Heights",
    "FORT GREENE":               "Fort Greene",
    "DOWNTOWN BROOKLYN":         "Downtown Brooklyn",
    "DUMBO":                     "DUMBO",
    "VINEGAR HILL":              "Vinegar Hill",
    "COLUMBIA WATERFRONT":       "Columbia Waterfront District",
    "GREENPOINT":                "Greenpoint",
    "GREENWOOD HEIGHTS":         "Greenwood Heights",
    "DITMAS PARK":               "Ditmas Park",
    "CANARSIE":                  "Canarsie",
    "BROWNSVILLE":               "Brownsville",
    "EAST NEW YORK":             "East New York",
    "CYPRESS HILLS":             "Cypress Hills",
    "OCEAN HILL":                "Ocean Hill",
    "WEEKSVILLE":                "Weeksville",
    "BRIGHTON BEACH":            "Brighton Beach",
    "SHEEPSHEAD BAY":            "Sheepshead Bay",
    "HOMECREST":                 "Homecrest",
    "GRAVESEND":                 "Gravesend",
    "CONEY ISLAND":              "Coney Island",
    "MARINE PARK":               "Marine Park",
    "FLATLANDS":                 "Flatbush",
    "MILL BASIN":                "Old Mill Basin",
    "WINDSOR TERRACE":           "Windsor Terrace",
    "KENSINGTON":                "Kensington",
    "LEFFERTS GARDEN":           "Lefferts Garden",
    "PROSPECT PARK SOUTH":       "Prospect Park South",
    # Queens
    "ASTORIA":                   "Astoria",
    "LONG ISLAND CITY":          "Long Island City",
    "DUTCH KILLS":               "Dutch Kills",
    "SUNNYSIDE":                 "Sunnyside",
    "JACKSON HEIGHTS":           "Jackson Heights",
    "ELMHURST":                  "Elmhurst",
    "CORONA":                    "Corona",
    "FLUSHING":                  "Flushing",
    "EAST FLUSHING":             "East Flushing",
    "FOREST HILLS":              "Forest Hills",
    "REGO PARK":                 "Rego Park",
    "KEW GARDENS":               "Kew Gardens",
    "JAMAICA":                   "Jamaica",
    "SOUTH JAMAICA":             "South Jamaica",
    "HOLLIS":                    "Hollis",
    "QUEENS VILLAGE":            "Queens Village",
    "ROSEDALE":                  "Rosedale",
    "LAURELTON":                 "Laurelton",
    "ROCHDALE":                  "Rochdale",
    "SPRINGFIELD GARDEN":        "Springfield Garden",
    "SAINT ALBANS":              "Saint Albans",
    "BAYSIDE":                   "Bayside",
    "WHITESTONE":                "Whitestone",
    "COLLEGE POINT":             "College Point",
    "LITTLE NECK":               "Little Neck",
    "DOUGLASTON":                "Douglaston",
    "FRESH MEADOWS":             "Fresh Meadows",
    "MIDDLE VILLAGE":            "Middle Village",
    "GLENDALE":                  "Glendale",
    "RIDGEWOOD":                 "Ridgewood",
    "MASPETH":                   "Maspeth",
    "OZONE PARK":                "Ozone Park",
    "SOUTH OZONE PARK":          "South Ozone Park",
    "RICHMOND HILL":             "Richmond Hill",
    "WOODHAVEN":                 "Woodhaven",
    "HOWARD BEACH":              "Rockaway",
    "ARVERNE":                   "Arverne",
    "ROCKAWAY BEACH":            "Rockaway Beach",
    "FAR ROCKAWAY":              "Rockaway",
    "EAST ROCKAWAY":             "East Rockaway",
    "EDGEMERE":                  "Edgemere",
    "EAST ELMHURST":             "East Elmhurst",
    "WILLET'S POINT":            "Willet's Point",
    "POMONOK":                   "Pomonok",
    "BRIARWOOD":                 "Briarwood",
    "CAMBRIA HEIGHTS":           "Cambria Heights",
    "OLD ASTORIA":               "Old Astoria",
    # Bronx
    "MOTT HAVEN":                "Mott Haven",
    "PORT MORRIS":               "Port Morris",
    "LONGWOOD":                  "Longwood",
    "MELROSE":                   "Melrose",
    "SOUTH BRONX":               "Mott Haven",
    "HUNTS POINT":               "Hunts Point",
    "SOUNDVIEW":                 "Soundview",
    "CLASON POINT":              "Clason Point",
    "WEST FARMS":                "West Farms",
    "TREMONT":                   "Tremont",
    "EAST TREMONT":              "East Tremont",
    "CROTONA":                   "Crotona",
    "BELMONT":                   "Belmont",
    "FORDHAM":                   "Fordham",
    "UNIVERSITY HEIGHTS":        "University Heights",
    "MORRIS HEIGHTS":            "Morris Heights",
    "HIGH BRIDGE":               "High Bridge",
    "CONCOURSE":                 "Concourse",
    "BATHGATE":                  "Bathgate",
    "MORRISANIA":                "Morrisania",
    "FOXHURST":                  "Foxhurst",
    "CLAREMONT":                 "Claremont",
    "MOUNT EDEN":                "Mount Eden",
    "MOUNT HOPE":                "Mount Hope",
    "JEROME PARK":               "Jerome Park",
    "KINGSBRIDGE":               "Kingsbridge",
    "KINGSBRIDGE HEIGHTS":       "Kingsbridge Heights",
    "RIVERDALE":                 "Riverdale",
    "FIELDSTON":                 "Fieldston",
    "NORTH RIVERDALE":           "North Riverdale",
    "NORWOOD":                   "Norwood",
    "BEDFORD PARK":              "Bedford Park",
    "WILLIAMS BRIDGE":           "Williamsbridge",
    "WILLIAMSBRIDGE":            "Williamsbridge",
    "WAKEFIELD":                 "Wakefield",
    "WOODLAWN":                  "Woodlawn",
    "ALLERTON":                  "Allerton",
    "LACONIA":                   "Laconia",
    "OLINVILLE":                 "Olinville",
    "EDENWALD":                  "Edenwald",
    "EASTCHESTER":               "Eastchester",
    "PELHAM BAY":                "Pelham Bay",
    "PELHAM PARKWAY":            "Pelham Parkway",
    "VAN NEST":                  "Van Nest",
    "MORRIS PARK":               "Morris Park",
    "PARKCHESTER":               "Parkchester",
    "UNIONPORT":                 "Unionport",
    "WESTCHESTER SQUARE":        "Westchester Square",
    "VAN CORTLAND VILLAGE":      "Van Cortland Village",
    "BAYCHESTER":                "Edenwald",
    "COUNTRY CLUB":              "Pelham Bay",
    "CITY ISLAND":               "Pelham Bay",
    "CO-OP CITY":                "Edenwald",
    "SCHUYLERVILLE":             "Pelham Bay",
}

# Asset type mapping from DB values → exact Airtable multi-select option text
ASSET_TYPE_MAP = {
    "Residential Development Site": "Residential Development Site",
    "Residential Dev Site":         "Residential Development Site",
    "Residential Property":         "Residential Property",
    "Industrial Development Site":  "Industrial Development Site",
    "Industrial Dev Site":          "Industrial Development Site",
    "Industrial Building":          "Industrial Building",
    "Vacant Land":                  "Vacant Land",
    "Mixed Use":                    "Mixed Use",
    "Commercial":                   "Commercial",
}


def fuzzy_match(name: str, candidates: list[str], threshold: float = 0.6) -> str | None:
    """Find best fuzzy match for name in candidates list."""
    best_score = 0
    best_match = None
    name_lower = name.lower()
    for c in candidates:
        score = SequenceMatcher(None, name_lower, c.lower()).ratio()
        if score > best_score:
            best_score = score
            best_match = c
    return best_match if best_score >= threshold else None


def build_area_lookup(area_records: list[dict]) -> dict[str, str]:
    """
    Returns dict: airtable_name_lower → record_id
    e.g. {"south williamsburg": "recXXXXX", ...}
    """
    return {
        rec["fields"].get("Name", "").lower(): rec["id"]
        for rec in area_records
        if rec.get("fields", {}).get("Name")
    }


def resolve_area(db_neighborhood: str, area_lookup: dict[str, str],
                 airtable_names: list[str]) -> list[str] | None:
    """
    Returns Airtable record ID array for a neighborhood name, or None if no match.
    Priority: manual map → exact → fuzzy
    """
    if not db_neighborhood:
        return None

    db_clean = db_neighborhood.strip().upper()

    # 1. Manual override
    mapped = MANUAL_NEIGHBORHOOD_MAP.get(db_clean)
    if mapped:
        rec_id = area_lookup.get(mapped.lower())
        if rec_id:
            return [rec_id]

    # 2. Exact match (case-insensitive)
    rec_id = area_lookup.get(db_clean.lower())
    if rec_id:
        return [rec_id]

    # 3. Fuzzy match
    match = fuzzy_match(db_clean, airtable_names, threshold=0.65)
    if match:
        rec_id = area_lookup.get(match.lower())
        if rec_id:
            return [rec_id]

    return None


# ── Database ───────────────────────────────────────────────────────────────────

def get_db_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST,
        dbname=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
        port=SUPABASE_PORT,
        sslmode="require",
    )


def fetch_properties(limit: int = None) -> list[dict]:
    """Fetch records from comps_dev_base that should be synced."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = """
                SELECT
                    -- Identity
                    address,
                    bbl,
                    borough,
                    neighborhood,

                    -- Classification
                    asset_type,
                    zoning_district AS zones,
                    building_class,
                    year_built,

                    -- Lot & building
                    lot_area_sf AS lot_sqft,
                    bldg_area_sf,
                    buildable_sf,
                    res_far,
                    comm_far,
                    num_floors,
                    num_units,

                    -- Sale
                    sale_price       AS closing_price,
                    sale_date        AS closing_date,
                    price_per_bsf    AS ppbsf,

                    -- Buyer
                    buyer_name,

                    -- Meta
                    data_source,
                    sync_tag

                FROM comps_dev_base
                WHERE (sync_tag IS NULL OR sync_tag != 'Manual')
                  AND sale_date >= '2022-01-01'
                  AND sale_price BETWEEN 100000 AND 20000000
                ORDER BY sale_date DESC
            """
            if limit:
                query += f" LIMIT {limit}"
            cur.execute(query)
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_existing_sync_records(airtable_records: list[dict]) -> set[str]:
    """Return set of BBLs already in Airtable (from Auto-Sync records)."""
    existing = set()
    for rec in airtable_records:
        fields = rec.get("fields", {})
        if fields.get("Sync Tag") == "Auto-Sync" or fields.get("Data Source") == "Auto-Sync":
            bbl = fields.get("BBL") or fields.get("bbl")
            if bbl:
                existing.add(str(bbl))
    return existing


# ── Field mapping ──────────────────────────────────────────────────────────────

def build_airtable_record(
    prop: dict,
    area_lookup: dict[str, str],
    airtable_names: list[str],
    area_miss_log: list,
) -> dict:
    """Convert a DB row to an Airtable fields dict."""

    fields = {}

    # ── Text / number fields ──────────────────────────────────────────────────
    if prop.get("address"):
        fields["Address"] = str(prop["address"])
    if prop.get("bbl"):
        fields["BBL"] = str(prop["bbl"])
    if prop.get("borough"):
        fields["Borough"] = str(prop["borough"])
    if prop.get("building_class"):
        fields["Building Class"] = str(prop["building_class"])
    if prop.get("year_built") and int(prop["year_built"]) > 0:
        fields["Year Built"] = int(prop["year_built"])
    if prop.get("lot_sqft"):
        fields["Lot Sqft"] = float(prop["lot_sqft"])
    if prop.get("bldg_area_sf"):
        fields["Building Sq. Ft."] = float(prop["bldg_area_sf"])
    if prop.get("buildable_sf"):
        fields["Buildable Sq. Ft."] = float(prop["buildable_sf"])
    if prop.get("res_far"):
        fields["Residential FAR"] = float(prop["res_far"])
    if prop.get("num_floors"):
        fields["Number of Floors"] = int(prop["num_floors"])
    if prop.get("num_units"):
        fields["Number of Units"] = int(prop["num_units"])
    if prop.get("closing_price"):
        fields["Closing Price"] = float(prop["closing_price"])
    if prop.get("ppbsf"):
        fields["PPBSF"] = float(prop["ppbsf"])
    if prop.get("buyer_name"):
        fields["Buyer Name"] = str(prop["buyer_name"])

    # ── Date field ────────────────────────────────────────────────────────────
    if prop.get("closing_date"):
        d = prop["closing_date"]
        # Airtable wants "YYYY-MM-DD"
        if hasattr(d, "strftime"):
            fields["Closing Date"] = d.strftime("%Y-%m-%d")
        else:
            fields["Closing Date"] = str(d)[:10]

    # ── Zoning (text) ─────────────────────────────────────────────────────────
    if prop.get("zones"):
        fields["Zones"] = str(prop["zones"])

    # ── Asset Type → MULTI-SELECT (must be a list!) ───────────────────────────
    if prop.get("asset_type"):
        raw = str(prop["asset_type"]).strip()
        mapped = ASSET_TYPE_MAP.get(raw, raw)  # fallback to raw if not in map
        fields["Asset Type"] = [mapped]  # ← Airtable multi-select needs array

    # ── Area → LINKED RECORD (must be array of record IDs!) ──────────────────
    if prop.get("neighborhood"):
        rec_ids = resolve_area(prop["neighborhood"], area_lookup, airtable_names)
        if rec_ids:
            fields["Area"] = rec_ids  # ← e.g. ["recABC123"]
        else:
            area_miss_log.append(prop["neighborhood"])
            # Leave Area blank rather than error-out the whole record

    # ── Sync tag ──────────────────────────────────────────────────────────────
    fields["Sync Tag"] = "Auto-Sync"

    return fields


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sync Supabase dev comps to Airtable")
    parser.add_argument("--all",    action="store_true", help="Sync all eligible records")
    parser.add_argument("--limit",  type=int, default=None, help="Limit number of records")
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    args = parser.parse_args()

    print("=" * 60)
    print("AIRTABLE SYNC")
    print("=" * 60)

    # 1. Connect to Airtable and load Areas
    print(f"Connected to base: {AIRTABLE_BASE_ID}")
    print("Loading Areas table...")
    area_records = get_all_records(AREAS_TABLE)
    area_lookup = build_area_lookup(area_records)
    airtable_names = list(area_lookup.keys())
    print(f"Loaded {len(area_records)} areas")

    # 2. Load existing Airtable Property Comps (to detect duplicates)
    print("Checking for existing Auto-Sync records...")
    existing_comps = get_all_records(COMPS_TABLE)
    existing_bbls = get_existing_sync_records(existing_comps)
    print(f"Found {len(existing_bbls)} existing Auto-Sync records")

    # 3. Fetch from DB
    limit = args.limit if not args.all else None
    print(f"Fetching properties from Supabase...")
    properties = fetch_properties(limit=limit)
    print(f"Fetched {len(properties)} properties")

    # 4. Deduplicate
    new_props = [p for p in properties if str(p.get("bbl", "")) not in existing_bbls]
    skipped = len(properties) - len(new_props)
    print(f"Skipping {skipped} already-synced records → {len(new_props)} to upload")

    if args.dry_run:
        print("\n[DRY RUN] First 3 records that would be uploaded:")
        area_miss_log = []
        for p in new_props[:3]:
            rec = build_airtable_record(p, area_lookup, airtable_names, area_miss_log)
            print(f"  {rec.get('Address', 'N/A')} → Area: {rec.get('Area', 'NO MATCH')}, "
                  f"AssetType: {rec.get('Asset Type', 'N/A')}")
        return

    # 5. Build records
    area_miss_log = []
    airtable_records = []
    for prop in new_props:
        try:
            rec = build_airtable_record(prop, area_lookup, airtable_names, area_miss_log)
            airtable_records.append(rec)
        except Exception as e:
            print(f"  ✗ Error building record for {prop.get('address')}: {e}")

    # 6. Upload
    print(f"\nUploading {len(airtable_records)} records...")
    result = upsert_records(COMPS_TABLE, airtable_records, field_to_match="BBL")

    # 7. Report
    print("\n" + "=" * 60)
    print(f"✅ Created:  {result['created']}")
    print(f"🔄 Updated:  {result['updated']}")
    print(f"❌ Errors:   {result['errors']}")
    print(f"⏭  Skipped:  {skipped}")

    if area_miss_log:
        unique_misses = sorted(set(area_miss_log))
        print(f"\n⚠️  {len(unique_misses)} unique neighborhoods had no Area match:")
        for n in unique_misses[:30]:
            print(f"   '{n}'")
        if len(unique_misses) > 30:
            print(f"   ... and {len(unique_misses) - 30} more")
        print("\nAdd these to MANUAL_NEIGHBORHOOD_MAP in the script to fix them.")

    print("=" * 60)


if __name__ == "__main__":
    main()