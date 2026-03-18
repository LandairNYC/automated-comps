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
    # Williamsburg
    "WILLIAMSBURG-SOUTH":        "South Williamsburg",
    "WILLIAMSBURG-NORTH":        "North Williamsburg",
    "WILLIAMSBURG":              "North Williamsburg",
    "EAST WILLIAMSBURG":         "East Williamsburg",
    "WILLIAMSBURG-EAST":         "East Williamsburg",
    # Harlem
    "CENTRAL HARLEM":            "Central Harlem",
    "CENTRAL HARLEM-NORTH":      "Central Harlem",
    "HARLEM-CENTRAL":            "Central Harlem",
    "HARLEM-EAST":               "East Harlem",
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
    "FLATBUSH-CENTRAL":          "Flatbush",
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
    "OCEAN PARKWAY-SOUTH":       "Ocean Parkway",
    "BUSH TERMINAL":             "Sunset Park",
    # Queens
    "ASTORIA":                   "Astoria",
    "LONG ISLAND CITY":          "Long Island City",
    "DUTCH KILLS":               "Dutch Kills",
    "SUNNYSIDE":                 "Sunnyside",
    "JACKSON HEIGHTS":           "Jackson Heights",
    "ELMHURST":                  "Elmhurst",
    "CORONA":                    "Corona",
    "FLUSHING":                  "Flushing",
    "FLUSHING-NORTH":            "Flushing",
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
    "ROCKAWAY PARK":             "Rockaway Beach",
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
    "MOTT HAVEN/PORT MORRIS":    "Mott Haven",
    "PORT MORRIS":               "Port Morris",
    "LONGWOOD":                  "Longwood",
    "MORRISANIA/LONGWOOD":       "Morrisania",
    "MELROSE":                   "Melrose",
    "SOUTH BRONX":               "Mott Haven",
    "HUNTS POINT":               "Hunts Point",
    "SOUNDVIEW":                 "Soundview",
    "CLASON POINT":              "Clason Point",
    "WEST FARMS":                "West Farms",
    "TREMONT":                   "Tremont",
    "EAST TREMONT":              "East Tremont",
    "CROTONA":                   "Crotona",
    "CROTONA PARK":              "Crotona",
    "BELMONT":                   "Belmont",
    "FORDHAM":                   "Fordham",
    "UNIVERSITY HEIGHTS":        "University Heights",
    "MORRIS HEIGHTS":            "Morris Heights",
    "HIGHBRIDGE/MORRIS HEIGHTS": "High Bridge",
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
    "BEDFORD PARK/NORWOOD":      "Bedford Park",
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
    "PELHAM PARKWAY SOUTH":      "Pelham Parkway",
    "VAN NEST":                  "Van Nest",
    "MORRIS PARK":               "Morris Park",
    "PARKCHESTER":               "Parkchester",
    "UNIONPORT":                 "Unionport",
    "WESTCHESTER":               "Westchester Square",
    "WESTCHESTER SQUARE":        "Westchester Square",
    "VAN CORTLAND VILLAGE":      "Van Cortland Village",
    "BAYCHESTER":                "Edenwald",
    "COUNTRY CLUB":              "Pelham Bay",
    "CITY ISLAND":               "Pelham Bay",
    "CO-OP CITY":                "Edenwald",
    "SCHUYLERVILLE":             "Pelham Bay",
    "SCHUYLERVILLE/PELHAM BAY":  "Pelham Bay",
    # Staten Island
    "PORT RICHMOND":             "Port Richmond",
    "STAPLETON":                 "Stapleton",
    "ST. GEORGE":                "St. George",
    "TOMPKINSVILLE":             "Tompkinsville",
    "CLIFTON":                   "Clifton",
    "ROSEBANK":                  "Rosebank",
    "GRASMERE":                  "Grasmere",
    "OLD TOWN":                  "Old Town",
    "DONGAN HILLS":              "Dongan Hills",
    "MIDLAND BEACH":             "Midland Beach",
    "NEW DORP":                  "New Dorp",
    "OAKWOOD":                   "Oakwood",
    "GREAT KILLS":               "Great Kills",
    "ELTINGVILLE":               "Eltingville",
    "ANNADALE":                  "Annadale",
    "HUGUENOT":                  "Huguenot",
    "TOTTENVILLE":               "Tottenville",
    "RICHMOND VALLEY":           "Richmond Valley",
    "CHARLESTON":                "Charleston",
    "ROSSVILLE":                 "Rossville",
    "WOODROW":                   "Woodrow",
    "TRAVIS":                    "Travis",
    "NEW SPRINGVILLE":           "New Springville",
    "BULLS HEAD":                "Bulls Head",
    "WESTERLEIGH":               "Westerleigh",
    "CASTLETON CORNERS":         "Castleton Corners",
    "WEST BRIGHTON":             "West Brighton",
    "NEW BRIGHTON":              "New Brighton",
    "SILVER LAKE":               "Silver Lake",
    "SUNNYSIDE SI":              "Sunnyside",
    "WILLOWBROOK":               "Willowbrook",
    "MANOR HEIGHTS":             "Manor Heights",
    "TODT HILL":                 "Todt Hill",
    "LIGHTHOUSE HILL":           "Lighthouse Hill",
    "RICHMOND TOWN":             "Richmond Town",
    # Missing neighborhoods
    "KINGSBRIDGE/JEROME PARK": "Jerome Park",
    "MOUNT HOPE/MOUNT EDEN": "Mount Hope",
    "OCEAN PARKWAY-NORTH": "Midwood",
    "DOWNTOWN-METROTECH": "Downtown Brooklyn",
    "DOWNTOWN-FULTON MALL": "Downtown Brooklyn",
    "CASTLE HILL/UNIONPORT": "Unionport",
    "SO. JAMAICA-BAISLEY PARK": "South Jamaica",
    "LITTLE ITALY": "SOHO",
    "HARLEM-WEST": "West Harlem",
    "HARLEM-UPPER": "Central Harlem",
    "HAMMELS": "Rockaway",
    "JAMAICA ESTATES": "Jamaica",
    "SPRING CREEK": "East New York",
    "CIVIC CENTER": "Downtown Brooklyn",
    "JAVITS CENTER": "Hudson Yards",
    "MADISON": "Midtown",
    # After adding Port Richmond, New Brighton to Airtable Areas:
    "NEW BRIGHTON-ST. GEORGE": "New Brighton",
}


ASSET_TYPE_MAP = {
    "Development Site":             "Development Site",
    "Residential Property":         "Residential Property",
    "Vacant Land":                  "Vacant Land",
    "Retail Building":              "Retail Building",
    "Industrial Building":          "Industrial Building",
    "Industrial Development Site":  "Industrial Development Site",
    "Mixed Use":                    "Mixed Use",
    # legacy fallbacks
    "Residential Development Site": "Development Site",
    "Industrial Dev Site":          "Industrial Development Site",
    "Commercial":                   "Commercial",
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
    return mapped if mapped else raw


def fetch_properties(limit: Optional[int] = None, since_date: Optional[str] = None) -> List[dict]:
    and_clause = ""
    if since_date:
        and_clause = f"AND sale_date >= '{since_date}'"

    query = f"""
        SELECT
            -- Identity
            address, bbl, borough, block, lot,
            neighborhood, zip_code,

            -- Transaction
            sale_price_clean, sale_date, document_date, doc_type,

            -- Classification
            zoning, zoning_base, asset_type,
            building_class, building_class_name,
            landuse, year_built,

            -- Lot & building
            lotarea, bldgarea, resarea, comarea,
            lot_frontage, lot_depth,
            num_buildings, unitsres, unitstotal, numfloors,

            -- FAR & buildable (single value — pluto_resid_far * lotarea)
            buildable_sf,
            pluto_resid_far,
            pluto_comm_far,
            ppbsf,
            price_per_land_sf,
            price_per_bldg_sf,

            -- Parties
            buyer_names, seller_names, ownername,

            -- Scores & flags
            development_potential_score,
            is_portfolio, portfolio_parcel_count, portfolio_flag,
            outlier_flag,

            -- Location
            latitude, longitude,

            -- Nearest comps (computed by scripts/compute_nearest_comps.py)
            nearest_comps_proximity,
            nearest_comps_smart

        FROM comps_dev_base_v2
        WHERE asset_type != 'Residential Property'
        {and_clause}
        ORDER BY sale_date DESC
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return [dict(r) for r in cur.fetchall()]


def map_row_to_airtable_fields(row: dict, area_lookup: Dict[str, str], area_names: List[str], area_misses: List[str]):
    # ── Date formatting ───────────────────────────────────────────────────────
    def fmt_date(val):
        if not val:
            return None
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        return str(val)[:10]

    fields = {
        # Identity
        "Address":              row.get("address"),
        "Zip Code":             row.get("zip_code"),

        # Transaction
        "Closing Price":        safe_float(row.get("sale_price_clean")),
        "Closing Date":         fmt_date(row.get("sale_date")),
        "Document Date":        fmt_date(row.get("document_date")),
        "Doc Type":             row.get("doc_type"),

        # Classification
        "Zones (Manual)":       row.get("zoning"),
        "Zoning Base":          row.get("zoning_base"),
        "Building Class RAW":   row.get("building_class"),
        "Building Class Name":  row.get("building_class_name"),
        "Year Built":           safe_int(row.get("year_built")) if row.get("year_built") and int(row.get("year_built") or 0) > 0 else None,

        # Lot & building
        "Lot Sqft":             safe_float(row.get("lotarea")),
        "Building Sq. Ft.":     safe_float(row.get("bldgarea")),
        "Residential Area SF":  safe_float(row.get("resarea")),
        "Commercial Area SF":   safe_float(row.get("comarea")),
        "Lot Width (Feet)":     safe_float(row.get("lot_frontage")),
        "Lot Depth (Feet)":     safe_float(row.get("lot_depth")),
        "Floors":               safe_int(row.get("numfloors")),
        "Residential Units":    safe_int(row.get("unitsres")),
        "Total Units":          safe_int(row.get("unitstotal")),
        "Num Buildings":        safe_int(row.get("num_buildings")),

        # FAR & buildable — single value (pluto_resid_far * lotarea)
        "Buildable Sq. Ft.":    safe_float(row.get("buildable_sf")),
        "Res FAR":              safe_float(row.get("pluto_resid_far")),
        "Comm FAR":             safe_float(row.get("pluto_comm_far")),
        # PPBSF is a formula field in Airtable — do not write
        # "PPLSF":                safe_float(row.get("price_per_land_sf")),
        # "PPSF":                 safe_float(row.get("price_per_bldg_sf")),

        # Parties
        "Buyer Name Text":      row.get("buyer_names"),
        "Seller Name Text":     row.get("seller_names"),
        "Owner Name":           row.get("ownername"),

        # Scores & flags
        "Dev Score":            safe_int(row.get("development_potential_score")),
        "Is Portfolio":         row.get("is_portfolio"),
        "Portfolio Parcel Count": safe_int(row.get("portfolio_parcel_count")),
        "Portfolio Flag":       row.get("portfolio_flag"),
        "Outlier Flag":         row.get("outlier_flag"),

        # Location
        "Latitude":             safe_float(row.get("latitude")),
        "Longitude":            safe_float(row.get("longitude")),

        # Nearest comps (computed by scripts/compute_nearest_comps.py)
        "Nearest Comps (Proximity)": row.get("nearest_comps_proximity"),
        "Nearest Comps (Smart)":     row.get("nearest_comps_smart"),

        # Meta
        "BSF Type":             "Market Rate",
        "Data Source":          "Auto-Sync",
    }

    # Asset Type — multi-select array
    asset = map_asset_type(row.get("asset_type"))
    if asset:
        fields["Asset Type"] = asset

    # Block & Lot key for upsert deduplication
    key = format_block_lot(row.get("borough"), row.get("block"), row.get("lot"))
    if key:
        fields["Block & Lot"] = key

    # Area — linked record (must be array of record IDs)
    area_ids = resolve_area_id(row.get("neighborhood"), area_lookup, area_names)
    if area_ids:
        fields["Area"] = area_ids
    else:
        if row.get("neighborhood"):
            area_misses.append(str(row.get("neighborhood")))

    # Strip None values — Airtable rejects null fields
    return {k: v for k, v in fields.items() if v is not None}


def build_existing_map(comps_table) -> Dict[str, str]:
    try:
        existing = comps_table.all(formula="{Data Source} = 'Auto-Sync'")
        m = {}
        for rec in existing:
            key = rec.get("fields", {}).get("Block & Lot")
            if key:
                m[str(key)] = rec["id"]
        return m
    except Exception:
        return {}


def sync(limit: Optional[int] = None, dry_run: bool = False, since_date: Optional[str] = None):
    comps_table, areas_table = get_airtable_tables()

    print("=" * 70)
    print(f"AIRTABLE SYNC (comps_dev_base_v2 -> {AIRTABLE_TABLE_NAME})")
    print("=" * 70)
    print(f"Base:  {AIRTABLE_BASE_ID}")
    print(f"Table: {AIRTABLE_TABLE_NAME}")
    if since_date:
        print(f"Since:  {since_date} (incremental)")
    else:
        print("Since:  all records (full sync)")

    print("Loading Areas...")
    area_lookup, area_names = build_area_cache(areas_table)
    print(f"Areas loaded: {len(area_lookup)}")

    print("Loading existing Auto-Sync records...")
    existing_map = build_existing_map(comps_table)
    print(f"Existing Auto-Sync keys: {len(existing_map)}")

    print("Fetching from DB...")
    rows = fetch_properties(limit=limit, since_date=since_date)
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
    parser.add_argument("--since", type=str, default=None, help="Only sync records since this date YYYY-MM-DD")
    args = parser.parse_args()
    sync(limit=args.limit, dry_run=args.dry_run, since_date=args.since)