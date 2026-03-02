import os
from pyairtable import Api
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_PAT = os.getenv('AIRTABLE_PAT')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME', 'Property Comps')
DATABASE_URL = os.getenv('DATABASE_URL')

AREA_CACHE = {}

# Map database asset types to Airtable options
ASSET_TYPE_MAP = {
    'Residential Development Site': 'Residential Development Site',
    'Residential Property': 'Residential Development Site',  # Map to closest match
    'Vacant Land': 'Residential Development Site',  # Map to closest match
    'Industrial Development Site': 'Industrial Land',
    'Industrial Building': 'Industrial Building',
    'Industrial + Office Building': 'Industrial + Office Building',
}


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def get_airtable_table():
    api = Api(AIRTABLE_PAT)
    return api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)


def get_areas_table():
    api = Api(AIRTABLE_PAT)
    return api.table(AIRTABLE_BASE_ID, 'Areas')


def format_bbl(borough, block, lot):
    if not all([borough, block, lot]):
        return None
    return f"{borough}-{str(block).zfill(5)}-{str(lot).zfill(4)}"


def map_asset_type(db_asset_type):
    """Map database asset type to Airtable option"""
    if not db_asset_type:
        return None

    mapped = ASSET_TYPE_MAP.get(db_asset_type)
    if mapped:
        return [mapped]  # Return as array for multiple select

    # If not found in map, return None (skip field)
    print(f"Warning: Unknown asset type '{db_asset_type}' - skipping")
    return None


def map_property_to_airtable(row):
    ppbsf_narrow = None
    if row.get('sale_price_clean') and row.get('buildable_sf_narrow'):
        ppbsf_narrow = float(row['sale_price_clean']) / float(row['buildable_sf_narrow'])

    fields = {
        'Address': row.get('address'),
        'Closing Price': float(row['sale_price_clean']) if row.get('sale_price_clean') else None,
        'Closing Date': row.get('sale_date').strftime('%Y-%m-%d') if row.get('sale_date') else None,
        'Zones (Manual)': row.get('zoning'),
        'Lot Sqft': float(row['lotarea']) if row.get('lotarea') else None,
        'Building Sq. Ft.': float(row['bldgarea']) if row.get('bldgarea') else None,
        'Lot Width (Feet)': float(row['lot_frontage']) if row.get('lot_frontage') else None,
        'Lot Depth (Feet)': float(row['lot_depth']) if row.get('lot_depth') else None,
        'Buyer Name': row.get('buyer_names'),
        'Buildable Sq. Ft.': float(row['buildable_sf_narrow']) if row.get('buildable_sf_narrow') else None,
        'PPBSF': ppbsf_narrow,
        'PPLSF': float(row['price_per_land_sf']) if row.get('price_per_land_sf') else None,
        'PPSF': float(row['price_per_bldg_sf']) if row.get('price_per_bldg_sf') else None,
        'Res Units': int(row['unitsres']) if row.get('unitsres') else None,
        'Buildable SF Wide': float(row['buildable_sf_wide']) if row.get('buildable_sf_wide') else None,
        'Res FAR': float(row['official_far_narrow']) if row.get('official_far_narrow') else None,
        'Seller Name': row.get('seller_names'),
        'Block & Lot': format_bbl(row.get('borough'), row.get('block'), row.get('lot')),
        'Building Class RAW': row.get('building_class'),
        'BSF Type': 'Market Rate',
        'Data Source': 'Auto-Sync',
    }

    # Map Asset Type
    asset_type = map_asset_type(row.get('asset_type'))
    if asset_type:
        fields['Asset Type'] = asset_type

    # Handle Area
    if row.get('neighborhood') and row['neighborhood'] in AREA_CACHE:
        fields['Area'] = [AREA_CACHE[row['neighborhood']]]

    return {k: v for k, v in fields.items() if v is not None}


def fetch_properties_from_db(limit=None):
    query = """
            SELECT address, \
                   borough, \
                   block, \
                   lot, \
                   neighborhood, \
                   sale_price_clean, \
                   sale_date, \
                   zoning, \
                   asset_type, \
                   building_class, \
                   lotarea, \
                   bldgarea, \
                   lot_frontage, \
                   lot_depth, \
                   buildable_sf_narrow, \
                   buildable_sf_wide, \
                   official_far_narrow, \
                   price_per_land_sf, \
                   price_per_bldg_sf, \
                   buyer_names, \
                   seller_names, \
                   unitsres
            FROM comps_dev_base
            ORDER BY sale_date DESC \
            """

    if limit:
        query += f" LIMIT {limit}"

    conn = get_db_connection()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
        print(f"Fetched {len(rows)} properties")
        return rows, conn


def sync_to_airtable(test=False, limit=None):
    print("=" * 60)
    print("AIRTABLE SYNC")
    print("=" * 60)

    if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
        print("ERROR: Missing Airtable credentials")
        return False

    table = get_airtable_table()
    print(f"Connected to base: {AIRTABLE_BASE_ID}")

    # Pre-load Area cache
    print("Loading Areas table...")
    try:
        areas_table = get_areas_table()
        all_areas = areas_table.all()
        for area in all_areas:
            area_name = area['fields'].get('Name')
            if area_name:
                AREA_CACHE[area_name] = area['id']
        print(f"Loaded {len(AREA_CACHE)} areas")
    except Exception as e:
        print(f"Warning: Could not load areas: {e}")

    rows, conn = fetch_properties_from_db(limit=limit)

    if not test:
        print("Checking for existing Auto-Sync records...")
        try:
            existing = table.all(formula="{Data Source} = 'Auto-Sync'")
            print(f"Found {len(existing)} existing")

            if existing:
                print("Deleting old records...")
                for i in range(0, len(existing), 10):
                    batch = existing[i:i + 10]
                    table.batch_delete([r['id'] for r in batch])
                print(f"Deleted {len(existing)}")
        except Exception as e:
            print(f"Note: {e}")

    print(f"Uploading {len(rows)} records...")

    uploaded = 0
    errors = 0

    for i in range(0, len(rows), 10):
        batch = rows[i:i + 10]
        airtable_records = []

        for row in batch:
            try:
                fields = map_property_to_airtable(row)
                airtable_records.append(fields)
            except Exception as e:
                print(f"Map error: {row.get('address')}: {e}")
                errors += 1

        if airtable_records:
            try:
                table.batch_create(airtable_records)
                uploaded += len(airtable_records)
                if (i // 10 + 1) % 10 == 0:
                    print(f"Progress: {uploaded}/{len(rows)}")
            except Exception as e:
                print(f"Upload error batch {i // 10 + 1}: {e}")
                errors += len(airtable_records)
                if uploaded == 0:
                    print("First batch failed - stopping")
                    break

    conn.close()

    print("=" * 60)
    print(f"Uploaded: {uploaded}")
    print(f"Errors: {errors}")
    if uploaded > 0:
        print(f"Total in Airtable: ~{uploaded + 40}")
    print("=" * 60)

    return True


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--all', action='store_true')

    args = parser.parse_args()

    if args.test:
        sync_to_airtable(test=True, limit=10)
    elif args.limit:
        sync_to_airtable(test=False, limit=args.limit)
    elif args.all:
        sync_to_airtable(test=False, limit=None)
    else:
        print("Usage: python sync_to_airtable.py --test|--limit N|--all")