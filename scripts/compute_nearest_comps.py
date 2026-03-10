"""
compute_nearest_comps.py
========================
For every record in comps_dev_base_v2, compute two nearest-comps fields:

  nearest_comps_proximity  — top 6 closest by pure distance (no filters)
  nearest_comps_smart      — top 6 closest with same zoning_base

Both stored as pipe-delimited text strings, e.g.:
  123 Atlantic Ave, Brooklyn | R6A | $2.1M | 0.3 mi
  456 Dean St, Brooklyn | R6A | $1.8M | 0.5 mi

Usage:
  python scripts/compute_nearest_comps.py
  python scripts/compute_nearest_comps.py --dry-run   # print sample, no DB write
"""

import argparse
import os
import sys
import math
from typing import List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
TOP_N = 6


# ── Haversine distance ────────────────────────────────────────────────────────

def haversine_miles(lat1, lon1, lat2, lon2) -> float:
    """Straight-line distance between two lat/lng points in miles."""
    R = 3958.8  # Earth radius in miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── Formatting ────────────────────────────────────────────────────────────────

BOROUGH_NAMES = {1: "Manhattan", 2: "Bronx", 3: "Brooklyn", 4: "Queens", 5: "Staten Island"}

def fmt_price(price) -> str:
    if price is None:
        return "N/A"
    p = float(price)
    if p >= 1_000_000:
        return f"${p/1_000_000:.1f}M"
    return f"${p/1_000:.0f}K"

def fmt_comp(row, distance_miles: float) -> str:
    borough = BOROUGH_NAMES.get(row["borough"], "")
    address = row.get("address") or "Unknown"
    zoning  = row.get("zoning_base") or row.get("zoning") or "?"
    price   = fmt_price(row.get("sale_price_clean"))
    dist    = f"{distance_miles:.2f} mi"
    return f"{address}, {borough} | {zoning} | {price} | {dist}"


# ── Core computation ──────────────────────────────────────────────────────────

def compute_nearest(
    target: dict,
    all_records: List[dict],
    top_n: int = TOP_N,
    zoning_filter: bool = False,
) -> Optional[str]:
    """
    Return a newline-delimited string of the top N nearest comps.
    Excludes the target record itself.
    If zoning_filter=True, only considers records with same zoning_base.
    Returns None if fewer than 1 neighbor found.
    """
    lat1 = target.get("latitude")
    lon1 = target.get("longitude")
    bbl  = target.get("bbl")

    if lat1 is None or lon1 is None:
        return None

    candidates = []
    for rec in all_records:
        if rec.get("bbl") == bbl:
            continue  # skip self

        lat2 = rec.get("latitude")
        lon2 = rec.get("longitude")
        if lat2 is None or lon2 is None:
            continue

        if zoning_filter:
            if rec.get("zoning_base") != target.get("zoning_base"):
                continue

        dist = haversine_miles(lat1, lon1, lat2, lon2)
        candidates.append((dist, rec))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    top = candidates[:top_n]

    lines = [fmt_comp(rec, dist) for dist, rec in top]
    return "\n".join(lines)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL in .env")
    return psycopg2.connect(DATABASE_URL)


def ensure_columns(conn):
    """Add nearest_comps columns if they don't exist yet."""
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE comps_dev_base_v2
            ADD COLUMN IF NOT EXISTS nearest_comps_proximity TEXT,
            ADD COLUMN IF NOT EXISTS nearest_comps_smart TEXT
        """)
    conn.commit()
    print("✅  Columns ensured: nearest_comps_proximity, nearest_comps_smart")


def fetch_all_records(conn) -> List[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT bbl, address, borough, sale_price_clean,
                   zoning, zoning_base, latitude, longitude
            FROM comps_dev_base_v2
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY sale_date DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def write_results(conn, results: List[dict]):
    """Bulk update nearest_comps fields."""
    with conn.cursor() as cur:
        for row in results:
            cur.execute("""
                UPDATE comps_dev_base_v2
                SET nearest_comps_proximity = %s,
                    nearest_comps_smart     = %s
                WHERE bbl = %s
            """, (
                row["nearest_comps_proximity"],
                row["nearest_comps_smart"],
                row["bbl"],
            ))
    conn.commit()
    print(f"✅  Wrote nearest comps for {len(results)} records")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False):
    print("=" * 60)
    print("  CompScope — Compute Nearest Comps")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 60)

    conn = get_connection()

    if not dry_run:
        ensure_columns(conn)

    print("Fetching all records from comps_dev_base_v2...")
    records = fetch_all_records(conn)
    print(f"Loaded {len(records)} records with coordinates")

    results = []
    skipped = 0

    print(f"Computing nearest comps (top {TOP_N} each)...")
    for i, target in enumerate(records):
        if i % 100 == 0:
            print(f"  [{i}/{len(records)}] processing...")

        proximity = compute_nearest(target, records, top_n=TOP_N, zoning_filter=False)
        smart     = compute_nearest(target, records, top_n=TOP_N, zoning_filter=True)

        if proximity is None:
            skipped += 1
            continue

        results.append({
            "bbl":                      target["bbl"],
            "nearest_comps_proximity":  proximity,
            "nearest_comps_smart":      smart,  # may be None if no zoning matches
        })

    print(f"\nComputed: {len(results)} records | Skipped (no coords): {skipped}")

    # ── Sample output ─────────────────────────────────────────────────────────
    print("\n--- SAMPLE: PROXIMITY (first record) ---")
    if results:
        print(f"Target BBL: {results[0]['bbl']}")
        print("Proximity comps:")
        for line in (results[0]["nearest_comps_proximity"] or "").split("\n"):
            print(f"  {line}")
        print("\nSmart comps (same zoning):")
        smart_sample = results[0]["nearest_comps_smart"]
        if smart_sample:
            for line in smart_sample.split("\n"):
                print(f"  {line}")
        else:
            print("  (no zoning matches found)")

    if dry_run:
        print("\n[DRY RUN] No changes written to DB.")
        return

    print("\nWriting to comps_dev_base_v2...")
    write_results(conn, results)

    conn.close()
    print("\n✅  Done. Run sync_airtable.py to push to Airtable.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print sample output, don't write to DB")
    args = parser.parse_args()
    run(dry_run=args.dry_run)