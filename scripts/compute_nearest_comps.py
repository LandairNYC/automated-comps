"""
compute_nearest_comps.py
========================
Computes nearest comps for two use cases:

  DEFAULT (no flag):
    For every record in comps_dev_base_v2, compute nearest comps
    where neighbors are also drawn from comps_dev_base_v2.
    Writes: nearest_comps_proximity, nearest_comps_smart

  --leads:
    For every record in leads_geocoded, compute nearest comps
    where neighbors are drawn from comps_dev_base_v2 (the sales data).
    Writes: nearest_comps_proximity to leads_geocoded only.
    (No smart/zoning field for leads — leads don't have zoning data)

Output format (same for both modes):
  123 Atlantic Ave, Brooklyn | R6A | $2.1M | 0.3 mi
  456 Dean St, Brooklyn | R6A | $1.8M | 0.5 mi

Usage:
  python scripts/compute_nearest_comps.py              # comps vs comps
  python scripts/compute_nearest_comps.py --leads      # leads vs comps
  python scripts/compute_nearest_comps.py --dry-run    # no DB write
  python scripts/compute_nearest_comps.py --leads --dry-run
"""

import argparse
import os
import math
from typing import List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
TOP_N        = 6
BATCH_SIZE   = 500


# ── Haversine distance ────────────────────────────────────────────────────────

def haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
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
    borough = BOROUGH_NAMES.get(row.get("borough"), "")
    address = row.get("address") or "Unknown"
    zoning  = row.get("zoning_base") or row.get("zoning") or "?"
    price   = fmt_price(row.get("sale_price_clean"))
    dist    = f"{distance_miles:.2f} mi"
    return f"{address}, {borough} | {zoning} | {price} | {dist}"


# ── Core computation ──────────────────────────────────────────────────────────

def compute_nearest(
    target: dict,
    neighbor_pool: List[dict],
    top_n: int = TOP_N,
    zoning_filter: bool = False,
    exclude_bbl: Optional[str] = None,
) -> Optional[str]:
    """
    Find top N closest neighbors to target from neighbor_pool.

    target:        dict with latitude, longitude
    neighbor_pool: list of comp records to search through
    zoning_filter: only consider neighbors with same zoning_base as target
    exclude_bbl:   skip record with this BBL (used when target is in the pool)
    """
    lat1 = float(target.get("latitude"))  if target.get("latitude")  is not None else None
    lon1 = float(target.get("longitude")) if target.get("longitude") is not None else None

    if lat1 is None or lon1 is None:
        return None

    candidates = []
    for rec in neighbor_pool:
        if exclude_bbl and rec.get("bbl") == exclude_bbl:
            continue

        lat2 = float(rec.get("latitude"))  if rec.get("latitude")  is not None else None
        lon2 = float(rec.get("longitude")) if rec.get("longitude") is not None else None
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
    lines = [fmt_comp(rec, dist) for dist, rec in candidates[:top_n]]
    return "\n".join(lines)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL in .env")
    return psycopg2.connect(DATABASE_URL)


def fetch_comps(conn) -> List[dict]:
    """Load all comps from comps_dev_base_v2 with coordinates."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT bbl, address, borough, sale_price_clean,
                   zoning, zoning_base, latitude, longitude
            FROM comps_dev_base_v2
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY sale_date DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def fetch_leads(conn) -> List[dict]:
    """Load all leads from leads_geocoded with coordinates."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT airtable_record_id, full_address, bbl,
                   latitude, longitude
            FROM leads_geocoded
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY created_at ASC
        """)
        return [dict(r) for r in cur.fetchall()]


def ensure_comp_columns(conn):
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE comps_dev_base_v2
            ADD COLUMN IF NOT EXISTS nearest_comps_proximity TEXT,
            ADD COLUMN IF NOT EXISTS nearest_comps_smart     TEXT
        """)
    conn.commit()
    print("✅  Columns ensured on comps_dev_base_v2")


def write_comp_results(conn, results: List[dict]):
    total = written = 0
    total = len(results)
    for i in range(0, total, BATCH_SIZE):
        batch      = results[i:i + BATCH_SIZE]
        batch_conn = get_connection()
        batch_conn.autocommit = False
        try:
            with batch_conn.cursor() as cur:
                for row in batch:
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
            batch_conn.commit()
            written += len(batch)
            print(f"  Wrote {written}/{total} comp records...")
        except Exception as e:
            batch_conn.rollback()
            raise
        finally:
            batch_conn.close()
    print(f"✅  Wrote nearest comps for {written} comp records")


def write_lead_results(conn, results: List[dict]):
    total = written = 0
    total = len(results)
    for i in range(0, total, BATCH_SIZE):
        batch      = results[i:i + BATCH_SIZE]
        batch_conn = get_connection()
        batch_conn.autocommit = False
        try:
            with batch_conn.cursor() as cur:
                for row in batch:
                    cur.execute("""
                        UPDATE leads_geocoded
                        SET nearest_comps_proximity = %s
                        WHERE airtable_record_id = %s
                    """, (
                        row["nearest_comps_proximity"],
                        row["airtable_record_id"],
                    ))
            batch_conn.commit()
            written += len(batch)
            print(f"  Wrote {written}/{total} lead records...")
        except Exception as e:
            batch_conn.rollback()
            raise
        finally:
            batch_conn.close()
    print(f"✅  Wrote nearest comps for {written} lead records")


# ── Modes ─────────────────────────────────────────────────────────────────────

def run_comps_mode(conn, dry_run: bool):
    print("\n📦 MODE: Comps vs Comps (comps_dev_base_v2)")
    print("-" * 50)

    ensure_comp_columns(conn)

    print("Fetching comps...")
    comps = fetch_comps(conn)
    print(f"Loaded {len(comps)} comps with coordinates")

    results = []
    skipped = 0

    print(f"Computing nearest comps (top {TOP_N} each)...")
    for i, target in enumerate(comps):
        if i % 100 == 0:
            print(f"  [{i}/{len(comps)}] processing...")

        proximity = compute_nearest(target, comps, zoning_filter=False, exclude_bbl=target.get("bbl"))
        smart     = compute_nearest(target, comps, zoning_filter=True,  exclude_bbl=target.get("bbl"))

        if proximity is None:
            skipped += 1
            continue

        results.append({
            "bbl":                     target["bbl"],
            "nearest_comps_proximity": proximity,
            "nearest_comps_smart":     smart,
        })

    print(f"\nComputed: {len(results)} | Skipped: {skipped}")

    if results:
        print("\n--- SAMPLE ---")
        print(f"Target BBL: {results[0]['bbl']}")
        for line in (results[0]["nearest_comps_proximity"] or "").split("\n"):
            print(f"  {line}")

    if dry_run:
        print("\n[DRY RUN] No changes written.")
        return

    write_comp_results(conn, results)
    print("\n✅  Done. Run sync_airtable.py to push to Airtable.")


def run_leads_mode(conn, dry_run: bool):
    print("\n🏠 MODE: Leads vs Comps (leads_geocoded → comps_dev_base_v2)")
    print("-" * 50)

    print("Fetching comps as neighbor pool...")
    comps = fetch_comps(conn)
    print(f"Loaded {len(comps)} comps")

    print("Fetching leads...")
    leads = fetch_leads(conn)
    print(f"Loaded {len(leads)} leads with coordinates")

    results = []
    skipped = 0

    print(f"\nComputing nearest comps for each lead (top {TOP_N} each)...")
    for i, lead in enumerate(leads):
        if i % 100 == 0:
            print(f"  [{i}/{len(leads)}] processing...")

        # No exclude_bbl — leads and comps are separate tables
        proximity = compute_nearest(lead, comps, zoning_filter=False, exclude_bbl=None)

        if proximity is None:
            skipped += 1
            continue

        results.append({
            "airtable_record_id":      lead["airtable_record_id"],
            "nearest_comps_proximity": proximity,
        })

    print(f"\nComputed: {len(results)} leads | Skipped: {skipped}")

    if results:
        print("\n--- SAMPLE ---")
        print(f"Lead: {results[0]['airtable_record_id']}")
        for line in (results[0]["nearest_comps_proximity"] or "").split("\n"):
            print(f"  {line}")

    if dry_run:
        print("\n[DRY RUN] No changes written.")
        return

    write_lead_results(conn, results)
    print("\n✅  Done. Run sync_leads_to_airtable.py to push to Airtable Properties.")


# ── Entry point ───────────────────────────────────────────────────────────────

def run(leads_mode: bool = False, dry_run: bool = False):
    print("=" * 60)
    print("  CompScope — Compute Nearest Comps")
    print(f"  Mode: {'LEADS' if leads_mode else 'COMPS'} | {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 60)

    conn = get_connection()
    if leads_mode:
        run_leads_mode(conn, dry_run)
    else:
        run_comps_mode(conn, dry_run)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--leads",   action="store_true", help="Compute nearest comps for leads")
    parser.add_argument("--dry-run", action="store_true", help="Print sample, no DB writes")
    args = parser.parse_args()
    run(leads_mode=args.leads, dry_run=args.dry_run)