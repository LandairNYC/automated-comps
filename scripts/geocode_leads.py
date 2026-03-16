"""
geocode_leads.py
================
Step 1 & 2 of lead proximity pipeline.

  Step 1: Pull leads from Airtable Properties table → land in Supabase `leads_geocoded`
  Step 2: Geocode each address via NYC Geosearch API → write lat/lng back to `leads_geocoded`

After this script, run:
  python scripts/compute_nearest_comps.py --leads

Usage:
  python scripts/geocode_leads.py                  # Full run
  python scripts/geocode_leads.py --dry-run        # Pull + geocode sample, no DB writes
  python scripts/geocode_leads.py --skip-pull      # Skip Airtable pull, geocode existing rows
  python scripts/geocode_leads.py --limit 50       # Test with first 50 leads

Env vars required (.env):
  DATABASE_URL
  AIRTABLE_PAT
  AIRTABLE_BASE_ID
  AIRTABLE_PROPERTIES_TABLE   (default: "Properties")
"""

import argparse
import os
import re
import time
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

DATABASE_URL              = os.getenv("DATABASE_URL")
AIRTABLE_PAT              = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID          = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_PROPERTIES_TABLE = os.getenv("AIRTABLE_PROPERTIES_TABLE", "Properties")

GEOCODE_API = "https://geosearch.planninglabs.nyc/v2/search"
RATE_SLEEP  = 0.05    # 50ms between requests (~20 req/sec)
BATCH_SIZE  = 500


# ── Logging ────────────────────────────────────────────────────────────────────

def log(msg: str, level: str = "INFO"):
    icon = {"INFO": "✅", "WARN": "⚠️ ", "ERROR": "❌", "STEP": "🔄"}.get(level, "  ")
    print(f"{icon} {msg}")


# ── Address cleaning ───────────────────────────────────────────────────────────

def clean_address(raw: str) -> str:
    """
    Clean messy agent-entered addresses for geocoding.
    - Strip newlines (some have neighborhood on line 2)
    - Remove double commas
    - Strip trailing punctuation/whitespace
    - Append ', New York' if no borough/state context found
      so the NYC geocoder has enough to work with
    """
    if not raw:
        return ""

    # Replace newlines with comma+space
    cleaned = raw.replace("\n", ", ")

    # Remove double commas
    cleaned = re.sub(r",\s*,", ",", cleaned)

    # Strip trailing commas, spaces, periods
    cleaned = cleaned.strip(" ,.")

    # If no borough/state context, append New York
    has_context = any(b.lower() in cleaned.lower() for b in [
        "manhattan", "brooklyn", "bronx", "queens", "staten island",
        "ny", "new york"
    ])
    if not has_context:
        cleaned = cleaned + ", New York"

    return cleaned.strip()


# ── BBL normalization ──────────────────────────────────────────────────────────

def normalize_bbl(raw: Optional[str]) -> Optional[str]:
    """
    Normalize messy BBL to standard format.

    Properties table uses BLOCK/LOT only (no boro prefix), e.g.:
      '2814/76'   → '2814-76'
      '10197/35'  → '10197-35'
      '3-05458-0037' → kept as-is (3-part = already has boro)

    Boro prefix can be inferred from address later when needed.
    """
    if not raw:
        return None

    raw = str(raw).strip()
    if not raw or raw.lower() == "none":
        return None

    # Replace slashes and spaces with dash
    normalized = re.sub(r"[\s/]+", "-", raw).strip("-")

    parts = normalized.split("-")

    # 2 parts = BLOCK-LOT (no boro) — common in Properties table
    if len(parts) == 2:
        return normalized

    # 3 parts = BORO-BLOCK-LOT — already standard
    if len(parts) == 3:
        return normalized

    # Try raw 10-digit BBL string
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        boro  = digits[0]
        block = digits[1:6].lstrip("0") or "0"
        lot   = digits[6:].lstrip("0") or "0"
        return f"{boro}-{block.zfill(5)}-{lot.zfill(4)}"

    return normalized if normalized else None


# ── Geocoding ──────────────────────────────────────────────────────────────────

def geocode_address(address: str) -> dict:
    """
    Geocode a single address via NYC Planning Geosearch API.
    Returns dict: latitude, longitude, confidence, normalized_address
    """
    result = {
        "latitude":           None,
        "longitude":          None,
        "geocode_confidence": None,
        "normalized_address": None,
    }

    cleaned = clean_address(address)
    if not cleaned:
        return result

    try:
        resp = requests.get(
            GEOCODE_API,
            params={"text": cleaned, "size": 1},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            return result

        feature = features[0]
        props   = feature.get("properties", {})
        coords  = feature.get("geometry", {}).get("coordinates", [])

        if len(coords) >= 2:
            result["longitude"]          = coords[0]
            result["latitude"]           = coords[1]
            result["geocode_confidence"] = props.get("confidence")
            result["normalized_address"] = props.get("label")

    except Exception as e:
        log(f"Geocode error for '{address[:60]}': {e}", "WARN")

    return result


# ── Airtable pull ──────────────────────────────────────────────────────────────

def pull_leads_from_airtable(limit: Optional[int] = None) -> list:
    """
    Pull leads from Airtable Properties table.
    Only fetches: Full Address, Block/Lot (+ record ID auto-included).
    """
    log(f"Connecting to Airtable → '{AIRTABLE_PROPERTIES_TABLE}'...", "STEP")

    api   = Api(AIRTABLE_PAT)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_PROPERTIES_TABLE)

    log("Fetching records (may take ~60s for 4,000 rows)...", "STEP")
    all_records = table.all(fields=["Full Address", "Block/Lot"])

    if limit:
        all_records = all_records[:limit]
        log(f"Limited to first {limit} records")

    leads   = []
    no_addr = 0

    for rec in all_records:
        fields       = rec.get("fields", {})
        full_address = str(fields.get("Full Address", "")).strip()
        bbl_raw      = str(fields.get("Block/Lot", "")).strip()

        if not full_address or full_address.lower() in ("none", ""):
            no_addr += 1
            continue

        leads.append({
            "airtable_record_id": rec["id"],
            "full_address":       full_address,
            "bbl_raw":            bbl_raw,
            "bbl":                normalize_bbl(bbl_raw),
        })

    log(f"Pulled {len(leads)} leads | Skipped {no_addr} with no address")
    return leads


# ── Supabase ───────────────────────────────────────────────────────────────────

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS leads_geocoded (
                airtable_record_id      TEXT PRIMARY KEY,
                full_address            TEXT,
                bbl_raw                 TEXT,
                bbl                     TEXT,
                latitude                DOUBLE PRECISION,
                longitude               DOUBLE PRECISION,
                geocode_confidence      DOUBLE PRECISION,
                normalized_address      TEXT,
                nearest_comps_proximity TEXT,
                geocoded_at             TIMESTAMP,
                created_at              TIMESTAMP DEFAULT NOW()
            )
        """)
    conn.commit()
    log("Table leads_geocoded ready")


def upsert_leads(conn, leads: list):
    log(f"Upserting {len(leads)} leads into Supabase...", "STEP")

    for i in range(0, len(leads), BATCH_SIZE):
        batch = leads[i:i + BATCH_SIZE]
        with conn.cursor() as cur:
            for lead in batch:
                cur.execute("""
                    INSERT INTO leads_geocoded
                        (airtable_record_id, full_address, bbl_raw, bbl)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (airtable_record_id) DO UPDATE SET
                        full_address = EXCLUDED.full_address,
                        bbl_raw      = EXCLUDED.bbl_raw,
                        bbl          = EXCLUDED.bbl
                """, (
                    lead["airtable_record_id"],
                    lead["full_address"],
                    lead["bbl_raw"],
                    lead["bbl"],
                ))
        conn.commit()
        log(f"  Upserted {min(i + BATCH_SIZE, len(leads))}/{len(leads)}...")

    log("Leads landed in Supabase ✅")


def geocode_all(conn):
    """Geocode all rows in leads_geocoded missing coordinates."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT airtable_record_id, full_address
            FROM leads_geocoded
            WHERE latitude IS NULL
            ORDER BY created_at ASC
        """)
        rows = cur.fetchall()

    total   = len(rows)
    success = 0
    failed  = 0

    log(f"Geocoding {total} leads without coordinates...", "STEP")

    for i, row in enumerate(rows):
        if i % 100 == 0 and i > 0:
            pct = round(i / total * 100, 1)
            log(f"  [{i}/{total} — {pct}%] success={success} failed={failed}")

        geo = geocode_address(row["full_address"])
        time.sleep(RATE_SLEEP)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE leads_geocoded
                SET latitude           = %s,
                    longitude          = %s,
                    geocode_confidence = %s,
                    normalized_address = %s,
                    geocoded_at        = NOW()
                WHERE airtable_record_id = %s
            """, (
                geo["latitude"],
                geo["longitude"],
                geo["geocode_confidence"],
                geo["normalized_address"],
                row["airtable_record_id"],
            ))
        conn.commit()

        if geo["latitude"] is not None:
            success += 1
        else:
            failed += 1

    log(f"Geocoding complete → {success} success | {failed} failed | {total} total")
    if failed > 0:
        log(f"To review failures: SELECT full_address FROM leads_geocoded WHERE latitude IS NULL", "WARN")


def print_summary(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM leads_geocoded")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM leads_geocoded WHERE latitude IS NOT NULL")
        geocoded = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM leads_geocoded WHERE latitude IS NULL")
        missing = cur.fetchone()[0]

    pct = round(geocoded / total * 100, 1) if total else 0
    print("\n" + "=" * 50)
    print("  leads_geocoded — Final Summary")
    print("=" * 50)
    print(f"  Total:          {total}")
    print(f"  Geocoded ✅:    {geocoded}  ({pct}%)")
    print(f"  Failed ⚠️ :     {missing}")
    print("=" * 50)
    print("\nNext step:")
    print("  python scripts/compute_nearest_comps.py --leads")


# ── Main ───────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False, skip_pull: bool = False, limit: Optional[int] = None):
    print("=" * 60)
    print("  CompScope — Geocode Leads")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 60)

    conn = psycopg2.connect(DATABASE_URL)
    ensure_table(conn)

    # Step 1 — Pull from Airtable → Supabase
    if not skip_pull:
        leads = pull_leads_from_airtable(limit=limit)
        if dry_run:
            log(f"[DRY RUN] Would upsert {len(leads)} leads — showing first 5:")
            for lead in leads[:5]:
                print(f"  {lead['airtable_record_id']} | {lead['full_address'][:60]} | BBL: {lead['bbl']}")
        else:
            upsert_leads(conn, leads)
    else:
        log("Skipping Airtable pull (--skip-pull)")

    # Step 2 — Geocode
    if dry_run:
        log("[DRY RUN] Sample geocode results using real addresses from Properties:")
        samples = [
            "288 East Burnside Ave, Bronx, NY",
            "2412 DORSEY STREET",
            "110-20 Merrick Blvd, Jamaica, NY 11433",
            "207 MOTHER GASTON BLVD, Brooklyn NY",
            "2120 Glebe Ave",
            "502 OCEAN VIEW AVENUE, Brighton Beach",
        ]
        for addr in samples:
            geo = geocode_address(addr)
            if geo["latitude"]:
                print(f"  ✅ '{addr[:50]}' → {geo['latitude']:.5f}, {geo['longitude']:.5f} (conf={geo['geocode_confidence']})")
            else:
                print(f"  ❌ '{addr[:50]}' → FAILED")
    else:
        geocode_all(conn)
        print_summary(conn)

    conn.close()
    log("Done ✅")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",   action="store_true", help="No DB writes — show sample output only")
    parser.add_argument("--skip-pull", action="store_true", help="Skip Airtable pull, geocode existing rows")
    parser.add_argument("--limit",     type=int,            help="Limit leads pulled (for testing)")
    args = parser.parse_args()
    run(dry_run=args.dry_run, skip_pull=args.skip_pull, limit=args.limit)