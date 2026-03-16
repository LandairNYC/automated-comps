"""
sync_leads_to_airtable.py
=========================
Reads nearest_comps_proximity from leads_geocoded (Supabase)
and PATCHes it onto each matching record in the Airtable Properties table.

Only touches the 'Nearest Comps' field — all 397 other fields are untouched.
Uses airtable_record_id (saved during geocode_leads.py) to match records exactly.

Usage:
  python scripts/sync_leads_to_airtable.py            # Full sync
  python scripts/sync_leads_to_airtable.py --dry-run  # Print what would be patched
  python scripts/sync_leads_to_airtable.py --limit 20 # Test on first 20 records

Env vars required (.env):
  DATABASE_URL
  AIRTABLE_PAT
  AIRTABLE_BASE_ID
  AIRTABLE_PROPERTIES_TABLE   (default: "Properties")
  AIRTABLE_NEAREST_COMPS_FIELD (default: "Nearest Comps")
"""

import argparse
import os
import time
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

DATABASE_URL               = os.getenv("DATABASE_URL")
AIRTABLE_PAT               = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID           = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_PROPERTIES_TABLE  = os.getenv("AIRTABLE_PROPERTIES_TABLE",    "Properties")
AIRTABLE_NEAREST_COMPS_FIELD = os.getenv("AIRTABLE_NEAREST_COMPS_FIELD", "Nearest Comps")

# Airtable allows 10 requests/second on most plans
# We batch 10 records per request using batch_update
RATE_SLEEP  = 0.15   # 150ms between batches (~6-7 req/sec, safe)
BATCH_SIZE  = 10     # Airtable batch update max is 10 records


def log(msg: str, level: str = "INFO"):
    icon = {"INFO": "✅", "WARN": "⚠️ ", "ERROR": "❌", "STEP": "🔄"}.get(level, "  ")
    print(f"{icon} {msg}")


def fetch_leads_with_comps(conn, limit: Optional[int] = None) -> list:
    """
    Fetch all leads that have nearest_comps_proximity computed.
    Only returns rows where we actually have something to write.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            SELECT airtable_record_id, full_address, nearest_comps_proximity
            FROM leads_geocoded
            WHERE nearest_comps_proximity IS NOT NULL
            ORDER BY created_at ASC
        """
        if limit:
            query += f" LIMIT {limit}"
        cur.execute(query)
        return [dict(r) for r in cur.fetchall()]


def sync_to_airtable(leads: list, dry_run: bool = False):
    """
    PATCH nearest_comps_proximity onto each Airtable Properties record.
    Uses batch_update (10 records per API call) for efficiency.
    """
    log(f"Connecting to Airtable → '{AIRTABLE_PROPERTIES_TABLE}'...", "STEP")
    api   = Api(AIRTABLE_PAT)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_PROPERTIES_TABLE)

    total   = len(leads)
    updated = 0
    failed  = 0

    log(f"Syncing {total} leads to Airtable field '{AIRTABLE_NEAREST_COMPS_FIELD}'...", "STEP")

    for i in range(0, total, BATCH_SIZE):
        batch = leads[i:i + BATCH_SIZE]

        # Build list of {id, fields} dicts for batch_update
        updates = [
            {
                "id":     lead["airtable_record_id"],
                "fields": {
                    AIRTABLE_NEAREST_COMPS_FIELD: lead["nearest_comps_proximity"]
                }
            }
            for lead in batch
        ]

        if dry_run:
            print(f"\n  [DRY RUN] Batch {i // BATCH_SIZE + 1} — would update {len(batch)} records:")
            for u in updates[:2]:
                comps_preview = (u["fields"][AIRTABLE_NEAREST_COMPS_FIELD] or "")[:80]
                print(f"    {u['id']} → {comps_preview}...")
            updated += len(batch)
            continue

        try:
            table.batch_update(updates)
            updated += len(batch)

            if updated % 100 == 0 or updated == total:
                pct = round(updated / total * 100, 1)
                log(f"  [{updated}/{total} — {pct}%] synced...")

        except Exception as e:
            log(f"Batch failed at record {i}: {e}", "ERROR")
            failed += len(batch)

        time.sleep(RATE_SLEEP)

    return updated, failed


def run(dry_run: bool = False, limit: Optional[int] = None):
    print("=" * 60)
    print("  CompScope — Sync Lead Comps to Airtable")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  Target field: '{AIRTABLE_NEAREST_COMPS_FIELD}'")
    print("=" * 60)

    conn = psycopg2.connect(DATABASE_URL)

    # Fetch leads that have comps computed
    log("Fetching leads with computed nearest comps from Supabase...", "STEP")
    leads = fetch_leads_with_comps(conn, limit=limit)
    conn.close()

    if not leads:
        log("No leads found with nearest_comps_proximity. Run compute_nearest_comps.py --leads first.", "WARN")
        return

    log(f"Found {len(leads)} leads ready to sync")

    # Show a sample
    print(f"\n--- SAMPLE (first lead) ---")
    sample = leads[0]
    print(f"  Record ID: {sample['airtable_record_id']}")
    print(f"  Address:   {sample['full_address']}")
    print(f"  Comps:")
    for line in (sample["nearest_comps_proximity"] or "").split("\n"):
        print(f"    {line}")
    print()

    # Sync
    updated, failed = sync_to_airtable(leads, dry_run=dry_run)

    print()
    print("=" * 50)
    print("  Sync Summary")
    print("=" * 50)
    print(f"  Updated ✅:  {updated}")
    print(f"  Failed  ❌:  {failed}")
    print(f"  Total:       {len(leads)}")
    print("=" * 50)

    if dry_run:
        print("\n[DRY RUN] No changes written to Airtable.")
    else:
        print(f"\n✅ Done. '{AIRTABLE_NEAREST_COMPS_FIELD}' field updated on {updated} Properties records.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print what would be synced, no Airtable writes")
    parser.add_argument("--limit",   type=int,            help="Limit number of records synced (for testing)")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)