"""
sync_leads_to_airtable.py
=========================
Reads nearest comp data from leads_geocoded (Supabase) and writes
to each matching record in the Airtable Properties table:

  1. Nearest Comps  (text field)    — plain text, always written, fallback
  2. Closest Comp 1–6 (linked records) — actual Airtable record links
     pointing into CompScope Beta, written when BBL resolves

Only touches these 7 fields — all other fields on Properties are untouched.
Uses airtable_record_id saved during geocode_leads.py to match exactly.

CHANGES FROM PREVIOUS VERSION:
  - Loads BBL → record ID map from CompScope Beta on startup
  - Reads nearest_comps_bbls from leads_geocoded (written by compute_nearest_comps.py)
  - Writes Closest Comp 1–6 as linked records in addition to text field
  - Added --text-only flag to skip linked records (useful if fields not yet created)
  - Added AIRTABLE_COMPSCOPE_TABLE env var

Usage:
  python scripts/sync_leads_to_airtable.py              # Full sync (text + linked)
  python scripts/sync_leads_to_airtable.py --text-only  # Text field only, skip linked
  python scripts/sync_leads_to_airtable.py --dry-run    # Print what would be patched
  python scripts/sync_leads_to_airtable.py --limit 20   # Test on first 20 records

Env vars required (.env):
  DATABASE_URL
  AIRTABLE_PAT
  AIRTABLE_BASE_ID
  AIRTABLE_PROPERTIES_TABLE        (default: "Properties")
  AIRTABLE_NEAREST_COMPS_FIELD     (default: "Nearest Comps")
  AIRTABLE_COMPSCOPE_TABLE         (default: "CompScope Beta")
"""

import argparse
import os
import time
from typing import Optional, Dict

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

DATABASE_URL                 = os.getenv("DATABASE_URL")
AIRTABLE_PAT                 = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID             = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_PROPERTIES_TABLE    = os.getenv("AIRTABLE_PROPERTIES_TABLE",    "Properties")
AIRTABLE_NEAREST_COMPS_FIELD = os.getenv("AIRTABLE_NEAREST_COMPS_FIELD", "Nearest Comps")
AIRTABLE_COMPSCOPE_TABLE     = os.getenv("AIRTABLE_COMPSCOPE_TABLE",     "CompScope Beta")

# Linked record field names in Airtable Properties table
# These must be created manually in Airtable before running
LINKED_COMP_FIELDS = [
    "Closest Comp 1",
    "Closest Comp 2",
    "Closest Comp 3",
    "Closest Comp 4",
    "Closest Comp 5",
    "Closest Comp 6",
]

RATE_SLEEP = 0.15   # 150ms between batches (~6-7 req/sec)
BATCH_SIZE = 10     # Airtable batch update max


def log(msg: str, level: str = "INFO"):
    icon = {"INFO": "✅", "WARN": "⚠️ ", "ERROR": "❌", "STEP": "🔄"}.get(level, "  ")
    print(f"{icon} {msg}")


# ── Supabase ──────────────────────────────────────────────────────────────────

def fetch_leads_with_comps(conn, limit: Optional[int] = None) -> list:
    """Fetch leads that have nearest comps computed — both text and BBLs."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            SELECT airtable_record_id, full_address,
                   nearest_comps_proximity,
                   nearest_comps_bbls
            FROM leads_geocoded
            WHERE nearest_comps_proximity IS NOT NULL
            ORDER BY created_at ASC
        """
        if limit:
            query += f" LIMIT {limit}"
        cur.execute(query)
        return [dict(r) for r in cur.fetchall()]


# ── BBL → Airtable record ID map ──────────────────────────────────────────────

def build_bbl_to_record_id_map(api: Api) -> Dict[str, str]:
    """
    Load all records from CompScope Beta and build a BBL → Airtable record ID map.
    Used to convert BBL strings into actual linked record IDs.
    Only fetches Block & Lot to keep it fast.
    """
    log(f"Loading BBL → record ID map from '{AIRTABLE_COMPSCOPE_TABLE}'...", "STEP")
    table   = api.table(AIRTABLE_BASE_ID, AIRTABLE_COMPSCOPE_TABLE)
    bbl_map: Dict[str, str] = {}

    for rec in table.all(fields=["Block & Lot"]):
        bbl = rec.get("fields", {}).get("Block & Lot")
        if bbl:
            bbl_map[str(bbl).strip()] = rec["id"]

    log(f"Loaded {len(bbl_map)} BBL → record ID mappings")
    return bbl_map


def resolve_linked_records(
    bbl_string: Optional[str],
    bbl_map: Dict[str, str],
) -> Dict[str, list]:
    """
    Given a pipe-delimited BBL string:
      "3-02156-0005|3-01324-0014|3-03565-0030|2-02869-0027|4-00865-0033|1-00177-0028"

    Return a dict of Airtable field updates for linked records:
      {
        "Closest Comp 1": ["recXXXXXXXXXXXXXX"],
        "Closest Comp 2": ["recYYYYYYYYYYYYYY"],
        ...
      }

    Skips any BBL not found in CompScope Beta — this happens when the nearest
    comp is a Residential Property excluded from the CompScope sync filter.
    """
    if not bbl_string:
        return {}

    bbls   = [b.strip() for b in bbl_string.split("|") if b.strip()]
    linked = {}
    slot   = 0  # Track which Closest Comp field we're filling

    for bbl in bbls:
        if slot >= len(LINKED_COMP_FIELDS):
            break
        rec_id = bbl_map.get(bbl)
        if rec_id:
            linked[LINKED_COMP_FIELDS[slot]] = [rec_id]
            slot += 1

    return linked


# ── Airtable sync ─────────────────────────────────────────────────────────────

def sync_to_airtable(
    leads: list,
    bbl_map: Dict[str, str],
    dry_run: bool = False,
    text_only: bool = False,
):
    log(f"Connecting to Airtable → '{AIRTABLE_PROPERTIES_TABLE}'...", "STEP")
    api   = Api(AIRTABLE_PAT)
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_PROPERTIES_TABLE)

    total          = len(leads)
    updated        = 0
    failed         = 0
    linked_written = 0
    linked_skipped = 0

    log(f"Syncing {total} leads...", "STEP")

    for i in range(0, total, BATCH_SIZE):
        batch = leads[i:i + BATCH_SIZE]

        updates = []
        for lead in batch:
            # Always write the plain text field
            fields = {
                AIRTABLE_NEAREST_COMPS_FIELD: lead["nearest_comps_proximity"]
            }

            # Write linked records unless --text-only
            if not text_only:
                linked = resolve_linked_records(
                    lead.get("nearest_comps_bbls"), bbl_map
                )
                fields.update(linked)
                if linked:
                    linked_written += 1
                else:
                    linked_skipped += 1

            updates.append({
                "id":     lead["airtable_record_id"],
                "fields": fields,
            })

        if dry_run:
            print(f"\n  [DRY RUN] Batch {i // BATCH_SIZE + 1} — would update {len(batch)} records:")
            for u in updates[:2]:
                preview = (u["fields"].get(AIRTABLE_NEAREST_COMPS_FIELD) or "")[:80]
                linked_count = sum(1 for k in u["fields"] if k.startswith("Closest Comp"))
                print(f"    {u['id']} → text✅ | linked_records={linked_count}")
                print(f"    {preview}...")
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

    return updated, failed, linked_written, linked_skipped


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False, limit: Optional[int] = None, text_only: bool = False):
    print("=" * 60)
    print("  CompScope — Sync Lead Comps to Airtable")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  Linked records: {'DISABLED (--text-only)' if text_only else 'ENABLED'}")
    print(f"  Text field: '{AIRTABLE_NEAREST_COMPS_FIELD}'")
    print("=" * 60)

    conn = psycopg2.connect(DATABASE_URL)
    log("Fetching leads with computed nearest comps from Supabase...", "STEP")
    leads = fetch_leads_with_comps(conn, limit=limit)
    conn.close()

    if not leads:
        log("No leads found with nearest_comps_proximity. Run compute_nearest_comps.py --leads first.", "WARN")
        return

    log(f"Found {len(leads)} leads ready to sync")

    # Show sample
    print(f"\n--- SAMPLE (first lead) ---")
    s = leads[0]
    print(f"  Record ID: {s['airtable_record_id']}")
    print(f"  Address:   {s['full_address']}")
    print(f"  BBLs:      {s.get('nearest_comps_bbls', 'none')}")
    print(f"  Comps:")
    for line in (s["nearest_comps_proximity"] or "").split("\n"):
        print(f"    {line}")
    print()

    # Build BBL map (skip if text only)
    api     = Api(AIRTABLE_PAT)
    bbl_map = {} if text_only else build_bbl_to_record_id_map(api)

    # Sync
    updated, failed, linked_written, linked_skipped = sync_to_airtable(
        leads, bbl_map, dry_run=dry_run, text_only=text_only
    )

    print()
    print("=" * 50)
    print("  Sync Summary")
    print("=" * 50)
    print(f"  Updated ✅:         {updated}")
    print(f"  Failed  ❌:         {failed}")
    print(f"  Total:              {len(leads)}")
    if not text_only:
        print(f"  Linked records ✅:  {linked_written}")
        print(f"  Linked skipped ⚠️:  {linked_skipped}  (BBL not in CompScope Beta)")
    print("=" * 50)

    if dry_run:
        print("\n[DRY RUN] No changes written to Airtable.")
    else:
        print(f"\n✅ Done. {updated} Properties records updated.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",   action="store_true", help="No Airtable writes — show sample output")
    parser.add_argument("--text-only", action="store_true", help="Only write text field, skip linked records")
    parser.add_argument("--limit",     type=int,            help="Limit records synced (for testing)")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, text_only=args.text_only)