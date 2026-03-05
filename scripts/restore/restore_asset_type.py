"""
Restore Asset Type values in Airtable Property Comps table.
Matches records by Address between live table and backup CSV.
Only updates Asset Type field — nothing else is touched.

Usage:
    python restore_asset_type.py --dry-run   # preview matches
    python restore_asset_type.py             # run the update
"""

import os
import sys
import time
import argparse
import csv
from difflib import SequenceMatcher
from dotenv import load_dotenv
from pyairtable import Api

load_dotenv()

AIRTABLE_PAT      = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID  = os.getenv("AIRTABLE_BASE_ID")
TABLE_NAME        = "Property Comps"
BACKUP_CSV        = "Property_Comps-PlayZone.csv"  # put in same directory as this script
RATE_SLEEP        = 0.21


def normalize_address(addr: str) -> str:
    """Lowercase, strip whitespace and punctuation for fuzzy matching."""
    if not addr:
        return ""
    return addr.lower().strip().rstrip(",").replace("  ", " ")


def load_backup(path: str) -> dict:
    """
    Load backup CSV. Returns dict of normalized_address -> asset_type.
    Skips rows with blank Asset Type in the backup.
    """
    mapping = {}
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            addr = normalize_address(row.get("Address", ""))
            asset = row.get("Asset Type", "").strip()
            if addr and asset:
                mapping[addr] = asset
    return mapping


def fuzzy_match(addr: str, candidates: list, threshold: float = 0.85) -> str | None:
    best = None
    best_score = 0.0
    for c in candidates:
        score = SequenceMatcher(None, addr, c).ratio()
        if score > best_score:
            best_score = score
            best = c
    return best if best_score >= threshold else None


def restore(dry_run: bool = False):
    print("=" * 60)
    print("ASSET TYPE RESTORE — Property Comps")
    print("=" * 60)

    # Load backup
    print(f"Loading backup CSV: {BACKUP_CSV}")
    backup = load_backup(BACKUP_CSV)
    print(f"Backup entries with Asset Type: {len(backup)}")

    # Connect to Airtable
    api = Api(AIRTABLE_PAT)
    table = api.table(AIRTABLE_BASE_ID, TABLE_NAME)

    # Fetch all live records
    print("Fetching live Property Comps records...")
    all_records = table.all()
    print(f"Live records fetched: {len(all_records)}")

    backup_addresses = list(backup.keys())

    matched        = []
    already_set    = []
    no_match       = []

    for rec in all_records:
        fields   = rec.get("fields", {})
        rec_id   = rec["id"]
        raw_addr = fields.get("Address", "")
        norm     = normalize_address(raw_addr)
        current_asset = fields.get("Asset Type", "")

        # Skip Auto-Sync records — those come from Supabase
        if fields.get("Data Source") == "Auto-Sync":
            continue

        # Skip if already has a value
        if current_asset:
            already_set.append(raw_addr)
            continue

        # Try exact match first
        asset = backup.get(norm)

        # Try fuzzy if no exact match
        if not asset:
            best = fuzzy_match(norm, backup_addresses, threshold=0.85)
            if best:
                asset = backup[best]

        if asset:
            matched.append((rec_id, raw_addr, asset))
        else:
            no_match.append(raw_addr)

    print(f"\nResults:")
    print(f"  Already have Asset Type (skipped): {len(already_set)}")
    print(f"  Matched — will update:             {len(matched)}")
    print(f"  No match found:                    {len(no_match)}")

    if dry_run:
        print("\n[DRY RUN] First 20 matches:")
        for rec_id, addr, asset in matched[:20]:
            print(f"  {addr[:50]:<50} → {asset}")
        if no_match:
            print(f"\n[DRY RUN] First 20 unmatched:")
            for addr in no_match[:20]:
                print(f"  {addr}")
        print("\nRun without --dry-run to apply changes.")
        return

    # Apply updates
    print(f"\nUpdating {len(matched)} records...")
    updated = 0
    errors  = 0

    for i, (rec_id, addr, asset) in enumerate(matched):
        try:
            table.update(rec_id, {"Asset Type": [asset]})
            updated += 1
        except Exception as e:
            errors += 1
            print(f"  Error on {addr}: {e}")
        time.sleep(RATE_SLEEP)

        if (i + 1) % 100 == 0:
            print(f"  ... {i + 1}/{len(matched)} done")

    print("\n" + "=" * 60)
    print(f"✅ Updated:    {updated}")
    print(f"❌ Errors:     {errors}")
    print(f"⏭  No match:  {len(no_match)}")
    if no_match:
        print("\nUnmatched addresses (first 30):")
        for addr in no_match[:30]:
            print(f"  - {addr}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    restore(dry_run=args.dry_run)