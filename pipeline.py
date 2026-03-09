#!/usr/bin/env python3
"""
CompScope Incremental Pipeline
===============================
Safe weekly runner. NEVER drops or modifies existing data.

What it does:
  1. Pull new sales from NYC Open Data (since last successful run)
  2. Refresh PLUTO + ACRIS staging tables (raw data only -- safe)
  3. Insert only NEW records into comps_dev_base_v2
  4. Sync only NEW records to Airtable (CompScope Beta)
  5. Post Slack summary

Usage:
  python pipeline.py                        # Normal weekly run
  python pipeline.py --skip-extract         # Skip NYC Open Data pull
  python pipeline.py --skip-sync            # No Airtable sync
  python pipeline.py --dry-run              # Dry-run Airtable sync
  python pipeline.py --since 2026-01-01     # Override cutoff date manually
  python pipeline.py --limit 50             # Limit Airtable sync (testing)
  python pipeline.py --notify               # Send Slack notification
"""

import argparse
import os
import sys
import time
import subprocess
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SQL_PATH     = Path(__file__).parent / "src" / "sql" / "incremental_update.sql"
SYNC_SCRIPT  = Path(__file__).parent / "src" / "airtable" / "sync_airtable.py"

DATASETS_FULL    = ["pluto", "acris_master", "acris_parties", "acris_legals"]
DEFAULT_DAYS_BACK = 30


# ── Helpers ────────────────────────────────────────────────────────────────────

def log(msg: str, level: str = "INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    icon = {"INFO": "✅", "WARN": "⚠️ ", "ERROR": "❌", "STEP": "🔄"}.get(level, "  ")
    print(f"[{ts}] {icon} {msg}")


def elapsed(start: float) -> str:
    s = int(time.time() - start)
    return f"{s // 60}m {s % 60}s" if s >= 60 else f"{s}s"


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set in .env")
    return psycopg2.connect(DATABASE_URL)


def get_table_count(table: str) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            return cur.fetchone()[0]


# ── Last Run Date ──────────────────────────────────────────────────────────────

def get_cutoff_date() -> str:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT MAX(finished_at) as last_run
                    FROM etl_ingestion_runs
                    WHERE dataset_key = 'sales_rolling'
                      AND status = 'success'
                """)
                row = cur.fetchone()
                if row and row["last_run"]:
                    cutoff = row["last_run"] - timedelta(days=3)
                    return cutoff.strftime("%Y-%m-%d")
    except Exception as e:
        log(f"Could not read run log: {e}", "WARN")

    cutoff = datetime.now(timezone.utc) - timedelta(days=DEFAULT_DAYS_BACK)
    log(f"No prior run found — defaulting to {DEFAULT_DAYS_BACK} days back", "WARN")
    return cutoff.strftime("%Y-%m-%d")


# ── Step 1: Extract ────────────────────────────────────────────────────────────

def run_extract(cutoff_date: str):
    log("STEP 1: Refreshing staging data from NYC Open Data", "STEP")
    start = time.time()

    for dataset in DATASETS_FULL:
        log(f"  Refreshing {dataset}...")
        ds_start = time.time()
        result = subprocess.run(
            [sys.executable, "scripts/load.py", dataset],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Extract failed for {dataset}:\n{result.stderr[-500:]}")
        log(f"  {dataset} done ({elapsed(ds_start)})")

    # Sales: only pull since cutoff + 3 day buffer
    days_back = (
        datetime.now(timezone.utc) -
        datetime.strptime(cutoff_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    ).days + 3

    log(f"  Refreshing sales_rolling ({days_back} days back)...")
    ds_start = time.time()
    result = subprocess.run(
        [sys.executable, "-c",
         f"from src.nyc_open_data.etl.sales_rolling import load_sales_rolling; load_sales_rolling(days_back={days_back})"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"sales_rolling failed:\n{result.stderr[-500:]}")
    log(f"  sales_rolling done ({elapsed(ds_start)})")
    log(f"Extract complete ({elapsed(start)})")


# ── Step 2: Incremental Transform ─────────────────────────────────────────────

def run_transform(cutoff_date: str) -> int:
    log(f"STEP 2: Incremental SQL transform (since {cutoff_date})", "STEP")
    start = time.time()

    count_before = get_table_count("comps_dev_base_v2")
    log(f"  comps_dev_base_v2 before: {count_before:,}")

    sql = SQL_PATH.read_text().replace(':cutoff_date', f"'{cutoff_date}'")
    raw_parts = re.split(r';[ \t]*(\n|$)', sql)
    statements = []
    for part in raw_parts:
        part = part.strip()
        if not part or part == '\n':
            continue
        # Strip leading comment lines to find actual SQL
        lines = [l for l in part.splitlines() if not l.strip().startswith('--')]
        sql_content = '\n'.join(lines).strip()
        if sql_content:
            statements.append(part)                                            

    with get_conn() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for i, stmt in enumerate(statements):
                first_line = next(
                    (l.strip() for l in stmt.splitlines()
                     if l.strip() and not l.strip().startswith("--")),
                    stmt[:60]
                )
                log(f"  [{i+1}/{len(statements)}] {first_line[:80]}")
                try:
                    cur.execute(stmt)
                except Exception as e:
                    log(f"  SQL error: {e}", "ERROR")
                    raise

    count_after = get_table_count("comps_dev_base_v2")
    new_records  = count_after - count_before
    log(f"  comps_dev_base_v2 after: {count_after:,} (+{new_records:,} new)")
    log(f"Transform complete ({elapsed(start)})")
    return new_records


# ── Step 3: Airtable Sync ─────────────────────────────────────────────────────

def run_sync(cutoff_date: str, dry_run: bool = False, limit: Optional[int] = None):
    log("STEP 3: Syncing to Airtable (CompScope Beta)", "STEP")
    start = time.time()

    cmd = [sys.executable, str(SYNC_SCRIPT), "--since", cutoff_date]
    if dry_run:
        cmd.append("--dry-run")
    if limit:
        cmd.extend(["--limit", str(limit)])

    result = subprocess.run(cmd, capture_output=False, text=True)
    if result.returncode != 0:
        raise RuntimeError("Airtable sync failed")
    log(f"Sync complete ({elapsed(start)})")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CompScope incremental pipeline")
    parser.add_argument("--skip-extract", action="store_true")
    parser.add_argument("--skip-sync",    action="store_true")
    parser.add_argument("--dry-run",      action="store_true")
    parser.add_argument("--since",        type=str, default=None,
                        help="Override cutoff date YYYY-MM-DD")
    parser.add_argument("--limit",        type=int, default=None)
    parser.add_argument("--notify",       action="store_true",
                        help="Send Slack notification")
    args = parser.parse_args()

    pipeline_start = time.time()
    cutoff_date    = args.since or get_cutoff_date()
    current_stage  = "init"
    new_records    = 0

    print()
    print("=" * 60)
    print("  CompScope Incremental Pipeline")
    print(f"  Started:     {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Cutoff:      {cutoff_date}")
    print(f"  Mode:        {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)
    print()

    try:
        current_stage = "extract"
        if args.skip_extract:
            log("Skipping extract (--skip-extract)", "WARN")
        else:
            run_extract(cutoff_date)

        current_stage = "transform"
        new_records = run_transform(cutoff_date)

        current_stage = "sync"
        if args.skip_sync:
            log("Skipping sync (--skip-sync)", "WARN")
        elif new_records == 0 and not args.dry_run:
            log("No new records — skipping Airtable sync")
        else:
            run_sync(cutoff_date, dry_run=args.dry_run, limit=args.limit)

        duration = int(time.time() - pipeline_start)
        total    = get_table_count("comps_dev_base_v2")

        if args.notify:
            try:
                from src.notifications.slack import notify_success
                notify_success(
                    new_records=new_records,
                    updated_records=0,
                    total_records=total,
                    duration_seconds=duration,
                    cutoff_date=cutoff_date,
                    dry_run=args.dry_run,
                )
                log("Slack notification sent")
            except Exception as e:
                log(f"Slack notify failed: {e}", "WARN")

        print()
        print("=" * 60)
        log(f"Done in {elapsed(pipeline_start)}")
        log(f"New records: {new_records:,}  |  Total: {total:,}")
        print("=" * 60)
        print()

    except Exception as e:
        duration = int(time.time() - pipeline_start)
        if args.notify:
            try:
                from src.notifications.slack import notify_failure
                notify_failure(stage=current_stage, error_message=str(e), duration_seconds=duration)
            except Exception:
                pass
        print()
        print("=" * 60)
        log(f"FAILED at [{current_stage}] after {elapsed(pipeline_start)}: {e}", "ERROR")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
