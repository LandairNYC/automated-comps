"""
Quick spot check for nearest comps columns.
Run from your project root:
  python check_comps.py
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv("DATABASE_URL"))

with conn.cursor(cursor_factory=RealDictCursor) as cur:

    # ── Test 1: Basic spot check — first 5 records ────────────────────────
    print("\n" + "="*60)
    print("TEST 1: Basic spot check (first 5 records)")
    print("="*60)
    cur.execute("""
        SELECT bbl, address, zoning_base,
               nearest_comps_proximity,
               nearest_comps_smart
        FROM comps_dev_base_v2
        WHERE nearest_comps_proximity IS NOT NULL
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"\n  {row['address']} | {row['zoning_base']} | BBL: {row['bbl']}")
        print(f"  PROXIMITY:")
        for line in row['nearest_comps_proximity'].split('\n'):
            print(f"    {line}")
        print(f"  SMART (same zoning):")
        if row['nearest_comps_smart']:
            for line in row['nearest_comps_smart'].split('\n'):
                print(f"    {line}")
        else:
            print("    (no zoning matches)")

    # ── Test 2: Where proximity and smart actually diverge ────────────────
    print("\n" + "="*60)
    print("TEST 2: Records where smart filter actually changes results")
    print("="*60)
    cur.execute("""
        SELECT bbl, address, zoning_base,
               nearest_comps_proximity,
               nearest_comps_smart
        FROM comps_dev_base_v2
        WHERE nearest_comps_proximity IS NOT NULL
          AND nearest_comps_smart IS NOT NULL
          AND nearest_comps_proximity != nearest_comps_smart
        LIMIT 3
    """)
    for row in cur.fetchall():
        print(f"\n  {row['address']} | {row['zoning_base']}")
        print(f"  PROXIMITY (no filter):")
        for line in row['nearest_comps_proximity'].split('\n'):
            print(f"    {line}")
        print(f"  SMART ({row['zoning_base']} only):")
        for line in row['nearest_comps_smart'].split('\n'):
            print(f"    {line}")

    # ── Test 3: Coverage summary ──────────────────────────────────────────
    print("\n" + "="*60)
    print("TEST 3: Coverage summary")
    print("="*60)
    cur.execute("""
        SELECT
            COUNT(*)                                        AS total,
            COUNT(nearest_comps_proximity)                  AS has_proximity,
            COUNT(nearest_comps_smart)                      AS has_smart,
            COUNT(*) FILTER (
                WHERE nearest_comps_proximity != nearest_comps_smart
            )                                               AS diverge_count
        FROM comps_dev_base_v2
    """)
    row = cur.fetchone()
    print(f"  Total records:            {row['total']}")
    print(f"  Has proximity comps:      {row['has_proximity']}")
    print(f"  Has smart comps:          {row['has_smart']}")
    print(f"  Smart differs from prox:  {row['diverge_count']}")

conn.close()