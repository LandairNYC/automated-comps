#!/usr/bin/env python3
import argparse
import sys
import os

from src.nyc_open_data.etl.acris_legals import load_acris_legals
from src.nyc_open_data.etl.acris_parties import load_acris_parties
from src.nyc_open_data.etl.pluto import load_pluto
from src.nyc_open_data.etl.acris_master import load_acris_master
from src.nyc_open_data.etl.sales_rolling import load_sales_rolling

# Keep a registry so it's easy to add more loaders later
LOADERS = {
    "pluto": load_pluto,
    "acris_master": load_acris_master,
    "acris_parties": load_acris_parties,
    "acris_legals": load_acris_legals,
    "sales_rolling": load_sales_rolling,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Load NYC Open Data datasets into Postgres.")
    parser.add_argument("dataset", choices=LOADERS.keys(), help="Which dataset to load")
    parser.add_argument("--max-rows", type=int, default=None, help="Maximum rows to fetch (for testing)")
    args = parser.parse_args()

    loader_fn = LOADERS[args.dataset]

    # All your loaders accept (engine=None, max_rows=None)
    loader_fn(engine=None, max_rows=args.max_rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
