import pandas as pd
from pathlib import Path
from src.nyc_open_data.etl.config import get_engine


def load_historical_sales(year: int, data_dir: str):
    """Load historical sales Excel files into stg_sales_raw"""

    engine = get_engine()
    data_path = Path(data_dir)

    boroughs = ['bronx', 'brooklyn', 'manhattan', 'queens', 'staten_island']

    total_rows = 0

    for boro in boroughs:
        file_path = data_path / f"{year}_{boro}.xlsx"

        if not file_path.exists():
            print(f"Skipping {file_path} (not found)")
            continue

        print(f"[{year}] Loading {boro}...")

        # Read Excel, skip header rows
        df = pd.read_excel(file_path, skiprows=6)

        # Drop empty rows
        df = df.dropna(how='all')

        # Normalize column names: lowercase, replace spaces/newlines with underscores
        df.columns = (df.columns
                      .str.lower()
                      .str.replace(' ', '_')
                      .str.replace('\n', '_')
                      .str.replace('__', '_')
                      .str.strip('_'))

        # Only keep columns that exist in stg_sales_raw
        keep_cols = [
            'borough', 'neighborhood', 'building_class_category',
            'tax_class_at_present', 'block', 'lot',
            'building_class_at_present', 'address', 'zip_code',
            'residential_units', 'commercial_units', 'total_units',
            'land_square_feet', 'gross_square_feet', 'year_built',
            'tax_class_at_time_of_sale', 'building_class_at_time_of',
            'sale_price', 'sale_date', 'apartment_number'
        ]

        df = df[[col for col in keep_cols if col in df.columns]]

        # Append to existing table
        df.to_sql('stg_sales_raw', engine, if_exists='append', index=False)

        total_rows += len(df)
        print(f"  {len(df):,} rows")

    print(f"\n{year} complete: {total_rows:,} total rows\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python -m scripts.load_historical_sales <year> <data_dir>")
        print("Example: python -m scripts.load_historical_sales 2024 /Users/connormoss/Desktop/2024_Sales")
        sys.exit(1)

    year = int(sys.argv[1])
    data_dir = sys.argv[2]

    load_historical_sales(year, data_dir)