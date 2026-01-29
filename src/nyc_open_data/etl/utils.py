from typing import Dict, Optional, List
import pandas as pd
import requests
from sqlalchemy.engine import Engine
from .config import BASE_URL, DATASETS, DatasetConfig

# Fetch a single page of data from the dataset return it as a DataFrame
def fetch_page(dataset_id: str, limit: int = 50_000, offset: int = 0, extra_params: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    url = f"{BASE_URL}{dataset_id}.json"
    params: Dict[str, str] = {
        "$limit": str(limit),
        "$offset": str(offset), 
    }

    if extra_params:
        params.update(extra_params)

    print(f"[utils] Requesting {url} with offset={offset} and limit={limit}")
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    print(f"[utils] Received {len(data)} records")

    return pd.DataFrame(data)

# Fetch all data for a given dataset until no data or max_rows is reached
def fetch_all(config: DatasetConfig, extra_params: Optional[Dict[str, str]] = None, ) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    offset = 0
    max_rows = config.max_rows

    while True:
        remaining = None if max_rows is None else max_rows - offset
        page_limit = config.limit if remaining is None else max(0, min(config.limit, remaining))
        if page_limit == 0:
            break

        df_chunk = fetch_page(
            dataset_id = config.dataset_id,
            limit = page_limit,
            offset = offset,
            extra_params = extra_params,
        )

        if df_chunk.empty:
            print("[utils] No more data returned")
            break

        frames.append(df_chunk)
        offset += len(df_chunk)
        print(f"[utils] Total rows so far: {offset}")

        if max_rows is not None and offset >= max_rows:
            break

    if not frames: 
        return pd.DataFrame()
    
    return pd.concat(frames, ignore_index=True)

# Normalize Dataframe columns to lowercase for consistency
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [col.lower() for col in df.columns]

    return df


# Write a Datarame to Postgres using the table name from the DatasetConfig.
def write_dataframe(df: pd.DataFrame, config: DatasetConfig, engine: Engine, if_exists: str = "replace") -> None:
    print(f"[utils] Writing {len(df)} rows to table `{config.table_name}` "
          f"(if_exists=`{if_exists}`)")
    
    df.to_sql(config.table_name, engine, if_exists=if_exists, index=False)
    
    print("[utils] Write complete")
