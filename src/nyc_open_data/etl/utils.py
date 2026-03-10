from typing import Dict, Optional, List
import os

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import BASE_URL, DATASETS, DatasetConfig


def build_session() -> requests.Session:
    s = requests.Session()

    retry = Retry(
        total=6,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
    if token:
        s.headers.update({"X-App-Token": token})

    return s


def write_dataframe_safe_replace(df: pd.DataFrame, table_name: str, engine: Engine, run_id: str) -> None:
    tmp_table = f"{table_name}__tmp_{run_id.replace('-', '')}"
    chunk_size = 100_000

    # Write to temp table in chunks to avoid Supabase statement timeout on large datasets
    total = len(df)
    for i, start in enumerate(range(0, total, chunk_size)):
        chunk = df.iloc[start:start + chunk_size]
        mode = "replace" if i == 0 else "append"
        print(f"[utils] Writing chunk {i+1} ({start+1}–{min(start+chunk_size, total)} of {total})")
        chunk.to_sql(tmp_table, engine, if_exists=mode, index=False)

    # atomic swap
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
        conn.execute(text(f'ALTER TABLE "{tmp_table}" RENAME TO "{table_name}"'))


# Fetch a single page of data from the dataset return it as a DataFrame
def fetch_page(
    dataset_id: str,
    limit: int = 50_000,
    offset: int = 0,
    extra_params: Optional[Dict[str, str]] = None,
    session: Optional[requests.Session] = None,
) -> pd.DataFrame:
    url = f"{BASE_URL}{dataset_id}.json"
    params: Dict[str, str] = {
        "$limit": str(limit),
        "$offset": str(offset),
    }

    if extra_params:
        params.update(extra_params)

    http = session or requests

    print(f"[utils] Requesting {url} with offset={offset} and limit={limit}")
    resp = http.get(url, params=params, timeout=(10, 180))

    if resp.status_code == 429:
        raise RuntimeError("Rate limited (429) after retries")
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")

    data = resp.json()
    print(f"[utils] Received {len(data)} records")

    return pd.DataFrame(data)


# Fetch all data for a given dataset until no data or max_rows is reached
def fetch_all(
    config: DatasetConfig,
    extra_params: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    offset = 0
    max_rows = config.max_rows

    session = build_session()

    while True:
        remaining = None if max_rows is None else max_rows - offset
        page_limit = config.limit if remaining is None else max(0, min(config.limit, remaining))
        if page_limit == 0:
            break

        df_chunk = fetch_page(
            dataset_id=config.dataset_id,
            limit=page_limit,
            offset=offset,
            extra_params=extra_params,
            session=session,
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


# Write a Dataframe to Postgres using the table name from the DatasetConfig.
def write_dataframe(
    df: pd.DataFrame,
    config: DatasetConfig,
    engine: Engine,
    if_exists: str = "replace",
    run_id: Optional[str] = None,
) -> None:
    print(
        f"[utils] Writing {len(df)} rows to table `{config.table_name}` "
        f"(if_exists=`{if_exists}`)"
    )

    if if_exists == "replace":
        if not run_id:
            raise ValueError("run_id is required when if_exists='replace' (for atomic swap)")
        write_dataframe_safe_replace(df, config.table_name, engine, run_id=run_id)
    else:
        df.to_sql(config.table_name, engine, if_exists=if_exists, index=False)

    print("[utils] Write complete")