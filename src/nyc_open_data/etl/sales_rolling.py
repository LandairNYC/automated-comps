from typing import Optional, Dict
from sqlalchemy.engine import Engine
from .config import DATASETS, DatasetConfig, get_engine
from .utils import fetch_all, normalize_columns, write_dataframe
from datetime import datetime, timedelta, timezone

def load_sales_rolling(engine: Optional[Engine] = None, max_rows: Optional[int] = None, days_back: int = 45,) -> None:
    config: DatasetConfig = DATASETS["sales_rolling"]

    if max_rows is not None:
        config = DatasetConfig(
            name=config.name,
            dataset_id=config.dataset_id,
            table_name=config.table_name,
            limit=config.limit,
            max_rows=max_rows,
        )

    if engine is None:
        engine = get_engine()

    # Rolling window filter (field name depends on dataset schema; we'll confirm next)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S.000")

    extra_params: Dict[str, str] = {
        "$order": "sale_date DESC",
        "$where": f"sale_date >= '{cutoff_str}'",
    }

    print(f"[sales] Loading dataset '{config.name}' ({config.dataset_id})")

    df = fetch_all(config, extra_params=extra_params)
    if df.empty:
        print("[sales] No rows returned.")
        return

    df = normalize_columns(df)
    write_dataframe(df, config, engine, if_exists="replace")
    print("[sales] Done.")
