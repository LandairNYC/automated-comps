from typing import Optional
from sqlalchemy.engine import Engine
from .config import DATASETS, get_engine, DatasetConfig
from .utils import fetch_all, normalize_columns, write_dataframe

def load_acris_master(engine: Optional[Engine] = None, max_rows: Optional[int] = None) -> None:
    config: DatasetConfig = DATASETS["acris_master"]

    if max_rows is not None: 
        config = DatasetConfig(
            name = config.name,
            dataset_id = config.dataset_id,
            table_name = config.table_name,
            limit = config.limit,
            max_rows = max_rows
        )

    if engine is None: 
        engine = get_engine()

    print(f"[acris_master] Loading dataset `{config.name}` ({config.dataset_id})")

    df = fetch_all(config)

    if df.empty: 
        print("[acris_master] No rows returned from ACRIS Master.")
        return
    
    print(f"[acris_master] DataFrame shape: {df.shape}")
    df = normalize_columns(df)

    write_dataframe(df, config, engine, if_exists="replace")
    print("[acris_master] Done.")

