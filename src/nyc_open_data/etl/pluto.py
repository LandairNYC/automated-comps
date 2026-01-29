from typing import Optional
from sqlalchemy.engine import Engine
from .config import DATASETS, DatasetConfig, get_engine
from .utils import fetch_all, normalize_columns, write_dataframe

def load_pluto(engine: Optional[Engine] = None, max_rows: Optional[int] = None) -> None:
    config: DatasetConfig = DATASETS["pluto"]

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

    print(f"[pluto] Loading dataset '{config.name}' ({config.dataset_id})")

    df = fetch_all(config)

    if df.empty:
        print("[pluto] No rows returned from PLUTO.")
        return 
    
    print (f"[pluto] DataFrame shape: {df.shape}")
    df = normalize_columns(df)

    write_dataframe(df, config, engine, if_exists="replace")
    print("[pluto] Done.")