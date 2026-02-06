from typing import Optional
from sqlalchemy.engine import Engine
from .config import DATASETS, DatasetConfig, get_engine
from .utils import fetch_all, normalize_columns, write_dataframe
from .runlog import start_run, finish_run_success, finish_run_failed  # <- add

def load_pluto(engine: Optional[Engine] = None, max_rows: Optional[int] = None) -> None:
    config: DatasetConfig = DATASETS["pluto"]

    if max_rows is not None:
        config = DatasetConfig(
            name=config.name,
            dataset_id=config.dataset_id,
            table_name=config.table_name,
            limit=config.limit,
            max_rows=max_rows
        )

    if engine is None:
        engine = get_engine()

    run = start_run(engine, "pluto")  # <- add

    try:
        print(f"[pluto] Loading dataset '{config.name}' ({config.dataset_id})")

        df = fetch_all(config)

        if df.empty:
            raise RuntimeError("No rows returned from PLUTO")  # <- fail loud so run logs reflect it

        print(f"[pluto] DataFrame shape: {df.shape}")
        df = normalize_columns(df)

        write_dataframe(df, config, engine, if_exists="replace", run_id=run.run_id)  # <- pass run_id

        finish_run_success(engine, run, rows=len(df))  # <- add
        print("[pluto] Done.")

    except Exception as e:
        finish_run_failed(engine, run, e)  # <- add
        raise
