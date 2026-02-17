from typing import Optional, Dict
from sqlalchemy.engine import Engine

from .config import DATASETS, get_engine, DatasetConfig
from .utils import fetch_all, normalize_columns, write_dataframe
from .runlog import start_run, finish_run_success, finish_run_failed


def load_acris_master(engine: Optional[Engine] = None, max_rows: Optional[int] = None) -> None:
    config: DatasetConfig = DATASETS["acris_master"]

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

    run = start_run(engine, "acris_master")

    try:
        print(f"[acris_master] Loading dataset `{config.name}` ({config.dataset_id})")

        # Filter for recent data only (2020+)
        extra_params: Dict[str, str] = {
            "$where": "good_through_date >= '2020-01-01'"
        }

        df = fetch_all(config, extra_params=extra_params)

        if df.empty:
            raise RuntimeError("No rows returned from ACRIS Master")

        print(f"[acris_master] DataFrame shape: {df.shape}")
        df = normalize_columns(df)

        write_dataframe(df, config, engine, if_exists="replace", run_id=run.run_id)

        finish_run_success(engine, run, rows=len(df))
        print("[acris_master] Done.")

    except Exception as e:
        finish_run_failed(engine, run, e)
        raise