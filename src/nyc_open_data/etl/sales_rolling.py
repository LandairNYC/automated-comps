from typing import Optional, Dict
from datetime import datetime, timedelta, timezone

from sqlalchemy.engine import Engine

from .config import DATASETS, DatasetConfig, get_engine
from .utils import fetch_all, normalize_columns, write_dataframe_safe_replace
from .runlog import start_run, finish_run_success, finish_run_failed


def load_sales_rolling(
    engine: Optional[Engine] = None,
    max_rows: Optional[int] = None,
    days_back: int = 45,
) -> None:
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

    # Start a traceable run (Step 1E)
    run = start_run(engine, dataset_key="sales_rolling")

    try:
        # Rolling window filter
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
        cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S.000")

        extra_params: Dict[str, str] = {
            "$order": "sale_date DESC",
            "$where": f"sale_date >= '{cutoff_str}'",
        }

        print(f"[sales] Loading dataset '{config.name}' ({config.dataset_id})")
        df = fetch_all(config, extra_params=extra_params)

        if df.empty:
            # Decide how you want to treat "0 rows": success with rows=0
            finish_run_success(engine, run, rows=0)
            print("[sales] No rows returned.")
            return

        df = normalize_columns(df)

        # Atomic swap write (no corrupt reruns)
        write_dataframe_safe_replace(df, table_name=config.table_name, engine=engine, run_id=run.run_id)

        finish_run_success(engine, run, rows=len(df))
        print("[sales] Done.")

    except Exception as e:
        finish_run_failed(engine, run, e)
        raise
