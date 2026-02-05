from dataclasses import dataclass
from datetime import datetime, timezone
from sqlalchemy import text
import uuid

@dataclass
class IngestionRun:
    run_id: str
    dataset_key: str

def start_run(engine, dataset_key: str) -> IngestionRun:
    run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO etl_ingestion_runs
                (run_id, dataset_key, status, started_at)
                VALUES (:run_id, :dataset_key, 'running', :started_at)
            """),
            {"run_id": run_id, "dataset_key": dataset_key, "started_at": datetime.now(timezone.utc)},
        )
    return IngestionRun(run_id=run_id, dataset_key=dataset_key)

def finish_run_success(engine, run: IngestionRun, rows: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE etl_ingestion_runs
                SET status='success', finished_at=:finished_at, rows_fetched=:rows, error=NULL
                WHERE run_id=:run_id
            """),
            {"finished_at": datetime.now(timezone.utc), "rows": rows, "run_id": run.run_id},
        )

def finish_run_failed(engine, run: IngestionRun, err: Exception) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE etl_ingestion_runs
                SET status='failed', finished_at=:finished_at, error=:error
                WHERE run_id=:run_id
            """),
            {"finished_at": datetime.now(timezone.utc), "error": str(err)[:4000], "run_id": run.run_id},
        )
