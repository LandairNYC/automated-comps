import os
from dataclasses import dataclass
from typing import Optional, Dict

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

# ============================
# Database configuration
# ============================

@dataclass
class DatabaseConfig:
    user: str
    password: str
    host: str = "localhost"
    port: str = "5433"
    db: str = "landair"

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


def get_engine() -> Engine:
    """
    Priority:
    1) Use DATABASE_URL if provided (Supabase / hosted DB)
    2) Fall back to local PG_* variables
    """

    database_url = os.getenv("DATABASE_URL")

    if database_url:
        return create_engine(database_url)

    # ---- Local fallback (your current setup) ----

    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5433")
    db = os.getenv("PG_DB", "landair")

    if not user or password is None:
        raise RuntimeError(
            "Database credentials missing. Set DATABASE_URL or PG_* variables."
        )

    local_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(local_url)


# ============================
# NYC Open Data configuration
# ============================

BASE_URL: str = "https://data.cityofnewyork.us/resource/"


@dataclass
class DatasetConfig:
    name: str
    dataset_id: str
    table_name: str
    limit: int = 50_000
    max_rows: Optional[int] = 200_000


DATASETS: Dict[str, DatasetConfig] = {
    "pluto": DatasetConfig(
        name="Pluto",
        dataset_id="64uk-42ks",
        table_name="stg_pluto_raw",
        limit=50_000,
        max_rows=200_000,
    ),
    "acris_master": DatasetConfig(
        name="ACRIS Real Property Master",
        dataset_id="bnx9-e6tj",
        table_name="stg_acris_master",
        limit=20_000,
        max_rows=200_000,
    ),
    "acris_legals": DatasetConfig(
        name="ACRIS Real Property Legals",
        dataset_id="8h5j-fqxa",
        table_name="stg_acris_legals",
        limit=50_000,
        max_rows=200_000,
    ),
    "acris_parties": DatasetConfig(
        name="ACRIS Real Property Parties",
        dataset_id="636b-3b5g",
        table_name="stg_acris_parties",
        limit=50_000,
        max_rows=200_000,
    ),
    "sales_rolling": DatasetConfig(
        name="NYC Citywide Rolling Calendar Sales",
        dataset_id="usep-8jbt",
        table_name="stg_sales_raw",
        limit=50_000,
        max_rows=200_000,
    ),
}
