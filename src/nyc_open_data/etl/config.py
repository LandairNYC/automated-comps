import os 
from dataclasses import dataclass 
from typing import Optional, Dict
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

@dataclass
class DatabaseConfig:
    user: str
    password: str
    host: str = "localhost"
    port: str = "5433"
    db: str = "landair"

    @property
    def sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
    
@dataclass
class DatasetConfig:
    name: str
    dataset_id: str
    table_name: str
    limit: int = 50_000
    max_rows: Optional[int] = 200_000

def get_db_config() -> DatabaseConfig:
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5433")
    db = os.getenv("PG_DB", "landair")

    if not user or password is None: 
        raise RuntimeError("Database credentials missing. Ceheck your .env file.")
    
    return DatabaseConfig(
        user = user,
        password = password,
        host = host,
        port = port,
        db = db,
    )

def get_engine() -> Engine:
    db_config = get_db_config()
    url = db_config.sqlalchemy_url
    
    return create_engine(url)

BASE_URL: str = "https://data.cityofnewyork.us/resource/"

DATASETS: Dict[str, DatasetConfig] = {
    "pluto": DatasetConfig(
        name = "Pluto",
        dataset_id = "64uk-42ks",
        table_name = "stg_pluto_raw",
        limit=50_000,
        max_rows=200_000,
    ),

    "acris_master": DatasetConfig(
        name ="ACRIS Real Property Master",
        dataset_id = "bnx9-e6tj",
        table_name = "stg_acris_master",
        limit = 50_000,
        max_rows = 200_000,
    ),

    "acris_legal": DatasetConfig(
        name = "ACRIS Real Property Legals",
        dataset_id = "8h5j-fqxa",
        table_name = "stg_acris_legal",
        limit = 50_000,
        max_rows = 200_000,
    ),

    "acris_parties": DatasetConfig(
        name= "ACRIS Real Property Parties",
        dataset_id = "636b-3b5g",
        table_name = "stg_acris_parties",
        limit = 50_000,
        max_rows = 200_000,
    ),

    "sales_rolling": DatasetConfig(
        name="NYC Citywide Rolling Calendar Sales",
        dataset_id="usep-8jbt",
        table_name="stg_sales_raw",
        limit=50_000,
        max_rows=200_000,
    ),
}
