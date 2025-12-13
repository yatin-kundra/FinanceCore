from pathlib import Path
from pydantic import BaseModel


class Settings(BaseModel):
    # Environment
    ENV: str = "dev"

    # Project root (repo-level)
    PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

    # Storage (repo-scoped, versioned)
    DATA_DIR: Path = PROJECT_ROOT / "data_store"
    DB_PATH: Path = DATA_DIR / "market.duckdb"

    # Logging
    LOG_LEVEL: str = "INFO"

    # Scheduler
    INGESTION_TIMEZONE: str = "Asia/Kolkata"

    class Config:
        frozen = True


settings = Settings()

# Ensure base directories exist
settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
