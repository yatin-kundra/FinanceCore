import duckdb
from fincore.config.settings import settings


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(
        database=str(settings.DB_PATH),
        read_only=read_only
    )