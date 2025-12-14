from fincore.storage.connection import get_connection
from fincore.logging.logger import get_logger

log = get_logger("schema")


def create_schema():
    con = get_connection()
    log.info("schema_init_start")

    # -------------------------
    # Asset ID sequence (DuckDB-safe)
    # -------------------------
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS asset_id_seq START 1;
    """)

    # -------------------------
    # Assets master table
    # -------------------------
    con.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            asset_id BIGINT PRIMARY KEY DEFAULT nextval('asset_id_seq'),
            ticker TEXT UNIQUE NOT NULL,
            name TEXT,
            exchange TEXT,
            sector TEXT
        )
    """)

    # -------------------------
    # Prices
    # -------------------------
    con.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            asset_id BIGINT NOT NULL,
            date DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adj_close DOUBLE,
            volume BIGINT,
            PRIMARY KEY (asset_id, date)
        )
    """)

    # -------------------------
    # Benchmarks
    # -------------------------
    con.execute("""
        CREATE TABLE IF NOT EXISTS benchmarks (
            name TEXT PRIMARY KEY
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_prices (
            benchmark TEXT NOT NULL,
            date DATE NOT NULL,
            adj_close DOUBLE,
            PRIMARY KEY (benchmark, date)
        )
    """)

    # -------------------------
    # Indexes
    # -------------------------
    con.execute("CREATE INDEX IF NOT EXISTS idx_assets_ticker ON assets(ticker)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_prices_asset ON prices(asset_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_prices_date ON benchmark_prices(date)")

    log.info("schema_init_complete")
    con.close()
