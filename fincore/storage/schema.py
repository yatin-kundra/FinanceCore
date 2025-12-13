from fincore.storage.connection import get_connection
from fincore.logging.logger import get_logger

log = get_logger("schema")


def create_schema():
    """
    Create all core tables for fincore.

    Design principles:
    - assets define tickers
    - prices are keyed ONLY by asset_id
    - benchmarks are first-class time series
    - schema is ingestion- and analytics-safe
    """

    con = get_connection()

    log.info("schema_init_start")

    # -------------------------
    # Assets master table
    # -------------------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS assets (
            asset_id INTEGER PRIMARY KEY,
            ticker TEXT UNIQUE NOT NULL,
            name TEXT,
            exchange TEXT,
            sector TEXT
        )
        """
    )

    # -------------------------
    # Price history (asset_id based)
    # -------------------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            asset_id INTEGER NOT NULL,
            date DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adj_close DOUBLE,
            volume BIGINT,
            PRIMARY KEY (asset_id, date)
        )
        """
    )

    # -------------------------
    # Corporate actions
    # -------------------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS corporate_actions (
            asset_id INTEGER NOT NULL,
            date DATE NOT NULL,
            action_type TEXT,
            value DOUBLE,
            PRIMARY KEY (asset_id, date, action_type)
        )
        """
    )

    # -------------------------
    # Benchmarks master
    # -------------------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmarks (
            name TEXT PRIMARY KEY
        )
        """
    )

    # -------------------------
    # Benchmark membership (optional, future use)
    # -------------------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_membership (
            benchmark_name TEXT NOT NULL,
            asset_id INTEGER NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE,
            PRIMARY KEY (benchmark_name, asset_id, start_date)
        )
        """
    )

    # -------------------------
    # Benchmark price series
    # -------------------------
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_prices (
            benchmark TEXT NOT NULL,
            date DATE NOT NULL,
            adj_close DOUBLE,
            PRIMARY KEY (benchmark, date)
        )
        """
    )

    # -------------------------
    # Indexes (critical for scale)
    # -------------------------
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prices_date
        ON prices (date)
        """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prices_asset
        ON prices (asset_id)
        """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_assets_ticker
        ON assets (ticker)
        """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_benchmark_prices_date
        ON benchmark_prices (date)
        """
    )

    log.info("schema_init_complete")
    con.close()
