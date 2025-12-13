import yfinance as yf
import pandas as pd

from fincore.data.universe import BENCHMARKS
from fincore.storage.connection import get_connection
from fincore.logging.logger import get_logger

log = get_logger("ingest_benchmarks")


def ingest_benchmarks(start: str = "2000-01-01"):
    """
    Ingest benchmark adjusted close prices from Yahoo Finance.

    Benchmarks are treated as adjusted price series by definition.
    Data is ingested incrementally and idempotently.
    """

    con = get_connection()

    # Ensure table exists once (not inside loop)
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

    for name, ticker in BENCHMARKS.items():
        try:
            log.info("benchmark_fetch_start", benchmark=name, ticker=ticker)

            df = yf.download(
                ticker,
                start=start,
                progress=False,
                auto_adjust=False,  # IMPORTANT: we explicitly control adjustment
            )

            if df.empty:
                log.warning("benchmark_empty", benchmark=name)
                continue

            # Normalize
            df = df.reset_index()
            df = df.rename(
                columns={
                    "Date": "date",
                    "Adj Close": "adj_close",
                }
            )

            if "adj_close" not in df.columns:
                raise ValueError(
                    f"Adjusted close price missing for benchmark {name}"
                )

            df["benchmark"] = name
            df = df[["benchmark", "date", "adj_close"]]

            # Insert row-by-row (safe for DuckDB + IGNORE semantics)
            for _, row in df.iterrows():
                con.execute(
                    """
                    INSERT OR IGNORE INTO benchmark_prices
                    (benchmark, date, adj_close)
                    VALUES (?, ?, ?)
                    """,
                    [row.benchmark, row.date, float(row.adj_close)],
                )

            log.info(
                "benchmark_ingested",
                benchmark=name,
                rows=len(df),
            )

        except Exception as e:
            # DuckDB autocommits — no ROLLBACK needed
            log.error(
                "benchmark_ingestion_failed",
                benchmark=name,
                error=str(e),
            )
            raise

    con.close()
