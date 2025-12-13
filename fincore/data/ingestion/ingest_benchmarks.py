import yfinance as yf
import pandas as pd

from fincore.data.universe import BENCHMARKS
from fincore.storage.connection import get_connection
from fincore.logging.logger import get_logger

log = get_logger("ingest_benchmarks")


def ingest_benchmarks(start: str = "2000-01-01"):
    """
    Ingest benchmark adjusted close prices from Yahoo Finance.

    - Forces adjusted prices
    - Idempotent
    - DuckDB-safe
    - CI-safe
    """

    con = get_connection()

    # Create table once
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

    for benchmark, ticker in BENCHMARKS.items():
        try:
            log.info(
                "benchmark_fetch_start",
                benchmark=benchmark,
                ticker=ticker,
            )

            df = yf.download(
                ticker,
                start=start,
                progress=False,
                auto_adjust=False,
            )

            if df.empty:
                log.warning("benchmark_empty", benchmark=benchmark)
                continue

            # Reset index and flatten columns (VERY IMPORTANT)
            df = df.reset_index()
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

            if "Adj Close" not in df.columns:
                raise ValueError(
                    f"Adjusted close missing for benchmark {benchmark}"
                )

            df = df.rename(
                columns={
                    "Date": "date",
                    "Adj Close": "adj_close",
                }
            )

            df["benchmark"] = benchmark
            df = df[["benchmark", "date", "adj_close"]]

            # Convert to pure Python tuples (DuckDB-safe)
            records = [
                (benchmark, row.date, float(row.adj_close))
                for row in df.itertuples(index=False)
            ]

            con.executemany(
                """
                INSERT OR IGNORE INTO benchmark_prices
                (benchmark, date, adj_close)
                VALUES (?, ?, ?)
                """,
                records,
            )

            log.info(
                "benchmark_ingested",
                benchmark=benchmark,
                rows=len(records),
            )

        except Exception as e:
            log.error(
                "benchmark_ingestion_failed",
                benchmark=benchmark,
                error=str(e),
            )
            raise

    con.close()
