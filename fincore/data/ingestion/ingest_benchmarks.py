import yfinance as yf
from fincore.data.universe import BENCHMARKS
from fincore.storage.connection import get_connection
from fincore.logging.logger import get_logger

log = get_logger("ingest_benchmarks")


def ingest_benchmarks(start="2000-01-01"):
    con = get_connection()

    for name, ticker in BENCHMARKS.items():
        try:
            df = yf.download(
                ticker,
                start=start,
                progress=False,
            )

            if df.empty:
                log.warning("benchmark_empty", benchmark=name)
                continue

            df = df.reset_index()
            df["benchmark"] = name

            df = df[["Date", "Adj Close", "benchmark"]]
            df.columns = ["date", "adj_close", "benchmark"]

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

            for _, row in df.iterrows():
                con.execute(
                    """
                    INSERT OR IGNORE INTO benchmark_prices
                    (benchmark, date, adj_close)
                    VALUES (?, ?, ?)
                    """,
                    [row.benchmark, row.date, row.adj_close],
                )

            con.execute("COMMIT")
            log.info("benchmark_ingested", benchmark=name, rows=len(df))

        except Exception as e:
            con.execute("ROLLBACK")
            log.error("benchmark_failed", benchmark=name, error=str(e))

    con.close()
