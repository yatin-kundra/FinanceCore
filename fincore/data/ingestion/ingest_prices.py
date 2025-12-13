from fincore.storage.connection import get_connection
from fincore.data.yahoo import fetch_prices
from fincore.data.universe import NIFTY_500
from fincore.logging.logger import get_logger

log = get_logger("ingest_prices")


def ingest_all_prices(default_start="2000-01-01"):
    con = get_connection()

    for ticker in NIFTY_500:
        try:
            last_date = con.execute(
                """
                SELECT MAX(date) FROM prices
                WHERE ticker = ?
                """,
                [ticker],
            ).fetchone()[0]

            start = last_date or default_start

            df = fetch_prices(ticker, start=start)

            if df.empty:
                log.warning("no_data", ticker=ticker)
                continue

            df.to_sql(
                "prices",
                con,
                if_exists="append",
                index=False,
            )

            log.info(
                "prices_ingested",
                ticker=ticker,
                rows=len(df),
            )

        except Exception as e:
            log.error(
                "ingestion_failed",
                ticker=ticker,
                error=str(e),
            )
