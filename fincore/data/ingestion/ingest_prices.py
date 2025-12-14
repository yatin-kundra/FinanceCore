from datetime import date

from fincore.data.universe import NIFTY_500
from fincore.data.yahoo import ingest_prices_for_asset
from fincore.storage.connection import get_connection
from fincore.logging.logger import get_logger

log = get_logger("ingest_prices")


def _get_asset_id(ticker: str) -> int:
    con = get_connection(read_only=True)
    row = con.execute(
        "SELECT asset_id FROM assets WHERE ticker = ?",
        [ticker],
    ).fetchone()
    con.close()

    if row is None:
        raise ValueError(f"Asset not found for ticker: {ticker}")

    return row[0]


def ingest_all_prices(default_start: date = date(2000, 1, 1)):
    """
    Canonical ingestion flow:
    ticker → asset_id → Yahoo → prices(asset_id, date)
    """

    for ticker in NIFTY_500:
        try:
            asset_id = _get_asset_id(ticker)

            ingest_prices_for_asset(
                asset_id=asset_id,
                ticker=ticker,
                start_date=default_start,
            )

            log.info(
                "asset_prices_ingested",
                ticker=ticker,
                asset_id=asset_id,
            )

        except Exception as e:
            log.error(
                "ingestion_failed",
                ticker=ticker,
                error=str(e),
            )
