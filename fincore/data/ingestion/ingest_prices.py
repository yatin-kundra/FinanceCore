from datetime import date

from fincore.storage.connection import get_connection
from fincore.data.yahoo import ingest_prices_for_asset
from fincore.data.universe import NIFTY_500
from fincore.logging.logger import get_logger

log = get_logger("ingest_prices")


def _get_asset_id(ticker: str) -> int:
    con = get_connection(read_only=True)
    row = con.execute(
        "SELECT asset_id FROM assets WHERE ticker = ?",
        [ticker],
    ).fetchone()
    con.close()

    if not row:
        raise ValueError(f"Asset not found for ticker {ticker}")

    return row[0]


def ingest_all_prices(default_start: date = date(2000, 1, 1)):
    """
    Ingest prices for all assets in the universe.

    Flow:
    ticker -> asset_id -> incremental Yahoo ingestion -> repository insert
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
                "asset_ingested",
                ticker=ticker,
                asset_id=asset_id,
            )

        except Exception as e:
            log.error(
                "ingestion_failed",
                ticker=ticker,
                error=str(e),
            )
