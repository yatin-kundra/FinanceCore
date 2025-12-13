import pandas as pd
import yfinance as yf
from datetime import date, timedelta

from fincore.storage.connection import get_connection
from fincore.storage.repositories import PriceRepository
from fincore.logging.logger import get_logger

log = get_logger("yahoo_ingestion")


def _get_last_price_date(asset_id: int):
    con = get_connection(read_only=True)
    result = con.execute(
        "SELECT MAX(date) FROM prices WHERE asset_id = ?",
        [asset_id],
    ).fetchone()
    con.close()
    return result[0]


def ingest_prices_for_asset(
    asset_id: int,
    ticker: str,
    start_date: date,
):
    last_date = _get_last_price_date(asset_id)

    if last_date:
        fetch_start = last_date + timedelta(days=1)
    else:
        fetch_start = start_date

    if fetch_start > date.today():
        log.info(
            "prices_up_to_date",
            asset_id=asset_id,
            ticker=ticker,
        )
        return

    log.info(
        "price_fetch_start",
        asset_id=asset_id,
        ticker=ticker,
        start=str(fetch_start),
    )

    try:
        df = yf.download(
            ticker,
            start=fetch_start,
            progress=False,
            auto_adjust=False,
        )

        if df.empty:
            log.warning(
                "no_price_data",
                asset_id=asset_id,
                ticker=ticker,
            )
            return

        df = df.reset_index()
        df = df.rename(
            columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
        )

        df["asset_id"] = asset_id
        df = df[
            [
                "asset_id",
                "date",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
            ]
        ]

        PriceRepository.insert_prices(df)

    except Exception as e:
        log.error(
            "price_ingestion_failed",
            asset_id=asset_id,
            ticker=ticker,
            error=str(e),
        )
        raise
