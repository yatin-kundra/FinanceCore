import pandas as pd
from fincore.storage.repositories import AssetRepository
from fincore.logging.logger import get_logger

log = get_logger("assets")


NIFTY_50_ASSETS = [
    # asset_id, ticker, name, exchange, sector
    (1, "RELIANCE.NS", "Reliance Industries", "NSE", "Energy"),
    (2, "TCS.NS", "Tata Consultancy Services", "NSE", "IT"),
    (3, "INFY.NS", "Infosys", "NSE", "IT"),
    (4, "HDFCBANK.NS", "HDFC Bank", "NSE", "Financials"),
    (5, "ICICIBANK.NS", "ICICI Bank", "NSE", "Financials"),
    # ... we will extend gradually
]


def register_nifty50_assets():
    df = pd.DataFrame(
        NIFTY_50_ASSETS,
        columns=["asset_id", "ticker", "name", "exchange", "sector"],
    )

    AssetRepository.upsert_assets(df)
    log.info("nifty50_assets_registered", count=len(df))
