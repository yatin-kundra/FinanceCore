import pandas as pd

from fincore.data.universe import NIFTY_500
from fincore.storage.connection import get_connection
from fincore.logging.logger import get_logger

log = get_logger("ingest_assets")


def ingest_assets():
    df = pd.DataFrame({
        "ticker": NIFTY_500,
        "name": NIFTY_500,
        "exchange": "NSE",
        "sector": None,
    })

    con = get_connection()
    con.execute("BEGIN")

    try:
        con.register("assets_df", df)
        con.execute("""
            INSERT INTO assets (ticker, name, exchange, sector)
            SELECT ticker, name, exchange, sector
            FROM assets_df
            ON CONFLICT (ticker) DO NOTHING
        """)
        con.execute("COMMIT")
        log.info("assets_bootstrapped", count=len(df))
    except Exception as e:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
