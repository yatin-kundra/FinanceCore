import pandas as pd
from fincore.storage.connection import get_connection
from fincore.logging.logger import get_logger

log = get_logger("repository")


class AssetRepository:
    @staticmethod
    def upsert_assets(df: pd.DataFrame):
        con = get_connection()
        con.execute("BEGIN TRANSACTION")

        try:
            con.register("assets_df", df)
            con.execute("""
                INSERT OR IGNORE INTO assets
                SELECT * FROM assets_df
            """)
            con.execute("COMMIT")
            log.info("assets_upserted", rows=len(df))
        except Exception as e:
            con.execute("ROLLBACK")
            log.error("assets_upsert_failed", error=str(e))
            raise
        finally:
            con.close()


class PriceRepository:
    @staticmethod
    def insert_prices(df: pd.DataFrame):
        con = get_connection()
        con.execute("BEGIN TRANSACTION")

        try:
            con.register("prices_df", df)
            con.execute("""
                INSERT OR IGNORE INTO prices
                SELECT * FROM prices_df
            """)
            con.execute("COMMIT")
            log.info("prices_inserted", rows=len(df))
        except Exception as e:
            con.execute("ROLLBACK")
            log.error("prices_insert_failed", error=str(e))
            raise
        finally:
            con.close()
