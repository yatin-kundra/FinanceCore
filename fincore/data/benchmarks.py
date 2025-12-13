from fincore.storage.connection import get_connection
from fincore.logging.logger import get_logger

log = get_logger("benchmarks")

BENCHMARKS = [
    "NIFTY_50",
    "NIFTY_100",
    "NIFTY_500",
]


def register_benchmarks():
    con = get_connection()
    con.execute("BEGIN TRANSACTION")

    try:
        for name in BENCHMARKS:
            exists = con.execute(
                "SELECT 1 FROM benchmarks WHERE name = ?",
                [name],
            ).fetchone()

            if not exists:
                con.execute(
                    "INSERT INTO benchmarks (name) VALUES (?)",
                    [name],
                )

        con.execute("COMMIT")
        log.info("benchmarks_registered", count=len(BENCHMARKS))

    except Exception as e:
        con.execute("ROLLBACK")
        log.error("benchmark_registration_failed", error=str(e))
        raise
    finally:
        con.close()
