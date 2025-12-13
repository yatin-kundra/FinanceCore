import yfinance as yf
import pandas as pd
from fincore.logging.logger import get_logger

log = get_logger("benchmark_prices")


def fetch_benchmark_returns(
    ticker: str,
    start: str,
    end: str | None = None,
) -> pd.Series:
    """
    Fetch benchmark index returns from Yahoo Finance.

    Example tickers:
    - ^NSEI   -> NIFTY 50
    - ^NSEBANK -> BANK NIFTY
    """

    df = yf.download(
        ticker,
        start=start,
        end=end,
        progress=False,
        auto_adjust=True,
    )

    if df.empty:
        raise ValueError(f"No data returned for benchmark {ticker}")

    returns = df["Close"].pct_change().dropna()
    returns.name = ticker

    log.info(
        "benchmark_returns_fetched",
        ticker=ticker,
        rows=len(returns),
    )

    return returns
