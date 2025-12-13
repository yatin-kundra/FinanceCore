import pandas as pd
import numpy as np
from fincore.logging.logger import get_logger

log = get_logger("returns")


def compute_returns(
    prices: pd.DataFrame,
    price_col: str = "adj_close",
    method: str = "arithmetic",
) -> pd.Series:
    """
    prices: DataFrame indexed by date with a price column
    method: 'arithmetic' or 'log'
    """

    if price_col not in prices.columns:
        raise ValueError(f"Missing price column: {price_col}")

    px = prices[price_col].astype(float)

    if method == "arithmetic":
        ret = px.pct_change()
    elif method == "log":
        ret = np.log(px / px.shift(1))
    else:
        raise ValueError("method must be 'arithmetic' or 'log'")

    return ret

def compute_returns_matrix(
    prices: pd.DataFrame,
    method: str = "arithmetic",
) -> pd.DataFrame:
    """
    prices: DataFrame (date x asset_id)
    Returns: DataFrame (date x asset_id)
    """

    if not isinstance(prices, pd.DataFrame):
        raise TypeError("prices must be a DataFrame")

    returns = prices.copy()

    for col in prices.columns:
        returns[col] = compute_returns(
            prices[[col]].rename(columns={col: "adj_close"}),
            method=method,
        )

    return returns



FREQ_MAP = {
    "W": "W",
    "M": "ME",   # Month End
    "Q": "QE",   # Quarter End
    "Y": "YE",   # Year End
}


def resample_returns(
    daily_returns: pd.Series,
    freq: str,
    method: str = "compound",
) -> pd.Series:
    if freq not in FREQ_MAP:
        raise ValueError(f"Unsupported frequency: {freq}")

    pandas_freq = FREQ_MAP[freq]

    if method == "compound":
        return (1 + daily_returns).resample(pandas_freq).prod() - 1
    elif method == "sum":
        return daily_returns.resample(pandas_freq).sum()
    else:
        raise ValueError("method must be 'compound' or 'sum'")

