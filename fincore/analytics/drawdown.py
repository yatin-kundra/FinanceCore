import pandas as pd
from fincore.logging.logger import get_logger

log = get_logger("drawdown")


def compute_drawdown(returns: pd.Series) -> pd.DataFrame:
    """
    Returns a DataFrame with:
    - cumulative returns
    - running peak
    - drawdown
    """

    if returns.isna().all():
        raise ValueError("All returns are NaN")

    cumulative = (1 + returns.fillna(0)).cumprod()
    peak = cumulative.cummax()
    drawdown = (cumulative - peak) / peak

    return pd.DataFrame(
        {
            "cumulative": cumulative,
            "peak": peak,
            "drawdown": drawdown,
        }
    )


def max_drawdown(returns: pd.Series) -> float:
    dd = compute_drawdown(returns)["drawdown"]
    return dd.min()
