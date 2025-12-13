import pandas as pd
import numpy as np


def excess_returns(
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series,
) -> pd.Series:
    aligned_p, aligned_b = portfolio_returns.align(
        benchmark_returns, join="inner"
    )
    return aligned_p - aligned_b


def tracking_error(
    excess_returns: pd.Series,
    periods_per_year: int = 252,
) -> float:
    return excess_returns.std() * np.sqrt(periods_per_year)


def information_ratio(
    excess_returns: pd.Series,
    periods_per_year: int = 252,
) -> float:
    te = tracking_error(excess_returns, periods_per_year)
    te = float(te)
    if te == 0:
        return 0.0
    return excess_returns.mean() * periods_per_year / te
