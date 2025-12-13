import pandas as pd


def active_return(
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series,
) -> float:
    aligned_p, aligned_b = portfolio_returns.align(
        benchmark_returns, join="inner"
    )
    return (1 + aligned_p).prod() - (1 + aligned_b).prod()


def active_risk(
    excess_returns: pd.Series,
) -> float:
    return excess_returns.std()
