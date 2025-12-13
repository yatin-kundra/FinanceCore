import numpy as np
import pandas as pd
from fincore.logging.logger import get_logger

log = get_logger("risk")


def annualized_volatility(
    returns: pd.Series,
    periods_per_year: int = 252,
) -> float:
    return returns.std() * np.sqrt(periods_per_year)


def sharpe_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    excess = returns - risk_free_rate / periods_per_year
    vol = annualized_volatility(excess, periods_per_year)

    if vol == 0:
        return np.nan

    return excess.mean() * periods_per_year / vol


def downside_deviation(
    returns: pd.Series,
    threshold: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    downside = returns[returns < threshold]
    if downside.empty:
        return 0.0

    return np.sqrt((downside ** 2).mean()) * np.sqrt(periods_per_year)


def sortino_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    excess = returns - risk_free_rate / periods_per_year
    dd = downside_deviation(excess, periods_per_year=periods_per_year)

    if dd == 0:
        return np.nan

    return excess.mean() * periods_per_year / dd


def value_at_risk(
    returns: pd.Series,
    level: float = 0.05,
) -> float:
    return np.quantile(returns.dropna(), level)


def conditional_value_at_risk(
    returns: pd.Series,
    level: float = 0.05,
) -> float:
    var = value_at_risk(returns, level)
    tail = returns[returns <= var]

    if tail.empty:
        return np.nan

    return tail.mean()
