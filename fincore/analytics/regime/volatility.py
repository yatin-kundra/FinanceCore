import pandas as pd
import numpy as np


def rolling_annualized_vol(
    returns: pd.Series,
    window: int = 63,
    periods_per_year: int = 252,
) -> pd.Series:
    """
    Rolling annualized volatility.
    """
    return returns.rolling(window).std() * np.sqrt(periods_per_year)


def volatility_regime(
    returns: pd.Series,
    window: int = 63,
    lookback: int = 756,
) -> dict:
    """
    Classifies volatility regime based on historical percentiles.

    window   : rolling vol window (e.g. 3 months)
    lookback : history used to define regimes (e.g. 3 years)
    """

    vol = rolling_annualized_vol(returns, window=window)

    if len(vol.dropna()) < lookback:
        raise ValueError("Insufficient data for volatility regime detection")

    current_vol = vol.iloc[-1]

    historical = vol.iloc[-lookback:-1].dropna()

    low_th = historical.quantile(0.33)
    high_th = historical.quantile(0.67)

    if current_vol <= low_th:
        regime = "low"
    elif current_vol >= high_th:
        regime = "high"
    else:
        regime = "normal"

    confidence = (
        abs(current_vol - historical.median())
        / historical.std()
        if historical.std() > 0
        else 0.0
    )

    return {
        "volatility": float(current_vol),
        "regime": regime,
        "confidence": float(confidence),
        "low_threshold": float(low_th),
        "high_threshold": float(high_th),
    }
