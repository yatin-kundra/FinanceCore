import pandas as pd
import numpy as np


def average_correlation(
    returns: pd.DataFrame,
) -> float:
    """
    Computes average pairwise correlation.
    """
    corr = returns.corr()
    upper = corr.where(
        np.triu(np.ones(corr.shape), k=1).astype(bool)
    )
    return float(upper.stack().mean())


def correlation_regime(
    returns: pd.DataFrame,
    lookback: int = 252,
) -> dict:
    """
    Detects correlation stress.
    """

    recent = returns.iloc[-lookback:]
    hist = returns.iloc[:-1]

    if len(hist) < lookback:
        raise ValueError("Insufficient history for correlation regime")

    recent_corr = average_correlation(recent)
    hist_corr = average_correlation(hist)

    delta = recent_corr - hist_corr

    regime = "stressed" if delta > 0.1 else "normal"

    confidence = abs(delta) / hist_corr if hist_corr != 0 else 0.0

    return {
        "recent_corr": float(recent_corr),
        "historical_corr": float(hist_corr),
        "delta": float(delta),
        "regime": regime,
        "confidence": float(confidence),
    }
