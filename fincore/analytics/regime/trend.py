import pandas as pd
import numpy as np


def trend_strength(
    returns: pd.Series,
    short_window: int = 63,
    long_window: int = 252,
) -> float:
    """
    Measures trend persistence via moving average of returns.
    """
    short_mean = returns.rolling(short_window).mean()
    long_mean = returns.rolling(long_window).mean()

    return float((short_mean - long_mean).iloc[-1])


def trend_regime(
    returns: pd.Series,
    threshold: float = 0.0,
) -> dict:
    """
    Classifies trend vs mean-reversion.
    """

    strength = trend_strength(returns)

    if strength > threshold:
        regime = "trending"
    else:
        regime = "mean_reverting"

    confidence = abs(strength) / returns.std() if returns.std() > 0 else 0.0

    return {
        "trend_strength": float(strength),
        "regime": regime,
        "confidence": float(confidence),
    }
