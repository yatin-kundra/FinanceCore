import numpy as np
import pandas as pd


def mean_shift_test(
    returns: pd.Series,
    window: int = 252,
    threshold: float = 1.0,
) -> dict:
    """
    Detects structural break via mean shift.
    """

    if len(returns) < 2 * window:
        raise ValueError("Insufficient data for break detection")

    recent = returns.iloc[-window:]
    past = returns.iloc[-2 * window:-window]

    delta_mean = recent.mean() - past.mean()
    pooled_std = returns.std()

    z_score = delta_mean / pooled_std if pooled_std > 0 else 0.0

    regime = "break" if abs(z_score) > threshold else "stable"

    return {
        "z_score": float(z_score),
        "delta_mean": float(delta_mean),
        "regime": regime,
    }
