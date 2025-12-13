import numpy as np
import pandas as pd


def perturb_returns(
    returns: pd.DataFrame,
    noise_std: float = 0.001,
) -> pd.DataFrame:
    """
    Adds small Gaussian noise to returns.
    Used to test sensitivity.
    """
    noise = np.random.normal(
        loc=0.0,
        scale=noise_std,
        size=returns.shape,
    )
    return returns + noise


def weight_stability(
    w1: pd.Series,
    w2: pd.Series,
) -> float:
    """
    Measures how much weights change.
    Lower = more stable.
    """
    return (w1 - w2).abs().sum()
