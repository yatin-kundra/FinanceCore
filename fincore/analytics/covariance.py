import numpy as np
import pandas as pd
from sklearn.covariance import LedoitWolf, OAS


def estimate_covariance(
    returns: pd.DataFrame,
    method: str = "sample",
) -> np.ndarray:
    """
    returns: DataFrame (date x asset)
    method: 'sample', 'ledoit_wolf', 'oas'
    """

    if method == "sample":
        return returns.cov().values

    X = returns.values

    if method == "ledoit_wolf":
        return LedoitWolf().fit(X).covariance_

    if method == "oas":
        return OAS().fit(X).covariance_

    raise ValueError(f"Unknown covariance method: {method}")
