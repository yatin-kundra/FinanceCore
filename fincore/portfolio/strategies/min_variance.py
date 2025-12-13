import numpy as np
import pandas as pd
from fincore.logging.logger import get_logger
from fincore.analytics.covariance import estimate_covariance

log = get_logger("min_variance")


class MinimumVarianceStrategy:
    def __init__(
        self,
        cov_method: str = "sample",
        epsilon: float = 1e-6,
    ):
        """
        cov_method: 'sample', 'ledoit_wolf', 'oas'
        epsilon: numerical stability regularization
        """
        self.cov_method = cov_method
        self.epsilon = epsilon

    def compute_weights(self, returns: pd.DataFrame) -> pd.Series:
        if returns.empty:
            raise ValueError("Returns data is empty")

        cov = estimate_covariance(returns, self.cov_method)
        n = cov.shape[0]

        # Numerical stability
        cov = cov + np.eye(n) * self.epsilon

        inv_cov = np.linalg.inv(cov)
        ones = np.ones(n)

        weights = inv_cov @ ones
        weights /= ones @ inv_cov @ ones

        log.info(
            "min_variance_computed",
            assets=n,
            cov_method=self.cov_method,
        )

        return pd.Series(weights, index=returns.columns)
