import numpy as np
import pandas as pd
from fincore.logging.logger import get_logger

log = get_logger("risk_parity")


class RiskParityStrategy:
    def __init__(self, periods_per_year: int = 252):
        self.periods_per_year = periods_per_year

    def compute_weights(self, returns: pd.DataFrame) -> pd.Series:
        """
        returns: DataFrame (date x asset_id)
        """

        if returns.empty:
            raise ValueError("Returns data is empty")

        vol = returns.std() * np.sqrt(self.periods_per_year)

        if (vol <= 0).any():
            raise ValueError("Non-positive volatility encountered")

        inv_vol = 1.0 / vol
        weights = inv_vol / inv_vol.sum()

        log.info(
            "risk_parity_computed",
            assets=len(weights),
        )

        return weights
