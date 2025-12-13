import pandas as pd
from fincore.logging.logger import get_logger

log = get_logger("equal_weight")


class EqualWeightStrategy:
    def compute_weights(self, returns: pd.DataFrame) -> pd.Series:
        """
        returns: DataFrame (date x asset_id)
        Uses last available date only (cross-sectional)
        """

        if returns.empty:
            raise ValueError("Returns data is empty")

        assets = returns.columns
        n = len(assets)

        if n == 0:
            raise ValueError("No assets provided")

        weight = 1.0 / n
        weights = pd.Series(weight, index=assets)

        log.info(
            "equal_weight_computed",
            assets=n,
        )

        return weights
