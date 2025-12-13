import pandas as pd
from fincore.logging.logger import get_logger

log = get_logger("portfolio")


class Portfolio:
    """
    Portfolio orchestrates:
    - data selection
    - strategy execution
    - (future) rebalancing
    """

    def __init__(
        self,
        returns: pd.DataFrame,
        strategy,
    ):
        """
        returns: DataFrame (date x asset_id)
        strategy: object with .compute_weights(returns)
        """
        self.returns = returns
        self.strategy = strategy

        self.weights = None

    def construct(self):
        """
        Cross-sectional construction (Option A).
        """
        log.info("portfolio_construction_start")

        self.weights = self.strategy.compute_weights(self.returns)

        self._validate_weights()

        log.info(
            "portfolio_construction_complete",
            assets=len(self.weights),
        )

        return self.weights

    def _validate_weights(self):
        if self.weights is None:
            raise ValueError("Weights not computed")

        if not isinstance(self.weights, pd.Series):
            raise TypeError("Weights must be a pandas Series")

        if abs(self.weights.sum() - 1.0) > 1e-6:
            raise ValueError("Weights must sum to 1")

        if (self.weights < 0).any():
            raise ValueError("Negative weights not allowed (yet)")
