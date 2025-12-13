import pandas as pd
from fincore.logging.logger import get_logger

log = get_logger("portfolio_performance")


def compute_portfolio_returns(
    returns: pd.DataFrame,
    weights: pd.Series,
) -> pd.Series:
    """
    returns: DataFrame (date x asset_id)
    weights: Series (asset_id -> weight)
    """

    # Align columns
    returns = returns[weights.index]

    portfolio_returns = returns.dot(weights)

    log.info(
        "portfolio_returns_computed",
        periods=len(portfolio_returns),
    )

    return portfolio_returns
