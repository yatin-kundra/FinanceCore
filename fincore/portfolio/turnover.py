import pandas as pd


def compute_turnover(
    prev_weights: pd.Series,
    new_weights: pd.Series,
) -> float:
    """
    Turnover = 0.5 * sum(|w_new - w_old|)
    """

    aligned_old = prev_weights.reindex(new_weights.index).fillna(0.0)
    aligned_new = new_weights.reindex(prev_weights.index).fillna(0.0)

    turnover = 0.5 * (aligned_new - aligned_old).abs().sum()
    return turnover
