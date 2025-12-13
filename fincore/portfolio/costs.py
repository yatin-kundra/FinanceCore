def transaction_cost(
    turnover: float,
    cost_rate: float,
) -> float:
    """
    turnover: fraction of portfolio traded (0-1)
    cost_rate: cost per unit turnover (e.g. 0.001 = 10 bps)
    """
    return turnover * cost_rate
