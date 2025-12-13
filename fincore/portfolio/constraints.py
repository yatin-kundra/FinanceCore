import pandas as pd


def apply_constraints(
    weights: pd.Series,
    max_weight: float | None = None,
    min_weight: float | None = None,
    long_only: bool = True,
) -> pd.Series:
    """
    Applies portfolio constraints in a transparent way.

    Order:
    1. Enforce long-only
    2. Apply min / max caps
    3. Renormalize
    """

    w = weights.copy()

    # 1️⃣ Long-only
    if long_only:
        w[w < 0] = 0.0

    # 2️⃣ Min weight
    if min_weight is not None:
        w[w < min_weight] = min_weight

    # 3️⃣ Max weight
    if max_weight is not None:
        w[w > max_weight] = max_weight

    # Sanity check
    if w.sum() == 0:
        raise ValueError("All weights zero after constraints")

    # 4️⃣ Renormalize
    w = w / w.sum()

    return w
