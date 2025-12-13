import pandas as pd
from pandas.tseries.frequencies import to_offset
from fincore.logging.logger import get_logger

log = get_logger("backtest")


class Backtest:
    def __init__(
        self,
        returns: pd.DataFrame,
        strategy,
        rebalance_freq: str,
        lookback: str,
    ):
        """
        returns: DataFrame (date x asset_id)
        rebalance_freq: 'M' or 'Q'
        lookback: calendar window like '3Y', '5Y', '18M'
        """
        self.returns = returns.sort_index()
        self.strategy = strategy
        self.rebalance_freq = rebalance_freq
        self.lookback = lookback

        self.weights_history = {}
        self.portfolio_returns = None

    def run(self):
        dates = self.returns.index
        portfolio_returns = []

        # Determine rebalance dates (month-end / quarter-end)
        rebalance_dates = (
            dates.to_series()
            .resample(self.rebalance_freq)
            .last()
            .dropna()
        )

        for i, rebalance_date in enumerate(rebalance_dates[:-1]):
            lookback_start = rebalance_date - to_offset(self.lookback)

            window_returns = self.returns.loc[
                lookback_start : rebalance_date
            ]

            if len(window_returns) < 60:
                # insufficient data
                continue

            weights = self.strategy.compute_weights(window_returns)
            self.weights_history[rebalance_date] = weights

            # Apply weights until next rebalance
            next_rebalance = rebalance_dates.iloc[i + 1]
            holding_period = self.returns.loc[
                rebalance_date : next_rebalance
            ]

            for dt, row in holding_period.iterrows():
                r = (row * weights).sum()
                portfolio_returns.append((dt, r))

        self.portfolio_returns = (
            pd.Series(dict(portfolio_returns))
            .sort_index()
        )

        log.info(
            "backtest_complete",
            rebalances=len(self.weights_history),
            periods=len(self.portfolio_returns),
            rebalance_freq=self.rebalance_freq,
            lookback=self.lookback,
        )

        return self.portfolio_returns
