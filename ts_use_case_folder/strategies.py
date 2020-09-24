"""
Copyright (c) 2020 by Impulse Innovations Ltd. Private and confidential. Part of the causaLens product.
"""
import numpy as np


class BackTestStrat:
    def __init__(self, decision_func, backtest_dataframe,args=None):
        self.decision_func = decision_func
        self.bt_df = backtest_dataframe
        self.args = args

    def run_backtest(self):
        self.decisions = self.bt_df.apply(lambda x: self.decision_func(x, self.args), 1)
        self.pnls = self.bt_df["__target_0"] * self.decisions
        self.avg_trade_pnl = np.sum(self.pnls) / np.sum(np.abs(self.decisions))
        self.weekly_returns = self.pnls.groupby(self.pnls.index).sum() / self.decisions.groupby(self.decisions.index).sum().apply(lambda x: max(1,abs(x)))
        self.avg_pnl = self.weekly_returns.mean()
        self.pnl_std = self.weekly_returns.std()
        self.sharpe_ratio = self.avg_pnl / self.pnl_std
        return self.sharpe_ratio

    def report(self):
        print('{0: <30}'.format("Portfolio Avg 6wk Return:") + str(self.avg_pnl))
        print('{0: <30}'.format("Trade Avg 6wk Return:") + str(self.avg_trade_pnl))
        print('{0: <30}'.format("Portfolio Sharpe Ratio:") + str(self.sharpe_ratio))
        print('{0: <30}'.format("# of Trades:") + str(np.sum(np.abs(self.decisions))))
