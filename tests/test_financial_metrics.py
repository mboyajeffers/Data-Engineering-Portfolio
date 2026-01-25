"""
Tests for Financial Metrics
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime


def make_stock_df(prices, ticker="TEST"):
    """Create test DataFrame from closing prices."""
    n = len(prices)
    dates = pd.date_range(end="2026-01-15", periods=n, freq="B")
    df = pd.DataFrame({
        "date": dates,
        "ticker": ticker,
        "open": prices,
        "high": [p * 1.02 for p in prices],
        "low": [p * 0.98 for p in prices],
        "close": prices,
        "volume": [1_000_000] * n,
    })
    df["daily_return"] = df["close"].pct_change()
    return df


class TestReturns:
    def test_positive_return(self):
        prices = [100.0] * 49 + [150.0]
        df = make_stock_df(prices)
        ret = (df["close"].iloc[-1] / df["close"].iloc[0] - 1) * 100
        assert abs(ret - 50.0) < 0.01

    def test_negative_return(self):
        prices = [100.0] * 49 + [80.0]
        df = make_stock_df(prices)
        ret = (df["close"].iloc[-1] / df["close"].iloc[0] - 1) * 100
        assert abs(ret - (-20.0)) < 0.01

    def test_flat_return(self):
        df = make_stock_df([100.0] * 50)
        ret = (df["close"].iloc[-1] / df["close"].iloc[0] - 1) * 100
        assert abs(ret) < 0.01


class TestVolatility:
    def test_flat_prices_zero_vol(self):
        df = make_stock_df([100.0] * 50)
        vol = df["daily_return"].dropna().std()
        assert vol < 0.001 or np.isnan(vol)

    def test_volatile_prices_high_vol(self):
        prices = [100.0]
        for i in range(49):
            prices.append(prices[-1] * (1.10 if i % 2 == 0 else 0.90))
        df = make_stock_df(prices)
        vol = df["daily_return"].dropna().std() * np.sqrt(252)
        assert vol > 1.0

    def test_vol_nonnegative(self):
        np.random.seed(42)
        prices = list(np.random.lognormal(0, 0.02, 50).cumprod() * 100)
        df = make_stock_df(prices)
        assert df["daily_return"].dropna().std() >= 0


class TestDrawdown:
    def test_drawdown_negative(self):
        prices = [100, 120, 140, 200, 180, 160, 150, 155, 160] + [160] * 41
        df = make_stock_df(prices)
        cum = (1 + df["daily_return"].dropna()).cumprod()
        max_dd = ((cum - cum.cummax()) / cum.cummax()).min()
        assert max_dd < 0

    def test_no_drawdown_monotonic(self):
        prices = [100 + i for i in range(50)]
        df = make_stock_df(prices)
        cum = (1 + df["daily_return"].dropna()).cumprod()
        max_dd = ((cum - cum.cummax()) / cum.cummax()).min()
        assert abs(max_dd) < 0.001


class TestSharpe:
    def test_positive_sharpe_good_returns(self):
        prices = [100 * (1.01 ** i) for i in range(50)]
        df = make_stock_df(prices)
        ret = df["daily_return"].dropna()
        rf = 0.05 / 252
        sharpe = ((ret.mean() - rf) / ret.std()) * np.sqrt(252)
        assert sharpe > 0

    def test_negative_sharpe_losses(self):
        prices = [100 * (0.99 ** i) for i in range(50)]
        df = make_stock_df(prices)
        ret = df["daily_return"].dropna()
        rf = 0.05 / 252
        sharpe = ((ret.mean() - rf) / ret.std()) * np.sqrt(252)
        assert sharpe < 0


class TestVaR:
    def test_var_within_bounds(self):
        np.random.seed(42)
        returns = np.random.normal(0.001, 0.02, 100)
        var_95 = np.percentile(returns, 5)
        assert -1.0 <= var_95 <= 0.0

    def test_higher_vol_more_negative_var(self):
        np.random.seed(42)
        low = np.random.normal(0, 0.01, 1000)
        high = np.random.normal(0, 0.05, 1000)
        assert np.percentile(high, 5) < np.percentile(low, 5)


class TestMultiTicker:
    def test_three_tickers(self):
        dfs = []
        for tk, base in [("AA", 100), ("BB", 200), ("CC", 50)]:
            np.random.seed(hash(tk) % 2**32)
            prices = [base * (1 + np.random.normal(0, 0.02)) for _ in range(50)]
            dfs.append(make_stock_df(prices, tk))
        combined = pd.concat(dfs, ignore_index=True)
        assert combined["ticker"].nunique() == 3

    def test_perfect_correlation(self):
        prices = [100 * (1.01 ** i) for i in range(50)]
        df1 = make_stock_df(prices, "X")
        df2 = make_stock_df(prices, "Y")
        combined = pd.concat([df1, df2])
        pivot = combined.pivot_table(index="date", columns="ticker", values="daily_return")
        corr = pivot.corr().iloc[0, 1]
        assert abs(corr - 1.0) < 0.01
