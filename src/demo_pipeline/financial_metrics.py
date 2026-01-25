"""
Financial Metrics
=================

Portfolio and per-ticker KPI computation for stock data.
Implements Sharpe, Sortino, VaR, drawdown, and correlation metrics.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

TRADING_DAYS = 252
RISK_FREE_RATE = 0.05


@dataclass
class FinancialKPI:
    name: str
    value: Any
    unit: str
    description: str
    ticker: str = "portfolio"


@dataclass
class FinancialMetrics:
    kpis: list[FinancialKPI] = field(default_factory=list)

    def add(self, name, value, unit, desc, ticker="portfolio"):
        self.kpis.append(FinancialKPI(name, value, unit, desc, ticker))


def compute_financial_kpis(df: pd.DataFrame) -> FinancialMetrics:
    """Compute all financial KPIs from validated stock data."""
    metrics = FinancialMetrics()
    _portfolio_metrics(df, metrics)
    for ticker in sorted(df['ticker'].unique()):
        _ticker_metrics(df[df['ticker'] == ticker], ticker, metrics)
    logger.info(f"Computed {len(metrics.kpis)} financial KPIs")
    return metrics


def _portfolio_metrics(df: pd.DataFrame, m: FinancialMetrics):
    """Equal-weight portfolio metrics."""
    pivot = df.pivot_table(index='date', columns='ticker', values='daily_return')
    port_ret = pivot.mean(axis=1).dropna()
    n = len(port_ret)

    # Cumulative return
    cum = (1 + port_ret).cumprod()
    total = (cum.iloc[-1] - 1) * 100
    m.add("portfolio_total_return", round(total, 2), "%", "Cumulative equal-weight return")

    # Annualized
    ann = ((1 + total/100) ** (TRADING_DAYS/n) - 1) * 100
    m.add("portfolio_annualized_return", round(ann, 2), "%", "Annualized return")

    # Volatility
    vol = port_ret.std() * np.sqrt(TRADING_DAYS) * 100
    m.add("portfolio_volatility", round(vol, 2), "%", "Annualized volatility")

    # Sharpe
    rf_daily = RISK_FREE_RATE / TRADING_DAYS
    excess = port_ret - rf_daily
    sharpe = (excess.mean() / excess.std()) * np.sqrt(TRADING_DAYS)
    m.add("portfolio_sharpe_ratio", round(sharpe, 3), "ratio", "Sharpe ratio (rf=5%)")

    # Sortino
    downside = port_ret[port_ret < rf_daily]
    down_std = downside.std() * np.sqrt(TRADING_DAYS)
    sortino = (port_ret.mean() - rf_daily) * TRADING_DAYS / down_std if down_std > 0 else 0
    m.add("portfolio_sortino_ratio", round(sortino, 3), "ratio", "Sortino ratio")

    # Max drawdown
    dd = (cum - cum.cummax()) / cum.cummax()
    m.add("portfolio_max_drawdown", round(dd.min() * 100, 2), "%", "Max drawdown")

    # VaR 95%
    var = np.percentile(port_ret, 5) * 100
    m.add("portfolio_var_95", round(var, 3), "%", "Daily VaR (95%)")

    # Avg correlation
    corr = pivot.corr()
    mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
    avg_corr = corr.where(mask).stack().mean()
    m.add("avg_pairwise_correlation", round(avg_corr, 3), "ratio", "Avg pairwise correlation")

    m.add("trading_days_analyzed", n, "days", "Trading days in window")


def _ticker_metrics(df: pd.DataFrame, ticker: str, m: FinancialMetrics):
    """Per-ticker metrics."""
    ret = df['daily_return'].dropna()
    close = df['close']

    total = ((close.iloc[-1] / close.iloc[0]) - 1) * 100
    m.add("total_return", round(total, 2), "%", "Period return", ticker)

    vol = ret.std() * np.sqrt(TRADING_DAYS) * 100
    m.add("annualized_volatility", round(vol, 2), "%", "Annualized vol", ticker)

    rf_daily = RISK_FREE_RATE / TRADING_DAYS
    sharpe = ((ret.mean() - rf_daily) / ret.std()) * np.sqrt(TRADING_DAYS)
    m.add("sharpe_ratio", round(sharpe, 3), "ratio", "Sharpe ratio", ticker)

    cum = (1 + ret).cumprod()
    max_dd = ((cum - cum.cummax()) / cum.cummax()).min() * 100
    m.add("max_drawdown", round(max_dd, 2), "%", "Max drawdown", ticker)

    m.add("avg_daily_volume", int(df['volume'].mean()), "shares", "Avg volume", ticker)
    m.add("price_high", round(close.max(), 2), "USD", "Period high", ticker)
    m.add("price_low", round(close.min(), 2), "USD", "Period low", ticker)
