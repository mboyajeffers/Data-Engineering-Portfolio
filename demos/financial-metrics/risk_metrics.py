#!/usr/bin/env python3
"""
Financial Risk Metrics Calculator
=================================
Production-grade implementations of standard risk metrics used in
portfolio management and quantitative finance.

Metrics included:
- Value at Risk (VaR) - Parametric and Historical
- Sharpe Ratio
- Sortino Ratio
- Maximum Drawdown
- Beta (vs benchmark)

Author: Mboya Jeffers
"""

import numpy as np
import pandas as pd
from typing import Optional, Tuple
from scipy import stats


def calculate_returns(prices: pd.Series, method: str = 'log') -> pd.Series:
    """
    Calculate returns from price series.

    Args:
        prices: Series of asset prices
        method: 'log' for log returns, 'simple' for arithmetic returns

    Returns:
        Series of returns
    """
    if method == 'log':
        return np.log(prices / prices.shift(1)).dropna()
    else:
        return prices.pct_change().dropna()


def parametric_var(returns: pd.Series, confidence: float = 0.95,
                   horizon: int = 1) -> float:
    """
    Calculate Value at Risk using the parametric (variance-covariance) method.

    Assumes returns are normally distributed.

    Args:
        returns: Series of historical returns
        confidence: Confidence level (e.g., 0.95 for 95% VaR)
        horizon: Time horizon in periods

    Returns:
        VaR as a positive percentage loss

    Example:
        >>> returns = pd.Series([0.01, -0.02, 0.015, -0.005, 0.008])
        >>> var = parametric_var(returns, confidence=0.95)
        >>> print(f"95% VaR: {var:.4f}")
    """
    mu = returns.mean()
    sigma = returns.std()

    # Z-score for confidence level
    z = stats.norm.ppf(1 - confidence)

    # Scale for time horizon (square root of time rule)
    var = -(mu * horizon + z * sigma * np.sqrt(horizon))

    return float(var)


def historical_var(returns: pd.Series, confidence: float = 0.95) -> float:
    """
    Calculate Value at Risk using the historical simulation method.

    No distributional assumptions - uses actual historical percentiles.

    Args:
        returns: Series of historical returns
        confidence: Confidence level (e.g., 0.95 for 95% VaR)

    Returns:
        VaR as a positive percentage loss
    """
    percentile = (1 - confidence) * 100
    var = -np.percentile(returns, percentile)

    return float(var)


def sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0,
                 periods_per_year: int = 252) -> float:
    """
    Calculate the Sharpe Ratio (risk-adjusted return).

    Sharpe = (Rp - Rf) / σp

    Args:
        returns: Series of portfolio returns
        risk_free_rate: Annual risk-free rate (default 0)
        periods_per_year: Trading periods per year (252 for daily)

    Returns:
        Annualized Sharpe Ratio
    """
    # Convert annual risk-free rate to per-period
    rf_per_period = risk_free_rate / periods_per_year

    excess_returns = returns - rf_per_period

    # Tolerance, not exact equality: pandas .std() leaves floating-point
    # noise (~1e-18) even on identical values, so a true zero-volatility
    # series was slipping past `== 0` and blowing up the ratio on division.
    if excess_returns.std() < 1e-9:
        return 0.0

    # Annualize
    sharpe = (excess_returns.mean() / excess_returns.std()) * np.sqrt(periods_per_year)

    return float(sharpe)


def sortino_ratio(returns: pd.Series, risk_free_rate: float = 0.0,
                  target_return: float = 0.0,
                  periods_per_year: int = 252) -> float:
    """
    Calculate the Sortino Ratio (downside risk-adjusted return).

    Like Sharpe, but only penalizes downside volatility.

    Sortino = (Rp - T) / σd

    Args:
        returns: Series of portfolio returns
        risk_free_rate: Annual risk-free rate
        target_return: Minimum acceptable return (default 0)
        periods_per_year: Trading periods per year

    Returns:
        Annualized Sortino Ratio
    """
    rf_per_period = risk_free_rate / periods_per_year
    target_per_period = target_return / periods_per_year

    excess_returns = returns - rf_per_period

    # Downside returns only
    downside_returns = returns[returns < target_per_period]

    if len(downside_returns) == 0 or downside_returns.std() == 0:
        return float('inf') if excess_returns.mean() > 0 else 0.0

    downside_std = downside_returns.std()

    sortino = (excess_returns.mean() / downside_std) * np.sqrt(periods_per_year)

    return float(sortino)


def maximum_drawdown(prices: pd.Series) -> Tuple[float, pd.Timestamp, pd.Timestamp]:
    """
    Calculate the Maximum Drawdown and its timing.

    Maximum Drawdown = largest peak-to-trough decline.

    Args:
        prices: Series of asset prices (not returns)

    Returns:
        Tuple of (max_drawdown, peak_date, trough_date)
    """
    # Running maximum
    rolling_max = prices.expanding().max()

    # Drawdown series
    drawdown = (prices - rolling_max) / rolling_max

    # Maximum drawdown
    max_dd = drawdown.min()

    # Find trough date
    trough_idx = drawdown.idxmin()

    # Find peak date (last peak before trough)
    peak_idx = prices[:trough_idx].idxmax()

    return float(abs(max_dd)), peak_idx, trough_idx


def beta(asset_returns: pd.Series, benchmark_returns: pd.Series) -> float:
    """
    Calculate Beta (systematic risk) vs a benchmark.

    Beta = Cov(Ra, Rb) / Var(Rb)

    Args:
        asset_returns: Series of asset returns
        benchmark_returns: Series of benchmark returns (e.g., S&P 500)

    Returns:
        Beta coefficient
    """
    # Align the series
    aligned = pd.concat([asset_returns, benchmark_returns], axis=1).dropna()

    if len(aligned) < 2:
        return 1.0

    asset = aligned.iloc[:, 0]
    benchmark = aligned.iloc[:, 1]

    covariance = asset.cov(benchmark)
    variance = benchmark.var()

    if variance == 0:
        return 1.0

    return float(covariance / variance)


def calculate_all_metrics(prices: pd.Series,
                          benchmark_prices: Optional[pd.Series] = None,
                          risk_free_rate: float = 0.02) -> dict:
    """
    Calculate all risk metrics for a price series.

    Args:
        prices: Series of asset prices
        benchmark_prices: Optional benchmark prices for beta calculation
        risk_free_rate: Annual risk-free rate

    Returns:
        Dictionary of all calculated metrics
    """
    returns = calculate_returns(prices, method='simple')

    metrics = {
        'total_return': float((prices.iloc[-1] / prices.iloc[0]) - 1),
        'annualized_return': float(returns.mean() * 252),
        'annualized_volatility': float(returns.std() * np.sqrt(252)),
        'var_95_parametric': parametric_var(returns, 0.95),
        'var_99_parametric': parametric_var(returns, 0.99),
        'var_95_historical': historical_var(returns, 0.95),
        'sharpe_ratio': sharpe_ratio(returns, risk_free_rate),
        'sortino_ratio': sortino_ratio(returns, risk_free_rate),
    }

    max_dd, peak, trough = maximum_drawdown(prices)
    metrics['max_drawdown'] = max_dd
    metrics['max_drawdown_peak'] = str(peak)
    metrics['max_drawdown_trough'] = str(trough)

    if benchmark_prices is not None:
        benchmark_returns = calculate_returns(benchmark_prices, method='simple')
        metrics['beta'] = beta(returns, benchmark_returns)

    return metrics


# Demo with sample data
if __name__ == '__main__':
    # Generate sample price data
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=252, freq='B')

    # Simulate stock prices with drift and volatility
    daily_returns = np.random.normal(0.0005, 0.02, 252)
    prices = 100 * np.cumprod(1 + daily_returns)
    price_series = pd.Series(prices, index=dates)

    # Simulate benchmark (market)
    market_returns = np.random.normal(0.0004, 0.015, 252)
    market_prices = 100 * np.cumprod(1 + market_returns)
    market_series = pd.Series(market_prices, index=dates)

    print("=" * 50)
    print("Financial Risk Metrics - Sample Calculation")
    print("=" * 50)

    metrics = calculate_all_metrics(price_series, market_series, risk_free_rate=0.02)

    print(f"\nTotal Return:          {metrics['total_return']:.2%}")
    print(f"Annualized Return:     {metrics['annualized_return']:.2%}")
    print(f"Annualized Volatility: {metrics['annualized_volatility']:.2%}")
    print(f"\n95% VaR (Parametric):  {metrics['var_95_parametric']:.2%}")
    print(f"99% VaR (Parametric):  {metrics['var_99_parametric']:.2%}")
    print(f"95% VaR (Historical):  {metrics['var_95_historical']:.2%}")
    print(f"\nSharpe Ratio:          {metrics['sharpe_ratio']:.2f}")
    print(f"Sortino Ratio:         {metrics['sortino_ratio']:.2f}")
    print(f"Beta:                  {metrics['beta']:.2f}")
    print(f"\nMax Drawdown:          {metrics['max_drawdown']:.2%}")
    print(f"  Peak:                {metrics['max_drawdown_peak']}")
    print(f"  Trough:              {metrics['max_drawdown_trough']}")
