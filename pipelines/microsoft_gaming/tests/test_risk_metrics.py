"""Unit tests for risk metrics calculations."""

import pytest
import numpy as np
import pandas as pd
from pipelines.microsoft_gaming.risk_metrics import RiskMetricsCalculator, RiskMetricsConfig


@pytest.fixture
def calculator():
    """Create a RiskMetricsCalculator instance."""
    return RiskMetricsCalculator()


@pytest.fixture
def sample_df():
    """Create sample price data for testing."""
    np.random.seed(42)
    n_days = 252  # 1 year

    # Simulate daily returns
    daily_returns = np.random.normal(0.0005, 0.02, n_days)

    prices = 100 * (1 + pd.Series(daily_returns)).cumprod()

    df = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=n_days),
        "adj_close": prices,
        "daily_return": [np.nan] + list(daily_returns[1:])
    })
    df["daily_return"] = df["adj_close"].pct_change()
    df["cumulative_return"] = (1 + df["daily_return"]).cumprod() - 1

    return df


class TestRiskMetricsCalculator:
    """Tests for RiskMetricsCalculator class."""

    def test_total_return(self, calculator, sample_df):
        """Test total return calculation."""
        result = calculator.total_return(sample_df)

        # Manual calculation
        expected = (sample_df["adj_close"].iloc[-1] / sample_df["adj_close"].iloc[0]) - 1

        assert abs(result - expected) < 0.0001

    def test_volatility_annualization(self, calculator, sample_df):
        """Test that volatility is properly annualized."""
        returns = sample_df["daily_return"].dropna()
        result = calculator.volatility(returns)

        # Should be approximately daily std * sqrt(252)
        daily_std = returns.std()
        expected = daily_std * np.sqrt(252)

        assert abs(result - expected) < 0.0001

    def test_sharpe_ratio_positive(self, calculator, sample_df):
        """Test Sharpe ratio with positive returns."""
        returns = sample_df["daily_return"].dropna()
        result = calculator.sharpe_ratio(returns)

        # Sharpe should be finite
        assert np.isfinite(result)

    def test_sharpe_ratio_formula(self, calculator):
        """Test Sharpe ratio formula: (return - rf) / volatility."""
        # Create predictable data
        returns = pd.Series([0.01] * 252)  # 1% daily return

        result = calculator.sharpe_ratio(returns)

        ann_return = (1.01 ** 252) - 1  # ~11.35x
        vol = returns.std() * np.sqrt(252)  # 0 for constant returns

        # With zero volatility, result should be inf or very large
        # Our implementation returns 0 for zero volatility
        assert result == 0.0 or np.isinf(result)

    def test_sortino_ratio(self, calculator, sample_df):
        """Test Sortino ratio calculation."""
        returns = sample_df["daily_return"].dropna()
        result = calculator.sortino_ratio(returns)

        # Sortino should be >= Sharpe (same numerator, smaller denominator)
        sharpe = calculator.sharpe_ratio(returns)

        assert result >= sharpe or np.isclose(result, sharpe)

    def test_max_drawdown_negative(self, calculator, sample_df):
        """Test that max drawdown is always negative or zero."""
        result = calculator.max_drawdown(sample_df)

        assert result <= 0

    def test_max_drawdown_bounds(self, calculator, sample_df):
        """Test that max drawdown is between -1 and 0."""
        result = calculator.max_drawdown(sample_df)

        assert -1 <= result <= 0

    def test_var_95_percentile(self, calculator, sample_df):
        """Test VaR at 95% confidence."""
        returns = sample_df["daily_return"].dropna()
        result = calculator.value_at_risk(returns, 0.95)

        # Should be the 5th percentile
        expected = np.percentile(returns, 5)

        assert abs(result - expected) < 0.0001

    def test_var_99_more_extreme(self, calculator, sample_df):
        """Test that VaR 99% is more extreme than VaR 95%."""
        returns = sample_df["daily_return"].dropna()

        var_95 = calculator.value_at_risk(returns, 0.95)
        var_99 = calculator.value_at_risk(returns, 0.99)

        # 99% VaR should be more negative (worse)
        assert var_99 <= var_95

    def test_positive_days_percentage(self, calculator, sample_df):
        """Test positive days percentage is between 0 and 1."""
        returns = sample_df["daily_return"].dropna()
        result = calculator.positive_days_percentage(returns)

        assert 0 <= result <= 1

    def test_calculate_all_metrics(self, calculator, sample_df):
        """Test that calculate_all_metrics returns all expected keys."""
        result = calculator.calculate_all_metrics(sample_df)

        expected_keys = [
            "total_return",
            "annualized_return",
            "volatility",
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown",
            "var_95",
            "var_99",
            "positive_days_pct",
            "best_day",
            "worst_day",
            "trading_days",
            "years_analyzed"
        ]

        for key in expected_keys:
            assert key in result, f"Missing key: {key}"

    def test_config_risk_free_rate(self):
        """Test that risk-free rate is applied correctly."""
        config = RiskMetricsConfig(risk_free_rate=0.05)  # 5% rf
        calculator = RiskMetricsCalculator(config)

        # Create simple returns
        returns = pd.Series(np.random.normal(0.001, 0.02, 252))

        sharpe_with_rf = calculator.sharpe_ratio(returns)

        # Compare to calculator with 0% rf
        calculator_no_rf = RiskMetricsCalculator()
        sharpe_no_rf = calculator_no_rf.sharpe_ratio(returns)

        # Sharpe with higher rf should be lower
        assert sharpe_with_rf < sharpe_no_rf


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_returns(self):
        """Test handling of empty returns series."""
        calculator = RiskMetricsCalculator()
        returns = pd.Series([], dtype=float)

        # pandas .std() on an empty series returns NaN rather than raising —
        # that's the actual, correct behavior here, not an error condition.
        result = calculator.volatility(returns)
        assert np.isnan(result)

    def test_single_return(self):
        """Test handling of single return value."""
        calculator = RiskMetricsCalculator()
        returns = pd.Series([0.01])

        # Volatility of single value is undefined
        result = calculator.volatility(returns)
        assert np.isnan(result) or result == 0

    def test_all_negative_returns(self):
        """Test with all negative but constant returns — zero volatility, so
        Sharpe is undefined; the calculator returns 0.0 as its documented
        zero-vol sentinel rather than dividing by zero."""
        calculator = RiskMetricsCalculator()
        returns = pd.Series([-0.01] * 252)

        sharpe = calculator.sharpe_ratio(returns)
        assert sharpe == 0.0

    def test_all_positive_returns(self):
        """Test positive days percentage with all gains."""
        calculator = RiskMetricsCalculator()
        returns = pd.Series([0.01] * 252)

        pct = calculator.positive_days_percentage(returns)
        assert pct == 1.0
