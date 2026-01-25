"""
Metrics Module Tests
====================

Tests for KPI computation functions.
Synthetic demo output for portfolio purposes.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from demo_pipeline.metrics import (
    compute_total_revenue,
    compute_total_orders,
    compute_average_order_value,
    compute_unique_customers,
    compute_revenue_by_category,
    compute_repeat_rate,
    compute_discount_utilization,
    compute_all_kpis,
)


@pytest.fixture
def sample_dataframe():
    """Create a sample transaction dataframe for testing."""
    return pd.DataFrame({
        'order_id': ['A001', 'A002', 'A003', 'A004', 'A005'],
        'customer_id': ['C001', 'C002', 'C001', 'C003', 'C002'],  # C001 and C002 are repeat
        'order_total': [100.0, 250.0, 150.0, 300.0, 200.0],
        'order_status': ['Completed', 'Completed', 'Completed', 'Completed', 'Refunded'],
        'product_category': ['Rings', 'Necklaces', 'Rings', 'Bracelets', 'Earrings'],
        'discount_amount': [0, 25.0, 10.0, 0, 0],
    })


class TestTotalRevenue:
    """Tests for total revenue KPI."""

    def test_calculates_sum_of_completed_orders(self, sample_dataframe):
        """Should sum only completed order totals."""
        result = compute_total_revenue(sample_dataframe)

        # Should exclude the refunded order (A005: $200)
        expected = 100.0 + 250.0 + 150.0 + 300.0
        assert result.value == expected
        assert result.unit == 'USD'
        assert result.kpi_id == 'KPI-001'

    def test_handles_empty_dataframe(self):
        """Should return 0 for empty dataframe."""
        df = pd.DataFrame(columns=['order_total', 'order_status'])
        result = compute_total_revenue(df)

        assert result.value == 0


class TestTotalOrders:
    """Tests for total orders KPI."""

    def test_counts_distinct_completed_orders(self, sample_dataframe):
        """Should count only completed orders."""
        result = compute_total_orders(sample_dataframe)

        # 5 orders, but one is refunded
        assert result.value == 4
        assert result.kpi_id == 'KPI-002'


class TestAverageOrderValue:
    """Tests for AOV KPI."""

    def test_calculates_mean_order_value(self, sample_dataframe):
        """Should calculate mean of completed order totals."""
        result = compute_average_order_value(sample_dataframe)

        # Completed orders: 100, 250, 150, 300 = 800 / 4 = 200
        assert result.value == 200.0
        assert result.kpi_id == 'KPI-003'


class TestUniqueCustomers:
    """Tests for unique customers KPI."""

    def test_counts_distinct_customers(self, sample_dataframe):
        """Should count distinct customer IDs."""
        result = compute_unique_customers(sample_dataframe)

        # C001, C002, C003 (but C002's refund might still count)
        # For completed: C001, C002, C003 = 3
        assert result.value == 3
        assert result.kpi_id == 'KPI-006'


class TestRevenueByCategory:
    """Tests for revenue by category KPI."""

    def test_groups_by_category(self, sample_dataframe):
        """Should break down revenue by product category."""
        result = compute_revenue_by_category(sample_dataframe)

        assert result.kpi_id == 'KPI-004'
        assert isinstance(result.value, dict)

        # Completed orders by category:
        # Rings: 100 + 150 = 250
        # Necklaces: 250
        # Bracelets: 300
        # (Earrings refunded, excluded)
        assert result.value.get('Rings') == 250.0
        assert result.value.get('Necklaces') == 250.0
        assert result.value.get('Bracelets') == 300.0


class TestRepeatRate:
    """Tests for repeat purchase rate KPI."""

    def test_calculates_repeat_percentage(self, sample_dataframe):
        """Should calculate percentage of repeat customers."""
        result = compute_repeat_rate(sample_dataframe)

        # C001: 2 orders, C002: 2 orders, C003: 1 order
        # 2 repeat customers out of 3 = 66.67%
        assert result.kpi_id == 'KPI-008'
        assert 66 <= result.value <= 67  # Allow for rounding


class TestDiscountUtilization:
    """Tests for discount utilization KPI."""

    def test_calculates_discount_percentage(self, sample_dataframe):
        """Should calculate percentage of orders with discounts."""
        result = compute_discount_utilization(sample_dataframe)

        # 2 orders with discount (A002, A003) out of 5 = 40%
        assert result.kpi_id == 'KPI-015'
        assert result.value == 40.0


class TestComputeAllKpis:
    """Integration tests for full KPI computation."""

    def test_computes_all_kpis(self, sample_dataframe):
        """Should compute all defined KPIs."""
        report = compute_all_kpis(sample_dataframe)

        assert len(report.kpis) > 0
        assert report.metadata['total_records'] == 5

        # Check we can retrieve specific KPIs
        revenue = report.get_kpi('KPI-001')
        assert revenue is not None
        assert revenue.value > 0

    def test_handles_missing_columns_gracefully(self):
        """Should handle dataframes with missing optional columns."""
        df = pd.DataFrame({
            'order_id': ['A001'],
            'order_total': [100.0],
        })

        report = compute_all_kpis(df)

        # Should still compute what it can
        assert len(report.kpis) > 0
