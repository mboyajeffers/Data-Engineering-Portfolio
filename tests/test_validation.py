"""
Validation Module Tests
=======================

Tests for data validation functions.
Synthetic demo output for portfolio purposes.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from demo_pipeline.validate import (
    check_required_columns,
    check_null_values,
    check_duplicates,
    check_value_range,
    check_allowed_values,
    validate_dataframe,
)


class TestRequiredColumns:
    """Tests for required column validation."""

    def test_all_columns_present(self):
        """Should pass when all required columns exist."""
        df = pd.DataFrame({
            'order_id': ['A001', 'A002'],
            'customer_id': ['C001', 'C002'],
            'order_total': [100.0, 200.0],
        })
        result = check_required_columns(df, ['order_id', 'customer_id', 'order_total'])

        assert result.passed is True
        assert result.severity == 'BLOCKER'

    def test_missing_columns(self):
        """Should fail when required columns are missing."""
        df = pd.DataFrame({
            'order_id': ['A001', 'A002'],
        })
        result = check_required_columns(df, ['order_id', 'customer_id'])

        assert result.passed is False
        assert 'customer_id' in result.affected_columns


class TestNullChecks:
    """Tests for null value validation."""

    def test_no_nulls(self):
        """Should pass when no nulls present."""
        df = pd.DataFrame({
            'order_id': ['A001', 'A002', 'A003'],
        })
        result = check_null_values(df, 'order_id', max_null_pct=0)

        assert result.passed is True

    def test_nulls_exceed_threshold(self):
        """Should fail when null percentage exceeds threshold."""
        df = pd.DataFrame({
            'order_id': ['A001', None, 'A003', None],
        })
        result = check_null_values(df, 'order_id', max_null_pct=10)

        assert result.passed is False
        assert result.affected_rows == 2

    def test_nulls_within_threshold(self):
        """Should pass when null percentage within threshold."""
        df = pd.DataFrame({
            'marketing_channel': ['Email', None, 'Social', 'Search'],
        })
        result = check_null_values(df, 'marketing_channel', max_null_pct=30)

        assert result.passed is True


class TestDuplicateChecks:
    """Tests for duplicate detection."""

    def test_no_duplicates(self):
        """Should pass when no duplicates exist."""
        df = pd.DataFrame({
            'order_id': ['A001', 'A002', 'A003'],
        })
        result = check_duplicates(df, ['order_id'])

        assert result.passed is True
        assert result.affected_rows == 0

    def test_has_duplicates(self):
        """Should fail when duplicates exist."""
        df = pd.DataFrame({
            'order_id': ['A001', 'A002', 'A001', 'A003'],
        })
        result = check_duplicates(df, ['order_id'])

        assert result.passed is False
        assert result.affected_rows == 1  # One duplicate (keeps first)


class TestRangeChecks:
    """Tests for numeric range validation."""

    def test_values_in_range(self):
        """Should pass when all values within range."""
        df = pd.DataFrame({
            'order_total': [50.0, 100.0, 150.0],
        })
        result = check_value_range(df, 'order_total', min_value=0, max_value=500)

        assert result.passed is True

    def test_values_below_minimum(self):
        """Should fail when values below minimum."""
        df = pd.DataFrame({
            'order_total': [50.0, -10.0, 100.0],
        })
        result = check_value_range(df, 'order_total', min_value=0)

        assert result.passed is False
        assert result.affected_rows == 1

    def test_values_above_maximum(self):
        """Should fail when values above maximum."""
        df = pd.DataFrame({
            'discount_percent': [10, 25, 150],  # 150% is invalid
        })
        result = check_value_range(df, 'discount_percent', min_value=0, max_value=100)

        assert result.passed is False


class TestAllowedValues:
    """Tests for categorical value validation."""

    def test_all_values_allowed(self):
        """Should pass when all values in allowed set."""
        df = pd.DataFrame({
            'customer_segment': ['VIP', 'Premium', 'Standard'],
        })
        result = check_allowed_values(
            df, 'customer_segment',
            allowed=['VIP', 'Premium', 'Standard', 'New']
        )

        assert result.passed is True

    def test_invalid_values(self):
        """Should fail when values not in allowed set."""
        df = pd.DataFrame({
            'customer_segment': ['VIP', 'Unknown', 'Premium'],
        })
        result = check_allowed_values(
            df, 'customer_segment',
            allowed=['VIP', 'Premium', 'Standard', 'New']
        )

        assert result.passed is False
        assert result.affected_rows == 1


class TestValidateDataframe:
    """Integration tests for full dataframe validation."""

    def test_valid_dataframe(self):
        """Should pass with valid data."""
        df = pd.DataFrame({
            'order_id': ['A001', 'A002'],
            'customer_id': ['C001', 'C002'],
            'order_total': [100.0, 200.0],
            'order_status': ['Completed', 'Completed'],
        })
        schema = {
            'required_columns': ['order_id', 'customer_id', 'order_total'],
            'primary_key': ['order_id'],
        }
        rules = {
            'null_checks': {
                'order_id': {'max_null_pct': 0, 'severity': 'BLOCKER'},
            },
            'range_checks': {
                'order_total': {'min': 0, 'severity': 'BLOCKER'},
            },
        }

        valid_df, quarantine_df, report = validate_dataframe(df, schema, rules)

        assert report.passed is True
        assert len(valid_df) == 2
        assert len(quarantine_df) == 0

    def test_invalid_dataframe(self):
        """Should quarantine invalid records."""
        df = pd.DataFrame({
            'order_id': ['A001', 'A001', 'A003'],  # Duplicate
            'customer_id': ['C001', 'C002', 'C003'],
            'order_total': [100.0, -50.0, 200.0],  # Negative value
        })
        schema = {
            'required_columns': ['order_id'],
            'primary_key': ['order_id'],
        }
        rules = {
            'range_checks': {
                'order_total': {'min': 0, 'severity': 'BLOCKER'},
            },
        }

        valid_df, quarantine_df, report = validate_dataframe(df, schema, rules)

        # Should quarantine duplicate and negative total
        assert len(quarantine_df) > 0
        assert report.quarantined_rows > 0
