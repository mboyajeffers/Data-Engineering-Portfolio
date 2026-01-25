"""
Data Transformation Module
==========================

Normalizes and enriches validated data for metrics computation.
Handles column aliasing, date standardization, and derived fields.

Synthetic demo output for portfolio purposes.
"""

import pandas as pd
from typing import Optional
import logging

logger = logging.getLogger(__name__)


# Column alias mappings for common variations
COLUMN_ALIASES = {
    'order_id': ['orderid', 'order_number', 'transaction_id', 'txn_id', 'invoice_id'],
    'order_date': ['transaction_date', 'purchase_date', 'date', 'sale_date'],
    'customer_id': ['cust_id', 'client_id', 'buyer_id', 'user_id'],
    'order_total': ['total', 'grand_total', 'amount', 'revenue', 'sale_amount'],
    'product_category': ['category', 'product_type', 'item_category'],
    'customer_segment': ['segment', 'tier', 'customer_tier', 'membership'],
    'unit_price': ['price', 'item_price', 'sale_price'],
    'quantity': ['qty', 'units', 'count', 'units_sold'],
    'discount_amount': ['discount', 'discount_value', 'promo_discount'],
    'discount_percent': ['discount_pct', 'discount_rate'],
    'marketing_channel': ['channel', 'acquisition_channel', 'source'],
    'sales_channel': ['channel_type', 'order_channel'],
}


def apply_column_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to canonical names using alias mappings.

    Only renames if the canonical name doesn't already exist.
    """
    df = df.copy()

    for canonical, aliases in COLUMN_ALIASES.items():
        if canonical not in df.columns:
            for alias in aliases:
                if alias in df.columns:
                    df = df.rename(columns={alias: canonical})
                    logger.debug(f"Aliased '{alias}' -> '{canonical}'")
                    break

    return df


def standardize_dates(
    df: pd.DataFrame,
    date_columns: Optional[list[str]] = None,
    target_tz: str = 'UTC'
) -> pd.DataFrame:
    """
    Convert date columns to standardized datetime format.

    - Parses various date formats
    - Converts to target timezone
    - Adds derived date fields (year, month, quarter, day_of_week)
    """
    df = df.copy()

    if date_columns is None:
        # Auto-detect date columns
        date_columns = [col for col in df.columns if 'date' in col.lower()]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

            # Add derived fields from primary date column
            if col == 'order_date':
                df['order_year'] = df[col].dt.year
                df['order_month'] = df[col].dt.month
                df['order_quarter'] = df[col].dt.quarter
                df['order_day_of_week'] = df[col].dt.day_name()
                df['order_week'] = df[col].dt.isocalendar().week

    return df


def standardize_categoricals(
    df: pd.DataFrame,
    mappings: Optional[dict] = None
) -> pd.DataFrame:
    """
    Standardize categorical values to canonical forms.

    Example mappings:
        {'customer_segment': {'vip': 'VIP', 'PREMIUM': 'Premium'}}
    """
    df = df.copy()

    # Default standardizations
    default_mappings = {
        'customer_segment': {
            'vip': 'VIP',
            'premium': 'Premium',
            'standard': 'Standard',
            'new': 'New',
        },
        'order_status': {
            'complete': 'Completed',
            'completed': 'Completed',
            'pending': 'Pending',
            'cancelled': 'Cancelled',
            'canceled': 'Cancelled',
            'refunded': 'Refunded',
            'returned': 'Refunded',
        },
    }

    if mappings:
        default_mappings.update(mappings)

    for col, value_map in default_mappings.items():
        if col in df.columns:
            # Case-insensitive mapping
            lower_map = {k.lower(): v for k, v in value_map.items()}
            df[col] = df[col].str.lower().map(lower_map).fillna(df[col])

    return df


def calculate_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add calculated fields useful for analytics.
    """
    df = df.copy()

    # Net revenue (if components exist)
    if all(col in df.columns for col in ['subtotal', 'discount_amount']):
        if 'net_revenue' not in df.columns:
            df['net_revenue'] = df['subtotal'] - df['discount_amount'].fillna(0)

    # Discount flag
    if 'discount_amount' in df.columns and 'has_discount' not in df.columns:
        df['has_discount'] = df['discount_amount'] > 0

    # High-value order flag (top 10% by total)
    if 'order_total' in df.columns and 'is_high_value' not in df.columns:
        threshold = df['order_total'].quantile(0.90)
        df['is_high_value'] = df['order_total'] >= threshold

    return df


def transform_dataframe(
    df: pd.DataFrame,
    date_columns: Optional[list[str]] = None,
    categorical_mappings: Optional[dict] = None
) -> pd.DataFrame:
    """
    Apply all transformations to prepare data for metrics.

    Transformation sequence:
    1. Column aliasing
    2. Date standardization + derived date fields
    3. Categorical standardization
    4. Derived field calculation

    Args:
        df: Validated DataFrame
        date_columns: List of date columns to process
        categorical_mappings: Custom category value mappings

    Returns:
        Transformed DataFrame ready for metrics computation
    """
    logger.info(f"Transforming {len(df):,} rows...")

    # Apply transformations in sequence
    df = apply_column_aliases(df)
    df = standardize_dates(df, date_columns)
    df = standardize_categoricals(df, categorical_mappings)
    df = calculate_derived_fields(df)

    logger.info(f"Transformation complete: {len(df.columns)} columns")

    return df
