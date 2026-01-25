"""
KPI Metrics Computation Module
==============================

Computes business KPIs from transformed transaction data.
Implements the KPI catalog definitions.

Synthetic demo output for portfolio purposes.
"""

import pandas as pd
from typing import Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class KPIResult:
    """Container for a single KPI result."""
    kpi_id: str
    name: str
    value: float | int | str
    unit: str
    grain: str
    description: str = ""

    def to_dict(self) -> dict:
        return {
            'kpi_id': self.kpi_id,
            'name': self.name,
            'value': self.value,
            'unit': self.unit,
            'grain': self.grain,
            'description': self.description,
        }


@dataclass
class MetricsReport:
    """Collection of computed KPIs."""
    kpis: list[KPIResult] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            'metadata': self.metadata,
            'kpis': [kpi.to_dict() for kpi in self.kpis],
        }

    def get_kpi(self, kpi_id: str) -> Optional[KPIResult]:
        for kpi in self.kpis:
            if kpi.kpi_id == kpi_id:
                return kpi
        return None


def compute_total_revenue(df: pd.DataFrame, status_filter: str = 'Completed') -> KPIResult:
    """KPI-001: Total Revenue"""
    filtered = df[df['order_status'] == status_filter] if 'order_status' in df.columns else df
    value = filtered['order_total'].sum() if 'order_total' in df.columns else 0

    return KPIResult(
        kpi_id='KPI-001',
        name='Total Revenue',
        value=round(value, 2),
        unit='USD',
        grain='Total',
        description='Sum of completed order totals',
    )


def compute_total_orders(df: pd.DataFrame, status_filter: str = 'Completed') -> KPIResult:
    """KPI-002: Total Orders"""
    filtered = df[df['order_status'] == status_filter] if 'order_status' in df.columns else df
    value = filtered['order_id'].nunique() if 'order_id' in df.columns else len(filtered)

    return KPIResult(
        kpi_id='KPI-002',
        name='Total Orders',
        value=int(value),
        unit='count',
        grain='Total',
        description='Count of distinct completed orders',
    )


def compute_average_order_value(df: pd.DataFrame, status_filter: str = 'Completed') -> KPIResult:
    """KPI-003: Average Order Value (AOV)"""
    filtered = df[df['order_status'] == status_filter] if 'order_status' in df.columns else df

    if 'order_total' in df.columns:
        # Group by order_id to get order-level totals, then average
        if 'order_id' in df.columns:
            order_totals = filtered.groupby('order_id')['order_total'].first()
            value = order_totals.mean()
        else:
            value = filtered['order_total'].mean()
    else:
        value = 0

    return KPIResult(
        kpi_id='KPI-003',
        name='Average Order Value',
        value=round(value, 2),
        unit='USD',
        grain='Per Order',
        description='Mean revenue per completed order',
    )


def compute_unique_customers(df: pd.DataFrame, status_filter: str = 'Completed') -> KPIResult:
    """KPI-006: Unique Customers"""
    filtered = df[df['order_status'] == status_filter] if 'order_status' in df.columns else df
    value = filtered['customer_id'].nunique() if 'customer_id' in df.columns else 0

    return KPIResult(
        kpi_id='KPI-006',
        name='Unique Customers',
        value=int(value),
        unit='count',
        grain='Total',
        description='Distinct customers with completed orders',
    )


def compute_revenue_by_category(df: pd.DataFrame, status_filter: str = 'Completed') -> KPIResult:
    """KPI-004: Revenue by Category"""
    filtered = df[df['order_status'] == status_filter] if 'order_status' in df.columns else df

    if 'product_category' in df.columns and 'order_total' in df.columns:
        breakdown = filtered.groupby('product_category')['order_total'].sum().round(2).to_dict()
    else:
        breakdown = {}

    return KPIResult(
        kpi_id='KPI-004',
        name='Revenue by Category',
        value=breakdown,
        unit='USD',
        grain='Category',
        description='Revenue breakdown by product category',
    )


def compute_revenue_by_segment(df: pd.DataFrame, status_filter: str = 'Completed') -> KPIResult:
    """KPI-007: Revenue by Customer Segment"""
    filtered = df[df['order_status'] == status_filter] if 'order_status' in df.columns else df

    if 'customer_segment' in df.columns and 'order_total' in df.columns:
        breakdown = filtered.groupby('customer_segment')['order_total'].sum().round(2).to_dict()
    else:
        breakdown = {}

    return KPIResult(
        kpi_id='KPI-007',
        name='Revenue by Segment',
        value=breakdown,
        unit='USD',
        grain='Segment',
        description='Revenue breakdown by customer segment',
    )


def compute_repeat_rate(df: pd.DataFrame) -> KPIResult:
    """KPI-008: Repeat Purchase Rate"""
    if 'customer_id' not in df.columns or 'order_id' not in df.columns:
        return KPIResult(
            kpi_id='KPI-008',
            name='Repeat Purchase Rate',
            value=0,
            unit='%',
            grain='Total',
            description='Percentage of customers with >1 order',
        )

    customer_orders = df.groupby('customer_id')['order_id'].nunique()
    repeat_customers = (customer_orders > 1).sum()
    total_customers = len(customer_orders)

    value = (repeat_customers / total_customers * 100) if total_customers > 0 else 0

    return KPIResult(
        kpi_id='KPI-008',
        name='Repeat Purchase Rate',
        value=round(value, 2),
        unit='%',
        grain='Total',
        description='Percentage of customers with >1 order',
    )


def compute_discount_utilization(df: pd.DataFrame) -> KPIResult:
    """KPI-015: Discount Utilization Rate"""
    if 'discount_amount' not in df.columns:
        return KPIResult(
            kpi_id='KPI-015',
            name='Discount Utilization Rate',
            value=0,
            unit='%',
            grain='Total',
            description='Percentage of orders using discounts',
        )

    discounted = (df['discount_amount'] > 0).sum()
    total = len(df)
    value = (discounted / total * 100) if total > 0 else 0

    return KPIResult(
        kpi_id='KPI-015',
        name='Discount Utilization Rate',
        value=round(value, 2),
        unit='%',
        grain='Total',
        description='Percentage of orders using discounts',
    )


def compute_all_kpis(
    df: pd.DataFrame,
    status_filter: str = 'Completed'
) -> MetricsReport:
    """
    Compute all defined KPIs from the transformed dataset.

    Args:
        df: Transformed DataFrame
        status_filter: Order status to filter for revenue KPIs

    Returns:
        MetricsReport containing all computed KPIs
    """
    logger.info(f"Computing KPIs for {len(df):,} records...")

    report = MetricsReport(
        metadata={
            'total_records': len(df),
            'status_filter': status_filter,
            'columns_available': list(df.columns),
        }
    )

    # Compute each KPI
    kpi_functions = [
        compute_total_revenue,
        compute_total_orders,
        compute_average_order_value,
        compute_unique_customers,
        compute_revenue_by_category,
        compute_revenue_by_segment,
        compute_repeat_rate,
        compute_discount_utilization,
    ]

    for func in kpi_functions:
        try:
            if func in [compute_repeat_rate, compute_discount_utilization]:
                result = func(df)
            else:
                result = func(df, status_filter)
            report.kpis.append(result)
            logger.debug(f"Computed {result.kpi_id}: {result.value}")
        except Exception as e:
            logger.warning(f"Error computing {func.__name__}: {e}")

    logger.info(f"Computed {len(report.kpis)} KPIs")

    return report
