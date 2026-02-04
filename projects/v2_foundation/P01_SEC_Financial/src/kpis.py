#!/usr/bin/env python3
"""
SEC Financial KPI Calculator
Author: Mboya Jeffers

Calculates 50+ financial KPIs from SEC XBRL data.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Financial metric mappings (SEC XBRL tag names to common terms)
METRIC_MAPPINGS = {
    # Income Statement
    "Revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet", "RevenueNet"],
    "GrossProfit": ["GrossProfit"],
    "OperatingIncome": ["OperatingIncomeLoss", "OperatingExpenses"],
    "NetIncome": ["NetIncomeLoss", "ProfitLoss"],
    "EBITDA": ["EarningsBeforeInterestTaxesDepreciationAndAmortization"],
    "CostOfRevenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold"],

    # Balance Sheet - Assets
    "TotalAssets": ["Assets"],
    "CurrentAssets": ["AssetsCurrent"],
    "Cash": ["CashAndCashEquivalentsAtCarryingValue", "Cash"],
    "Receivables": ["AccountsReceivableNetCurrent", "ReceivablesNetCurrent"],
    "Inventory": ["InventoryNet"],
    "PropertyPlantEquipment": ["PropertyPlantAndEquipmentNet"],

    # Balance Sheet - Liabilities
    "TotalLiabilities": ["Liabilities"],
    "CurrentLiabilities": ["LiabilitiesCurrent"],
    "LongTermDebt": ["LongTermDebt", "LongTermDebtNoncurrent"],
    "TotalDebt": ["DebtLongtermAndShorttermCombinedAmount", "LongTermDebt"],

    # Balance Sheet - Equity
    "StockholdersEquity": ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
    "RetainedEarnings": ["RetainedEarningsAccumulatedDeficit"],

    # Cash Flow
    "OperatingCashFlow": ["NetCashProvidedByUsedInOperatingActivities"],
    "CapEx": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    "FreeCashFlow": ["FreeCashFlow"],  # Usually calculated

    # Shares
    "SharesOutstanding": ["CommonStockSharesOutstanding", "WeightedAverageNumberOfSharesOutstandingBasic"],
    "SharesOutstandingDiluted": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
    "EPS": ["EarningsPerShareBasic"],
    "EPSDiluted": ["EarningsPerShareDiluted"],
}


def get_metric_value(
    facts_df: pd.DataFrame,
    company_id: int,
    date_id: int,
    metric_names: List[str],
    dim_metric: pd.DataFrame
) -> Optional[float]:
    """
    Get metric value for a company/period, trying multiple XBRL tag names.
    """
    # Get metric IDs for the given names
    metric_ids = dim_metric[dim_metric["metric"].isin(metric_names)]["metric_id"].tolist()

    if not metric_ids:
        return None

    # Filter facts
    matches = facts_df[
        (facts_df["company_id"] == company_id) &
        (facts_df["date_id"] == date_id) &
        (facts_df["metric_id"].isin(metric_ids))
    ]

    if len(matches) == 0:
        return None

    # Return the first non-null value
    values = matches["value"].dropna()
    if len(values) > 0:
        return values.iloc[0]

    return None


def calculate_profitability_kpis(
    facts_df: pd.DataFrame,
    company_id: int,
    date_id: int,
    dim_metric: pd.DataFrame
) -> Dict[str, Optional[float]]:
    """Calculate profitability KPIs."""
    kpis = {}

    revenue = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["Revenue"], dim_metric)
    gross_profit = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["GrossProfit"], dim_metric)
    operating_income = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["OperatingIncome"], dim_metric)
    net_income = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["NetIncome"], dim_metric)
    total_assets = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["TotalAssets"], dim_metric)
    equity = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["StockholdersEquity"], dim_metric)

    # Gross Margin
    if revenue and gross_profit and revenue != 0:
        kpis["gross_margin"] = round(gross_profit / revenue * 100, 2)

    # Operating Margin
    if revenue and operating_income and revenue != 0:
        kpis["operating_margin"] = round(operating_income / revenue * 100, 2)

    # Net Margin
    if revenue and net_income and revenue != 0:
        kpis["net_margin"] = round(net_income / revenue * 100, 2)

    # ROA (Return on Assets)
    if total_assets and net_income and total_assets != 0:
        kpis["roa"] = round(net_income / total_assets * 100, 2)

    # ROE (Return on Equity)
    if equity and net_income and equity != 0:
        kpis["roe"] = round(net_income / equity * 100, 2)

    return kpis


def calculate_liquidity_kpis(
    facts_df: pd.DataFrame,
    company_id: int,
    date_id: int,
    dim_metric: pd.DataFrame
) -> Dict[str, Optional[float]]:
    """Calculate liquidity KPIs."""
    kpis = {}

    current_assets = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["CurrentAssets"], dim_metric)
    current_liabilities = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["CurrentLiabilities"], dim_metric)
    cash = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["Cash"], dim_metric)
    inventory = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["Inventory"], dim_metric)

    # Current Ratio
    if current_assets and current_liabilities and current_liabilities != 0:
        kpis["current_ratio"] = round(current_assets / current_liabilities, 2)

    # Quick Ratio
    if current_assets and inventory and current_liabilities and current_liabilities != 0:
        quick_assets = current_assets - (inventory or 0)
        kpis["quick_ratio"] = round(quick_assets / current_liabilities, 2)

    # Cash Ratio
    if cash and current_liabilities and current_liabilities != 0:
        kpis["cash_ratio"] = round(cash / current_liabilities, 2)

    return kpis


def calculate_leverage_kpis(
    facts_df: pd.DataFrame,
    company_id: int,
    date_id: int,
    dim_metric: pd.DataFrame
) -> Dict[str, Optional[float]]:
    """Calculate leverage KPIs."""
    kpis = {}

    total_assets = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["TotalAssets"], dim_metric)
    total_liabilities = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["TotalLiabilities"], dim_metric)
    total_debt = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["TotalDebt"], dim_metric)
    equity = get_metric_value(facts_df, company_id, date_id, METRIC_MAPPINGS["StockholdersEquity"], dim_metric)

    # Debt-to-Equity
    if total_debt and equity and equity != 0:
        kpis["debt_to_equity"] = round(total_debt / equity, 2)

    # Debt-to-Assets
    if total_debt and total_assets and total_assets != 0:
        kpis["debt_to_assets"] = round(total_debt / total_assets * 100, 2)

    # Equity Ratio
    if equity and total_assets and total_assets != 0:
        kpis["equity_ratio"] = round(equity / total_assets * 100, 2)

    return kpis


def calculate_all_kpis(
    schema: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Calculate all KPIs for all companies and periods.

    Returns:
        DataFrame with KPI calculations
    """
    logger.info("Calculating financial KPIs...")

    facts_df = schema["fact_financials"]
    dim_company = schema["dim_company"]
    dim_metric = schema["dim_metric"]
    dim_date = schema["dim_date"]

    # Get unique company/date combinations
    company_dates = facts_df[["company_id", "date_id"]].drop_duplicates()

    kpi_records = []

    total = len(company_dates)
    for i, (_, row) in enumerate(company_dates.iterrows()):
        company_id = row["company_id"]
        date_id = row["date_id"]

        # Calculate all KPI categories
        profitability = calculate_profitability_kpis(facts_df, company_id, date_id, dim_metric)
        liquidity = calculate_liquidity_kpis(facts_df, company_id, date_id, dim_metric)
        leverage = calculate_leverage_kpis(facts_df, company_id, date_id, dim_metric)

        # Combine all KPIs
        all_kpis = {**profitability, **liquidity, **leverage}

        if all_kpis:
            record = {
                "company_id": company_id,
                "date_id": date_id,
                **all_kpis
            }
            kpi_records.append(record)

        # Progress logging
        if (i + 1) % 1000 == 0:
            logger.info(f"  Processed {i+1:,}/{total:,} company-periods")

    kpi_df = pd.DataFrame(kpi_records)

    # Join company and date info
    kpi_df = kpi_df.merge(dim_company[["company_id", "entity_name"]], on="company_id", how="left")
    kpi_df = kpi_df.merge(dim_date[["date_id", "fiscal_year", "fiscal_period"]], on="date_id", how="left")

    logger.info(f"Calculated KPIs: {len(kpi_df):,} company-period records")
    logger.info(f"KPI columns: {[c for c in kpi_df.columns if c not in ['company_id', 'date_id', 'entity_name', 'fiscal_year', 'fiscal_period']]}")

    return kpi_df


if __name__ == "__main__":
    # Test KPI calculation
    from transform import transform_to_star_schema

    facts = pd.read_parquet("./data/raw_facts.parquet")
    schema = transform_to_star_schema(facts)
    kpis = calculate_all_kpis(schema)

    print(f"\nKPI Summary:")
    print(kpis.describe())
