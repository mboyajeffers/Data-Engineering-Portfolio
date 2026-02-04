#!/usr/bin/env python3
"""
SEC Financial Facts Transformer
Author: Mboya Jeffers

Transforms raw SEC XBRL facts into analytics-ready star schema.
Handles 1M+ rows with proper data typing and validation.
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_dim_company(facts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create company dimension from facts.

    Returns:
        DataFrame with company_id, cik, entity_name, ticker (if available)
    """
    companies = facts_df[["cik", "entity_name"]].drop_duplicates()
    companies = companies.reset_index(drop=True)
    companies["company_id"] = companies.index + 1

    # Reorder columns
    companies = companies[["company_id", "cik", "entity_name"]]

    logger.info(f"Created dim_company: {len(companies)} companies")
    return companies


def create_dim_metric(facts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create metric dimension from facts.

    Categorizes metrics into financial statement categories.
    """
    metrics = facts_df[["metric", "taxonomy"]].drop_duplicates()
    metrics = metrics.reset_index(drop=True)
    metrics["metric_id"] = metrics.index + 1

    # Categorize metrics
    def categorize_metric(metric_name: str) -> str:
        metric_lower = metric_name.lower()

        # Income Statement
        if any(x in metric_lower for x in ["revenue", "sales", "income", "expense", "cost", "profit", "loss", "earning"]):
            return "Income Statement"

        # Balance Sheet - Assets
        if any(x in metric_lower for x in ["asset", "cash", "inventory", "receivable", "property", "equipment"]):
            return "Balance Sheet - Assets"

        # Balance Sheet - Liabilities
        if any(x in metric_lower for x in ["liabilit", "payable", "debt", "obligation"]):
            return "Balance Sheet - Liabilities"

        # Balance Sheet - Equity
        if any(x in metric_lower for x in ["equity", "stock", "retain", "capital"]):
            return "Balance Sheet - Equity"

        # Cash Flow
        if any(x in metric_lower for x in ["cashflow", "operating", "investing", "financing"]):
            return "Cash Flow"

        # Shares/EPS
        if any(x in metric_lower for x in ["share", "eps", "dilut"]):
            return "Per Share Data"

        return "Other"

    metrics["category"] = metrics["metric"].apply(categorize_metric)

    logger.info(f"Created dim_metric: {len(metrics)} unique metrics")
    logger.info(f"Categories: {metrics['category'].value_counts().to_dict()}")

    return metrics[["metric_id", "metric", "taxonomy", "category"]]


def create_dim_date(facts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create date dimension from filing data.
    """
    # Get unique fiscal year/period combinations
    dates = facts_df[["fiscal_year", "fiscal_period"]].drop_duplicates()
    dates = dates.dropna(subset=["fiscal_year"])
    dates["fiscal_year"] = dates["fiscal_year"].astype(int)
    dates = dates.sort_values(["fiscal_year", "fiscal_period"])
    dates = dates.reset_index(drop=True)
    dates["date_id"] = dates.index + 1

    # Create fiscal quarter label
    dates["quarter_label"] = dates.apply(
        lambda r: f"{int(r['fiscal_year'])}{r['fiscal_period']}" if pd.notna(r['fiscal_period']) else str(int(r['fiscal_year'])),
        axis=1
    )

    logger.info(f"Created dim_date: {len(dates)} periods")

    return dates[["date_id", "fiscal_year", "fiscal_period", "quarter_label"]]


def create_fact_financials(
    facts_df: pd.DataFrame,
    dim_company: pd.DataFrame,
    dim_metric: pd.DataFrame,
    dim_date: pd.DataFrame
) -> pd.DataFrame:
    """
    Create fact table by joining dimensions.
    """
    logger.info(f"Creating fact_financials from {len(facts_df):,} raw facts...")

    # Merge company dimension
    fact = facts_df.merge(
        dim_company[["company_id", "cik"]],
        on="cik",
        how="left"
    )

    # Merge metric dimension
    fact = fact.merge(
        dim_metric[["metric_id", "metric", "taxonomy"]],
        on=["metric", "taxonomy"],
        how="left"
    )

    # Merge date dimension
    fact = fact.merge(
        dim_date[["date_id", "fiscal_year", "fiscal_period"]],
        on=["fiscal_year", "fiscal_period"],
        how="left"
    )

    # Convert value to numeric
    fact["value"] = pd.to_numeric(fact["value"], errors="coerce")

    # Select and order columns for fact table
    fact_table = fact[[
        "company_id", "metric_id", "date_id",
        "value", "unit", "form",
        "filed", "start_date", "end_date",
        "accession_number"
    ]].copy()

    # Add surrogate key
    fact_table = fact_table.reset_index(drop=True)
    fact_table["fact_id"] = fact_table.index + 1

    # Reorder
    fact_table = fact_table[[
        "fact_id", "company_id", "metric_id", "date_id",
        "value", "unit", "form", "filed",
        "start_date", "end_date", "accession_number"
    ]]

    logger.info(f"Created fact_financials: {len(fact_table):,} rows")

    return fact_table


def transform_to_star_schema(facts_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Transform raw facts into star schema.

    Args:
        facts_df: Raw extracted facts

    Returns:
        Dict of dimension and fact DataFrames
    """
    logger.info(f"\n{'='*60}")
    logger.info("TRANSFORMING TO STAR SCHEMA")
    logger.info(f"{'='*60}")
    logger.info(f"Input rows: {len(facts_df):,}")

    # Filter to annual and quarterly filings only
    facts_df = facts_df[facts_df["form"].isin(["10-K", "10-Q", "10-K/A", "10-Q/A"])]
    logger.info(f"After filtering to 10-K/10-Q: {len(facts_df):,} rows")

    # Create dimensions
    dim_company = create_dim_company(facts_df)
    dim_metric = create_dim_metric(facts_df)
    dim_date = create_dim_date(facts_df)

    # Create fact table
    fact_financials = create_fact_financials(
        facts_df, dim_company, dim_metric, dim_date
    )

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("STAR SCHEMA SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"dim_company:     {len(dim_company):>10,} rows")
    logger.info(f"dim_metric:      {len(dim_metric):>10,} rows")
    logger.info(f"dim_date:        {len(dim_date):>10,} rows")
    logger.info(f"fact_financials: {len(fact_financials):>10,} rows")
    logger.info(f"{'='*60}")

    return {
        "dim_company": dim_company,
        "dim_metric": dim_metric,
        "dim_date": dim_date,
        "fact_financials": fact_financials
    }


def validate_star_schema(schema: Dict[str, pd.DataFrame]) -> Dict[str, any]:
    """
    Validate star schema integrity.
    """
    logger.info("\nValidating star schema...")

    results = {
        "passed": True,
        "checks": []
    }

    # Check 1: No orphan facts
    fact = schema["fact_financials"]
    orphan_companies = fact[fact["company_id"].isna()]
    orphan_metrics = fact[fact["metric_id"].isna()]
    orphan_dates = fact[fact["date_id"].isna()]

    results["checks"].append({
        "check": "orphan_company_ids",
        "passed": len(orphan_companies) == 0,
        "count": len(orphan_companies)
    })

    results["checks"].append({
        "check": "orphan_metric_ids",
        "passed": len(orphan_metrics) == 0,
        "count": len(orphan_metrics)
    })

    results["checks"].append({
        "check": "orphan_date_ids",
        "passed": len(orphan_dates) == 0,
        "count": len(orphan_dates)
    })

    # Check 2: Value nulls
    null_values = fact["value"].isna().sum()
    null_pct = null_values / len(fact) * 100

    results["checks"].append({
        "check": "null_values",
        "passed": null_pct < 10,  # Allow up to 10% nulls
        "count": null_values,
        "percentage": round(null_pct, 2)
    })

    # Overall pass/fail
    results["passed"] = all(c["passed"] for c in results["checks"])

    for check in results["checks"]:
        status = "PASS" if check["passed"] else "FAIL"
        logger.info(f"  [{status}] {check['check']}: {check.get('count', 'N/A')}")

    return results


if __name__ == "__main__":
    # Test transformation
    import sys

    # Load test data
    test_data = pd.read_parquet("./data/raw_facts.parquet")
    print(f"Loaded {len(test_data):,} raw facts")

    # Transform
    schema = transform_to_star_schema(test_data)

    # Validate
    validation = validate_star_schema(schema)

    print(f"\nValidation: {'PASSED' if validation['passed'] else 'FAILED'}")
