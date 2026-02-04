#!/usr/bin/env python3
"""
Federal Awards Analytics
Author: Mboya Jeffers

Generates spending analytics from federal award data.
"""

import pandas as pd
import numpy as np
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def calculate_agency_spending(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Calculate spending by agency.

    Returns:
        DataFrame with agency spending metrics
    """
    fact = schema["fact_awards"]
    dim_agency = schema["dim_agency"]
    dim_date = schema["dim_date"]

    # Aggregate by agency and year
    spending = fact.groupby(["agency_id", "date_id"]).agg({
        "award_amount": ["sum", "mean", "count"],
        "total_outlays": "sum"
    }).reset_index()

    # Flatten column names
    spending.columns = ["agency_id", "date_id", "total_obligated", "avg_award", "award_count", "total_outlays"]

    # Join agency info
    spending = spending.merge(dim_agency[["agency_id", "agency_name", "sub_agency"]], on="agency_id", how="left")

    # Join date info
    date_lookup = dim_date[dim_date["fiscal_quarter"] == "FY"][["date_id", "fiscal_year"]]
    spending = spending.merge(date_lookup, on="date_id", how="left")

    # Calculate YoY growth
    spending = spending.sort_values(["agency_id", "fiscal_year"])
    spending["yoy_growth"] = spending.groupby("agency_id")["total_obligated"].pct_change() * 100

    logger.info(f"Agency spending analysis: {len(spending)} records")

    return spending


def calculate_recipient_rankings(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Rank recipients by award volume.

    Returns:
        DataFrame with recipient rankings
    """
    fact = schema["fact_awards"]
    dim_recipient = schema["dim_recipient"]

    # Aggregate by recipient
    rankings = fact.groupby("recipient_id").agg({
        "award_amount": ["sum", "mean", "count"],
        "total_outlays": "sum"
    }).reset_index()

    rankings.columns = ["recipient_id", "total_awards", "avg_award", "award_count", "total_outlays"]

    # Join recipient info
    rankings = rankings.merge(dim_recipient, on="recipient_id", how="left")

    # Calculate rank
    rankings["rank"] = rankings["total_awards"].rank(ascending=False, method="dense")

    # Sort by total
    rankings = rankings.sort_values("total_awards", ascending=False)

    logger.info(f"Recipient rankings: {len(rankings)} recipients")

    return rankings


def calculate_geographic_distribution(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Analyze geographic distribution of awards.

    Returns:
        DataFrame with state-level spending
    """
    fact = schema["fact_awards"]
    dim_location = schema["dim_location"]

    # Aggregate by location
    geo = fact.groupby("location_id").agg({
        "award_amount": ["sum", "count"]
    }).reset_index()

    geo.columns = ["location_id", "total_spending", "award_count"]

    # Join location info
    geo = geo.merge(dim_location, on="location_id", how="left")

    # Aggregate by state
    state_summary = geo.groupby(["state_code", "state_name"]).agg({
        "total_spending": "sum",
        "award_count": "sum"
    }).reset_index()

    state_summary["avg_per_award"] = state_summary["total_spending"] / state_summary["award_count"]
    state_summary = state_summary.sort_values("total_spending", ascending=False)

    logger.info(f"Geographic analysis: {len(state_summary)} states")

    return state_summary


def calculate_award_type_analysis(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Analyze awards by type (contracts vs grants).

    Returns:
        DataFrame with award type breakdown
    """
    fact = schema["fact_awards"]

    # Award type codes mapping
    AWARD_TYPES = {
        "A": "BPA Call",
        "B": "Purchase Order",
        "C": "Delivery Order",
        "D": "Definitive Contract",
        "02": "Block Grant",
        "03": "Formula Grant",
        "04": "Project Grant",
        "05": "Cooperative Agreement",
        "06": "Direct Payment",
        "07": "Direct Loan",
        "08": "Guaranteed Loan",
        "09": "Insurance",
        "10": "Direct Payment with Unrestricted Use",
        "11": "Other Financial Assistance"
    }

    # Aggregate by award type
    type_analysis = fact.groupby("award_type").agg({
        "award_amount": ["sum", "mean", "count"]
    }).reset_index()

    type_analysis.columns = ["award_type", "total_amount", "avg_amount", "count"]

    # Add type description
    type_analysis["type_description"] = type_analysis["award_type"].map(AWARD_TYPES).fillna("Unknown")

    # Categorize as contract or assistance
    contract_codes = ["A", "B", "C", "D"]
    type_analysis["category"] = type_analysis["award_type"].apply(
        lambda x: "Contract" if x in contract_codes else "Financial Assistance"
    )

    # Calculate percentage
    total = type_analysis["total_amount"].sum()
    type_analysis["pct_of_total"] = type_analysis["total_amount"] / total * 100

    type_analysis = type_analysis.sort_values("total_amount", ascending=False)

    logger.info(f"Award type analysis: {len(type_analysis)} types")

    return type_analysis


def calculate_naics_analysis(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Analyze awards by industry sector (NAICS).

    Returns:
        DataFrame with NAICS breakdown
    """
    fact = schema["fact_awards"]

    # Top NAICS sectors
    NAICS_SECTORS = {
        "54": "Professional Services",
        "33": "Manufacturing",
        "23": "Construction",
        "56": "Administrative Services",
        "62": "Health Care",
        "61": "Educational Services",
        "48": "Transportation",
        "51": "Information",
        "52": "Finance/Insurance",
        "72": "Accommodation/Food"
    }

    # Extract 2-digit NAICS
    fact_with_sector = fact.copy()
    fact_with_sector["naics_sector"] = fact_with_sector["naics_code"].astype(str).str[:2]

    # Aggregate
    naics_analysis = fact_with_sector.groupby("naics_sector").agg({
        "award_amount": ["sum", "count"]
    }).reset_index()

    naics_analysis.columns = ["naics_sector", "total_amount", "award_count"]

    # Add sector name
    naics_analysis["sector_name"] = naics_analysis["naics_sector"].map(NAICS_SECTORS).fillna("Other")

    # Calculate percentage
    total = naics_analysis["total_amount"].sum()
    naics_analysis["pct_of_total"] = naics_analysis["total_amount"] / total * 100

    naics_analysis = naics_analysis.sort_values("total_amount", ascending=False)

    logger.info(f"NAICS analysis: {len(naics_analysis)} sectors")

    return naics_analysis


def generate_all_analytics(schema: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Generate all analytics from schema.

    Returns:
        Dict of analytics DataFrames
    """
    logger.info(f"\n{'='*60}")
    logger.info("GENERATING ANALYTICS")
    logger.info(f"{'='*60}")

    analytics = {
        "agency_spending": calculate_agency_spending(schema),
        "recipient_rankings": calculate_recipient_rankings(schema),
        "geographic_distribution": calculate_geographic_distribution(schema),
        "award_type_analysis": calculate_award_type_analysis(schema),
        "naics_analysis": calculate_naics_analysis(schema)
    }

    logger.info(f"\n{'='*60}")
    logger.info("ANALYTICS COMPLETE")
    logger.info(f"{'='*60}")

    for name, df in analytics.items():
        logger.info(f"  {name}: {len(df):,} rows")

    return analytics


if __name__ == "__main__":
    # Test analytics
    import os

    schema = {
        "dim_agency": pd.read_parquet("./output/dim_agency.parquet"),
        "dim_recipient": pd.read_parquet("./output/dim_recipient.parquet"),
        "dim_location": pd.read_parquet("./output/dim_location.parquet"),
        "dim_date": pd.read_parquet("./output/dim_date.parquet"),
        "fact_awards": pd.read_parquet("./output/fact_awards.parquet")
    }

    analytics = generate_all_analytics(schema)

    print("\nTop 10 Agencies by Spending:")
    print(analytics["agency_spending"].nlargest(10, "total_obligated")[["agency_name", "total_obligated"]])
