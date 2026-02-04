#!/usr/bin/env python3
"""
Medicare Part D Analytics
Author: Mboya Jeffers

Generates prescribing analytics from Medicare Part D data.
"""

import pandas as pd
import numpy as np
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def calculate_prescriber_summary(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Calculate summary metrics by prescriber.

    Returns:
        DataFrame with prescriber-level aggregations
    """
    fact = schema["fact_prescriptions"]
    dim_prescriber = schema["dim_prescriber"]

    # Aggregate by prescriber
    summary = fact.groupby("prescriber_id").agg({
        "total_claims": "sum",
        "total_drug_cost": "sum",
        "total_beneficiaries": "sum",
        "total_day_supply": "sum",
        "drug_id": "nunique"
    }).reset_index()

    summary.columns = ["prescriber_id", "total_claims", "total_cost",
                       "total_beneficiaries", "total_day_supply", "unique_drugs"]

    # Calculate derived metrics
    summary["cost_per_beneficiary"] = summary["total_cost"] / summary["total_beneficiaries"]
    summary["claims_per_beneficiary"] = summary["total_claims"] / summary["total_beneficiaries"]
    summary["avg_cost_per_claim"] = summary["total_cost"] / summary["total_claims"]

    # Join prescriber info
    summary = summary.merge(dim_prescriber, on="prescriber_id", how="left")

    summary = summary.sort_values("total_claims", ascending=False)

    logger.info(f"Prescriber summary: {len(summary)} prescribers")

    return summary


def calculate_drug_utilization(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Calculate drug utilization metrics.

    Returns:
        DataFrame with drug-level aggregations
    """
    fact = schema["fact_prescriptions"]
    dim_drug = schema["dim_drug"]

    # Aggregate by drug
    utilization = fact.groupby("drug_id").agg({
        "total_claims": "sum",
        "total_drug_cost": "sum",
        "total_beneficiaries": "sum",
        "total_day_supply": "sum",
        "prescriber_id": "nunique"
    }).reset_index()

    utilization.columns = ["drug_id", "total_claims", "total_cost",
                           "total_beneficiaries", "total_day_supply", "prescriber_count"]

    # Calculate metrics
    utilization["avg_cost_per_claim"] = utilization["total_cost"] / utilization["total_claims"]
    utilization["avg_day_supply"] = utilization["total_day_supply"] / utilization["total_claims"]

    # Join drug info
    utilization = utilization.merge(dim_drug, on="drug_id", how="left")

    utilization = utilization.sort_values("total_claims", ascending=False)

    logger.info(f"Drug utilization: {len(utilization)} drugs")

    return utilization


def calculate_opioid_analysis(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Analyze opioid prescribing patterns.

    Returns:
        DataFrame with opioid-specific metrics
    """
    fact = schema["fact_prescriptions"]
    dim_drug = schema["dim_drug"]
    dim_prescriber = schema["dim_prescriber"]

    # Get opioid drug IDs
    opioid_drugs = dim_drug[dim_drug["is_opioid"] == True]["drug_id"].tolist()

    if not opioid_drugs:
        logger.warning("No opioid drugs found in dimension")
        return pd.DataFrame()

    # Filter to opioid prescriptions
    opioid_facts = fact[fact["drug_id"].isin(opioid_drugs)]

    # Aggregate by prescriber
    opioid_summary = opioid_facts.groupby("prescriber_id").agg({
        "total_claims": "sum",
        "total_drug_cost": "sum",
        "total_beneficiaries": "sum",
        "total_day_supply": "sum"
    }).reset_index()

    opioid_summary.columns = ["prescriber_id", "opioid_claims", "opioid_cost",
                              "opioid_beneficiaries", "opioid_day_supply"]

    # Get total claims per prescriber for rate calculation
    total_claims = fact.groupby("prescriber_id")["total_claims"].sum().reset_index()
    total_claims.columns = ["prescriber_id", "all_claims"]

    opioid_summary = opioid_summary.merge(total_claims, on="prescriber_id", how="left")
    opioid_summary["opioid_rate"] = opioid_summary["opioid_claims"] / opioid_summary["all_claims"] * 100

    # Join prescriber info
    opioid_summary = opioid_summary.merge(dim_prescriber, on="prescriber_id", how="left")

    opioid_summary = opioid_summary.sort_values("opioid_claims", ascending=False)

    logger.info(f"Opioid analysis: {len(opioid_summary)} prescribers")

    return opioid_summary


def calculate_specialty_analysis(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Analyze prescribing patterns by specialty.

    Returns:
        DataFrame with specialty-level metrics
    """
    fact = schema["fact_prescriptions"]
    dim_prescriber = schema["dim_prescriber"]

    # Join prescriber specialty to facts
    fact_with_specialty = fact.merge(
        dim_prescriber[["prescriber_id", "specialty"]],
        on="prescriber_id",
        how="left"
    )

    # Aggregate by specialty
    specialty_summary = fact_with_specialty.groupby("specialty").agg({
        "total_claims": "sum",
        "total_drug_cost": "sum",
        "total_beneficiaries": "sum",
        "prescriber_id": "nunique",
        "drug_id": "nunique"
    }).reset_index()

    specialty_summary.columns = ["specialty", "total_claims", "total_cost",
                                  "total_beneficiaries", "prescriber_count", "unique_drugs"]

    # Calculate derived metrics
    specialty_summary["avg_claims_per_prescriber"] = (
        specialty_summary["total_claims"] / specialty_summary["prescriber_count"]
    )
    specialty_summary["avg_cost_per_claim"] = (
        specialty_summary["total_cost"] / specialty_summary["total_claims"]
    )

    specialty_summary = specialty_summary.sort_values("total_claims", ascending=False)

    logger.info(f"Specialty analysis: {len(specialty_summary)} specialties")

    return specialty_summary


def calculate_geographic_analysis(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Analyze prescribing patterns by geography.

    Returns:
        DataFrame with state-level metrics
    """
    fact = schema["fact_prescriptions"]
    dim_location = schema["dim_location"]

    # Join location to facts
    fact_with_loc = fact.merge(
        dim_location[["location_id", "state", "state_name"]],
        on="location_id",
        how="left"
    )

    # Aggregate by state
    state_summary = fact_with_loc.groupby(["state", "state_name"]).agg({
        "total_claims": "sum",
        "total_drug_cost": "sum",
        "total_beneficiaries": "sum",
        "prescriber_id": "nunique"
    }).reset_index()

    state_summary.columns = ["state", "state_name", "total_claims", "total_cost",
                             "total_beneficiaries", "prescriber_count"]

    state_summary["cost_per_beneficiary"] = (
        state_summary["total_cost"] / state_summary["total_beneficiaries"]
    )
    state_summary["claims_per_beneficiary"] = (
        state_summary["total_claims"] / state_summary["total_beneficiaries"]
    )

    state_summary = state_summary.sort_values("total_claims", ascending=False)

    logger.info(f"Geographic analysis: {len(state_summary)} states")

    return state_summary


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
        "prescriber_summary": calculate_prescriber_summary(schema),
        "drug_utilization": calculate_drug_utilization(schema),
        "opioid_analysis": calculate_opioid_analysis(schema),
        "specialty_analysis": calculate_specialty_analysis(schema),
        "geographic_analysis": calculate_geographic_analysis(schema)
    }

    logger.info(f"\n{'='*60}")
    logger.info("ANALYTICS COMPLETE")
    logger.info(f"{'='*60}")

    for name, df in analytics.items():
        if df is not None and len(df) > 0:
            logger.info(f"  {name}: {len(df):,} rows")

    return analytics


if __name__ == "__main__":
    schema = {
        "dim_prescriber": pd.read_parquet("./output/dim_prescriber.parquet"),
        "dim_drug": pd.read_parquet("./output/dim_drug.parquet"),
        "dim_location": pd.read_parquet("./output/dim_location.parquet"),
        "dim_year": pd.read_parquet("./output/dim_year.parquet"),
        "fact_prescriptions": pd.read_parquet("./output/fact_prescriptions.parquet")
    }

    analytics = generate_all_analytics(schema)

    print("\nTop 10 Drugs by Claims:")
    print(analytics["drug_utilization"].head(10)[["drug_name", "total_claims", "total_cost"]])
