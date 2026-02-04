#!/usr/bin/env python3
"""
Energy Grid Star Schema Transformer
Author: Mboya Jeffers

Transforms raw EIA-930 grid data into analytics-ready star schema.
Handles 500K+ rows with proper dimensional modeling.
"""

import pandas as pd
import numpy as np
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Fuel type classifications
FUEL_CLASSIFICATIONS = {
    "COL": ("Coal", "Fossil", False, False),
    "NG": ("Natural Gas", "Fossil", False, False),
    "NUC": ("Nuclear", "Nuclear", False, True),
    "SUN": ("Solar", "Renewable", True, True),
    "WND": ("Wind", "Renewable", True, True),
    "WAT": ("Hydro", "Renewable", True, True),
    "OTH": ("Other", "Other", False, False),
    "OIL": ("Petroleum", "Fossil", False, False),
    "BIO": ("Biomass", "Renewable", True, False),
    "GEO": ("Geothermal", "Renewable", True, True),
    "PS": ("Pumped Storage", "Storage", False, False),
    "BAT": ("Battery", "Storage", False, True),
    "UNK": ("Unknown", "Other", False, False)
}


def create_dim_balancing_authority(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create balancing authority dimension.

    Returns:
        DataFrame with ba_id, ba_code, ba_name, region
    """
    bas = df[["ba_code", "ba_name", "region"]].drop_duplicates()
    bas = bas.dropna(subset=["ba_code"])
    bas = bas.reset_index(drop=True)
    bas["ba_id"] = bas.index + 1

    # Add timezone mapping
    TIMEZONE_MAP = {
        "Western": "America/Los_Angeles",
        "Eastern": "America/New_York",
        "Central": "America/Chicago",
        "Texas": "America/Chicago"
    }
    bas["timezone"] = bas["region"].map(TIMEZONE_MAP).fillna("America/New_York")

    logger.info(f"Created dim_balancing_authority: {len(bas)} BAs")
    logger.info(f"Regions: {bas['region'].value_counts().to_dict()}")

    return bas[["ba_id", "ba_code", "ba_name", "region", "timezone"]]


def create_dim_fuel_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create fuel type dimension.

    Returns:
        DataFrame with fuel_id, fuel_code, fuel_name, fuel_category, is_renewable, is_clean
    """
    # Get unique fuel types from data
    fuel_codes = df["fueltype"].dropna().unique().tolist()

    # Add standard fuel types
    all_fuel_codes = list(set(fuel_codes + list(FUEL_CLASSIFICATIONS.keys())))

    fuel_records = []
    for i, code in enumerate(all_fuel_codes):
        if code in FUEL_CLASSIFICATIONS:
            name, category, is_renewable, is_clean = FUEL_CLASSIFICATIONS[code]
        else:
            name, category, is_renewable, is_clean = (code, "Other", False, False)

        fuel_records.append({
            "fuel_id": i + 1,
            "fuel_code": code,
            "fuel_name": name,
            "fuel_category": category,
            "is_renewable": is_renewable,
            "is_clean": is_clean
        })

    fuel_df = pd.DataFrame(fuel_records)

    logger.info(f"Created dim_fuel_type: {len(fuel_df)} fuel types")
    logger.info(f"Renewable: {fuel_df['is_renewable'].sum()}, Clean: {fuel_df['is_clean'].sum()}")

    return fuel_df


def create_dim_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create datetime dimension from timestamps.

    Returns:
        DataFrame with datetime_id, timestamp, date, hour, etc.
    """
    if "timestamp" not in df.columns:
        df["timestamp"] = pd.to_datetime(df["period"])

    # Get unique timestamps
    timestamps = df["timestamp"].dropna().unique()
    timestamps = pd.to_datetime(timestamps)
    timestamps = sorted(timestamps)

    datetime_records = []
    for i, ts in enumerate(timestamps):
        ts = pd.Timestamp(ts)
        datetime_records.append({
            "datetime_id": i + 1,
            "timestamp": ts,
            "date": ts.date(),
            "hour": ts.hour,
            "day_of_week": ts.dayofweek,
            "day_name": ts.day_name(),
            "month": ts.month,
            "quarter": ts.quarter,
            "year": ts.year,
            "is_weekend": ts.dayofweek >= 5,
            "is_peak_hour": 14 <= ts.hour <= 19  # Afternoon peak
        })

    datetime_df = pd.DataFrame(datetime_records)

    logger.info(f"Created dim_datetime: {len(datetime_df)} periods")
    logger.info(f"Date range: {datetime_df['date'].min()} to {datetime_df['date'].max()}")

    return datetime_df


def create_fact_grid_ops(
    df: pd.DataFrame,
    dim_ba: pd.DataFrame,
    dim_fuel: pd.DataFrame,
    dim_datetime: pd.DataFrame
) -> pd.DataFrame:
    """
    Create fact table by joining dimensions.

    Returns:
        Fact table with dimension keys and measures
    """
    logger.info(f"Creating fact_grid_ops from {len(df):,} raw readings...")

    # Ensure timestamp column
    if "timestamp" not in df.columns:
        df["timestamp"] = pd.to_datetime(df["period"])

    # Merge BA dimension
    ba_lookup = dim_ba[["ba_id", "ba_code"]]
    fact = df.merge(ba_lookup, on="ba_code", how="left")

    # Merge fuel dimension (only for generation records)
    fuel_lookup = dim_fuel[["fuel_id", "fuel_code"]]
    fact = fact.merge(fuel_lookup, left_on="fueltype", right_on="fuel_code", how="left")

    # Merge datetime dimension
    datetime_lookup = dim_datetime[["datetime_id", "timestamp"]]
    fact = fact.merge(datetime_lookup, on="timestamp", how="left")

    # Create measures based on data type
    fact["demand_mw"] = np.where(fact["data_type"] == "demand", fact["value"], np.nan)
    fact["generation_mw"] = np.where(fact["data_type"] == "generation", fact["value"], np.nan)

    # Select columns for fact table
    fact_table = fact[[
        "ba_id", "datetime_id", "fuel_id",
        "demand_mw", "generation_mw", "value",
        "data_type", "type"
    ]].copy()

    # Add surrogate key
    fact_table = fact_table.reset_index(drop=True)
    fact_table["fact_id"] = fact_table.index + 1

    # Reorder columns
    fact_table = fact_table[[
        "fact_id", "ba_id", "datetime_id", "fuel_id",
        "demand_mw", "generation_mw", "value",
        "data_type", "type"
    ]]

    logger.info(f"Created fact_grid_ops: {len(fact_table):,} rows")

    return fact_table


def transform_to_star_schema(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Transform raw grid data into star schema.

    Args:
        df: Raw extracted grid data

    Returns:
        Dict of dimension and fact DataFrames
    """
    logger.info(f"\n{'='*60}")
    logger.info("TRANSFORMING TO STAR SCHEMA")
    logger.info(f"{'='*60}")
    logger.info(f"Input rows: {len(df):,}")

    # Create dimensions
    dim_ba = create_dim_balancing_authority(df)
    dim_fuel = create_dim_fuel_type(df)
    dim_datetime = create_dim_datetime(df)

    # Create fact table
    fact_grid_ops = create_fact_grid_ops(df, dim_ba, dim_fuel, dim_datetime)

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("STAR SCHEMA SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"dim_balancing_authority: {len(dim_ba):>10,} rows")
    logger.info(f"dim_fuel_type:           {len(dim_fuel):>10,} rows")
    logger.info(f"dim_datetime:            {len(dim_datetime):>10,} rows")
    logger.info(f"fact_grid_ops:           {len(fact_grid_ops):>10,} rows")
    logger.info(f"{'='*60}")

    return {
        "dim_balancing_authority": dim_ba,
        "dim_fuel_type": dim_fuel,
        "dim_datetime": dim_datetime,
        "fact_grid_ops": fact_grid_ops
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

    fact = schema["fact_grid_ops"]

    # Check for orphan dimension keys
    for dim_col in ["ba_id", "datetime_id"]:
        if dim_col in fact.columns:
            orphans = fact[fact[dim_col].isna()]
            results["checks"].append({
                "check": f"orphan_{dim_col}",
                "passed": len(orphans) < len(fact) * 0.1,
                "count": len(orphans),
                "percentage": round(len(orphans) / len(fact) * 100, 2)
            })

    # Check value nulls
    null_values = fact["value"].isna().sum()
    results["checks"].append({
        "check": "null_values",
        "passed": null_values < len(fact) * 0.05,
        "count": null_values,
        "percentage": round(null_values / len(fact) * 100, 2)
    })

    # Overall
    results["passed"] = all(c["passed"] for c in results["checks"])

    for check in results["checks"]:
        status = "PASS" if check["passed"] else "WARN"
        logger.info(f"  [{status}] {check['check']}: {check.get('count', 'N/A')} ({check.get('percentage', 0)}%)")

    return results


if __name__ == "__main__":
    test_data = pd.read_parquet("./data/raw_grid.parquet")
    print(f"Loaded {len(test_data):,} raw readings")

    schema = transform_to_star_schema(test_data)
    validation = validate_star_schema(schema)

    print(f"\nValidation: {'PASSED' if validation['passed'] else 'WARNINGS'}")
