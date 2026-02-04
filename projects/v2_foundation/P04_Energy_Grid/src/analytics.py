#!/usr/bin/env python3
"""
Energy Grid Analytics
Author: Mboya Jeffers

Generates grid operations analytics from EIA-930 data.
"""

import pandas as pd
import numpy as np
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def calculate_demand_analysis(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Calculate demand patterns by region and time.

    Returns:
        DataFrame with demand analytics
    """
    fact = schema["fact_grid_ops"]
    dim_ba = schema["dim_balancing_authority"]
    dim_datetime = schema["dim_datetime"]

    # Filter to demand records
    demand = fact[fact["data_type"] == "demand"].copy()

    if len(demand) == 0:
        demand = fact[fact["demand_mw"].notna()].copy()

    # Aggregate by BA and date
    demand_summary = demand.groupby("ba_id").agg({
        "demand_mw": ["mean", "max", "min", "std"],
        "value": ["sum", "count"]
    }).reset_index()

    demand_summary.columns = ["ba_id", "avg_demand", "peak_demand", "min_demand",
                              "demand_std", "total_demand", "reading_count"]

    # Calculate load factor
    demand_summary["load_factor"] = (
        demand_summary["avg_demand"] / demand_summary["peak_demand"]
    ).fillna(0)

    # Join BA info
    demand_summary = demand_summary.merge(dim_ba, on="ba_id", how="left")

    demand_summary = demand_summary.sort_values("peak_demand", ascending=False)

    logger.info(f"Demand analysis: {len(demand_summary)} BAs")

    return demand_summary


def calculate_generation_mix(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Calculate generation mix by fuel type.

    Returns:
        DataFrame with generation breakdown
    """
    fact = schema["fact_grid_ops"]
    dim_fuel = schema["dim_fuel_type"]
    dim_ba = schema["dim_balancing_authority"]

    # Filter to generation records
    gen = fact[fact["data_type"] == "generation"].copy()

    if len(gen) == 0:
        gen = fact[fact["generation_mw"].notna()].copy()

    # Aggregate by fuel type
    fuel_summary = gen.groupby("fuel_id").agg({
        "generation_mw": ["sum", "mean", "max"],
        "value": ["sum", "count"]
    }).reset_index()

    fuel_summary.columns = ["fuel_id", "total_gen", "avg_gen", "max_gen",
                            "total_value", "reading_count"]

    # Join fuel info
    fuel_summary = fuel_summary.merge(dim_fuel, on="fuel_id", how="left")

    # Calculate percentage
    total = fuel_summary["total_gen"].sum()
    fuel_summary["pct_of_total"] = fuel_summary["total_gen"] / total * 100

    fuel_summary = fuel_summary.sort_values("total_gen", ascending=False)

    logger.info(f"Generation mix: {len(fuel_summary)} fuel types")

    return fuel_summary


def calculate_renewable_analysis(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Analyze renewable energy generation.

    Returns:
        DataFrame with renewable metrics
    """
    fact = schema["fact_grid_ops"]
    dim_fuel = schema["dim_fuel_type"]
    dim_ba = schema["dim_balancing_authority"]

    # Get renewable fuel IDs
    renewable_fuels = dim_fuel[dim_fuel["is_renewable"] == True]["fuel_id"].tolist()

    if not renewable_fuels:
        logger.warning("No renewable fuels found")
        return pd.DataFrame()

    # Filter to generation records
    gen = fact[fact["data_type"] == "generation"].copy()

    # Calculate total and renewable by BA
    total_gen = gen.groupby("ba_id")["value"].sum().reset_index()
    total_gen.columns = ["ba_id", "total_generation"]

    renewable_gen = gen[gen["fuel_id"].isin(renewable_fuels)].groupby("ba_id")["value"].sum().reset_index()
    renewable_gen.columns = ["ba_id", "renewable_generation"]

    # Merge
    renewable_summary = total_gen.merge(renewable_gen, on="ba_id", how="left")
    renewable_summary["renewable_generation"] = renewable_summary["renewable_generation"].fillna(0)

    # Calculate percentage
    renewable_summary["renewable_pct"] = (
        renewable_summary["renewable_generation"] / renewable_summary["total_generation"] * 100
    ).fillna(0)

    # Join BA info
    renewable_summary = renewable_summary.merge(dim_ba, on="ba_id", how="left")

    renewable_summary = renewable_summary.sort_values("renewable_pct", ascending=False)

    logger.info(f"Renewable analysis: {len(renewable_summary)} BAs")

    return renewable_summary


def calculate_hourly_patterns(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Analyze hourly demand patterns.

    Returns:
        DataFrame with hourly patterns
    """
    fact = schema["fact_grid_ops"]
    dim_datetime = schema["dim_datetime"]

    # Filter to demand records
    demand = fact[fact["data_type"] == "demand"].copy()

    if len(demand) == 0:
        demand = fact[fact["demand_mw"].notna()].copy()

    # Join datetime info
    demand_with_time = demand.merge(
        dim_datetime[["datetime_id", "hour", "day_of_week", "is_peak_hour", "is_weekend"]],
        on="datetime_id",
        how="left"
    )

    # Aggregate by hour
    hourly = demand_with_time.groupby("hour").agg({
        "demand_mw": ["mean", "max", "min"],
        "value": ["mean", "sum"]
    }).reset_index()

    hourly.columns = ["hour", "avg_demand", "max_demand", "min_demand",
                      "avg_value", "total_value"]

    # Calculate peak factor
    overall_avg = hourly["avg_demand"].mean()
    hourly["peak_factor"] = hourly["avg_demand"] / overall_avg

    hourly = hourly.sort_values("hour")

    logger.info(f"Hourly patterns: {len(hourly)} hours")

    return hourly


def calculate_regional_summary(schema: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Calculate regional-level summary.

    Returns:
        DataFrame with regional metrics
    """
    fact = schema["fact_grid_ops"]
    dim_ba = schema["dim_balancing_authority"]

    # Aggregate by BA
    ba_summary = fact.groupby("ba_id").agg({
        "demand_mw": ["sum", "mean", "max"],
        "generation_mw": ["sum", "mean"],
        "value": ["sum", "count"]
    }).reset_index()

    ba_summary.columns = ["ba_id", "total_demand", "avg_demand", "peak_demand",
                          "total_generation", "avg_generation",
                          "total_value", "reading_count"]

    # Calculate net generation
    ba_summary["net_generation"] = ba_summary["total_generation"] - ba_summary["total_demand"]

    # Join BA info
    ba_summary = ba_summary.merge(dim_ba, on="ba_id", how="left")

    # Aggregate by region
    regional = ba_summary.groupby("region").agg({
        "total_demand": "sum",
        "peak_demand": "max",
        "total_generation": "sum",
        "ba_id": "count"
    }).reset_index()

    regional.columns = ["region", "total_demand", "peak_demand", "total_generation", "ba_count"]

    regional = regional.sort_values("total_demand", ascending=False)

    logger.info(f"Regional summary: {len(regional)} regions")

    return regional


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
        "demand_analysis": calculate_demand_analysis(schema),
        "generation_mix": calculate_generation_mix(schema),
        "renewable_analysis": calculate_renewable_analysis(schema),
        "hourly_patterns": calculate_hourly_patterns(schema),
        "regional_summary": calculate_regional_summary(schema)
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
        "dim_balancing_authority": pd.read_parquet("./output/dim_balancing_authority.parquet"),
        "dim_fuel_type": pd.read_parquet("./output/dim_fuel_type.parquet"),
        "dim_datetime": pd.read_parquet("./output/dim_datetime.parquet"),
        "fact_grid_ops": pd.read_parquet("./output/fact_grid_ops.parquet")
    }

    analytics = generate_all_analytics(schema)

    print("\nGeneration Mix:")
    print(analytics["generation_mix"][["fuel_name", "total_gen", "pct_of_total"]].head(10))
