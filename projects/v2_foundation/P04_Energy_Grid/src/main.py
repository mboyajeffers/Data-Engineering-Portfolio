#!/usr/bin/env python3
"""
Energy Grid Intelligence Pipeline - Main Orchestrator
Author: Mboya Jeffers

Enterprise-scale energy grid pipeline processing 500K+ readings.

Usage:
    python main.py --mode full        # Full 500K+ extraction
    python main.py --mode test        # Test with 100K readings
    python main.py --mode report      # Generate reports from existing data
"""

import argparse
import os
import sys
from datetime import datetime
import pandas as pd
import logging

# Add src to path
sys.path.insert(0, os.path.dirname(__file__))

from extract import extract_energy_grid_data
from transform import transform_to_star_schema, validate_star_schema
from analytics import generate_all_analytics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pipeline configuration
CONFIG = {
    "full": {
        "min_readings": 500_000,
        "months_back": 24
    },
    "test": {
        "min_readings": 100_000,
        "months_back": 6
    },
    "quick": {
        "min_readings": 50_000,
        "months_back": 1
    }
}


def run_extraction(mode: str, data_dir: str) -> pd.DataFrame:
    """Run extraction phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 1: EXTRACTION")
    logger.info(f"{'='*60}")

    config = CONFIG.get(mode, CONFIG["test"])

    raw_path = os.path.join(data_dir, "raw_grid.parquet")

    # Check if we already have data
    if os.path.exists(raw_path):
        existing = pd.read_parquet(raw_path)
        if len(existing) >= config["min_readings"]:
            logger.info(f"Using existing data: {len(existing):,} readings")
            return existing
        logger.info(f"Existing data ({len(existing):,}) below target, re-extracting...")

    df = extract_energy_grid_data(
        output_path=raw_path,
        min_readings=config["min_readings"],
        months_back=config["months_back"]
    )

    return df


def run_transformation(df: pd.DataFrame, output_dir: str) -> dict:
    """Run transformation phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 2: TRANSFORMATION")
    logger.info(f"{'='*60}")

    schema = transform_to_star_schema(df)

    # Validate
    validation = validate_star_schema(schema)

    # Save schema tables
    for name, table in schema.items():
        path = os.path.join(output_dir, f"{name}.parquet")
        table.to_parquet(path, index=False)
        logger.info(f"Saved {name}: {len(table):,} rows -> {path}")

    return schema


def run_analytics(schema: dict, output_dir: str) -> dict:
    """Run analytics phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 3: ANALYTICS")
    logger.info(f"{'='*60}")

    analytics = generate_all_analytics(schema)

    # Save analytics tables
    for name, df in analytics.items():
        if df is not None and len(df) > 0:
            csv_path = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved {name}: {len(df):,} rows -> {csv_path}")

    return analytics


def generate_reports(schema: dict, analytics: dict, output_dir: str):
    """Generate summary reports."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 4: REPORTING")
    logger.info(f"{'='*60}")

    # Top BAs by demand
    if "demand_analysis" in analytics:
        top_demand = analytics["demand_analysis"].nlargest(20, "peak_demand")
        top_demand.to_csv(os.path.join(output_dir, "top_demand_regions.csv"), index=False)
        logger.info(f"Generated top demand regions report: {len(top_demand)} BAs")

    # Generation mix
    if "generation_mix" in analytics:
        gen_mix = analytics["generation_mix"]
        gen_mix.to_csv(os.path.join(output_dir, "generation_mix_report.csv"), index=False)
        logger.info(f"Generated generation mix report: {len(gen_mix)} fuel types")

    # Renewable report
    if "renewable_analysis" in analytics and len(analytics["renewable_analysis"]) > 0:
        renewable = analytics["renewable_analysis"].nlargest(20, "renewable_pct")
        renewable.to_csv(os.path.join(output_dir, "renewable_leaders.csv"), index=False)
        logger.info(f"Generated renewable leaders report: {len(renewable)} BAs")

    # Summary report
    summary = generate_summary_report(schema, analytics)
    summary_path = os.path.join(output_dir, "pipeline_summary.txt")
    with open(summary_path, 'w') as f:
        f.write(summary)
    logger.info(f"Generated summary report: {summary_path}")


def generate_summary_report(schema: dict, analytics: dict) -> str:
    """Generate text summary of pipeline results."""
    fact = schema["fact_grid_ops"]
    ba_count = len(schema["dim_balancing_authority"])
    fuel_count = len(schema["dim_fuel_type"])
    datetime_count = len(schema["dim_datetime"])
    reading_count = len(fact)

    total_demand = fact["demand_mw"].sum() if "demand_mw" in fact.columns else 0
    total_generation = fact["generation_mw"].sum() if "generation_mw" in fact.columns else 0

    summary = f"""
{'='*70}
ENERGY GRID INTELLIGENCE PIPELINE - EXECUTION SUMMARY
{'='*70}

Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Author: Mboya Jeffers

DATA SCALE
{'-'*70}
Total Readings Processed:  {reading_count:>15,}
Total Demand (MW):         {total_demand:>15,.0f}
Total Generation (MW):     {total_generation:>15,.0f}
Balancing Authorities:     {ba_count:>15,}
Fuel Types:                {fuel_count:>15,}
Time Periods:              {datetime_count:>15,}

STAR SCHEMA
{'-'*70}
dim_balancing_authority:   {len(schema['dim_balancing_authority']):>15,} rows
dim_fuel_type:             {len(schema['dim_fuel_type']):>15,} rows
dim_datetime:              {len(schema['dim_datetime']):>15,} rows
fact_grid_ops:             {len(schema['fact_grid_ops']):>15,} rows

ANALYTICS GENERATED
{'-'*70}
"""

    for name, df in analytics.items():
        if df is not None and len(df) > 0:
            summary += f"{name:25} {len(df):>10,} rows\n"

    summary += f"""
{'='*70}
DATA SOURCE
{'-'*70}
Source: EIA-930 Hourly Electric Grid Monitor
URL: eia.gov/opendata
Documentation: EIA Open Data API
All data publicly verifiable at eia.gov

{'='*70}
"""

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Energy Grid Intelligence Pipeline"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "test", "quick", "report"],
        default="test",
        help="Execution mode: full (500K+), test (100K), quick (50K), report (existing data)"
    )
    parser.add_argument(
        "--data-dir",
        default="./data",
        help="Data directory"
    )
    parser.add_argument(
        "--output-dir",
        default="./output",
        help="Output directory"
    )

    args = parser.parse_args()

    # Create directories
    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)

    logger.info(f"\n{'#'*70}")
    logger.info("ENERGY GRID INTELLIGENCE PIPELINE")
    logger.info(f"Mode: {args.mode.upper()}")
    logger.info(f"Target: {CONFIG.get(args.mode, {}).get('min_readings', 'N/A'):,} readings")
    logger.info(f"{'#'*70}")

    start_time = datetime.now()

    # Run pipeline phases
    if args.mode == "report":
        # Load existing data
        df = pd.read_parquet(os.path.join(args.data_dir, "raw_grid.parquet"))
        schema = {
            "dim_balancing_authority": pd.read_parquet(os.path.join(args.output_dir, "dim_balancing_authority.parquet")),
            "dim_fuel_type": pd.read_parquet(os.path.join(args.output_dir, "dim_fuel_type.parquet")),
            "dim_datetime": pd.read_parquet(os.path.join(args.output_dir, "dim_datetime.parquet")),
            "fact_grid_ops": pd.read_parquet(os.path.join(args.output_dir, "fact_grid_ops.parquet")),
        }
        analytics = generate_all_analytics(schema)
    else:
        # Full pipeline
        df = run_extraction(args.mode, args.data_dir)
        schema = run_transformation(df, args.output_dir)
        analytics = run_analytics(schema, args.output_dir)

    # Generate reports
    generate_reports(schema, analytics, args.output_dir)

    # Final summary
    elapsed = datetime.now() - start_time
    logger.info(f"\n{'='*70}")
    logger.info("PIPELINE COMPLETE")
    logger.info(f"{'='*70}")
    logger.info(f"Total readings processed: {len(schema['fact_grid_ops']):,}")
    logger.info(f"Balancing authorities: {len(schema['dim_balancing_authority']):,}")
    logger.info(f"Fuel types: {len(schema['dim_fuel_type']):,}")
    logger.info(f"Elapsed time: {elapsed}")
    logger.info(f"Output: {args.output_dir}")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    main()
