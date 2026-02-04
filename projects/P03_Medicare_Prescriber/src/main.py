#!/usr/bin/env python3
"""
Medicare Prescriber Intelligence Pipeline - Main Orchestrator
Author: Mboya Jeffers

Enterprise-scale Medicare Part D pipeline processing 5M+ prescriptions.

Usage:
    python main.py --mode full        # Full 5M+ extraction
    python main.py --mode test        # Test with 500K records
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

from extract import extract_medicare_prescriptions
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
        "min_records": 5_000_000,
        "years": [2022, 2021, 2020, 2019]
    },
    "test": {
        "min_records": 500_000,
        "years": [2022, 2021]
    },
    "quick": {
        "min_records": 100_000,
        "years": [2022]
    }
}


def run_extraction(mode: str, data_dir: str) -> pd.DataFrame:
    """Run extraction phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 1: EXTRACTION")
    logger.info(f"{'='*60}")

    config = CONFIG.get(mode, CONFIG["test"])

    raw_path = os.path.join(data_dir, "raw_prescriptions.parquet")

    # Check if we already have data
    if os.path.exists(raw_path):
        existing = pd.read_parquet(raw_path)
        if len(existing) >= config["min_records"]:
            logger.info(f"Using existing data: {len(existing):,} records")
            return existing
        logger.info(f"Existing data ({len(existing):,}) below target, re-extracting...")

    df = extract_medicare_prescriptions(
        output_path=raw_path,
        min_records=config["min_records"],
        years=config["years"]
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

    # Top prescribers report
    if "prescriber_summary" in analytics:
        top_prescribers = analytics["prescriber_summary"].nlargest(100, "total_claims")
        top_prescribers.to_csv(os.path.join(output_dir, "top_prescribers.csv"), index=False)
        logger.info(f"Generated top prescribers report: {len(top_prescribers)} providers")

    # Top drugs report
    if "drug_utilization" in analytics:
        top_drugs = analytics["drug_utilization"].nlargest(100, "total_claims")
        top_drugs.to_csv(os.path.join(output_dir, "top_drugs.csv"), index=False)
        logger.info(f"Generated top drugs report: {len(top_drugs)} drugs")

    # Opioid report
    if "opioid_analysis" in analytics and len(analytics["opioid_analysis"]) > 0:
        opioid_report = analytics["opioid_analysis"].nlargest(100, "opioid_claims")
        opioid_report.to_csv(os.path.join(output_dir, "opioid_report.csv"), index=False)
        logger.info(f"Generated opioid report: {len(opioid_report)} prescribers")

    # Summary report
    summary = generate_summary_report(schema, analytics)
    summary_path = os.path.join(output_dir, "pipeline_summary.txt")
    with open(summary_path, 'w') as f:
        f.write(summary)
    logger.info(f"Generated summary report: {summary_path}")


def generate_summary_report(schema: dict, analytics: dict) -> str:
    """Generate text summary of pipeline results."""
    fact = schema["fact_prescriptions"]
    record_count = len(fact)
    prescriber_count = len(schema["dim_prescriber"])
    drug_count = len(schema["dim_drug"])
    location_count = len(schema["dim_location"])

    total_claims = fact["total_claims"].sum() if "total_claims" in fact.columns else 0
    total_cost = fact["total_drug_cost"].sum() if "total_drug_cost" in fact.columns else 0

    summary = f"""
{'='*70}
MEDICARE PRESCRIBER INTELLIGENCE PIPELINE - EXECUTION SUMMARY
{'='*70}

Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Author: Mboya Jeffers

DATA SCALE
{'-'*70}
Total Records Processed:   {record_count:>15,}
Total Prescription Claims: {total_claims:>15,.0f}
Total Drug Cost:           ${total_cost:>14,.0f}
Unique Prescribers:        {prescriber_count:>15,}
Unique Drugs:              {drug_count:>15,}
Locations:                 {location_count:>15,}

STAR SCHEMA
{'-'*70}
dim_prescriber:            {len(schema['dim_prescriber']):>15,} rows
dim_drug:                  {len(schema['dim_drug']):>15,} rows
dim_location:              {len(schema['dim_location']):>15,} rows
dim_year:                  {len(schema['dim_year']):>15,} rows
fact_prescriptions:        {len(schema['fact_prescriptions']):>15,} rows

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
Source: CMS Medicare Part D Public Use Files
URL: data.cms.gov
Documentation: CMS Provider Summary by Type of Service
All data publicly verifiable at cms.gov

{'='*70}
"""

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Medicare Prescriber Intelligence Pipeline"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "test", "quick", "report"],
        default="test",
        help="Execution mode: full (5M+), test (500K), quick (100K), report (existing data)"
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
    logger.info("MEDICARE PRESCRIBER INTELLIGENCE PIPELINE")
    logger.info(f"Mode: {args.mode.upper()}")
    logger.info(f"Target: {CONFIG.get(args.mode, {}).get('min_records', 'N/A'):,} records")
    logger.info(f"{'#'*70}")

    start_time = datetime.now()

    # Run pipeline phases
    if args.mode == "report":
        # Load existing data
        df = pd.read_parquet(os.path.join(args.data_dir, "raw_prescriptions.parquet"))
        schema = {
            "dim_prescriber": pd.read_parquet(os.path.join(args.output_dir, "dim_prescriber.parquet")),
            "dim_drug": pd.read_parquet(os.path.join(args.output_dir, "dim_drug.parquet")),
            "dim_location": pd.read_parquet(os.path.join(args.output_dir, "dim_location.parquet")),
            "dim_year": pd.read_parquet(os.path.join(args.output_dir, "dim_year.parquet")),
            "fact_prescriptions": pd.read_parquet(os.path.join(args.output_dir, "fact_prescriptions.parquet")),
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
    logger.info(f"Total records processed: {len(schema['fact_prescriptions']):,}")
    logger.info(f"Total prescribers: {len(schema['dim_prescriber']):,}")
    logger.info(f"Total drugs: {len(schema['dim_drug']):,}")
    logger.info(f"Elapsed time: {elapsed}")
    logger.info(f"Output: {args.output_dir}")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    main()
