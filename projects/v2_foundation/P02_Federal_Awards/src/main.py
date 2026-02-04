#!/usr/bin/env python3
"""
Federal Awards Intelligence Pipeline - Main Orchestrator
Author: Mboya Jeffers

Enterprise-scale federal spending pipeline processing 1M+ awards.

Usage:
    python main.py --mode full        # Full 1M+ extraction
    python main.py --mode test        # Test with 100K awards
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

from extract import extract_federal_awards
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
        "min_awards": 1_000_000,
        "fiscal_years": [2021, 2022, 2023, 2024]
    },
    "test": {
        "min_awards": 100_000,
        "fiscal_years": [2023, 2024]
    },
    "quick": {
        "min_awards": 50_000,
        "fiscal_years": [2024]
    }
}


def run_extraction(mode: str, data_dir: str) -> pd.DataFrame:
    """Run extraction phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 1: EXTRACTION")
    logger.info(f"{'='*60}")

    config = CONFIG.get(mode, CONFIG["test"])

    raw_path = os.path.join(data_dir, "raw_awards.parquet")

    # Check if we already have data
    if os.path.exists(raw_path):
        existing = pd.read_parquet(raw_path)
        if len(existing) >= config["min_awards"]:
            logger.info(f"Using existing data: {len(existing):,} awards")
            return existing
        logger.info(f"Existing data ({len(existing):,}) below target, re-extracting...")

    awards_df = extract_federal_awards(
        output_path=raw_path,
        min_awards=config["min_awards"],
        fiscal_years=config["fiscal_years"]
    )

    return awards_df


def run_transformation(awards_df: pd.DataFrame, output_dir: str) -> dict:
    """Run transformation phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 2: TRANSFORMATION")
    logger.info(f"{'='*60}")

    schema = transform_to_star_schema(awards_df)

    # Validate
    validation = validate_star_schema(schema)

    # Save schema tables
    for name, df in schema.items():
        path = os.path.join(output_dir, f"{name}.parquet")
        df.to_parquet(path, index=False)
        logger.info(f"Saved {name}: {len(df):,} rows -> {path}")

    return schema


def run_analytics(schema: dict, output_dir: str) -> dict:
    """Run analytics phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 3: ANALYTICS")
    logger.info(f"{'='*60}")

    analytics = generate_all_analytics(schema)

    # Save analytics tables
    for name, df in analytics.items():
        csv_path = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved {name}: {len(df):,} rows -> {csv_path}")

    return analytics


def generate_reports(schema: dict, analytics: dict, output_dir: str):
    """Generate summary reports."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 4: REPORTING")
    logger.info(f"{'='*60}")

    # Top agencies report
    if "agency_spending" in analytics:
        top_agencies = analytics["agency_spending"].nlargest(50, "total_obligated")
        top_agencies.to_csv(os.path.join(output_dir, "top_agencies.csv"), index=False)
        logger.info(f"Generated top agencies report: {len(top_agencies)} agencies")

    # Top recipients report
    if "recipient_rankings" in analytics:
        top_recipients = analytics["recipient_rankings"].nlargest(100, "total_awards")
        top_recipients.to_csv(os.path.join(output_dir, "top_recipients.csv"), index=False)
        logger.info(f"Generated top recipients report: {len(top_recipients)} recipients")

    # Summary report
    summary = generate_summary_report(schema, analytics)
    summary_path = os.path.join(output_dir, "pipeline_summary.txt")
    with open(summary_path, 'w') as f:
        f.write(summary)
    logger.info(f"Generated summary report: {summary_path}")


def generate_summary_report(schema: dict, analytics: dict) -> str:
    """Generate text summary of pipeline results."""
    award_count = len(schema["fact_awards"])
    agency_count = len(schema["dim_agency"])
    recipient_count = len(schema["dim_recipient"])
    location_count = len(schema["dim_location"])
    date_count = len(schema["dim_date"])

    total_spending = schema["fact_awards"]["award_amount"].sum()

    summary = f"""
{'='*70}
FEDERAL AWARDS INTELLIGENCE PIPELINE - EXECUTION SUMMARY
{'='*70}

Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Author: Mboya Jeffers

DATA SCALE
{'-'*70}
Total Awards Processed:    {award_count:>15,}
Total Spending:            ${total_spending:>14,.0f}
Agencies:                  {agency_count:>15,}
Recipients:                {recipient_count:>15,}
Locations:                 {location_count:>15,}
Date Periods:              {date_count:>15,}

STAR SCHEMA
{'-'*70}
dim_agency:                {len(schema['dim_agency']):>15,} rows
dim_recipient:             {len(schema['dim_recipient']):>15,} rows
dim_location:              {len(schema['dim_location']):>15,} rows
dim_date:                  {len(schema['dim_date']):>15,} rows
fact_awards:               {len(schema['fact_awards']):>15,} rows

ANALYTICS GENERATED
{'-'*70}
"""

    for name, df in analytics.items():
        summary += f"{name:25} {len(df):>10,} rows\n"

    summary += f"""
{'='*70}
DATA SOURCE
{'-'*70}
Source: USASpending.gov
API: Award Search API v2
Documentation: api.usaspending.gov
All data publicly verifiable at usaspending.gov

{'='*70}
"""

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Federal Awards Intelligence Pipeline"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "test", "quick", "report"],
        default="test",
        help="Execution mode: full (1M+), test (100K), quick (50K), report (existing data)"
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
    logger.info("FEDERAL AWARDS INTELLIGENCE PIPELINE")
    logger.info(f"Mode: {args.mode.upper()}")
    logger.info(f"Target: {CONFIG.get(args.mode, {}).get('min_awards', 'N/A'):,} awards")
    logger.info(f"{'#'*70}")

    start_time = datetime.now()

    # Run pipeline phases
    if args.mode == "report":
        # Load existing data
        awards_df = pd.read_parquet(os.path.join(args.data_dir, "raw_awards.parquet"))
        schema = {
            "dim_agency": pd.read_parquet(os.path.join(args.output_dir, "dim_agency.parquet")),
            "dim_recipient": pd.read_parquet(os.path.join(args.output_dir, "dim_recipient.parquet")),
            "dim_location": pd.read_parquet(os.path.join(args.output_dir, "dim_location.parquet")),
            "dim_date": pd.read_parquet(os.path.join(args.output_dir, "dim_date.parquet")),
            "fact_awards": pd.read_parquet(os.path.join(args.output_dir, "fact_awards.parquet")),
        }
        analytics = generate_all_analytics(schema)
    else:
        # Full pipeline
        awards_df = run_extraction(args.mode, args.data_dir)
        schema = run_transformation(awards_df, args.output_dir)
        analytics = run_analytics(schema, args.output_dir)

    # Generate reports
    generate_reports(schema, analytics, args.output_dir)

    # Final summary
    elapsed = datetime.now() - start_time
    logger.info(f"\n{'='*70}")
    logger.info("PIPELINE COMPLETE")
    logger.info(f"{'='*70}")
    logger.info(f"Total awards processed: {len(schema['fact_awards']):,}")
    logger.info(f"Total spending: ${schema['fact_awards']['award_amount'].sum():,.0f}")
    logger.info(f"Total agencies: {len(schema['dim_agency']):,}")
    logger.info(f"Total recipients: {len(schema['dim_recipient']):,}")
    logger.info(f"Elapsed time: {elapsed}")
    logger.info(f"Output: {args.output_dir}")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    main()
