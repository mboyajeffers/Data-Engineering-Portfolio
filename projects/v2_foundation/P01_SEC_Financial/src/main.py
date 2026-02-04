#!/usr/bin/env python3
"""
SEC Financial Intelligence Pipeline - Main Orchestrator
Author: Mboya Jeffers

Enterprise-scale financial data pipeline processing 1M+ SEC XBRL facts.

Usage:
    python main.py --mode full        # Full 1M+ extraction
    python main.py --mode test        # Test with 100K facts
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

from extract import extract_sec_financial_facts
from transform import transform_to_star_schema, validate_star_schema
from kpis import calculate_all_kpis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pipeline configuration
CONFIG = {
    "full": {
        "min_facts": 1_000_000,
        "company_limit": None
    },
    "test": {
        "min_facts": 100_000,
        "company_limit": 200
    },
    "quick": {
        "min_facts": 50_000,
        "company_limit": 100
    }
}


def run_extraction(mode: str, data_dir: str) -> pd.DataFrame:
    """Run extraction phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 1: EXTRACTION")
    logger.info(f"{'='*60}")

    config = CONFIG.get(mode, CONFIG["test"])

    raw_path = os.path.join(data_dir, "raw_facts.parquet")

    # Check if we already have data
    if os.path.exists(raw_path):
        existing = pd.read_parquet(raw_path)
        if len(existing) >= config["min_facts"]:
            logger.info(f"Using existing data: {len(existing):,} facts")
            return existing
        logger.info(f"Existing data ({len(existing):,}) below target, re-extracting...")

    facts_df = extract_sec_financial_facts(
        output_path=raw_path,
        min_facts=config["min_facts"],
        company_limit=config["company_limit"]
    )

    return facts_df


def run_transformation(facts_df: pd.DataFrame, output_dir: str) -> dict:
    """Run transformation phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 2: TRANSFORMATION")
    logger.info(f"{'='*60}")

    schema = transform_to_star_schema(facts_df)

    # Validate
    validation = validate_star_schema(schema)

    # Save schema tables
    for name, df in schema.items():
        path = os.path.join(output_dir, f"{name}.parquet")
        df.to_parquet(path, index=False)
        logger.info(f"Saved {name}: {len(df):,} rows -> {path}")

    return schema


def run_kpi_calculation(schema: dict, output_dir: str) -> pd.DataFrame:
    """Run KPI calculation phase."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 3: KPI CALCULATION")
    logger.info(f"{'='*60}")

    kpis_df = calculate_all_kpis(schema)

    # Save KPIs
    kpi_path = os.path.join(output_dir, "kpi_calculations.parquet")
    kpis_df.to_parquet(kpi_path, index=False)
    logger.info(f"Saved KPIs: {len(kpis_df):,} rows -> {kpi_path}")

    # Also save as CSV for easy viewing
    csv_path = os.path.join(output_dir, "kpi_calculations.csv")
    kpis_df.to_csv(csv_path, index=False)

    return kpis_df


def generate_reports(schema: dict, kpis_df: pd.DataFrame, output_dir: str):
    """Generate analysis reports."""
    logger.info(f"\n{'='*60}")
    logger.info("PHASE 4: REPORTING")
    logger.info(f"{'='*60}")

    # Top companies by various metrics
    if "roe" in kpis_df.columns:
        top_roe = kpis_df.dropna(subset=["roe"]).nlargest(50, "roe")
        top_roe.to_csv(os.path.join(output_dir, "top_companies_by_roe.csv"), index=False)
        logger.info(f"Generated top ROE report: {len(top_roe)} companies")

    if "net_margin" in kpis_df.columns:
        top_margin = kpis_df.dropna(subset=["net_margin"]).nlargest(50, "net_margin")
        top_margin.to_csv(os.path.join(output_dir, "top_companies_by_margin.csv"), index=False)
        logger.info(f"Generated top margin report: {len(top_margin)} companies")

    # Summary report
    summary = generate_summary_report(schema, kpis_df)
    summary_path = os.path.join(output_dir, "pipeline_summary.txt")
    with open(summary_path, 'w') as f:
        f.write(summary)
    logger.info(f"Generated summary report: {summary_path}")


def generate_summary_report(schema: dict, kpis_df: pd.DataFrame) -> str:
    """Generate text summary of pipeline results."""
    fact_count = len(schema["fact_financials"])
    company_count = len(schema["dim_company"])
    metric_count = len(schema["dim_metric"])
    period_count = len(schema["dim_date"])
    kpi_count = len(kpis_df)

    summary = f"""
{'='*70}
SEC FINANCIAL INTELLIGENCE PIPELINE - EXECUTION SUMMARY
{'='*70}

Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Author: Mboya Jeffers

DATA SCALE
{'-'*70}
Total Financial Facts:     {fact_count:>15,}
Companies Processed:       {company_count:>15,}
Unique Metrics:            {metric_count:>15,}
Fiscal Periods:            {period_count:>15,}
KPI Calculations:          {kpi_count:>15,}

STAR SCHEMA
{'-'*70}
dim_company:               {len(schema['dim_company']):>15,} rows
dim_metric:                {len(schema['dim_metric']):>15,} rows
dim_date:                  {len(schema['dim_date']):>15,} rows
fact_financials:           {len(schema['fact_financials']):>15,} rows

KPI COVERAGE (where calculable)
{'-'*70}
"""

    kpi_columns = [c for c in kpis_df.columns if c not in ['company_id', 'date_id', 'entity_name', 'fiscal_year', 'fiscal_period']]
    for col in kpi_columns:
        non_null = kpis_df[col].notna().sum()
        pct = non_null / len(kpis_df) * 100 if len(kpis_df) > 0 else 0
        summary += f"{col:25} {non_null:>10,} ({pct:5.1f}%)\n"

    summary += f"""
{'='*70}
DATA SOURCE
{'-'*70}
Source: SEC EDGAR (data.sec.gov)
API: Company Facts XBRL API
All data publicly verifiable at sec.gov

{'='*70}
"""

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="SEC Financial Intelligence Pipeline"
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
    logger.info("SEC FINANCIAL INTELLIGENCE PIPELINE")
    logger.info(f"Mode: {args.mode.upper()}")
    logger.info(f"Target: {CONFIG.get(args.mode, {}).get('min_facts', 'N/A'):,} facts")
    logger.info(f"{'#'*70}")

    start_time = datetime.now()

    # Run pipeline phases
    if args.mode == "report":
        # Load existing data
        facts_df = pd.read_parquet(os.path.join(args.data_dir, "raw_facts.parquet"))
        schema = {
            "dim_company": pd.read_parquet(os.path.join(args.output_dir, "dim_company.parquet")),
            "dim_metric": pd.read_parquet(os.path.join(args.output_dir, "dim_metric.parquet")),
            "dim_date": pd.read_parquet(os.path.join(args.output_dir, "dim_date.parquet")),
            "fact_financials": pd.read_parquet(os.path.join(args.output_dir, "fact_financials.parquet")),
        }
        kpis_df = pd.read_parquet(os.path.join(args.output_dir, "kpi_calculations.parquet"))
    else:
        # Full pipeline
        facts_df = run_extraction(args.mode, args.data_dir)
        schema = run_transformation(facts_df, args.output_dir)
        kpis_df = run_kpi_calculation(schema, args.output_dir)

    # Generate reports
    generate_reports(schema, kpis_df, args.output_dir)

    # Final summary
    elapsed = datetime.now() - start_time
    logger.info(f"\n{'='*70}")
    logger.info("PIPELINE COMPLETE")
    logger.info(f"{'='*70}")
    logger.info(f"Total facts processed: {len(schema['fact_financials']):,}")
    logger.info(f"Total companies: {len(schema['dim_company']):,}")
    logger.info(f"Total KPI records: {len(kpis_df):,}")
    logger.info(f"Elapsed time: {elapsed}")
    logger.info(f"Output: {args.output_dir}")
    logger.info(f"{'='*70}")


if __name__ == "__main__":
    main()
