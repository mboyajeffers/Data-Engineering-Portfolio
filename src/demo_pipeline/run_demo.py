#!/usr/bin/env python3
"""
Demo Pipeline Runner
====================

Orchestrates the full pipeline: ingest -> validate -> transform -> metrics -> report.

Usage:
    python -m demo_pipeline.run_demo [--input PATH] [--output-dir PATH]

Synthetic demo output for portfolio purposes.
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

import yaml

from .ingest import ingest_file, ingest_metadata
from .validate import validate_dataframe, ValidationReport
from .transform import transform_dataframe
from .metrics import compute_all_kpis
from .report_stub import generate_scorecard_markdown, generate_metrics_json


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s | %(levelname)-7s | %(name)s | %(message)s',
        datefmt='%H:%M:%S',
    )


def load_config(config_dir: Path) -> tuple[dict, dict]:
    """Load schema and validation configs from YAML files."""
    schema_path = config_dir / 'schema_contract.yaml'
    rules_path = config_dir / 'validation_rules.yaml'

    schema = {}
    rules = {}

    if schema_path.exists():
        with open(schema_path) as f:
            schema = yaml.safe_load(f) or {}

    if rules_path.exists():
        with open(rules_path) as f:
            rules = yaml.safe_load(f) or {}

    return schema, rules


def run_pipeline(
    input_file: Path,
    output_dir: Path,
    config_dir: Path,
    verbose: bool = False
) -> dict:
    """
    Execute the full demo pipeline.

    Args:
        input_file: Path to input CSV/JSON file
        output_dir: Directory for output artifacts
        config_dir: Directory containing config YAML files
        verbose: Enable debug logging

    Returns:
        Dict with pipeline results summary
    """
    setup_logging(verbose)
    logger = logging.getLogger('demo_pipeline')

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("DEMO PIPELINE START")
    logger.info("=" * 60)

    # Load configuration
    logger.info("Loading configuration...")
    schema, rules = load_config(config_dir)

    # Stage 1: Ingest
    logger.info("-" * 40)
    logger.info("STAGE 1: INGEST")
    logger.info("-" * 40)
    df = ingest_file(input_file)
    metadata = ingest_metadata(df, input_file)
    logger.info(f"  Rows: {metadata['row_count']:,}")
    logger.info(f"  Columns: {metadata['column_count']}")
    logger.info(f"  Memory: {metadata['memory_usage_mb']:.2f} MB")

    # Stage 2: Validate
    logger.info("-" * 40)
    logger.info("STAGE 2: VALIDATE")
    logger.info("-" * 40)
    valid_df, quarantine_df, validation = validate_dataframe(df, schema, rules)
    logger.info(f"  Valid: {validation.valid_rows:,}")
    logger.info(f"  Quarantined: {validation.quarantined_rows:,}")
    logger.info(f"  Blockers: {validation.blocker_count}")
    logger.info(f"  Warnings: {validation.warning_count}")

    # Stage 3: Transform
    logger.info("-" * 40)
    logger.info("STAGE 3: TRANSFORM")
    logger.info("-" * 40)
    transformed_df = transform_dataframe(valid_df)
    logger.info(f"  Output columns: {len(transformed_df.columns)}")

    # Stage 4: Metrics
    logger.info("-" * 40)
    logger.info("STAGE 4: COMPUTE METRICS")
    logger.info("-" * 40)
    metrics = compute_all_kpis(transformed_df)
    logger.info(f"  KPIs computed: {len(metrics.kpis)}")

    # Stage 5: Export
    logger.info("-" * 40)
    logger.info("STAGE 5: EXPORT")
    logger.info("-" * 40)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate scorecard
    scorecard_path = output_dir / 'Demo_Pipeline_Run_Scorecard.md'
    generate_scorecard_markdown(validation, metrics, input_file.name, scorecard_path)

    # Generate metrics JSON
    metrics_path = output_dir / 'metrics.json'
    generate_metrics_json(metrics, metrics_path)

    # Export cleaned dataset (sample)
    cleaned_path = output_dir / 'cleaned_sample.csv'
    transformed_df.head(100).to_csv(cleaned_path, index=False)
    logger.info(f"  Exported sample to {cleaned_path.name}")

    elapsed = (datetime.now() - start_time).total_seconds()

    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed:.2f}s")
    logger.info("=" * 60)

    # Print key results
    print("\n" + "=" * 60)
    print("DEMO PIPELINE RESULTS")
    print("=" * 60)
    print(f"Input:       {input_file.name}")
    print(f"Rows:        {metadata['row_count']:,} -> {validation.valid_rows:,} valid")
    print(f"Validation:  {'PASSED' if validation.passed else 'FAILED'}")
    print(f"KPIs:        {len(metrics.kpis)} computed")
    print("-" * 60)

    # Show key KPIs
    for kpi in metrics.kpis[:5]:  # Show first 5
        if not isinstance(kpi.value, dict):
            if kpi.unit == 'USD':
                print(f"  {kpi.name}: ${kpi.value:,.2f}")
            elif kpi.unit == '%':
                print(f"  {kpi.name}: {kpi.value:.2f}%")
            else:
                print(f"  {kpi.name}: {kpi.value:,}")

    print("-" * 60)
    print(f"Outputs:     {output_dir}")
    print(f"  - {scorecard_path.name}")
    print(f"  - {metrics_path.name}")
    print(f"  - {cleaned_path.name}")
    print("=" * 60 + "\n")

    return {
        'status': 'success' if validation.passed else 'failed',
        'input_rows': metadata['row_count'],
        'valid_rows': validation.valid_rows,
        'kpis_computed': len(metrics.kpis),
        'elapsed_seconds': elapsed,
    }


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Run the demo analytics pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--input', '-i',
        type=Path,
        default=Path('data/sample_luxury_jewelry_transactions.csv'),
        help='Input data file (CSV/JSON)',
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=Path,
        default=Path('docs/outputs/md'),
        help='Output directory for artifacts',
    )
    parser.add_argument(
        '--config-dir', '-c',
        type=Path,
        default=Path('configs'),
        help='Directory containing config YAML files',
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable debug logging',
    )

    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    try:
        result = run_pipeline(
            input_file=args.input,
            output_dir=args.output_dir,
            config_dir=args.config_dir,
            verbose=args.verbose,
        )
        sys.exit(0 if result['status'] == 'success' else 1)
    except Exception as e:
        print(f"Pipeline error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
