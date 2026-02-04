"""
P05 Gaming Analytics - Main Pipeline Orchestrator
Enterprise-scale gaming data pipeline.

Author: Mboya Jeffers
Target: 8M+ records
Usage:
    python main.py --mode full    # Full extraction (8M+ rows)
    python main.py --mode test    # Test mode (sample data)
"""

import argparse
import json
import hashlib
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from extract import SteamDataExtractor, download_kaggle_reviews
from transform import GamingStarSchemaTransformer
from analytics import GamingAnalytics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GamingPipeline:
    """
    End-to-end gaming analytics pipeline.

    Stages:
    1. Extract - Pull data from Steam API, SteamSpy, Kaggle
    2. Transform - Build star schema dimensional model
    3. Analyze - Calculate gaming KPIs
    4. Evidence - Generate verification artifacts
    """

    def __init__(self, base_dir: str = None):
        if base_dir is None:
            base_dir = Path(__file__).parent.parent

        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / "data"
        self.evidence_dir = self.base_dir / "evidence"

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.evidence_dir.mkdir(parents=True, exist_ok=True)

        self.pipeline_log = {
            'pipeline': 'P05_Gaming_Analytics',
            'version': '1.0.0',
            'start_time': None,
            'end_time': None,
            'mode': None,
            'stages': {},
            'total_rows': 0,
            'quality_score': 0,
        }

    def run_extraction(self, mode: str = 'test') -> dict:
        """Run data extraction stage."""
        logger.info("=" * 60)
        logger.info("STAGE 1: EXTRACTION")
        logger.info("=" * 60)

        stage_start = datetime.now()

        extractor = SteamDataExtractor(output_dir=str(self.data_dir))

        if mode == 'full':
            games_df, steamspy_df = extractor.run_full_extraction()
        else:
            games_df, steamspy_df = extractor.run_test_extraction(limit=100)

        stage_result = {
            'start_time': stage_start.isoformat(),
            'end_time': datetime.now().isoformat(),
            'games_extracted': len(games_df),
            'steamspy_records': len(steamspy_df),
            'status': 'SUCCESS' if len(games_df) > 0 else 'PARTIAL'
        }

        self.pipeline_log['stages']['extraction'] = stage_result
        return {'games': games_df, 'steamspy': steamspy_df}

    def run_transformation(self, extracted_data: dict) -> dict:
        """Run data transformation stage."""
        logger.info("=" * 60)
        logger.info("STAGE 2: TRANSFORMATION")
        logger.info("=" * 60)

        stage_start = datetime.now()

        games_df = extracted_data.get('games')
        if games_df is None or games_df.empty:
            logger.warning("No data to transform")
            return {}

        transformer = GamingStarSchemaTransformer(
            input_dir=str(self.data_dir),
            output_dir=str(self.data_dir / "star_schema")
        )

        tables = transformer.run_transformation(games_df)

        # Calculate total rows
        total_rows = sum(len(df) for df in tables.values() if not df.empty)

        stage_result = {
            'start_time': stage_start.isoformat(),
            'end_time': datetime.now().isoformat(),
            'tables_created': len(tables),
            'total_rows': total_rows,
            'table_counts': {name: len(df) for name, df in tables.items()},
            'status': 'SUCCESS'
        }

        self.pipeline_log['stages']['transformation'] = stage_result
        self.pipeline_log['total_rows'] = total_rows

        return tables

    def run_analytics(self) -> dict:
        """Run analytics stage."""
        logger.info("=" * 60)
        logger.info("STAGE 3: ANALYTICS")
        logger.info("=" * 60)

        stage_start = datetime.now()

        analytics = GamingAnalytics(data_dir=str(self.data_dir / "star_schema"))
        kpis = analytics.run_all_analytics()
        analytics.print_summary()

        stage_result = {
            'start_time': stage_start.isoformat(),
            'end_time': datetime.now().isoformat(),
            'kpis_calculated': len(kpis),
            'status': 'SUCCESS'
        }

        self.pipeline_log['stages']['analytics'] = stage_result
        return kpis

    def generate_evidence(self, kpis: dict) -> dict:
        """Generate evidence artifacts for verification."""
        logger.info("=" * 60)
        logger.info("STAGE 4: EVIDENCE GENERATION")
        logger.info("=" * 60)

        # Calculate quality score
        quality_checks = {
            'has_games': self.pipeline_log['stages'].get('extraction', {}).get('games_extracted', 0) > 0,
            'has_dimensions': self.pipeline_log['stages'].get('transformation', {}).get('tables_created', 0) >= 5,
            'has_facts': 'fact_game_metrics' in self.pipeline_log['stages'].get('transformation', {}).get('table_counts', {}),
            'has_kpis': len(kpis) > 0,
        }

        quality_score = sum(quality_checks.values()) / len(quality_checks) * 100

        # Generate file checksums
        checksums = {}
        for parquet_file in self.data_dir.rglob("*.parquet"):
            with open(parquet_file, 'rb') as f:
                checksums[parquet_file.name] = hashlib.sha256(f.read()).hexdigest()

        evidence = {
            'project': 'P05_Gaming_Analytics',
            'generated_at': datetime.now().isoformat(),
            'pipeline_version': '1.0.0',
            'author': 'Mboya Jeffers',

            'data_sources': {
                'steam_api': 'https://api.steampowered.com',
                'steamspy': 'https://steamspy.com/api.php',
                'store_api': 'https://store.steampowered.com/api'
            },

            'row_counts': self.pipeline_log['stages'].get('transformation', {}).get('table_counts', {}),
            'total_rows': self.pipeline_log['total_rows'],
            'target_rows': 8000000,

            'quality_checks': quality_checks,
            'quality_score': quality_score,

            'file_checksums': checksums,

            'pipeline_log': self.pipeline_log
        }

        self.pipeline_log['quality_score'] = quality_score

        # Save evidence
        evidence_path = self.evidence_dir / 'P05_evidence.json'
        with open(evidence_path, 'w') as f:
            json.dump(evidence, f, indent=2, default=str)

        logger.info(f"Evidence saved: {evidence_path}")
        logger.info(f"Quality Score: {quality_score:.1f}%")

        return evidence

    def run(self, mode: str = 'test'):
        """Run complete pipeline."""
        self.pipeline_log['start_time'] = datetime.now().isoformat()
        self.pipeline_log['mode'] = mode

        logger.info("=" * 60)
        logger.info("P05 GAMING ANALYTICS PIPELINE")
        logger.info(f"Mode: {mode.upper()}")
        logger.info("=" * 60)

        try:
            # Stage 1: Extract
            extracted_data = self.run_extraction(mode)

            # Stage 2: Transform
            tables = self.run_transformation(extracted_data)

            # Stage 3: Analyze
            kpis = self.run_analytics()

            # Stage 4: Evidence
            evidence = self.generate_evidence(kpis)

            self.pipeline_log['end_time'] = datetime.now().isoformat()
            self.pipeline_log['status'] = 'SUCCESS'

            # Save final pipeline log
            log_path = self.evidence_dir / 'P05_pipeline_log.json'
            with open(log_path, 'w') as f:
                json.dump(self.pipeline_log, f, indent=2, default=str)

            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETE")
            logger.info(f"Total Rows: {self.pipeline_log['total_rows']:,}")
            logger.info(f"Quality Score: {self.pipeline_log['quality_score']:.1f}%")
            logger.info("=" * 60)

            return evidence

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.pipeline_log['status'] = 'FAILED'
            self.pipeline_log['error'] = str(e)
            raise


def main():
    parser = argparse.ArgumentParser(description='P05 Gaming Analytics Pipeline')
    parser.add_argument('--mode', choices=['full', 'test'], default='test',
                        help='Extraction mode: full (8M+ rows) or test (sample)')

    args = parser.parse_args()

    pipeline = GamingPipeline()
    pipeline.run(mode=args.mode)


if __name__ == "__main__":
    main()
