#!/usr/bin/env python3
"""
P2-FED: Federal Procurement Spend Intelligence Pipeline
========================================================
Enterprise-grade data pipeline using USAspending.gov public API

Target: ~3M rows (DoD, HHS, DHS contracts for FY2024)
Author: Mboya Jeffers
Version: 1.0.0
Created: 2026-02-01
"""

import os
import sys
import json
import time
import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import requests
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
DOCS_DIR = PROJECT_ROOT / "docs"

# Create directories
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)
DOCS_DIR.mkdir(exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / 'pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API Configuration
API_BASE = "https://api.usaspending.gov/api/v2"
BATCH_SIZE = 100  # Records per API call
MAX_WORKERS = 4   # Concurrent API calls
RATE_LIMIT_DELAY = 0.25  # Seconds between calls

# Target agencies for FY2024
TARGET_AGENCIES = [
    {"type": "awarding", "tier": "toptier", "name": "Department of Defense"},
    {"type": "awarding", "tier": "toptier", "name": "Department of Health and Human Services"},
    {"type": "awarding", "tier": "toptier", "name": "Department of Homeland Security"}
]

# Contract award type codes (A=BPA, B=Purchase Order, C=Delivery Order, D=Definitive Contract)
AWARD_TYPE_CODES = ["A", "B", "C", "D"]

# Fields to request from API
AWARD_FIELDS = [
    "Award ID", "Recipient Name", "Award Amount", "Total Outlays",
    "Awarding Agency", "Awarding Sub Agency", "Award Type",
    "Start Date", "End Date", "Action Date",
    "NAICS Code", "NAICS Description",
    "PSC Code", "PSC Description",
    "Place of Performance State Code", "Place of Performance City Name",
    "Recipient UEI", "Description"
]


@dataclass
class QualityGate:
    """Quality gate result"""
    name: str
    passed: bool
    score: float  # 0.0 to 1.0
    threshold: float
    details: str
    issues: List[str] = field(default_factory=list)


@dataclass
class PipelineMetrics:
    """Pipeline execution metrics"""
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    raw_rows: int = 0
    cleaned_rows: int = 0
    modeled_rows: int = 0
    api_calls: int = 0
    api_errors: int = 0
    quality_gates: List[QualityGate] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0

    @property
    def overall_quality_score(self) -> float:
        if not self.quality_gates:
            return 0.0
        weights = {
            'schema_drift': 0.15,
            'freshness': 0.15,
            'completeness': 0.20,
            'duplicates': 0.15,
            'value_sanity': 0.20,
            'referential_integrity': 0.15
        }
        total = 0.0
        weight_sum = 0.0
        for gate in self.quality_gates:
            w = weights.get(gate.name, 0.1)
            total += gate.score * w
            weight_sum += w
        return total / weight_sum if weight_sum > 0 else 0.0


class USASpendingClient:
    """Client for USAspending.gov API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def search_awards(self,
                     time_period: List[Dict],
                     agencies: List[Dict],
                     award_type_codes: List[str],
                     fields: List[str],
                     limit: int = 100,
                     page: int = 1,
                     last_record_unique_id: Optional[int] = None,
                     last_record_sort_value: Optional[str] = None) -> Dict:
        """Search spending by award with pagination support"""

        payload = {
            "filters": {
                "time_period": time_period,
                "agencies": agencies,
                "award_type_codes": award_type_codes
            },
            "fields": fields,
            "limit": limit,
            "page": page
        }

        # Use cursor pagination for better performance
        if last_record_unique_id and last_record_sort_value:
            payload["last_record_unique_id"] = last_record_unique_id
            payload["last_record_sort_value"] = last_record_sort_value

        url = f"{API_BASE}/search/spending_by_award/"
        response = self.session.post(url, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()

    def get_award_count(self,
                       time_period: List[Dict],
                       agencies: List[Dict],
                       award_type_codes: List[str]) -> Dict:
        """Get total award counts by type"""

        payload = {
            "filters": {
                "time_period": time_period,
                "agencies": agencies,
                "award_type_codes": award_type_codes
            }
        }

        url = f"{API_BASE}/search/spending_by_award_count/"
        response = self.session.post(url, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()


class DataIngestion:
    """Data ingestion module"""

    def __init__(self, client: USASpendingClient, metrics: PipelineMetrics):
        self.client = client
        self.metrics = metrics

    def ingest_fiscal_year(self,
                          fiscal_year: int,
                          target_rows: int = 500000) -> pd.DataFrame:
        """Ingest data for a fiscal year"""

        # FY starts Oct 1 of prior year
        start_date = f"{fiscal_year - 1}-10-01"
        end_date = f"{fiscal_year}-09-30"
        time_period = [{"start_date": start_date, "end_date": end_date}]

        logger.info(f"Ingesting FY{fiscal_year} data ({start_date} to {end_date})")

        # Get total count first
        count_result = self.client.get_award_count(
            time_period, TARGET_AGENCIES, AWARD_TYPE_CODES
        )
        total_available = sum(count_result.get('results', {}).values())
        logger.info(f"Total available records: {total_available:,}")

        # Calculate how many pages we need
        pages_needed = min(target_rows // BATCH_SIZE + 1, total_available // BATCH_SIZE + 1)
        logger.info(f"Targeting {target_rows:,} rows ({pages_needed} pages)")

        # Ingest data in batches
        all_records = []
        last_id = None
        last_sort = None
        page = 1

        while len(all_records) < target_rows:
            try:
                result = self.client.search_awards(
                    time_period=time_period,
                    agencies=TARGET_AGENCIES,
                    award_type_codes=AWARD_TYPE_CODES,
                    fields=AWARD_FIELDS,
                    limit=BATCH_SIZE,
                    page=page,
                    last_record_unique_id=last_id,
                    last_record_sort_value=last_sort
                )

                self.metrics.api_calls += 1
                records = result.get('results', [])

                if not records:
                    break

                all_records.extend(records)

                # Update pagination cursor
                page_meta = result.get('page_metadata', {})
                last_id = page_meta.get('last_record_unique_id')
                last_sort = page_meta.get('last_record_sort_value')
                has_next = page_meta.get('hasNext', False)

                if page % 50 == 0:
                    logger.info(f"Progress: {len(all_records):,} records ingested")

                if not has_next:
                    break

                page += 1
                time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                self.metrics.api_errors += 1
                logger.error(f"API error on page {page}: {e}")
                if self.metrics.api_errors > 10:
                    logger.error("Too many API errors, stopping ingestion")
                    break
                time.sleep(2)  # Backoff on error

        df = pd.DataFrame(all_records)
        self.metrics.raw_rows = len(df)
        logger.info(f"Ingestion complete: {len(df):,} raw records")

        return df


class DataCleaner:
    """Data cleaning and transformation module"""

    def __init__(self, metrics: PipelineMetrics):
        self.metrics = metrics

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize raw data"""

        if df.empty:
            return df

        logger.info("Starting data cleaning...")

        # Standardize column names
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]

        # Type conversions
        numeric_cols = ['award_amount', 'total_outlays']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Date conversions
        date_cols = ['start_date', 'end_date', 'action_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Clean string fields
        string_cols = ['recipient_name', 'awarding_agency', 'awarding_sub_agency',
                      'naics_description', 'psc_description', 'description']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna('').str.strip()

        # Add derived fields
        if 'start_date' in df.columns:
            df['fiscal_year'] = df['start_date'].apply(
                lambda x: x.year + 1 if pd.notna(x) and x.month >= 10 else (x.year if pd.notna(x) else None)
            )
            df['fiscal_quarter'] = df['start_date'].apply(
                lambda x: ((x.month - 10) % 12) // 3 + 1 if pd.notna(x) else None
            )

        # Generate unique key for deduplication
        df['record_hash'] = df.apply(
            lambda r: hashlib.md5(
                f"{r.get('award_id', '')}{r.get('recipient_name', '')}{r.get('award_amount', '')}".encode()
            ).hexdigest()[:16],
            axis=1
        )

        self.metrics.cleaned_rows = len(df)
        logger.info(f"Cleaning complete: {len(df):,} records")

        return df


class DataModeler:
    """Data modeling module - creates fact/dim tables"""

    def __init__(self, metrics: PipelineMetrics):
        self.metrics = metrics

    def create_model(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create dimensional model from cleaned data"""

        logger.info("Creating dimensional model...")
        model = {}

        # Agency dimension
        agency_cols = ['awarding_agency', 'awarding_sub_agency']
        if all(c in df.columns for c in agency_cols):
            agency_dim = df[agency_cols].drop_duplicates()
            agency_dim['agency_id'] = range(1, len(agency_dim) + 1)
            agency_dim = agency_dim.rename(columns={
                'awarding_agency': 'toptier_name',
                'awarding_sub_agency': 'subtier_name'
            })
            model['agency_dim'] = agency_dim

        # Recipient dimension
        recipient_cols = ['recipient_name', 'recipient_uei']
        if all(c in df.columns for c in recipient_cols):
            recipient_dim = df[recipient_cols].drop_duplicates()
            recipient_dim['recipient_id'] = range(1, len(recipient_dim) + 1)
            recipient_dim = recipient_dim.rename(columns={'recipient_uei': 'uei'})
            model['recipient_dim'] = recipient_dim

        # Time dimension
        if 'start_date' in df.columns:
            dates = df['start_date'].dropna().unique()
            time_dim = pd.DataFrame({'date': dates})
            time_dim['date_id'] = range(1, len(time_dim) + 1)
            time_dim['year'] = pd.to_datetime(time_dim['date']).dt.year
            time_dim['month'] = pd.to_datetime(time_dim['date']).dt.month
            time_dim['quarter'] = pd.to_datetime(time_dim['date']).dt.quarter
            time_dim['fiscal_year'] = time_dim.apply(
                lambda r: r['year'] + 1 if r['month'] >= 10 else r['year'], axis=1
            )
            model['time_dim'] = time_dim

        # Geography dimension (if available)
        geo_cols = ['place_of_performance_state_code', 'place_of_performance_city_name']
        if all(c in df.columns for c in geo_cols):
            geo_dim = df[geo_cols].drop_duplicates()
            geo_dim['geo_id'] = range(1, len(geo_dim) + 1)
            geo_dim = geo_dim.rename(columns={
                'place_of_performance_state_code': 'state_code',
                'place_of_performance_city_name': 'city_name'
            })
            model['geo_dim'] = geo_dim

        # Award fact table
        fact_cols = ['award_id', 'award_amount', 'total_outlays', 'start_date', 'end_date',
                    'naics_code', 'naics_description', 'psc_code', 'psc_description',
                    'description', 'fiscal_year', 'fiscal_quarter', 'record_hash',
                    'internal_id', 'generated_internal_id']
        available_cols = [c for c in fact_cols if c in df.columns]
        award_fact = df[available_cols].copy()

        # Add foreign keys
        if 'agency_dim' in model:
            agency_map = model['agency_dim'].set_index(['toptier_name', 'subtier_name'])['agency_id']
            award_fact['agency_id'] = df.apply(
                lambda r: agency_map.get((r['awarding_agency'], r['awarding_sub_agency']), None), axis=1
            )

        if 'recipient_dim' in model:
            recipient_map = model['recipient_dim'].set_index(['recipient_name', 'uei'])['recipient_id']
            award_fact['recipient_id'] = df.apply(
                lambda r: recipient_map.get((r['recipient_name'], r.get('recipient_uei', '')), None), axis=1
            )

        model['award_fact'] = award_fact
        self.metrics.modeled_rows = len(award_fact)

        logger.info(f"Model created: {len(model)} tables")
        for name, table in model.items():
            logger.info(f"  {name}: {len(table):,} rows, {len(table.columns)} columns")

        return model


class QualityGateRunner:
    """Quality gate validation module"""

    EXPECTED_SCHEMA = {
        'award_id': 'object',
        'recipient_name': 'object',
        'award_amount': 'float64',
        'awarding_agency': 'object',
        'start_date': 'datetime64[ns]'
    }

    def __init__(self, metrics: PipelineMetrics):
        self.metrics = metrics

    def run_all_gates(self, df: pd.DataFrame, model: Dict[str, pd.DataFrame]) -> List[QualityGate]:
        """Run all quality gates"""

        logger.info("Running quality gates...")
        gates = []

        gates.append(self.check_schema_drift(df))
        gates.append(self.check_freshness(df))
        gates.append(self.check_completeness(df))
        gates.append(self.check_duplicates(df))
        gates.append(self.check_value_sanity(df))
        gates.append(self.check_referential_integrity(model))

        self.metrics.quality_gates = gates

        for gate in gates:
            status = "PASS" if gate.passed else "FAIL"
            logger.info(f"  {gate.name}: {status} (score: {gate.score:.2%})")

        return gates

    def check_schema_drift(self, df: pd.DataFrame) -> QualityGate:
        """Check for schema drift from expected structure"""
        issues = []

        for col, expected_type in self.EXPECTED_SCHEMA.items():
            col_lower = col.lower().replace(' ', '_')
            if col_lower not in df.columns:
                issues.append(f"Missing column: {col}")

        # Check for unexpected nulls in key columns
        key_cols = ['award_id', 'awarding_agency']
        for col in key_cols:
            if col in df.columns:
                null_pct = df[col].isna().mean()
                if null_pct > 0.01:
                    issues.append(f"High null rate in {col}: {null_pct:.1%}")

        score = 1.0 - (len(issues) * 0.1)
        score = max(0.0, min(1.0, score))

        return QualityGate(
            name='schema_drift',
            passed=len(issues) == 0,
            score=score,
            threshold=0.95,
            details=f"Checked {len(self.EXPECTED_SCHEMA)} expected columns",
            issues=issues
        )

    def check_freshness(self, df: pd.DataFrame) -> QualityGate:
        """Check data freshness"""
        issues = []

        if 'start_date' in df.columns:
            max_date = df['start_date'].max()
            if pd.notna(max_date):
                days_old = (datetime.now() - pd.Timestamp(max_date).to_pydatetime()).days
                if days_old > 365:
                    issues.append(f"Data is {days_old} days old")
                score = max(0.0, 1.0 - (days_old / 730))  # Penalize data older than 2 years
            else:
                score = 0.5
                issues.append("No valid dates found")
        else:
            score = 0.0
            issues.append("No date column available")

        return QualityGate(
            name='freshness',
            passed=score >= 0.8,
            score=score,
            threshold=0.80,
            details=f"Latest date: {max_date if 'start_date' in df.columns else 'N/A'}",
            issues=issues
        )

    def check_completeness(self, df: pd.DataFrame) -> QualityGate:
        """Check data completeness (null rates)"""
        issues = []

        key_dims = ['awarding_agency', 'recipient_name', 'award_amount']
        null_rates = {}

        for col in key_dims:
            if col in df.columns:
                null_rate = df[col].isna().mean()
                null_rates[col] = null_rate
                if null_rate > 0.05:
                    issues.append(f"{col}: {null_rate:.1%} null")

        avg_completeness = 1.0 - np.mean(list(null_rates.values())) if null_rates else 0.0

        return QualityGate(
            name='completeness',
            passed=avg_completeness >= 0.90,
            score=avg_completeness,
            threshold=0.90,
            details=f"Average completeness: {avg_completeness:.1%}",
            issues=issues
        )

    def check_duplicates(self, df: pd.DataFrame) -> QualityGate:
        """Check for duplicate records"""
        issues = []

        if 'record_hash' in df.columns:
            dupe_count = df['record_hash'].duplicated().sum()
            dupe_rate = dupe_count / len(df) if len(df) > 0 else 0

            if dupe_rate > 0.01:
                issues.append(f"{dupe_count:,} duplicate records ({dupe_rate:.1%})")

            score = 1.0 - dupe_rate
        else:
            # Fallback: check award_id + recipient_name
            if 'award_id' in df.columns and 'recipient_name' in df.columns:
                dupes = df.duplicated(subset=['award_id', 'recipient_name']).sum()
                dupe_rate = dupes / len(df) if len(df) > 0 else 0
                score = 1.0 - dupe_rate
                if dupe_rate > 0.01:
                    issues.append(f"{dupes:,} potential duplicates")
            else:
                score = 0.8
                issues.append("Cannot verify duplicates - missing key columns")

        return QualityGate(
            name='duplicates',
            passed=score >= 0.95,
            score=score,
            threshold=0.95,
            details=f"Duplicate rate: {(1-score):.2%}",
            issues=issues
        )

    def check_value_sanity(self, df: pd.DataFrame) -> QualityGate:
        """Check for anomalous values"""
        issues = []

        if 'award_amount' in df.columns:
            amounts = df['award_amount'].dropna()

            # Check for negative values
            neg_count = (amounts < 0).sum()
            if neg_count > 0:
                neg_pct = neg_count / len(amounts)
                if neg_pct > 0.05:
                    issues.append(f"{neg_count:,} negative amounts ({neg_pct:.1%})")

            # Check for extreme outliers (z-score > 5)
            if len(amounts) > 100:
                mean = amounts.mean()
                std = amounts.std()
                if std > 0:
                    z_scores = np.abs((amounts - mean) / std)
                    outlier_count = (z_scores > 5).sum()
                    if outlier_count > len(amounts) * 0.01:
                        issues.append(f"{outlier_count:,} extreme outliers (z>5)")

        score = 1.0 - (len(issues) * 0.15)
        score = max(0.0, min(1.0, score))

        return QualityGate(
            name='value_sanity',
            passed=score >= 0.85,
            score=score,
            threshold=0.85,
            details=f"Checked award_amount distribution",
            issues=issues
        )

    def check_referential_integrity(self, model: Dict[str, pd.DataFrame]) -> QualityGate:
        """Check referential integrity between fact and dimension tables"""
        issues = []

        if 'award_fact' not in model:
            return QualityGate(
                name='referential_integrity',
                passed=False,
                score=0.0,
                threshold=0.95,
                details="No fact table found",
                issues=["Missing award_fact table"]
            )

        fact = model['award_fact']
        integrity_scores = []

        # Check agency_id references
        if 'agency_dim' in model and 'agency_id' in fact.columns:
            valid_ids = set(model['agency_dim']['agency_id'])
            orphan_count = fact['agency_id'].apply(lambda x: x not in valid_ids if pd.notna(x) else True).sum()
            orphan_rate = orphan_count / len(fact)
            integrity_scores.append(1.0 - orphan_rate)
            if orphan_rate > 0.01:
                issues.append(f"agency_id: {orphan_count:,} orphan references")

        # Check recipient_id references
        if 'recipient_dim' in model and 'recipient_id' in fact.columns:
            valid_ids = set(model['recipient_dim']['recipient_id'])
            orphan_count = fact['recipient_id'].apply(lambda x: x not in valid_ids if pd.notna(x) else True).sum()
            orphan_rate = orphan_count / len(fact)
            integrity_scores.append(1.0 - orphan_rate)
            if orphan_rate > 0.01:
                issues.append(f"recipient_id: {orphan_count:,} orphan references")

        score = np.mean(integrity_scores) if integrity_scores else 0.8

        return QualityGate(
            name='referential_integrity',
            passed=score >= 0.95,
            score=score,
            threshold=0.95,
            details=f"Checked {len(integrity_scores)} foreign key relationships",
            issues=issues
        )


class KPICalculator:
    """KPI calculation module"""

    def __init__(self, model: Dict[str, pd.DataFrame]):
        self.model = model

    def calculate_all_kpis(self) -> Dict[str, Any]:
        """Calculate all KPIs"""

        logger.info("Calculating KPIs...")
        kpis = {}

        fact = self.model.get('award_fact', pd.DataFrame())

        if fact.empty:
            return kpis

        # Spend Trends
        kpis['spend_trends'] = self._calc_spend_trends(fact)

        # Vendor Concentration
        kpis['vendor_concentration'] = self._calc_vendor_concentration(fact)

        # Change Detection
        kpis['change_detection'] = self._calc_change_detection(fact)

        # Summary Stats
        kpis['summary'] = self._calc_summary_stats(fact)

        logger.info(f"Calculated {len(kpis)} KPI categories")
        return kpis

    def _calc_spend_trends(self, fact: pd.DataFrame) -> Dict:
        """Calculate spend trends by agency and time"""
        trends = {}

        if 'agency_id' in fact.columns and 'award_amount' in fact.columns:
            agency_spend = fact.groupby('agency_id')['award_amount'].agg(['sum', 'count', 'mean'])
            agency_spend.columns = ['total_spend', 'award_count', 'avg_award']
            trends['by_agency'] = agency_spend.to_dict()

        if 'fiscal_year' in fact.columns and 'award_amount' in fact.columns:
            yearly = fact.groupby('fiscal_year')['award_amount'].agg(['sum', 'count'])
            yearly.columns = ['total_spend', 'award_count']
            trends['by_fiscal_year'] = yearly.to_dict()

        if 'fiscal_quarter' in fact.columns and 'award_amount' in fact.columns:
            quarterly = fact.groupby(['fiscal_year', 'fiscal_quarter'])['award_amount'].sum()
            # Convert tuple keys to strings for JSON serialization
            trends['by_quarter'] = {f"{k[0]}_Q{k[1]}": v for k, v in quarterly.to_dict().items()}

        return trends

    def _calc_vendor_concentration(self, fact: pd.DataFrame) -> Dict:
        """Calculate vendor concentration metrics including HHI"""
        concentration = {}

        if 'recipient_id' not in fact.columns or 'award_amount' not in fact.columns:
            return concentration

        # Total spend by vendor
        vendor_spend = fact.groupby('recipient_id')['award_amount'].sum().sort_values(ascending=False)
        total_spend = vendor_spend.sum()

        if total_spend == 0:
            return concentration

        # Market shares
        market_shares = vendor_spend / total_spend

        # Top 10 share
        top_10_share = market_shares.head(10).sum()
        concentration['top_10_share'] = float(top_10_share)

        # Top 20 share
        top_20_share = market_shares.head(20).sum()
        concentration['top_20_share'] = float(top_20_share)

        # Herfindahl-Hirschman Index (HHI)
        # HHI = sum of squared market shares (scaled 0-10000)
        hhi = (market_shares ** 2).sum() * 10000
        concentration['hhi'] = float(hhi)

        # HHI interpretation
        if hhi < 1500:
            concentration['hhi_interpretation'] = 'Unconcentrated'
        elif hhi < 2500:
            concentration['hhi_interpretation'] = 'Moderately Concentrated'
        else:
            concentration['hhi_interpretation'] = 'Highly Concentrated'

        # Vendor count
        concentration['total_vendors'] = len(vendor_spend)
        concentration['vendors_with_spend'] = (vendor_spend > 0).sum()

        return concentration

    def _calc_change_detection(self, fact: pd.DataFrame) -> Dict:
        """Detect significant changes in spending patterns"""
        changes = {}

        if 'fiscal_quarter' not in fact.columns or 'award_amount' not in fact.columns:
            return changes

        # Quarter-over-quarter changes
        quarterly = fact.groupby(['fiscal_year', 'fiscal_quarter'])['award_amount'].sum()
        if len(quarterly) > 1:
            qoq_change = quarterly.pct_change()
            # Convert tuple keys to strings for JSON serialization
            changes['qoq_changes'] = {f"{k[0]}_Q{k[1]}": v for k, v in qoq_change.dropna().to_dict().items()}
            changes['max_qoq_increase'] = float(qoq_change.max()) if not qoq_change.isna().all() else 0
            changes['max_qoq_decrease'] = float(qoq_change.min()) if not qoq_change.isna().all() else 0

        # Vendor ranking changes (if multiple periods)
        if 'recipient_id' in fact.columns and 'fiscal_year' in fact.columns:
            years = fact['fiscal_year'].dropna().unique()
            if len(years) >= 2:
                years = sorted(years)
                recent = fact[fact['fiscal_year'] == years[-1]]
                prior = fact[fact['fiscal_year'] == years[-2]]

                recent_rank = recent.groupby('recipient_id')['award_amount'].sum().rank(ascending=False)
                prior_rank = prior.groupby('recipient_id')['award_amount'].sum().rank(ascending=False)

                # Find vendors with biggest rank improvements
                common = set(recent_rank.index) & set(prior_rank.index)
                if common:
                    rank_changes = {int(v): float(prior_rank[v] - recent_rank[v]) for v in common}
                    top_movers = sorted(rank_changes.items(), key=lambda x: x[1], reverse=True)[:5]
                    changes['top_rank_improvers'] = {str(k): v for k, v in top_movers}

        return changes

    def _calc_summary_stats(self, fact: pd.DataFrame) -> Dict:
        """Calculate summary statistics"""
        summary = {}

        if 'award_amount' in fact.columns:
            amounts = fact['award_amount'].dropna()
            summary['total_spend'] = float(amounts.sum())
            summary['avg_award'] = float(amounts.mean())
            summary['median_award'] = float(amounts.median())
            summary['std_award'] = float(amounts.std())
            summary['min_award'] = float(amounts.min())
            summary['max_award'] = float(amounts.max())
            summary['p25_award'] = float(amounts.quantile(0.25))
            summary['p75_award'] = float(amounts.quantile(0.75))
            summary['p95_award'] = float(amounts.quantile(0.95))

        summary['total_awards'] = len(fact)

        if 'recipient_id' in fact.columns:
            summary['unique_vendors'] = fact['recipient_id'].nunique()

        if 'agency_id' in fact.columns:
            summary['unique_agencies'] = fact['agency_id'].nunique()

        return summary


def run_pipeline(fiscal_year: int = 2024, target_rows: int = 500000) -> Tuple[Dict[str, pd.DataFrame], Dict, PipelineMetrics]:
    """Run the full P2-FED pipeline"""

    logger.info("=" * 60)
    logger.info("P2-FED: Federal Procurement Spend Intelligence Pipeline")
    logger.info("=" * 60)

    metrics = PipelineMetrics()

    # Initialize client
    client = USASpendingClient()

    # Data Ingestion
    ingestion = DataIngestion(client, metrics)
    raw_df = ingestion.ingest_fiscal_year(fiscal_year, target_rows)

    # Save raw data
    raw_path = DATA_DIR / f'raw_awards_fy{fiscal_year}.csv'
    raw_df.to_csv(raw_path, index=False)
    logger.info(f"Saved raw data: {raw_path}")

    # Data Cleaning
    cleaner = DataCleaner(metrics)
    cleaned_df = cleaner.clean(raw_df)

    # Save cleaned data
    cleaned_path = DATA_DIR / f'cleaned_awards_fy{fiscal_year}.csv'
    cleaned_df.to_csv(cleaned_path, index=False)
    logger.info(f"Saved cleaned data: {cleaned_path}")

    # Data Modeling
    modeler = DataModeler(metrics)
    model = modeler.create_model(cleaned_df)

    # Save model tables
    for name, table in model.items():
        table_path = DATA_DIR / f'{name}.csv'
        table.to_csv(table_path, index=False)
        logger.info(f"Saved model table: {table_path}")

    # Quality Gates
    gate_runner = QualityGateRunner(metrics)
    gates = gate_runner.run_all_gates(cleaned_df, model)

    # KPI Calculation
    kpi_calc = KPICalculator(model)
    kpis = kpi_calc.calculate_all_kpis()

    # Save KPIs
    kpi_path = DATA_DIR / 'kpis.json'
    with open(kpi_path, 'w') as f:
        json.dump(kpis, f, indent=2, default=str)
    logger.info(f"Saved KPIs: {kpi_path}")

    # Finalize metrics
    metrics.end_time = datetime.now(timezone.utc)

    # Save metrics (cast to Python types for JSON serialization)
    metrics_dict = {
        'start_time': metrics.start_time.isoformat(),
        'end_time': metrics.end_time.isoformat(),
        'duration_seconds': float(metrics.duration_seconds),
        'raw_rows': int(metrics.raw_rows),
        'cleaned_rows': int(metrics.cleaned_rows),
        'modeled_rows': int(metrics.modeled_rows),
        'api_calls': int(metrics.api_calls),
        'api_errors': int(metrics.api_errors),
        'overall_quality_score': float(metrics.overall_quality_score),
        'quality_gates': [
            {
                'name': str(g.name),
                'passed': bool(g.passed),
                'score': float(g.score),
                'threshold': float(g.threshold),
                'details': str(g.details),
                'issues': [str(i) for i in g.issues]
            }
            for g in metrics.quality_gates
        ]
    }
    metrics_path = DATA_DIR / 'pipeline_metrics.json'
    with open(metrics_path, 'w') as f:
        json.dump(metrics_dict, f, indent=2)
    logger.info(f"Saved metrics: {metrics_path}")

    logger.info("=" * 60)
    logger.info("Pipeline Complete!")
    logger.info(f"Duration: {metrics.duration_seconds:.1f}s")
    logger.info(f"Rows: {metrics.raw_rows:,} raw → {metrics.cleaned_rows:,} cleaned → {metrics.modeled_rows:,} modeled")
    logger.info(f"Quality Score: {metrics.overall_quality_score:.1%}")
    logger.info("=" * 60)

    return model, kpis, metrics


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='P2-FED Federal Procurement Pipeline')
    parser.add_argument('--fiscal-year', type=int, default=2024, help='Fiscal year to process')
    parser.add_argument('--target-rows', type=int, default=500000, help='Target number of rows to ingest')

    args = parser.parse_args()

    model, kpis, metrics = run_pipeline(args.fiscal_year, args.target_rows)

    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(f"Total Records: {metrics.modeled_rows:,}")
    print(f"Quality Score: {metrics.overall_quality_score:.1%}")
    print(f"API Calls: {metrics.api_calls}")
    print(f"Duration: {metrics.duration_seconds:.1f} seconds")
    print("\nQuality Gates:")
    for gate in metrics.quality_gates:
        status = "✓" if gate.passed else "✗"
        print(f"  {status} {gate.name}: {gate.score:.1%}")
