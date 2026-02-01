#!/usr/bin/env python3
"""
P2-SEC: SEC EDGAR XBRL Financial Facts Pipeline
================================================
Enterprise-grade data pipeline for public company financial data

Target: ~500K XBRL facts from 50 public companies
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

# SEC API Configuration
SEC_BASE = "https://data.sec.gov"
USER_AGENT = "PortfolioProject/1.0 (MboyaJeffers9@gmail.com)"
RATE_LIMIT_DELAY = 0.15  # SEC requests 10 req/sec max

# Target company cohort - 50 diverse companies across sectors
COMPANY_COHORT = [
    # Technology
    {"cik": "0000320193", "ticker": "AAPL", "name": "Apple Inc.", "sector": "Technology"},
    {"cik": "0000789019", "ticker": "MSFT", "name": "Microsoft Corporation", "sector": "Technology"},
    {"cik": "0001652044", "ticker": "GOOGL", "name": "Alphabet Inc.", "sector": "Technology"},
    {"cik": "0001018724", "ticker": "AMZN", "name": "Amazon.com Inc.", "sector": "Technology"},
    {"cik": "0001326801", "ticker": "META", "name": "Meta Platforms Inc.", "sector": "Technology"},
    {"cik": "0001045810", "ticker": "NVDA", "name": "NVIDIA Corporation", "sector": "Technology"},
    {"cik": "0001559720", "ticker": "CRM", "name": "Salesforce Inc.", "sector": "Technology"},
    {"cik": "0001403161", "ticker": "V", "name": "Visa Inc.", "sector": "Technology"},

    # Healthcare
    {"cik": "0000310158", "ticker": "JNJ", "name": "Johnson & Johnson", "sector": "Healthcare"},
    {"cik": "0000078003", "ticker": "PFE", "name": "Pfizer Inc.", "sector": "Healthcare"},
    {"cik": "0001551152", "ticker": "ABBV", "name": "AbbVie Inc.", "sector": "Healthcare"},
    {"cik": "0000064803", "ticker": "MRK", "name": "Merck & Co. Inc.", "sector": "Healthcare"},
    {"cik": "0001800", "ticker": "ABT", "name": "Abbott Laboratories", "sector": "Healthcare"},

    # Financial Services
    {"cik": "0000019617", "ticker": "JPM", "name": "JPMorgan Chase & Co.", "sector": "Financial"},
    {"cik": "0000070858", "ticker": "BAC", "name": "Bank of America Corp.", "sector": "Financial"},
    {"cik": "0001067983", "ticker": "BRK-B", "name": "Berkshire Hathaway", "sector": "Financial"},
    {"cik": "0000831001", "ticker": "C", "name": "Citigroup Inc.", "sector": "Financial"},
    {"cik": "0000072971", "ticker": "WFC", "name": "Wells Fargo & Co.", "sector": "Financial"},
    {"cik": "0000886982", "ticker": "GS", "name": "Goldman Sachs Group", "sector": "Financial"},

    # Consumer
    {"cik": "0000021344", "ticker": "KO", "name": "Coca-Cola Company", "sector": "Consumer"},
    {"cik": "0000077476", "ticker": "PEP", "name": "PepsiCo Inc.", "sector": "Consumer"},
    {"cik": "0000080424", "ticker": "PG", "name": "Procter & Gamble Co.", "sector": "Consumer"},
    {"cik": "0001018840", "ticker": "NKE", "name": "Nike Inc.", "sector": "Consumer"},
    {"cik": "0000104169", "ticker": "WMT", "name": "Walmart Inc.", "sector": "Consumer"},
    {"cik": "0000027419", "ticker": "DIS", "name": "Walt Disney Company", "sector": "Consumer"},
    {"cik": "0001551182", "ticker": "COST", "name": "Costco Wholesale", "sector": "Consumer"},

    # Industrial
    {"cik": "0000012927", "ticker": "BA", "name": "Boeing Company", "sector": "Industrial"},
    {"cik": "0000040545", "ticker": "GE", "name": "General Electric Co.", "sector": "Industrial"},
    {"cik": "0000034088", "ticker": "XOM", "name": "Exxon Mobil Corp.", "sector": "Energy"},
    {"cik": "0000093410", "ticker": "CVX", "name": "Chevron Corporation", "sector": "Energy"},
    {"cik": "0000050863", "ticker": "IBM", "name": "IBM Corporation", "sector": "Technology"},
    {"cik": "0000858877", "ticker": "CSCO", "name": "Cisco Systems Inc.", "sector": "Technology"},

    # Communications
    {"cik": "0000732717", "ticker": "T", "name": "AT&T Inc.", "sector": "Communications"},
    {"cik": "0000732712", "ticker": "VZ", "name": "Verizon Communications", "sector": "Communications"},
    {"cik": "0001288776", "ticker": "NFLX", "name": "Netflix Inc.", "sector": "Communications"},

    # Real Estate
    {"cik": "0001393311", "ticker": "AMT", "name": "American Tower Corp.", "sector": "Real Estate"},
    {"cik": "0001035002", "ticker": "PLD", "name": "Prologis Inc.", "sector": "Real Estate"},

    # Materials
    {"cik": "0000066740", "ticker": "LIN", "name": "Linde plc", "sector": "Materials"},

    # Utilities
    {"cik": "0001004980", "ticker": "NEE", "name": "NextEra Energy Inc.", "sector": "Utilities"},
    {"cik": "0000072741", "ticker": "DUK", "name": "Duke Energy Corp.", "sector": "Utilities"},
]

# Key financial concepts to extract
KEY_CONCEPTS = {
    # Income Statement
    'revenue': ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax',
                'SalesRevenueNet', 'RevenuesNetOfInterestExpense'],
    'cost_of_revenue': ['CostOfGoodsAndServicesSold', 'CostOfRevenue', 'CostOfGoodsSold'],
    'gross_profit': ['GrossProfit'],
    'operating_income': ['OperatingIncomeLoss', 'OperatingIncome'],
    'net_income': ['NetIncomeLoss', 'NetIncome', 'ProfitLoss'],
    'eps': ['EarningsPerShareDiluted', 'EarningsPerShareBasic'],

    # Balance Sheet
    'assets': ['Assets'],
    'liabilities': ['Liabilities'],
    'equity': ['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
    'cash': ['CashAndCashEquivalentsAtCarryingValue', 'Cash'],
    'current_assets': ['AssetsCurrent'],
    'current_liabilities': ['LiabilitiesCurrent'],

    # Cash Flow
    'operating_cash_flow': ['NetCashProvidedByUsedInOperatingActivities'],
    'investing_cash_flow': ['NetCashProvidedByUsedInInvestingActivities'],
    'financing_cash_flow': ['NetCashProvidedByUsedInFinancingActivities'],
    'capex': ['PaymentsToAcquirePropertyPlantAndEquipment'],
}


@dataclass
class QualityGate:
    """Quality gate result"""
    name: str
    passed: bool
    score: float
    threshold: float
    details: str
    issues: List[str] = field(default_factory=list)


@dataclass
class PipelineMetrics:
    """Pipeline execution metrics"""
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    raw_facts: int = 0
    cleaned_facts: int = 0
    companies_processed: int = 0
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
            'unit_consistency': 0.20,
            'period_logic': 0.20,
            'coverage': 0.25,
            'restatement': 0.15,
            'outliers': 0.20
        }
        total = 0.0
        weight_sum = 0.0
        for gate in self.quality_gates:
            w = weights.get(gate.name, 0.1)
            total += gate.score * w
            weight_sum += w
        return total / weight_sum if weight_sum > 0 else 0.0


class SECEdgarClient:
    """Client for SEC EDGAR API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept': 'application/json'
        })

    def get_company_submissions(self, cik: str) -> Optional[Dict]:
        """Get company filing submissions"""
        url = f"{SEC_BASE}/submissions/CIK{cik}.json"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching submissions for {cik}: {e}")
            return None

    def get_company_facts(self, cik: str) -> Optional[Dict]:
        """Get company XBRL facts"""
        url = f"{SEC_BASE}/api/xbrl/companyfacts/CIK{cik}.json"
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching facts for {cik}: {e}")
            return None


class DataIngestion:
    """Data ingestion module"""

    def __init__(self, client: SECEdgarClient, metrics: PipelineMetrics):
        self.client = client
        self.metrics = metrics

    def ingest_cohort(self, companies: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Ingest data for company cohort"""

        logger.info(f"Ingesting data for {len(companies)} companies...")

        all_facts = []
        all_filings = []
        company_data = []

        for i, company in enumerate(companies):
            cik = company['cik']
            ticker = company['ticker']
            name = company['name']
            sector = company['sector']

            logger.info(f"[{i+1}/{len(companies)}] Processing {ticker} ({name})...")

            # Get submissions
            submissions = self.client.get_company_submissions(cik)
            self.metrics.api_calls += 1

            if submissions:
                # Extract company info
                company_data.append({
                    'cik': cik,
                    'name': submissions.get('name', name),
                    'ticker': ticker,
                    'sector': sector,
                    'sic': submissions.get('sic'),
                    'sic_description': submissions.get('sicDescription'),
                    'fiscal_year_end': submissions.get('fiscalYearEnd'),
                    'state': submissions.get('stateOfIncorporation')
                })

                # Extract recent filings
                recent = submissions.get('filings', {}).get('recent', {})
                for j in range(min(50, len(recent.get('form', [])))):  # Last 50 filings
                    if recent.get('form', [])[j] in ['10-K', '10-Q', '8-K']:
                        all_filings.append({
                            'cik': cik,
                            'accession': recent.get('accessionNumber', [])[j] if j < len(recent.get('accessionNumber', [])) else None,
                            'form': recent.get('form', [])[j],
                            'filing_date': recent.get('filingDate', [])[j] if j < len(recent.get('filingDate', [])) else None,
                            'report_date': recent.get('reportDate', [])[j] if j < len(recent.get('reportDate', [])) else None,
                            'primary_document': recent.get('primaryDocument', [])[j] if j < len(recent.get('primaryDocument', [])) else None
                        })

            # Get XBRL facts
            facts = self.client.get_company_facts(cik)
            self.metrics.api_calls += 1

            if facts:
                self.metrics.companies_processed += 1

                # Process us-gaap facts
                us_gaap = facts.get('facts', {}).get('us-gaap', {})
                for concept, concept_data in us_gaap.items():
                    units_data = concept_data.get('units', {})
                    for unit, values in units_data.items():
                        for val in values:
                            all_facts.append({
                                'cik': cik,
                                'ticker': ticker,
                                'taxonomy': 'us-gaap',
                                'concept': concept,
                                'unit': unit,
                                'value': val.get('val'),
                                'period_start': val.get('start'),
                                'period_end': val.get('end'),
                                'accession': val.get('accn'),
                                'fiscal_year': val.get('fy'),
                                'fiscal_period': val.get('fp'),
                                'form': val.get('form'),
                                'filed': val.get('filed')
                            })

            if (i + 1) % 10 == 0:
                logger.info(f"Progress: {i+1}/{len(companies)} companies, {len(all_facts):,} facts collected")

        # Create DataFrames
        facts_df = pd.DataFrame(all_facts)
        filings_df = pd.DataFrame(all_filings)
        companies_df = pd.DataFrame(company_data)

        self.metrics.raw_facts = len(facts_df)
        logger.info(f"Ingestion complete: {len(facts_df):,} facts, {len(filings_df):,} filings, {len(companies_df)} companies")

        return facts_df, filings_df, companies_df


class DataCleaner:
    """Data cleaning and transformation module"""

    def __init__(self, metrics: PipelineMetrics):
        self.metrics = metrics

    def clean_facts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize facts data"""

        if df.empty:
            return df

        logger.info("Cleaning facts data...")

        # Type conversions
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        # Date conversions
        for col in ['period_start', 'period_end', 'filed']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Clean fiscal period
        df['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce')

        # Add derived fields
        df['period_type'] = df.apply(self._determine_period_type, axis=1)

        # Add canonical metric mapping
        df['canonical_metric'] = df['concept'].apply(self._map_to_canonical)

        # Generate unique key
        df['fact_id'] = df.apply(
            lambda r: hashlib.md5(
                f"{r['cik']}{r['concept']}{r['period_end']}{r['accession']}".encode()
            ).hexdigest()[:16],
            axis=1
        )

        # Drop rows with null values
        initial_count = len(df)
        df = df.dropna(subset=['value', 'period_end'])
        dropped = initial_count - len(df)
        if dropped > 0:
            logger.info(f"Dropped {dropped:,} rows with null values")

        self.metrics.cleaned_facts = len(df)
        logger.info(f"Cleaning complete: {len(df):,} facts")

        return df

    def _determine_period_type(self, row) -> str:
        """Determine if fact is instant, quarterly, or annual"""
        if pd.isna(row.get('period_start')):
            return 'instant'

        start = pd.to_datetime(row['period_start'])
        end = pd.to_datetime(row['period_end'])

        if pd.isna(start) or pd.isna(end):
            return 'unknown'

        days = (end - start).days

        if days < 100:
            return 'quarterly'
        elif days < 400:
            return 'annual'
        else:
            return 'multi-year'

    def _map_to_canonical(self, concept: str) -> Optional[str]:
        """Map XBRL concept to canonical metric name"""
        for canonical, concepts in KEY_CONCEPTS.items():
            if concept in concepts:
                return canonical
        return None


class DataModeler:
    """Data modeling module - creates dimensional model"""

    def __init__(self, metrics: PipelineMetrics):
        self.metrics = metrics

    def create_model(self, facts_df: pd.DataFrame, filings_df: pd.DataFrame,
                    companies_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create dimensional model"""

        logger.info("Creating dimensional model...")
        model = {}

        # Company dimension
        model['company_dim'] = companies_df.copy()
        model['company_dim']['company_id'] = range(1, len(companies_df) + 1)

        # Filings dimension
        if not filings_df.empty:
            model['filings_dim'] = filings_df.drop_duplicates(subset=['accession'])
            model['filings_dim']['filing_id'] = range(1, len(model['filings_dim']) + 1)

        # Concept mapping dimension
        concept_map = []
        for canonical, concepts in KEY_CONCEPTS.items():
            for concept in concepts:
                concept_map.append({
                    'raw_concept': concept,
                    'canonical_metric': canonical,
                    'category': self._get_concept_category(canonical)
                })
        model['concept_map'] = pd.DataFrame(concept_map)

        # XBRL Facts (fact table)
        model['xbrl_facts'] = facts_df.copy()

        # Add foreign keys
        company_map = dict(zip(companies_df['cik'], range(1, len(companies_df) + 1)))
        model['xbrl_facts']['company_id'] = model['xbrl_facts']['cik'].map(company_map)

        logger.info(f"Model created: {len(model)} tables")
        for name, table in model.items():
            logger.info(f"  {name}: {len(table):,} rows, {len(table.columns)} columns")

        return model

    def _get_concept_category(self, canonical: str) -> str:
        """Get financial statement category for concept"""
        income_stmt = ['revenue', 'cost_of_revenue', 'gross_profit', 'operating_income', 'net_income', 'eps']
        balance_sheet = ['assets', 'liabilities', 'equity', 'cash', 'current_assets', 'current_liabilities']
        cash_flow = ['operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow', 'capex']

        if canonical in income_stmt:
            return 'Income Statement'
        elif canonical in balance_sheet:
            return 'Balance Sheet'
        elif canonical in cash_flow:
            return 'Cash Flow'
        return 'Other'


class QualityGateRunner:
    """Quality gate validation module"""

    def __init__(self, metrics: PipelineMetrics):
        self.metrics = metrics

    def run_all_gates(self, facts_df: pd.DataFrame, model: Dict[str, pd.DataFrame]) -> List[QualityGate]:
        """Run all quality gates"""

        logger.info("Running quality gates...")
        gates = []

        gates.append(self.check_unit_consistency(facts_df))
        gates.append(self.check_period_logic(facts_df))
        gates.append(self.check_coverage(facts_df, model))
        gates.append(self.check_restatements(facts_df))
        gates.append(self.check_outliers(facts_df))

        self.metrics.quality_gates = gates

        for gate in gates:
            status = "PASS" if gate.passed else "FAIL"
            logger.info(f"  {gate.name}: {status} (score: {gate.score:.2%})")

        return gates

    def check_unit_consistency(self, df: pd.DataFrame) -> QualityGate:
        """Check unit consistency per concept"""
        issues = []

        # Group by concept and check units
        concept_units = df.groupby('concept')['unit'].nunique()
        inconsistent = concept_units[concept_units > 1]

        if len(inconsistent) > 0:
            pct = len(inconsistent) / len(concept_units) if len(concept_units) > 0 else 0
            if pct > 0.05:
                issues.append(f"{len(inconsistent)} concepts have multiple units ({pct:.1%})")

        score = 1.0 - (len(inconsistent) / len(concept_units)) if len(concept_units) > 0 else 0

        return QualityGate(
            name='unit_consistency',
            passed=score >= 0.90,
            score=min(1.0, max(0.0, score)),
            threshold=0.90,
            details=f"Checked {len(concept_units)} concepts for unit consistency",
            issues=issues
        )

    def check_period_logic(self, df: pd.DataFrame) -> QualityGate:
        """Check period logic (quarterly vs annual)"""
        issues = []

        # Check that period_type is valid
        valid_types = ['instant', 'quarterly', 'annual', 'multi-year']
        invalid = df[~df['period_type'].isin(valid_types)]

        invalid_pct = len(invalid) / len(df) if len(df) > 0 else 0
        if invalid_pct > 0.01:
            issues.append(f"{len(invalid):,} facts have invalid period logic ({invalid_pct:.1%})")

        score = 1.0 - invalid_pct

        return QualityGate(
            name='period_logic',
            passed=score >= 0.95,
            score=min(1.0, max(0.0, score)),
            threshold=0.95,
            details=f"Validated period types for {len(df):,} facts",
            issues=issues
        )

    def check_coverage(self, df: pd.DataFrame, model: Dict[str, pd.DataFrame]) -> QualityGate:
        """Check coverage of required canonical metrics"""
        issues = []

        required_metrics = ['revenue', 'net_income', 'assets', 'equity']
        companies = df['cik'].unique()

        coverage_scores = []
        for cik in companies:
            company_facts = df[df['cik'] == cik]
            company_metrics = set(company_facts['canonical_metric'].dropna().unique())
            coverage = len(company_metrics & set(required_metrics)) / len(required_metrics)
            coverage_scores.append(coverage)

        avg_coverage = np.mean(coverage_scores) if coverage_scores else 0

        if avg_coverage < 0.80:
            issues.append(f"Average metric coverage is {avg_coverage:.1%}")

        return QualityGate(
            name='coverage',
            passed=avg_coverage >= 0.80,
            score=avg_coverage,
            threshold=0.80,
            details=f"Checked coverage across {len(companies)} companies",
            issues=issues
        )

    def check_restatements(self, df: pd.DataFrame) -> QualityGate:
        """Detect potential restatements (same concept+period, different values)"""
        issues = []

        # Group by cik, concept, period_end and count unique values
        grouped = df.groupby(['cik', 'concept', 'period_end'])['value'].nunique()
        restatements = grouped[grouped > 1]

        restatement_rate = len(restatements) / len(grouped) if len(grouped) > 0 else 0

        if restatement_rate > 0.05:
            issues.append(f"{len(restatements):,} potential restatements detected ({restatement_rate:.1%})")

        # Restatements are informational, not failures
        score = 1.0 - min(0.5, restatement_rate)  # Cap penalty at 50%

        return QualityGate(
            name='restatement',
            passed=True,  # Informational only
            score=score,
            threshold=0.50,
            details=f"Detected {len(restatements):,} restatements across {len(df['cik'].unique())} companies",
            issues=issues
        )

    def check_outliers(self, df: pd.DataFrame) -> QualityGate:
        """Check for extreme outliers in values"""
        issues = []

        # Check for anomalous QoQ changes per company/metric
        key_metrics = df[df['canonical_metric'].notna()].copy()

        if len(key_metrics) > 0:
            # Calculate z-scores within each company/metric group
            def calc_zscore(group):
                if len(group) < 3:
                    return pd.Series([0] * len(group), index=group.index)
                values = group['value']
                return (values - values.mean()) / values.std()

            key_metrics['zscore'] = key_metrics.groupby(['cik', 'canonical_metric']).apply(
                lambda g: calc_zscore(g)
            ).reset_index(level=[0, 1], drop=True)

            extreme_outliers = key_metrics[key_metrics['zscore'].abs() > 5]
            outlier_rate = len(extreme_outliers) / len(key_metrics) if len(key_metrics) > 0 else 0

            if outlier_rate > 0.01:
                issues.append(f"{len(extreme_outliers):,} extreme outliers (z>5) detected ({outlier_rate:.1%})")

            score = 1.0 - min(0.3, outlier_rate * 10)
        else:
            score = 0.8
            issues.append("No key metrics found for outlier analysis")

        return QualityGate(
            name='outliers',
            passed=score >= 0.85,
            score=min(1.0, max(0.0, score)),
            threshold=0.85,
            details=f"Analyzed outliers in {len(key_metrics):,} key metric facts",
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

        facts = self.model.get('xbrl_facts', pd.DataFrame())
        companies = self.model.get('company_dim', pd.DataFrame())

        if facts.empty:
            return kpis

        # Get annual facts only for metrics
        annual_facts = facts[facts['period_type'] == 'annual'].copy()

        # Financial Metrics by Company
        kpis['company_metrics'] = self._calc_company_metrics(annual_facts, companies)

        # Cohort Benchmarking
        kpis['cohort_benchmarks'] = self._calc_cohort_benchmarks(annual_facts)

        # Coverage Stats
        kpis['coverage_stats'] = self._calc_coverage_stats(facts)

        # Summary Stats
        kpis['summary'] = self._calc_summary_stats(facts, companies)

        logger.info(f"Calculated {len(kpis)} KPI categories")
        return kpis

    def _calc_company_metrics(self, facts: pd.DataFrame, companies: pd.DataFrame) -> Dict:
        """Calculate financial metrics by company"""
        metrics = {}

        for _, company in companies.iterrows():
            cik = company['cik']
            ticker = company['ticker']

            company_facts = facts[facts['cik'] == cik]

            # Get latest year's metrics
            if company_facts.empty:
                continue

            latest_year = company_facts['fiscal_year'].max()
            latest_facts = company_facts[company_facts['fiscal_year'] == latest_year]

            company_metrics = {'fiscal_year': int(latest_year) if pd.notna(latest_year) else None}

            for metric in ['revenue', 'net_income', 'gross_profit', 'operating_income',
                          'assets', 'liabilities', 'equity', 'operating_cash_flow']:
                metric_facts = latest_facts[latest_facts['canonical_metric'] == metric]
                if not metric_facts.empty:
                    company_metrics[metric] = float(metric_facts['value'].iloc[0])

            # Calculate ratios
            if 'revenue' in company_metrics and 'net_income' in company_metrics:
                if company_metrics['revenue'] != 0:
                    company_metrics['net_margin'] = company_metrics['net_income'] / company_metrics['revenue']

            if 'revenue' in company_metrics and 'gross_profit' in company_metrics:
                if company_metrics['revenue'] != 0:
                    company_metrics['gross_margin'] = company_metrics['gross_profit'] / company_metrics['revenue']

            if 'assets' in company_metrics and 'net_income' in company_metrics:
                if company_metrics['assets'] != 0:
                    company_metrics['roa'] = company_metrics['net_income'] / company_metrics['assets']

            if 'equity' in company_metrics and 'net_income' in company_metrics:
                if company_metrics['equity'] != 0:
                    company_metrics['roe'] = company_metrics['net_income'] / company_metrics['equity']

            metrics[ticker] = company_metrics

        return metrics

    def _calc_cohort_benchmarks(self, facts: pd.DataFrame) -> Dict:
        """Calculate cohort percentile benchmarks"""
        benchmarks = {}

        # Get latest complete year
        latest_year = facts['fiscal_year'].max()
        latest_facts = facts[facts['fiscal_year'] == latest_year]

        for metric in ['revenue', 'net_income', 'assets']:
            metric_facts = latest_facts[latest_facts['canonical_metric'] == metric]
            if not metric_facts.empty:
                values = metric_facts.groupby('cik')['value'].first()

                benchmarks[metric] = {
                    'min': float(values.min()),
                    'p25': float(values.quantile(0.25)),
                    'median': float(values.median()),
                    'p75': float(values.quantile(0.75)),
                    'max': float(values.max()),
                    'mean': float(values.mean()),
                    'std': float(values.std())
                }

        return benchmarks

    def _calc_coverage_stats(self, facts: pd.DataFrame) -> Dict:
        """Calculate coverage statistics"""
        coverage = {}

        # Concept coverage
        total_concepts = facts['concept'].nunique()
        mapped_concepts = facts[facts['canonical_metric'].notna()]['concept'].nunique()

        coverage['total_concepts'] = int(total_concepts)
        coverage['mapped_concepts'] = int(mapped_concepts)
        coverage['mapping_rate'] = mapped_concepts / total_concepts if total_concepts > 0 else 0

        # Company coverage
        for metric in KEY_CONCEPTS.keys():
            companies_with = facts[facts['canonical_metric'] == metric]['cik'].nunique()
            total_companies = facts['cik'].nunique()
            coverage[f'{metric}_coverage'] = companies_with / total_companies if total_companies > 0 else 0

        return coverage

    def _calc_summary_stats(self, facts: pd.DataFrame, companies: pd.DataFrame) -> Dict:
        """Calculate summary statistics"""
        summary = {}

        summary['total_facts'] = int(len(facts))
        summary['total_companies'] = int(len(companies))
        summary['total_concepts'] = int(facts['concept'].nunique())

        # Facts by type
        type_counts = facts['period_type'].value_counts().to_dict()
        summary['facts_by_type'] = {str(k): int(v) for k, v in type_counts.items()}

        # Facts by form
        form_counts = facts['form'].value_counts().head(5).to_dict()
        summary['facts_by_form'] = {str(k): int(v) for k, v in form_counts.items()}

        # Sector breakdown
        if 'sector' in companies.columns:
            sector_counts = companies['sector'].value_counts().to_dict()
            summary['companies_by_sector'] = {str(k): int(v) for k, v in sector_counts.items()}

        return summary


def run_pipeline(companies: List[Dict] = None) -> Tuple[Dict[str, pd.DataFrame], Dict, PipelineMetrics]:
    """Run the full P2-SEC pipeline"""

    logger.info("=" * 60)
    logger.info("P2-SEC: SEC EDGAR XBRL Financial Facts Pipeline")
    logger.info("=" * 60)

    if companies is None:
        companies = COMPANY_COHORT

    metrics = PipelineMetrics()

    # Initialize client
    client = SECEdgarClient()

    # Data Ingestion
    ingestion = DataIngestion(client, metrics)
    facts_df, filings_df, companies_df = ingestion.ingest_cohort(companies)

    # Save raw data
    facts_df.to_csv(DATA_DIR / 'raw_xbrl_facts.csv', index=False)
    filings_df.to_csv(DATA_DIR / 'raw_filings.csv', index=False)
    companies_df.to_csv(DATA_DIR / 'raw_companies.csv', index=False)
    logger.info("Saved raw data files")

    # Data Cleaning
    cleaner = DataCleaner(metrics)
    cleaned_facts = cleaner.clean_facts(facts_df)
    cleaned_facts.to_csv(DATA_DIR / 'cleaned_xbrl_facts.csv', index=False)
    logger.info("Saved cleaned facts")

    # Data Modeling
    modeler = DataModeler(metrics)
    model = modeler.create_model(cleaned_facts, filings_df, companies_df)

    # Save model tables
    for name, table in model.items():
        table.to_csv(DATA_DIR / f'{name}.csv', index=False)
        logger.info(f"Saved model table: {name}")

    # Quality Gates
    gate_runner = QualityGateRunner(metrics)
    gates = gate_runner.run_all_gates(cleaned_facts, model)

    # KPI Calculation
    kpi_calc = KPICalculator(model)
    kpis = kpi_calc.calculate_all_kpis()

    # Save KPIs
    with open(DATA_DIR / 'kpis.json', 'w') as f:
        json.dump(kpis, f, indent=2, default=str)
    logger.info("Saved KPIs")

    # Finalize metrics
    metrics.end_time = datetime.now(timezone.utc)

    # Save metrics
    metrics_dict = {
        'start_time': metrics.start_time.isoformat(),
        'end_time': metrics.end_time.isoformat(),
        'duration_seconds': float(metrics.duration_seconds),
        'raw_facts': int(metrics.raw_facts),
        'cleaned_facts': int(metrics.cleaned_facts),
        'companies_processed': int(metrics.companies_processed),
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
    with open(DATA_DIR / 'pipeline_metrics.json', 'w') as f:
        json.dump(metrics_dict, f, indent=2)
    logger.info("Saved metrics")

    logger.info("=" * 60)
    logger.info("Pipeline Complete!")
    logger.info(f"Duration: {metrics.duration_seconds:.1f}s")
    logger.info(f"Facts: {metrics.raw_facts:,} raw → {metrics.cleaned_facts:,} cleaned")
    logger.info(f"Companies: {metrics.companies_processed}")
    logger.info(f"Quality Score: {metrics.overall_quality_score:.1%}")
    logger.info("=" * 60)

    return model, kpis, metrics


if __name__ == '__main__':
    model, kpis, metrics = run_pipeline()

    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(f"Total Facts: {metrics.cleaned_facts:,}")
    print(f"Companies: {metrics.companies_processed}")
    print(f"Quality Score: {metrics.overall_quality_score:.1%}")
    print(f"API Calls: {metrics.api_calls}")
    print(f"Duration: {metrics.duration_seconds:.1f} seconds")
    print("\nQuality Gates:")
    for gate in metrics.quality_gates:
        status = "✓" if gate.passed else "✗"
        print(f"  {status} {gate.name}: {gate.score:.1%}")
