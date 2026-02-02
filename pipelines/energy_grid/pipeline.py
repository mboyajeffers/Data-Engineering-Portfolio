#!/usr/bin/env python3
"""
P3-ENG: Energy Grid Intelligence Pipeline
==========================================
Grid reliability and renewables penetration using EIA Open Data API

Data Sources:
- EIA Open Data API: https://api.eia.gov/
- EIA-930 Hourly Electric Grid Monitor
- Daily Generation by Energy Source

Author: Mboya Jeffers
Version: 1.0.0
Created: 2026-02-01

QUALITY STANDARD COMPLIANCE:
- All data from REAL public APIs (EIA/US Government)
- Verifiable at https://www.eia.gov/opendata/
- No simulated data
"""

import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import time

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
DOCS_DIR = PROJECT_ROOT / "docs"
REPORTS_DIR = PROJECT_ROOT / "reports"

for d in [DATA_DIR, DOCS_DIR, REPORTS_DIR]:
    d.mkdir(exist_ok=True)

# EIA API Configuration
EIA_API_BASE = "https://api.eia.gov/v2"
EIA_API_KEY = "DEMO_KEY"  # Public demo key - limited rate

# Major balancing authorities (grid operators)
TARGET_REGIONS = [
    'CISO',   # California ISO
    'ERCO',   # ERCOT (Texas)
    'MISO',   # Midcontinent ISO
    'PJM',    # PJM Interconnection (Eastern US)
    'SWPP',   # Southwest Power Pool
    'NYIS',   # New York ISO
    'ISNE',   # ISO New England
]

# Fuel type categories
RENEWABLE_FUELS = ['SUN', 'WND', 'WAT', 'OTH']  # Solar, Wind, Hydro, Other renewables
FOSSIL_FUELS = ['NG', 'COL', 'PET']  # Natural Gas, Coal, Petroleum
NUCLEAR = ['NUC']


class EIAClient:
    """Client for EIA Open Data API"""

    def __init__(self, api_key: str = EIA_API_KEY):
        self.api_key = api_key
        self.session = requests.Session()
        self.api_calls = 0
        self.api_errors = 0

    def fetch_daily_generation(self, days_back: int = 30, regions: List[str] = None) -> pd.DataFrame:
        """Fetch daily generation by fuel type for specified regions"""

        if regions is None:
            regions = TARGET_REGIONS

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        url = f"{EIA_API_BASE}/electricity/rto/daily-fuel-type-data/data/"

        params = {
            'api_key': self.api_key,
            'frequency': 'daily',
            'data[0]': 'value',
            'start': start_date.strftime('%Y-%m-%d'),
            'end': end_date.strftime('%Y-%m-%d'),
            'sort[0][column]': 'period',
            'sort[0][direction]': 'desc',
            'offset': 0,
            'length': 5000
        }

        # Add facets for regions
        for i, region in enumerate(regions):
            params[f'facets[respondent][{i}]'] = region

        all_data = []

        try:
            print(f"  Fetching daily generation data...")
            self.api_calls += 1

            response = self.session.get(url, params=params, timeout=120)
            response.raise_for_status()

            data = response.json()
            records = data.get('response', {}).get('data', [])

            print(f"    Retrieved {len(records)} records")

            if records:
                df = pd.DataFrame(records)
                return df

            return pd.DataFrame()

        except requests.exceptions.RequestException as e:
            self.api_errors += 1
            print(f"    ERROR: {e}")
            return pd.DataFrame()

        finally:
            time.sleep(1)  # Rate limiting

    def fetch_interchange_data(self, days_back: int = 7) -> pd.DataFrame:
        """Fetch regional interchange (imports/exports) data"""

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        url = f"{EIA_API_BASE}/electricity/rto/daily-region-data/data/"

        params = {
            'api_key': self.api_key,
            'frequency': 'daily',
            'data[0]': 'value',
            'start': start_date.strftime('%Y-%m-%d'),
            'end': end_date.strftime('%Y-%m-%d'),
            'sort[0][column]': 'period',
            'sort[0][direction]': 'desc',
            'offset': 0,
            'length': 5000
        }

        try:
            print(f"  Fetching regional interchange data...")
            self.api_calls += 1

            response = self.session.get(url, params=params, timeout=120)
            response.raise_for_status()

            data = response.json()
            records = data.get('response', {}).get('data', [])

            print(f"    Retrieved {len(records)} records")

            if records:
                return pd.DataFrame(records)

            return pd.DataFrame()

        except requests.exceptions.RequestException as e:
            self.api_errors += 1
            print(f"    ERROR: {e}")
            return pd.DataFrame()


class EnergyAnalyzer:
    """Analyze energy generation and grid metrics"""

    def __init__(self, generation_df: pd.DataFrame):
        self.gen = generation_df

    def calculate_renewables_penetration(self) -> pd.DataFrame:
        """Calculate renewable energy percentage by region and day"""

        if self.gen.empty:
            return pd.DataFrame()

        # Clean the data
        df = self.gen.copy()
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        # Classify fuel types
        df['fuel_category'] = df['fueltype'].apply(self._classify_fuel)

        # Aggregate by region and date
        grouped = df.groupby(['respondent', 'period', 'fuel_category'])['value'].sum().reset_index()

        # Pivot to get fuel categories as columns
        pivot = grouped.pivot_table(
            index=['respondent', 'period'],
            columns='fuel_category',
            values='value',
            fill_value=0
        ).reset_index()

        # Calculate totals and percentages
        if 'renewable' in pivot.columns and 'fossil' in pivot.columns:
            pivot['total_generation'] = pivot['renewable'] + pivot['fossil'] + pivot.get('nuclear', 0) + pivot.get('other', 0)
            pivot['renewable_pct'] = (pivot['renewable'] / pivot['total_generation'] * 100).round(2)
            pivot['fossil_pct'] = (pivot['fossil'] / pivot['total_generation'] * 100).round(2)

        return pivot

    def _classify_fuel(self, fuel_type: str) -> str:
        """Classify fuel type into category"""
        if fuel_type in RENEWABLE_FUELS:
            return 'renewable'
        elif fuel_type in FOSSIL_FUELS:
            return 'fossil'
        elif fuel_type in NUCLEAR:
            return 'nuclear'
        else:
            return 'other'


class KPICalculator:
    """Calculate energy grid KPIs"""

    def __init__(self, generation_df: pd.DataFrame, renewables_df: pd.DataFrame):
        self.gen = generation_df
        self.renewables = renewables_df

    def calculate_kpis(self) -> Dict[str, Any]:
        """Calculate all KPIs"""

        if self.gen.empty:
            return self._empty_kpis()

        # Clean generation data
        gen = self.gen.copy()
        gen['value'] = pd.to_numeric(gen['value'], errors='coerce')

        kpis = {
            'metadata': {
                'pipeline': 'P3-ENG',
                'generated': datetime.now(timezone.utc).isoformat(),
                'source': 'EIA Open Data API (EIA-930)',
                'data_disclaimer': 'REAL US government energy data - no simulation'
            },
            'summary': {
                'total_records': len(gen),
                'regions_covered': gen['respondent'].nunique() if 'respondent' in gen.columns else 0,
                'days_analyzed': gen['period'].nunique() if 'period' in gen.columns else 0,
                'total_generation_mwh': int(gen['value'].sum()) if not gen['value'].isna().all() else 0
            },
            'regional_summary': self._regional_summary(gen),
            'fuel_mix': self._fuel_mix(gen),
            'renewables_penetration': self._renewables_analysis(),
            'top_renewable_days': self._top_renewable_days()
        }

        return kpis

    def _regional_summary(self, gen: pd.DataFrame) -> List[Dict]:
        """Summary by balancing authority"""
        if 'respondent' not in gen.columns:
            return []

        regional = gen.groupby('respondent').agg({
            'value': ['sum', 'mean', 'count']
        }).round(2)

        regional.columns = ['total_mwh', 'avg_daily_mwh', 'records']
        regional = regional.reset_index()

        return regional.to_dict('records')

    def _fuel_mix(self, gen: pd.DataFrame) -> Dict[str, float]:
        """Overall fuel type breakdown"""
        if 'fueltype' not in gen.columns:
            return {}

        fuel_totals = gen.groupby('fueltype')['value'].sum()
        total = fuel_totals.sum()

        if total == 0:
            return {}

        percentages = (fuel_totals / total * 100).round(2).to_dict()
        return percentages

    def _renewables_analysis(self) -> Dict[str, Any]:
        """Renewables penetration metrics"""
        if self.renewables.empty:
            return {}

        if 'renewable_pct' not in self.renewables.columns:
            return {}

        return {
            'avg_renewable_pct': round(self.renewables['renewable_pct'].mean(), 2),
            'max_renewable_pct': round(self.renewables['renewable_pct'].max(), 2),
            'min_renewable_pct': round(self.renewables['renewable_pct'].min(), 2),
            'by_region': self._renewables_by_region()
        }

    def _renewables_by_region(self) -> List[Dict]:
        """Renewables by region"""
        if self.renewables.empty or 'respondent' not in self.renewables.columns:
            return []

        regional = self.renewables.groupby('respondent').agg({
            'renewable_pct': 'mean',
            'total_generation': 'sum'
        }).round(2).reset_index()

        regional = regional.sort_values('renewable_pct', ascending=False)

        return regional.to_dict('records')

    def _top_renewable_days(self) -> List[Dict]:
        """Days with highest renewable penetration"""
        if self.renewables.empty or 'renewable_pct' not in self.renewables.columns:
            return []

        top = self.renewables.nlargest(10, 'renewable_pct')

        return top[['respondent', 'period', 'renewable_pct', 'total_generation']].to_dict('records')

    def _empty_kpis(self) -> Dict[str, Any]:
        """Return empty KPI structure"""
        return {
            'summary': {'total_records': 0},
            'regional_summary': [],
            'fuel_mix': {},
            'renewables_penetration': {},
            'top_renewable_days': []
        }


def run_pipeline():
    """Main pipeline execution"""

    print("=" * 60)
    print("P3-ENG: Energy Grid Intelligence Pipeline")
    print("=" * 60)

    start_time = datetime.now(timezone.utc)

    # Initialize client
    eia = EIAClient()

    # Phase 1: Fetch data
    print("\n[1/5] Fetching EIA energy data...")

    generation_df = eia.fetch_daily_generation(days_back=30)

    # Save raw data
    if not generation_df.empty:
        generation_df.to_csv(DATA_DIR / 'raw_generation.csv', index=False)
        print(f"  Saved {len(generation_df)} generation records")

    # Phase 2: Analyze
    print("\n[2/5] Analyzing energy mix...")

    analyzer = EnergyAnalyzer(generation_df)
    renewables_df = analyzer.calculate_renewables_penetration()

    if not renewables_df.empty:
        renewables_df.to_csv(DATA_DIR / 'renewables_analysis.csv', index=False)
        print(f"  Calculated renewables for {len(renewables_df)} region-days")

    # Phase 3: Quality gates
    print("\n[3/5] Running quality gates...")

    quality_score = 100.0
    quality_gates = []

    if not generation_df.empty:
        # Convert value column to numeric
        generation_df['value'] = pd.to_numeric(generation_df['value'], errors='coerce')

        # Check data completeness
        regions_found = generation_df['respondent'].nunique() if 'respondent' in generation_df.columns else 0
        regions_pct = (regions_found / len(TARGET_REGIONS)) * 100

        quality_gates.append({
            'gate': 'region_coverage',
            'value': round(regions_pct, 1),
            'threshold': 50,
            'passed': regions_pct >= 50
        })

        # Check for valid values
        valid_values = (generation_df['value'].notna() & (generation_df['value'] >= 0)).mean() * 100
        quality_gates.append({
            'gate': 'value_validity',
            'value': round(valid_values, 1),
            'threshold': 90,
            'passed': valid_values >= 90
        })

        for gate in quality_gates:
            status = "PASS" if gate['passed'] else "FAIL"
            print(f"  {gate['gate']}: {status} ({gate['value']}%)")
            if not gate['passed']:
                quality_score -= 25

    print(f"\n  Overall Quality Score: {quality_score}%")

    # Phase 4: Calculate KPIs
    print("\n[4/5] Calculating KPIs...")

    kpi_calc = KPICalculator(generation_df, renewables_df)
    kpis = kpi_calc.calculate_kpis()

    print(f"  Total records: {kpis['summary']['total_records']}")
    print(f"  Regions: {kpis['summary']['regions_covered']}")
    print(f"  Days: {kpis['summary']['days_analyzed']}")

    if kpis.get('renewables_penetration'):
        print(f"  Avg renewable: {kpis['renewables_penetration'].get('avg_renewable_pct', 'N/A')}%")

    # Save KPIs
    with open(DATA_DIR / 'kpis.json', 'w') as f:
        json.dump(kpis, f, indent=2, default=str)

    # Phase 5: Pipeline metrics
    print("\n[5/5] Saving pipeline metrics...")

    end_time = datetime.now(timezone.utc)

    metrics = {
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'duration_seconds': (end_time - start_time).total_seconds(),
        'api_calls': eia.api_calls,
        'api_errors': eia.api_errors,
        'records_fetched': len(generation_df),
        'regions_analyzed': generation_df['respondent'].nunique() if not generation_df.empty and 'respondent' in generation_df.columns else 0,
        'quality_score': quality_score,
        'quality_gates': quality_gates,
        'data_sources': {
            'eia_930': {
                'url': 'https://www.eia.gov/opendata/',
                'description': 'EIA Open Data API - Hourly Electric Grid Monitor',
                'verifiable': True
            }
        }
    }

    with open(DATA_DIR / 'pipeline_metrics.json', 'w') as f:
        json.dump(metrics, f, indent=2, default=str)

    print("\n" + "=" * 60)
    print("Pipeline Complete!")
    print(f"  Duration: {metrics['duration_seconds']:.1f} seconds")
    print(f"  Records: {len(generation_df)}")
    print(f"  Quality Score: {quality_score}%")
    print("=" * 60)


if __name__ == '__main__':
    run_pipeline()
