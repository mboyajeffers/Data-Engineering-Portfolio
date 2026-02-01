#!/usr/bin/env python3
"""
P3-MED: Healthcare Quality Intelligence Pipeline
================================================
Hospital quality benchmarking using Data.Medicare.gov (Hospital Compare)

Data Sources:
- Hospital General Information: https://data.cms.gov/provider-data/dataset/xubh-q36u
- Timely and Effective Care: https://data.cms.gov/provider-data/dataset/yv7e-xc69
- Patient Experience (HCAHPS): https://data.cms.gov/provider-data/dataset/dgck-syfz
- Readmissions and Deaths: https://data.cms.gov/provider-data/dataset/ynj2-r877

Author: Mboya Jeffers
Version: 1.0.0
Created: 2026-02-01

QUALITY STANDARD COMPLIANCE:
- All data from REAL public APIs (Data.Medicare.gov/Socrata)
- Verifiable by anyone with internet access
- No simulated data
"""

import requests
import pandas as pd
import numpy as np
import json
import hashlib
from datetime import datetime, timezone
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

# Data.Medicare.gov CMS Provider Data API endpoints (public, no API key required)
# These are REAL endpoints that can be verified at data.cms.gov
# Using CMS Provider Data API format: /provider-data/api/1/datastore/query/{id}/0
DATASETS = {
    'hospital_info': {
        'id': 'xubh-q36u',
        'name': 'Hospital General Information',
        'base_url': 'https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0'
    },
    'timely_care': {
        'id': 'yv7e-xc69',
        'name': 'Timely and Effective Care - Hospital',
        'base_url': 'https://data.cms.gov/provider-data/api/1/datastore/query/yv7e-xc69/0'
    },
    'patient_experience': {
        'id': 'dgck-syfz',
        'name': 'Patient Survey (HCAHPS)',
        'base_url': 'https://data.cms.gov/provider-data/api/1/datastore/query/dgck-syfz/0'
    },
    'readmissions': {
        'id': 'ynj2-r877',
        'name': 'Complications and Deaths',
        'base_url': 'https://data.cms.gov/provider-data/api/1/datastore/query/ynj2-r877/0'
    }
}

# States to focus on (limit scope for demo)
TARGET_STATES = ['CA', 'TX', 'FL', 'NY', 'PA', 'IL', 'OH', 'GA', 'NC', 'MI']


class MedicareDataClient:
    """Client for Data.Medicare.gov Socrata API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CMS-Portfolio-Demo/1.0 (mboyajeffers9@gmail.com)'
        })
        self.api_calls = 0
        self.api_errors = 0

    def fetch_dataset(self, dataset_key: str, limit: int = 5000, offset: int = 0,
                      state_filter: Optional[List[str]] = None) -> pd.DataFrame:
        """Fetch data from Medicare dataset using CMS Provider Data API"""

        if dataset_key not in DATASETS:
            raise ValueError(f"Unknown dataset: {dataset_key}")

        dataset = DATASETS[dataset_key]

        # CMS Provider Data API uses query params: size, offset
        params = {
            'size': limit,
            'offset': offset
        }

        try:
            print(f"  Fetching {dataset['name']}...")
            self.api_calls += 1

            response = self.session.get(dataset['base_url'], params=params, timeout=120)
            response.raise_for_status()

            data = response.json()

            # CMS API returns {'results': [...], 'count': N, ...}
            if 'results' in data:
                df = pd.DataFrame(data['results'])
                total_count = data.get('count', len(df))
                print(f"    Retrieved {len(df)} of {total_count} total records")
            elif isinstance(data, list):
                df = pd.DataFrame(data)
                print(f"    Retrieved {len(df)} records")
            else:
                df = pd.DataFrame()
                print(f"    Unexpected response format")

            # Filter by state after retrieval if needed
            if state_filter and not df.empty and 'state' in df.columns:
                df = df[df['state'].isin(state_filter)]
                print(f"    After state filter: {len(df)} records")

            return df

        except requests.exceptions.RequestException as e:
            self.api_errors += 1
            print(f"    ERROR: {e}")
            return pd.DataFrame()

        finally:
            time.sleep(0.5)  # Rate limiting


class DataCleaner:
    """Clean and normalize Medicare data"""

    @staticmethod
    def clean_hospital_info(df: pd.DataFrame) -> pd.DataFrame:
        """Clean hospital general information"""
        if df.empty:
            return df

        # Standardize column names
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]

        # Key columns we need
        keep_cols = [
            'facility_id', 'facility_name', 'address', 'citytown', 'state', 'zip_code',
            'county_name', 'phone_number', 'hospital_type', 'hospital_ownership',
            'emergency_services', 'hospital_overall_rating'
        ]

        # Keep only columns that exist
        keep_cols = [c for c in keep_cols if c in df.columns]
        df = df[keep_cols].copy()

        # Clean facility_id
        if 'facility_id' in df.columns:
            df['facility_id'] = df['facility_id'].astype(str).str.strip()

        # Clean overall rating
        if 'hospital_overall_rating' in df.columns:
            df['hospital_overall_rating'] = pd.to_numeric(
                df['hospital_overall_rating'], errors='coerce'
            )

        return df

    @staticmethod
    def clean_quality_measures(df: pd.DataFrame) -> pd.DataFrame:
        """Clean quality measure data (timely care, readmissions)"""
        if df.empty:
            return df

        df.columns = [c.lower().replace(' ', '_') for c in df.columns]

        # Common columns
        if 'facility_id' in df.columns:
            df['facility_id'] = df['facility_id'].astype(str).str.strip()

        # Convert score to numeric
        if 'score' in df.columns:
            df['score'] = pd.to_numeric(df['score'], errors='coerce')

        # Handle 'Not Available' values
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].replace(['Not Available', 'N/A', ''], np.nan)

        return df

    @staticmethod
    def clean_hcahps(df: pd.DataFrame) -> pd.DataFrame:
        """Clean HCAHPS patient survey data"""
        if df.empty:
            return df

        df.columns = [c.lower().replace(' ', '_') for c in df.columns]

        if 'facility_id' in df.columns:
            df['facility_id'] = df['facility_id'].astype(str).str.strip()

        # Convert percentages
        pct_cols = [c for c in df.columns if 'percent' in c.lower() or 'rate' in c.lower()]
        for col in pct_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df


class DataModeler:
    """Transform to star schema"""

    def __init__(self):
        self.hospital_dim = None
        self.measure_dim = None
        self.quality_fact = None

    def build_hospital_dim(self, hospital_df: pd.DataFrame) -> pd.DataFrame:
        """Build hospital dimension"""

        if hospital_df.empty:
            return pd.DataFrame()

        dim = hospital_df.copy()

        # Generate surrogate key
        dim['hospital_key'] = range(1, len(dim) + 1)

        # Reorder columns
        cols = ['hospital_key', 'facility_id'] + [c for c in dim.columns
                                                   if c not in ['hospital_key', 'facility_id']]
        dim = dim[cols]

        self.hospital_dim = dim
        return dim

    def build_quality_fact(self, measures_df: pd.DataFrame, measure_type: str) -> pd.DataFrame:
        """Build quality measures fact table"""

        if measures_df.empty:
            return pd.DataFrame()

        fact = measures_df.copy()
        fact['measure_type'] = measure_type
        fact['snapshot_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Generate fact ID
        fact['fact_id'] = fact.apply(
            lambda r: hashlib.md5(
                f"{r.get('facility_id', '')}_{r.get('measure_id', '')}_{measure_type}".encode()
            ).hexdigest()[:12],
            axis=1
        )

        return fact


class QualityGateRunner:
    """Run quality gates on data"""

    def __init__(self):
        self.gates = []

    def run_completeness_gate(self, df: pd.DataFrame, key_cols: List[str],
                              threshold: float = 0.90) -> Dict:
        """Check completeness of key columns"""

        if df.empty:
            return {
                'name': 'completeness',
                'passed': False,
                'score': 0.0,
                'threshold': threshold,
                'details': 'Empty dataframe',
                'issues': ['No data to validate']
            }

        completeness_scores = []
        issues = []

        for col in key_cols:
            if col in df.columns:
                non_null = df[col].notna().mean()
                completeness_scores.append(non_null)
                if non_null < threshold:
                    issues.append(f"{col}: {non_null*100:.1f}% complete")

        avg_score = np.mean(completeness_scores) if completeness_scores else 0
        passed = avg_score >= threshold

        gate = {
            'name': 'completeness',
            'passed': passed,
            'score': float(avg_score),
            'threshold': threshold,
            'details': f"Checked {len(key_cols)} key columns",
            'issues': issues
        }
        self.gates.append(gate)
        return gate

    def run_uniqueness_gate(self, df: pd.DataFrame, key_cols: List[str],
                            threshold: float = 0.95) -> Dict:
        """Check uniqueness of key combination"""

        if df.empty:
            return {
                'name': 'uniqueness',
                'passed': False,
                'score': 0.0,
                'threshold': threshold,
                'details': 'Empty dataframe',
                'issues': ['No data to validate']
            }

        existing_cols = [c for c in key_cols if c in df.columns]
        if not existing_cols:
            return {
                'name': 'uniqueness',
                'passed': False,
                'score': 0.0,
                'threshold': threshold,
                'details': 'Key columns not found',
                'issues': [f"Missing columns: {key_cols}"]
            }

        total = len(df)
        unique = len(df.drop_duplicates(subset=existing_cols))
        score = unique / total if total > 0 else 0

        gate = {
            'name': 'uniqueness',
            'passed': score >= threshold,
            'score': float(score),
            'threshold': threshold,
            'details': f"{unique:,} unique of {total:,} records",
            'issues': [] if score >= threshold else [f"Duplicate rate: {(1-score)*100:.2f}%"]
        }
        self.gates.append(gate)
        return gate

    def run_range_gate(self, df: pd.DataFrame, col: str,
                       min_val: float, max_val: float,
                       threshold: float = 0.95) -> Dict:
        """Check values are within expected range"""

        if df.empty or col not in df.columns:
            return {
                'name': f'range_{col}',
                'passed': False,
                'score': 0.0,
                'threshold': threshold,
                'details': f'Column {col} not found',
                'issues': [f'Missing column: {col}']
            }

        valid = df[col].notna()
        in_range = (df[col] >= min_val) & (df[col] <= max_val)

        valid_count = valid.sum()
        in_range_count = (valid & in_range).sum()

        score = in_range_count / valid_count if valid_count > 0 else 0

        gate = {
            'name': f'range_{col}',
            'passed': score >= threshold,
            'score': float(score),
            'threshold': threshold,
            'details': f"{in_range_count:,} of {valid_count:,} in range [{min_val}, {max_val}]",
            'issues': [] if score >= threshold else [f"Out of range: {(1-score)*100:.2f}%"]
        }
        self.gates.append(gate)
        return gate

    def run_suppression_gate(self, df: pd.DataFrame, threshold: float = 0.70) -> Dict:
        """Track suppressed/not available values"""

        if df.empty:
            return {
                'name': 'suppression',
                'passed': True,
                'score': 1.0,
                'threshold': threshold,
                'details': 'Empty dataframe',
                'issues': []
            }

        # Count non-suppressed values
        if 'score' in df.columns:
            non_suppressed = df['score'].notna().mean()
        else:
            non_suppressed = 1.0

        gate = {
            'name': 'suppression',
            'passed': non_suppressed >= threshold,
            'score': float(non_suppressed),
            'threshold': threshold,
            'details': f"{non_suppressed*100:.1f}% of measures have valid scores",
            'issues': [] if non_suppressed >= threshold else
                     [f"High suppression rate: {(1-non_suppressed)*100:.1f}%"]
        }
        self.gates.append(gate)
        return gate

    def get_overall_score(self) -> float:
        """Calculate weighted overall quality score"""
        if not self.gates:
            return 0.0

        weights = {
            'completeness': 0.25,
            'uniqueness': 0.25,
            'suppression': 0.20
        }

        total_weight = 0
        weighted_sum = 0

        for gate in self.gates:
            weight = weights.get(gate['name'].split('_')[0], 0.15)
            weighted_sum += gate['score'] * weight
            total_weight += weight

        return weighted_sum / total_weight if total_weight > 0 else 0


class KPICalculator:
    """Calculate healthcare quality KPIs"""

    def __init__(self, hospital_df: pd.DataFrame, quality_df: pd.DataFrame):
        self.hospitals = hospital_df
        self.quality = quality_df

    def calculate_kpis(self) -> Dict[str, Any]:
        """Calculate all KPIs"""

        kpis = {
            'summary': self._summary_stats(),
            'ratings_distribution': self._ratings_distribution(),
            'state_comparison': self._state_comparison(),
            'ownership_analysis': self._ownership_analysis(),
            'top_hospitals': self._top_hospitals(),
            'bottom_hospitals': self._bottom_hospitals()
        }

        return kpis

    def _summary_stats(self) -> Dict:
        """Overall summary statistics"""

        return {
            'total_hospitals': len(self.hospitals),
            'states_covered': self.hospitals['state'].nunique() if 'state' in self.hospitals.columns else 0,
            'with_ratings': self.hospitals['hospital_overall_rating'].notna().sum()
                           if 'hospital_overall_rating' in self.hospitals.columns else 0,
            'avg_rating': float(self.hospitals['hospital_overall_rating'].mean())
                         if 'hospital_overall_rating' in self.hospitals.columns else None,
            'quality_measures': len(self.quality) if not self.quality.empty else 0
        }

    def _ratings_distribution(self) -> Dict:
        """Distribution of hospital ratings"""

        if 'hospital_overall_rating' not in self.hospitals.columns:
            return {}

        dist = self.hospitals['hospital_overall_rating'].value_counts().sort_index()
        return {f"{int(k)}_star": int(v) for k, v in dist.items() if pd.notna(k)}

    def _state_comparison(self) -> List[Dict]:
        """Compare quality by state"""

        if 'state' not in self.hospitals.columns or 'hospital_overall_rating' not in self.hospitals.columns:
            return []

        state_stats = self.hospitals.groupby('state').agg({
            'facility_id': 'count',
            'hospital_overall_rating': ['mean', 'median']
        }).round(2)

        state_stats.columns = ['hospital_count', 'avg_rating', 'median_rating']
        state_stats = state_stats.reset_index()

        return state_stats.sort_values('avg_rating', ascending=False).head(10).to_dict('records')

    def _ownership_analysis(self) -> List[Dict]:
        """Analyze quality by ownership type"""

        if 'hospital_ownership' not in self.hospitals.columns:
            return []

        ownership_stats = self.hospitals.groupby('hospital_ownership').agg({
            'facility_id': 'count',
            'hospital_overall_rating': 'mean'
        }).round(2)

        ownership_stats.columns = ['count', 'avg_rating']
        ownership_stats = ownership_stats.reset_index()

        return ownership_stats.sort_values('avg_rating', ascending=False).to_dict('records')

    def _top_hospitals(self, n: int = 10) -> List[Dict]:
        """Get top rated hospitals"""

        if 'hospital_overall_rating' not in self.hospitals.columns:
            return []

        top = self.hospitals.nlargest(n, 'hospital_overall_rating')

        return top[['facility_id', 'facility_name', 'citytown', 'state',
                    'hospital_overall_rating']].to_dict('records')

    def _bottom_hospitals(self, n: int = 10) -> List[Dict]:
        """Get lowest rated hospitals"""

        if 'hospital_overall_rating' not in self.hospitals.columns:
            return []

        # Only include hospitals with ratings
        rated = self.hospitals[self.hospitals['hospital_overall_rating'].notna()]
        bottom = rated.nsmallest(n, 'hospital_overall_rating')

        return bottom[['facility_id', 'facility_name', 'citytown', 'state',
                       'hospital_overall_rating']].to_dict('records')


def run_pipeline():
    """Execute the full P3-MED pipeline"""

    print("=" * 60)
    print("P3-MED: Healthcare Quality Intelligence Pipeline")
    print("=" * 60)

    start_time = datetime.now(timezone.utc)

    # Initialize components
    client = MedicareDataClient()
    cleaner = DataCleaner()
    modeler = DataModeler()
    gate_runner = QualityGateRunner()

    # Step 1: Fetch data
    print("\n[1/6] Fetching data from Data.Medicare.gov...")

    raw_hospitals = client.fetch_dataset('hospital_info', limit=10000, state_filter=TARGET_STATES)
    raw_timely = client.fetch_dataset('timely_care', limit=10000, state_filter=TARGET_STATES)

    # Save raw data
    if not raw_hospitals.empty:
        raw_hospitals.to_csv(DATA_DIR / 'raw_hospitals.csv', index=False)
        print(f"  Saved {len(raw_hospitals)} raw hospital records")

    if not raw_timely.empty:
        raw_timely.to_csv(DATA_DIR / 'raw_timely_care.csv', index=False)
        print(f"  Saved {len(raw_timely)} raw timely care records")

    # Step 2: Clean data
    print("\n[2/6] Cleaning and normalizing data...")

    clean_hospitals = cleaner.clean_hospital_info(raw_hospitals)
    clean_timely = cleaner.clean_quality_measures(raw_timely)

    if not clean_hospitals.empty:
        clean_hospitals.to_csv(DATA_DIR / 'cleaned_hospitals.csv', index=False)
        print(f"  Cleaned hospitals: {len(clean_hospitals)} records")

    if not clean_timely.empty:
        clean_timely.to_csv(DATA_DIR / 'cleaned_timely_care.csv', index=False)
        print(f"  Cleaned timely care: {len(clean_timely)} records")

    # Step 3: Build star schema
    print("\n[3/6] Building star schema...")

    hospital_dim = modeler.build_hospital_dim(clean_hospitals)
    quality_fact = modeler.build_quality_fact(clean_timely, 'timely_care')

    if not hospital_dim.empty:
        hospital_dim.to_csv(DATA_DIR / 'hospital_dim.csv', index=False)
        print(f"  Hospital dimension: {len(hospital_dim)} records")

    if not quality_fact.empty:
        quality_fact.to_csv(DATA_DIR / 'quality_fact.csv', index=False)
        print(f"  Quality fact: {len(quality_fact)} records")

    # Step 4: Run quality gates
    print("\n[4/6] Running quality gates...")

    gate_runner.run_completeness_gate(
        hospital_dim,
        ['facility_id', 'facility_name', 'state', 'hospital_type'],
        threshold=0.90
    )

    gate_runner.run_uniqueness_gate(
        hospital_dim,
        ['facility_id'],
        threshold=0.99
    )

    if 'hospital_overall_rating' in hospital_dim.columns:
        gate_runner.run_range_gate(
            hospital_dim,
            'hospital_overall_rating',
            min_val=1,
            max_val=5,
            threshold=0.95
        )

    gate_runner.run_suppression_gate(quality_fact, threshold=0.60)

    for gate in gate_runner.gates:
        status = "PASS" if gate['passed'] else "FAIL"
        print(f"  {gate['name']}: {status} ({gate['score']*100:.1f}%)")

    overall_quality = gate_runner.get_overall_score()
    print(f"\n  Overall Quality Score: {overall_quality*100:.1f}%")

    # Step 5: Calculate KPIs
    print("\n[5/6] Calculating KPIs...")

    kpi_calc = KPICalculator(hospital_dim, quality_fact)
    kpis = kpi_calc.calculate_kpis()

    print(f"  Total hospitals: {kpis['summary']['total_hospitals']:,}")
    print(f"  States covered: {kpis['summary']['states_covered']}")
    print(f"  Avg rating: {kpis['summary']['avg_rating']:.2f}" if kpis['summary']['avg_rating'] else "  Avg rating: N/A")

    # Save KPIs
    with open(DATA_DIR / 'kpis.json', 'w') as f:
        json.dump(kpis, f, indent=2, default=str)

    # Step 6: Save pipeline metrics
    print("\n[6/6] Saving pipeline metrics...")

    end_time = datetime.now(timezone.utc)

    metrics = {
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'duration_seconds': (end_time - start_time).total_seconds(),
        'api_calls': client.api_calls,
        'api_errors': client.api_errors,
        'raw_hospitals': len(raw_hospitals),
        'raw_measures': len(raw_timely),
        'cleaned_hospitals': len(clean_hospitals),
        'modeled_hospitals': len(hospital_dim),
        'quality_measures': len(quality_fact),
        'overall_quality_score': overall_quality,
        'quality_gates': [
            {
                'name': g['name'],
                'passed': bool(g['passed']),
                'score': float(g['score']),
                'threshold': float(g['threshold']),
                'details': g['details'],
                'issues': g['issues']
            }
            for g in gate_runner.gates
        ],
        'data_sources': {
            'hospital_info': {
                'dataset_id': DATASETS['hospital_info']['id'],
                'url': 'https://data.cms.gov/provider-data/dataset/xubh-q36u',
                'verifiable': True
            },
            'timely_care': {
                'dataset_id': DATASETS['timely_care']['id'],
                'url': 'https://data.cms.gov/provider-data/dataset/yv7e-xc69',
                'verifiable': True
            }
        }
    }

    with open(DATA_DIR / 'pipeline_metrics.json', 'w') as f:
        json.dump(metrics, f, indent=2)

    print("\n" + "=" * 60)
    print("Pipeline Complete!")
    print(f"  Duration: {metrics['duration_seconds']:.1f} seconds")
    print(f"  API Calls: {client.api_calls}")
    print(f"  Hospitals: {len(hospital_dim):,}")
    print(f"  Quality Score: {overall_quality*100:.1f}%")
    print("=" * 60)

    return metrics


if __name__ == '__main__':
    run_pipeline()
