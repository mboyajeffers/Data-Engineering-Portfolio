#!/usr/bin/env python3
"""
P3-SEC: Cybersecurity Vulnerability Intelligence Pipeline
=========================================================
CVE prioritization using NIST NVD + CISA KEV + FIRST EPSS

Data Sources:
- NIST NVD 2.0 API: https://nvd.nist.gov/developers/vulnerabilities
- CISA KEV: https://www.cisa.gov/known-exploited-vulnerabilities-catalog
- FIRST EPSS: https://api.first.org/epss

Author: Mboya Jeffers
Version: 1.0.0
Created: 2026-02-01

QUALITY STANDARD COMPLIANCE:
- All data from REAL public APIs (government sources)
- Verifiable by anyone with internet access
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

# API Endpoints
NIST_NVD_API = "https://services.nvd.nist.gov/rest/json/cves/2.0"
CISA_KEV_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
EPSS_API = "https://api.first.org/data/v1/epss"


class NVDClient:
    """Client for NIST National Vulnerability Database API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CMS-Portfolio-Demo/1.0 (mboyajeffers9@gmail.com)'
        })
        self.api_calls = 0
        self.api_errors = 0

    def fetch_recent_cves(self, days_back: int = 30, max_results: int = 2000) -> pd.DataFrame:
        """Fetch CVEs published in the last N days"""

        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)

        params = {
            'pubStartDate': start_date.strftime('%Y-%m-%dT00:00:00.000'),
            'pubEndDate': end_date.strftime('%Y-%m-%dT23:59:59.999'),
            'resultsPerPage': min(max_results, 2000),
            'startIndex': 0
        }

        try:
            print(f"  Fetching CVEs from last {days_back} days...")
            self.api_calls += 1

            response = self.session.get(NIST_NVD_API, params=params, timeout=120)
            response.raise_for_status()

            data = response.json()

            cves = []
            for vuln in data.get('vulnerabilities', []):
                cve = vuln.get('cve', {})
                cve_id = cve.get('id', '')

                # Extract CVSS scores
                metrics = cve.get('metrics', {})
                cvss_v3 = None
                cvss_v2 = None
                severity = 'UNKNOWN'

                if 'cvssMetricV31' in metrics:
                    cvss_data = metrics['cvssMetricV31'][0]['cvssData']
                    cvss_v3 = cvss_data.get('baseScore')
                    severity = cvss_data.get('baseSeverity', 'UNKNOWN')
                elif 'cvssMetricV30' in metrics:
                    cvss_data = metrics['cvssMetricV30'][0]['cvssData']
                    cvss_v3 = cvss_data.get('baseScore')
                    severity = cvss_data.get('baseSeverity', 'UNKNOWN')

                if 'cvssMetricV2' in metrics:
                    cvss_v2 = metrics['cvssMetricV2'][0]['cvssData'].get('baseScore')

                # Extract descriptions
                descriptions = cve.get('descriptions', [])
                description = next(
                    (d['value'] for d in descriptions if d['lang'] == 'en'),
                    ''
                )

                # Extract weaknesses (CWE)
                weaknesses = cve.get('weaknesses', [])
                cwe_ids = []
                for w in weaknesses:
                    for desc in w.get('description', []):
                        if desc.get('value', '').startswith('CWE-'):
                            cwe_ids.append(desc['value'])

                cves.append({
                    'cve_id': cve_id,
                    'published': cve.get('published'),
                    'last_modified': cve.get('lastModified'),
                    'cvss_v3': cvss_v3,
                    'cvss_v2': cvss_v2,
                    'severity': severity,
                    'cwe_ids': ','.join(cwe_ids) if cwe_ids else None,
                    'description': description[:500] if description else None
                })

            df = pd.DataFrame(cves)
            total_results = data.get('totalResults', len(df))
            print(f"    Retrieved {len(df)} of {total_results} CVEs")

            return df

        except requests.exceptions.RequestException as e:
            self.api_errors += 1
            print(f"    ERROR: {e}")
            return pd.DataFrame()

        finally:
            time.sleep(6)  # NVD rate limit: 5 requests per 30 seconds without API key


class CISAClient:
    """Client for CISA Known Exploited Vulnerabilities catalog"""

    def __init__(self):
        self.session = requests.Session()
        self.api_calls = 0
        self.api_errors = 0

    def fetch_kev(self) -> pd.DataFrame:
        """Fetch the CISA KEV catalog"""

        try:
            print("  Fetching CISA KEV catalog...")
            self.api_calls += 1

            response = self.session.get(CISA_KEV_URL, timeout=60)
            response.raise_for_status()

            data = response.json()

            vulns = data.get('vulnerabilities', [])
            df = pd.DataFrame(vulns)

            print(f"    Retrieved {len(df)} known exploited vulnerabilities")

            return df

        except requests.exceptions.RequestException as e:
            self.api_errors += 1
            print(f"    ERROR: {e}")
            return pd.DataFrame()


class EPSSClient:
    """Client for FIRST EPSS API"""

    def __init__(self):
        self.session = requests.Session()
        self.api_calls = 0
        self.api_errors = 0

    def fetch_epss_scores(self, cve_ids: List[str]) -> pd.DataFrame:
        """Fetch EPSS scores for given CVE IDs"""

        if not cve_ids:
            return pd.DataFrame()

        # EPSS API accepts batch requests
        batch_size = 100
        all_scores = []

        for i in range(0, len(cve_ids), batch_size):
            batch = cve_ids[i:i+batch_size]

            try:
                self.api_calls += 1

                # EPSS uses comma-separated CVE IDs
                params = {'cve': ','.join(batch)}

                response = self.session.get(EPSS_API, params=params, timeout=60)
                response.raise_for_status()

                data = response.json()
                scores = data.get('data', [])
                all_scores.extend(scores)

                print(f"    EPSS batch {i//batch_size + 1}: {len(scores)} scores")

            except requests.exceptions.RequestException as e:
                self.api_errors += 1
                print(f"    EPSS ERROR: {e}")

            time.sleep(1)  # Rate limiting

        if all_scores:
            df = pd.DataFrame(all_scores)
            return df

        return pd.DataFrame()


class VulnerabilityAnalyzer:
    """Analyze and prioritize vulnerabilities"""

    def __init__(self, cves_df: pd.DataFrame, kev_df: pd.DataFrame, epss_df: pd.DataFrame):
        self.cves = cves_df
        self.kev = kev_df
        self.epss = epss_df

    def enrich_cves(self) -> pd.DataFrame:
        """Enrich CVEs with KEV and EPSS data"""

        if self.cves.empty:
            return pd.DataFrame()

        df = self.cves.copy()

        # Mark known exploited vulnerabilities
        if not self.kev.empty and 'cveID' in self.kev.columns:
            kev_ids = set(self.kev['cveID'].tolist())
            df['in_kev'] = df['cve_id'].isin(kev_ids)
        else:
            df['in_kev'] = False

        # Add EPSS scores
        if not self.epss.empty and 'cve' in self.epss.columns:
            epss_map = dict(zip(self.epss['cve'], self.epss['epss'].astype(float)))
            df['epss_score'] = df['cve_id'].map(epss_map)
        else:
            df['epss_score'] = None

        # Calculate priority score
        df['priority_score'] = self._calculate_priority(df)

        return df

    def _calculate_priority(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate priority score based on:
        - CVSS score (0-10)
        - EPSS score (0-1, scaled to 0-10)
        - KEV membership (bonus +3)
        """

        cvss = df['cvss_v3'].fillna(df['cvss_v2']).fillna(5.0)
        epss = df['epss_score'].fillna(0.01) * 10  # Scale to 0-10
        kev_bonus = df['in_kev'].astype(int) * 3

        # Weighted combination
        priority = (cvss * 0.5) + (epss * 0.3) + kev_bonus

        return priority.round(2)


class KPICalculator:
    """Calculate cybersecurity KPIs"""

    def __init__(self, enriched_df: pd.DataFrame, kev_df: pd.DataFrame):
        self.df = enriched_df
        self.kev = kev_df

    def calculate_kpis(self) -> Dict[str, Any]:
        """Calculate all KPIs"""

        if self.df.empty:
            return self._empty_kpis()

        kpis = {
            'metadata': {
                'pipeline': 'P3-SEC',
                'generated': datetime.now(timezone.utc).isoformat(),
                'source': 'NIST NVD + CISA KEV + FIRST EPSS',
                'data_disclaimer': 'REAL public vulnerability data - no simulation'
            },
            'summary': {
                'total_cves_analyzed': len(self.df),
                'critical_severity': len(self.df[self.df['severity'] == 'CRITICAL']),
                'high_severity': len(self.df[self.df['severity'] == 'HIGH']),
                'in_kev_catalog': len(self.df[self.df['in_kev'] == True]),
                'avg_cvss_v3': round(self.df['cvss_v3'].mean(), 2) if not self.df['cvss_v3'].isna().all() else None,
                'avg_epss': round(self.df['epss_score'].mean(), 4) if not self.df['epss_score'].isna().all() else None
            },
            'severity_distribution': self._severity_distribution(),
            'kev_stats': self._kev_stats(),
            'top_priority_cves': self._top_priority(),
            'cwe_analysis': self._cwe_analysis()
        }

        return kpis

    def _severity_distribution(self) -> Dict[str, int]:
        """Severity breakdown"""
        counts = self.df['severity'].value_counts().to_dict()
        return {k: int(v) for k, v in counts.items()}

    def _kev_stats(self) -> Dict[str, Any]:
        """KEV catalog statistics"""
        if self.kev.empty:
            return {'total_kev': 0, 'recent_additions': 0}

        total = len(self.kev)

        # Count recent additions (last 30 days)
        if 'dateAdded' in self.kev.columns:
            cutoff = datetime.now() - timedelta(days=30)
            self.kev['dateAdded'] = pd.to_datetime(self.kev['dateAdded'])
            recent = len(self.kev[self.kev['dateAdded'] >= cutoff])
        else:
            recent = 0

        return {
            'total_kev_vulnerabilities': total,
            'recent_additions_30d': recent
        }

    def _top_priority(self) -> List[Dict]:
        """Top 10 priority vulnerabilities"""
        if self.df.empty:
            return []

        top = self.df.nlargest(10, 'priority_score')

        return top[['cve_id', 'severity', 'cvss_v3', 'epss_score',
                    'in_kev', 'priority_score']].to_dict('records')

    def _cwe_analysis(self) -> List[Dict]:
        """Top vulnerability types (CWE)"""
        if self.df.empty or 'cwe_ids' not in self.df.columns:
            return []

        # Explode CWE IDs
        cwe_series = self.df['cwe_ids'].dropna().str.split(',').explode()
        cwe_counts = cwe_series.value_counts().head(10)

        return [{'cwe': k, 'count': int(v)} for k, v in cwe_counts.items()]

    def _empty_kpis(self) -> Dict[str, Any]:
        """Return empty KPI structure"""
        return {
            'summary': {'total_cves_analyzed': 0},
            'severity_distribution': {},
            'kev_stats': {},
            'top_priority_cves': [],
            'cwe_analysis': []
        }


def run_pipeline():
    """Main pipeline execution"""

    print("=" * 60)
    print("P3-SEC: Cybersecurity Vulnerability Intelligence Pipeline")
    print("=" * 60)

    start_time = datetime.now(timezone.utc)

    # Initialize clients
    nvd = NVDClient()
    cisa = CISAClient()
    epss = EPSSClient()

    # Phase 1: Fetch data
    print("\n[1/5] Fetching vulnerability data...")

    cves_df = nvd.fetch_recent_cves(days_back=30, max_results=500)
    kev_df = cisa.fetch_kev()

    # Fetch EPSS scores for our CVEs
    epss_df = pd.DataFrame()
    if not cves_df.empty:
        print("  Fetching EPSS scores...")
        epss_df = epss.fetch_epss_scores(cves_df['cve_id'].tolist())

    # Save raw data
    if not cves_df.empty:
        cves_df.to_csv(DATA_DIR / 'raw_cves.csv', index=False)
        print(f"  Saved {len(cves_df)} raw CVE records")

    if not kev_df.empty:
        kev_df.to_csv(DATA_DIR / 'kev_catalog.csv', index=False)
        print(f"  Saved {len(kev_df)} KEV records")

    # Phase 2: Analyze and enrich
    print("\n[2/5] Enriching vulnerability data...")

    analyzer = VulnerabilityAnalyzer(cves_df, kev_df, epss_df)
    enriched_df = analyzer.enrich_cves()

    if not enriched_df.empty:
        enriched_df.to_csv(DATA_DIR / 'enriched_cves.csv', index=False)
        print(f"  Enriched {len(enriched_df)} vulnerabilities")

    # Phase 3: Quality gates
    print("\n[3/5] Running quality gates...")

    quality_score = 100.0
    quality_gates = []

    if not enriched_df.empty:
        # Check CVSS completeness
        cvss_pct = (enriched_df['cvss_v3'].notna().sum() / len(enriched_df)) * 100
        quality_gates.append({
            'gate': 'cvss_completeness',
            'value': round(cvss_pct, 1),
            'threshold': 50,
            'passed': bool(cvss_pct >= 50)
        })

        # Check for valid severity values
        valid_severities = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'UNKNOWN']
        severity_valid = bool(enriched_df['severity'].isin(valid_severities).all())
        quality_gates.append({
            'gate': 'severity_validity',
            'value': severity_valid,
            'threshold': True,
            'passed': severity_valid
        })

        for gate in quality_gates:
            status = "PASS" if gate['passed'] else "FAIL"
            print(f"  {gate['gate']}: {status} ({gate['value']})")
            if not gate['passed']:
                quality_score -= 25

    print(f"\n  Overall Quality Score: {quality_score}%")

    # Phase 4: Calculate KPIs
    print("\n[4/5] Calculating KPIs...")

    kpi_calc = KPICalculator(enriched_df, kev_df)
    kpis = kpi_calc.calculate_kpis()

    print(f"  Total CVEs: {kpis['summary']['total_cves_analyzed']}")
    print(f"  Critical: {kpis['summary']['critical_severity']}")
    print(f"  High: {kpis['summary']['high_severity']}")
    print(f"  In KEV: {kpis['summary']['in_kev_catalog']}")

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
        'api_calls': nvd.api_calls + cisa.api_calls + epss.api_calls,
        'api_errors': nvd.api_errors + cisa.api_errors + epss.api_errors,
        'cves_fetched': len(cves_df),
        'kev_records': len(kev_df),
        'enriched_vulnerabilities': len(enriched_df),
        'quality_score': quality_score,
        'quality_gates': quality_gates,
        'data_sources': {
            'nvd': {
                'url': 'https://nvd.nist.gov/developers/vulnerabilities',
                'description': 'NIST National Vulnerability Database',
                'verifiable': True
            },
            'cisa_kev': {
                'url': 'https://www.cisa.gov/known-exploited-vulnerabilities-catalog',
                'description': 'CISA Known Exploited Vulnerabilities',
                'verifiable': True
            },
            'epss': {
                'url': 'https://api.first.org/epss',
                'description': 'FIRST Exploit Prediction Scoring System',
                'verifiable': True
            }
        }
    }

    with open(DATA_DIR / 'pipeline_metrics.json', 'w') as f:
        json.dump(metrics, f, indent=2, default=lambda x: bool(x) if isinstance(x, np.bool_) else str(x))

    print("\n" + "=" * 60)
    print("Pipeline Complete!")
    print(f"  Duration: {metrics['duration_seconds']:.1f} seconds")
    print(f"  CVEs Analyzed: {len(enriched_df)}")
    print(f"  KEV Records: {len(kev_df)}")
    print(f"  Quality Score: {quality_score}%")
    print("=" * 60)


if __name__ == '__main__':
    run_pipeline()
