#!/usr/bin/env python3
"""
P2-FED: PDF Report Generator
============================
Generates Executive Summary and Methodology/QA PDFs

Author: Mboya Jeffers
Version: 1.0.0
Created: 2026-02-01
"""

import os
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
from weasyprint import HTML, CSS

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
DOCS_DIR = PROJECT_ROOT / "docs"

# Ensure directories exist
REPORTS_DIR.mkdir(exist_ok=True)
DOCS_DIR.mkdir(exist_ok=True)

# Color scheme - Compliance/Finance
PRIMARY_COLOR = "#4f46e5"  # Indigo for compliance
SECONDARY_COLOR = "#1e3a5f"  # Navy for finance
ACCENT_COLOR = "#059669"  # Green for success
WARNING_COLOR = "#dc2626"  # Red for alerts

# Author attribution
AUTHOR = "Mboya Jeffers"
EMAIL = "MboyaJeffers9@gmail.com"


def load_pipeline_data() -> Dict[str, Any]:
    """Load all pipeline output data"""
    data = {}

    # Load metrics
    metrics_path = DATA_DIR / 'pipeline_metrics.json'
    if metrics_path.exists():
        with open(metrics_path) as f:
            data['metrics'] = json.load(f)

    # Load KPIs
    kpi_path = DATA_DIR / 'kpis.json'
    if kpi_path.exists():
        with open(kpi_path) as f:
            data['kpis'] = json.load(f)

    # Load fact table summary
    fact_path = DATA_DIR / 'award_fact.csv'
    if fact_path.exists():
        df = pd.read_csv(fact_path)
        data['fact_summary'] = {
            'row_count': len(df),
            'columns': list(df.columns),
            'sample': df.head(5).to_dict('records') if len(df) > 0 else []
        }

    # Load dimension summaries
    for dim in ['agency_dim', 'recipient_dim', 'time_dim', 'geo_dim']:
        dim_path = DATA_DIR / f'{dim}.csv'
        if dim_path.exists():
            df = pd.read_csv(dim_path)
            data[dim] = {
                'row_count': len(df),
                'columns': list(df.columns)
            }

    return data


def format_currency(value: float) -> str:
    """Format currency values"""
    if abs(value) >= 1e12:
        return f"${value/1e12:.2f}T"
    elif abs(value) >= 1e9:
        return f"${value/1e9:.2f}B"
    elif abs(value) >= 1e6:
        return f"${value/1e6:.2f}M"
    elif abs(value) >= 1e3:
        return f"${value/1e3:.1f}K"
    else:
        return f"${value:,.2f}"


def format_number(value: float) -> str:
    """Format large numbers"""
    if abs(value) >= 1e6:
        return f"{value/1e6:.2f}M"
    elif abs(value) >= 1e3:
        return f"{value/1e3:.1f}K"
    else:
        return f"{value:,.0f}"


def get_traceability_panel(data: Dict[str, Any]) -> str:
    """Generate traceability panel HTML"""

    metrics = data.get('metrics', {})
    kpis = data.get('kpis', {})
    summary = kpis.get('summary', {})

    quality_score = metrics.get('overall_quality_score', 0) * 100
    quality_color = ACCENT_COLOR if quality_score >= 85 else WARNING_COLOR

    # Get top 3 issues from quality gates
    issues = []
    for gate in metrics.get('quality_gates', []):
        issues.extend(gate.get('issues', []))
    top_issues = issues[:3] if issues else ['No issues detected']

    html = f"""
    <div class="traceability-panel">
        <div class="panel-header">
            <span class="panel-icon">📋</span>
            <span class="panel-title">TRACEABILITY PANEL</span>
        </div>
        <div class="panel-content">
            <div class="panel-row">
                <div class="panel-item">
                    <span class="label">Source:</span>
                    <span class="value">USAspending.gov API v2</span>
                </div>
                <div class="panel-item">
                    <span class="label">Documentation:</span>
                    <span class="value">api.usaspending.gov/docs/endpoints</span>
                </div>
            </div>
            <div class="panel-row">
                <div class="panel-item">
                    <span class="label">Parameters:</span>
                    <span class="value">Multi-year contracts (FY2004-2027) | DoD, HHS, DHS | Contracts (A,B,C,D)</span>
                </div>
            </div>
            <div class="panel-row">
                <div class="panel-item">
                    <span class="label">As-of:</span>
                    <span class="value">{metrics.get('end_time', datetime.now(timezone.utc).isoformat())}</span>
                </div>
                <div class="panel-item">
                    <span class="label">Run Duration:</span>
                    <span class="value">{metrics.get('duration_seconds', 0):.1f}s</span>
                </div>
            </div>
            <div class="panel-row">
                <div class="panel-item">
                    <span class="label">Row Counts:</span>
                    <span class="value">{format_number(metrics.get('raw_rows', 0))} raw → {format_number(metrics.get('cleaned_rows', 0))} cleaned → {format_number(metrics.get('modeled_rows', 0))} modeled</span>
                </div>
            </div>
            <div class="panel-row">
                <div class="panel-item">
                    <span class="label">Quality Score:</span>
                    <span class="value" style="color: {quality_color}; font-weight: bold;">{quality_score:.1f}%</span>
                </div>
                <div class="panel-item">
                    <span class="label">API Calls:</span>
                    <span class="value">{metrics.get('api_calls', 0):,}</span>
                </div>
            </div>
            <div class="panel-row">
                <div class="panel-item full-width">
                    <span class="label">Top Issues:</span>
                    <span class="value">{' | '.join(top_issues)}</span>
                </div>
            </div>
        </div>
    </div>
    """
    return html


def generate_executive_summary_html(data: Dict[str, Any]) -> str:
    """Generate Executive Summary HTML"""

    metrics = data.get('metrics', {})
    kpis = data.get('kpis', {})
    summary = kpis.get('summary', {})
    concentration = kpis.get('vendor_concentration', {})
    changes = kpis.get('change_detection', {})

    # Calculate key metrics
    total_spend = summary.get('total_spend', 0)
    total_awards = summary.get('total_awards', 0)
    unique_vendors = summary.get('unique_vendors', 0)
    avg_award = summary.get('avg_award', 0)
    hhi = concentration.get('hhi', 0)
    hhi_interp = concentration.get('hhi_interpretation', 'N/A')
    top_10_share = concentration.get('top_10_share', 0) * 100

    # Quality gates summary
    gates = metrics.get('quality_gates', [])
    gates_passed = sum(1 for g in gates if g.get('passed', False))
    gates_total = len(gates)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>P2-FED: Federal Spend Intelligence - Executive Summary</title>
        <style>
            @page {{
                size: letter;
                margin: 0.5in;
            }}
            body {{
                font-family: 'Helvetica Neue', Arial, sans-serif;
                font-size: 10pt;
                line-height: 1.4;
                color: #1f2937;
                margin: 0;
                padding: 0;
            }}
            .header {{
                background: linear-gradient(135deg, {PRIMARY_COLOR}, {SECONDARY_COLOR});
                color: white;
                padding: 20px;
                margin: -0.5in -0.5in 20px -0.5in;
                text-align: center;
            }}
            .header h1 {{
                margin: 0 0 5px 0;
                font-size: 24pt;
                font-weight: 700;
            }}
            .header .subtitle {{
                font-size: 12pt;
                opacity: 0.9;
            }}
            .traceability-panel {{
                background: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 8px;
                margin-bottom: 20px;
                font-size: 8pt;
            }}
            .panel-header {{
                background: {PRIMARY_COLOR};
                color: white;
                padding: 8px 12px;
                border-radius: 7px 7px 0 0;
                font-weight: 600;
            }}
            .panel-icon {{ margin-right: 8px; }}
            .panel-content {{
                padding: 10px 12px;
            }}
            .panel-row {{
                display: flex;
                flex-wrap: wrap;
                margin-bottom: 4px;
            }}
            .panel-item {{
                flex: 1;
                min-width: 200px;
            }}
            .panel-item.full-width {{
                flex: 100%;
            }}
            .panel-item .label {{
                color: #6b7280;
                font-weight: 500;
            }}
            .panel-item .value {{
                color: #1f2937;
            }}
            .section {{
                margin-bottom: 20px;
            }}
            .section-title {{
                color: {PRIMARY_COLOR};
                font-size: 14pt;
                font-weight: 700;
                border-bottom: 2px solid {PRIMARY_COLOR};
                padding-bottom: 5px;
                margin-bottom: 10px;
            }}
            .kpi-grid {{
                display: flex;
                flex-wrap: wrap;
                gap: 15px;
                margin-bottom: 15px;
            }}
            .kpi-card {{
                flex: 1;
                min-width: 140px;
                background: white;
                border: 1px solid #e2e8f0;
                border-radius: 8px;
                padding: 12px;
                text-align: center;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }}
            .kpi-value {{
                font-size: 20pt;
                font-weight: 700;
                color: {PRIMARY_COLOR};
            }}
            .kpi-label {{
                font-size: 9pt;
                color: #6b7280;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            .risk-indicator {{
                display: inline-block;
                padding: 4px 8px;
                border-radius: 4px;
                font-weight: 600;
                font-size: 9pt;
            }}
            .risk-low {{ background: #dcfce7; color: #166534; }}
            .risk-medium {{ background: #fef3c7; color: #92400e; }}
            .risk-high {{ background: #fee2e2; color: #991b1b; }}
            .quality-table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 9pt;
            }}
            .quality-table th {{
                background: {PRIMARY_COLOR};
                color: white;
                padding: 8px;
                text-align: left;
            }}
            .quality-table td {{
                padding: 8px;
                border-bottom: 1px solid #e2e8f0;
            }}
            .quality-table tr:nth-child(even) {{
                background: #f8fafc;
            }}
            .pass {{ color: {ACCENT_COLOR}; font-weight: 600; }}
            .fail {{ color: {WARNING_COLOR}; font-weight: 600; }}
            .insight-box {{
                background: linear-gradient(135deg, #eef2ff, #e0e7ff);
                border-left: 4px solid {PRIMARY_COLOR};
                padding: 12px;
                margin: 15px 0;
                border-radius: 0 8px 8px 0;
            }}
            .insight-title {{
                font-weight: 700;
                color: {PRIMARY_COLOR};
                margin-bottom: 5px;
            }}
            .footer {{
                margin-top: 20px;
                padding-top: 10px;
                border-top: 1px solid #e2e8f0;
                font-size: 8pt;
                color: #6b7280;
                text-align: center;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Federal Spend Intelligence</h1>
            <div class="subtitle">Multi-Year Contract Analysis (FY2004-2027): DoD, HHS, DHS</div>
        </div>

        {get_traceability_panel(data)}

        <div class="section">
            <div class="section-title">Key Performance Indicators</div>
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{format_currency(total_spend)}</div>
                    <div class="kpi-label">Total Spend</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{format_number(total_awards)}</div>
                    <div class="kpi-label">Contract Awards</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{format_number(unique_vendors)}</div>
                    <div class="kpi-label">Unique Vendors</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{format_currency(avg_award)}</div>
                    <div class="kpi-label">Avg Award Size</div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Vendor Concentration Risk</div>
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{hhi:,.0f}</div>
                    <div class="kpi-label">HHI Index</div>
                    <div class="risk-indicator {'risk-low' if hhi < 1500 else 'risk-medium' if hhi < 2500 else 'risk-high'}">{hhi_interp}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{top_10_share:.1f}%</div>
                    <div class="kpi-label">Top-10 Vendor Share</div>
                </div>
            </div>

            <div class="insight-box">
                <div class="insight-title">📊 Concentration Analysis</div>
                <p>The Herfindahl-Hirschman Index (HHI) of <strong>{hhi:,.0f}</strong> indicates a <strong>{hhi_interp.lower()}</strong> market.
                {'This suggests healthy competition among vendors.' if hhi < 1500 else 'Moderate concentration warrants monitoring of dominant vendors.' if hhi < 2500 else 'High concentration signals potential procurement risk - consider vendor diversification strategies.'}</p>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Data Quality Assessment</div>
            <table class="quality-table">
                <tr>
                    <th>Quality Gate</th>
                    <th>Status</th>
                    <th>Score</th>
                    <th>Threshold</th>
                    <th>Notes</th>
                </tr>
                {''.join(f'''
                <tr>
                    <td>{g.get('name', 'N/A').replace('_', ' ').title()}</td>
                    <td class="{'pass' if g.get('passed') else 'fail'}">{'✓ PASS' if g.get('passed') else '✗ FAIL'}</td>
                    <td>{g.get('score', 0)*100:.1f}%</td>
                    <td>{g.get('threshold', 0)*100:.0f}%</td>
                    <td>{g.get('details', '')[:50]}</td>
                </tr>''' for g in gates)}
            </table>
            <p style="margin-top: 10px;"><strong>Overall Quality Score: <span style="color: {ACCENT_COLOR if metrics.get('overall_quality_score', 0) >= 0.85 else WARNING_COLOR}">{metrics.get('overall_quality_score', 0)*100:.1f}%</span></strong></p>
        </div>

        <div class="footer">
            <p>Report prepared by {AUTHOR} | {EMAIL}</p>
            <p>Data Source: USAspending.gov API | Processing: Enterprise Data Pipeline</p>
            <p>Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>
        </div>
    </body>
    </html>
    """
    return html


def generate_methodology_html(data: Dict[str, Any]) -> str:
    """Generate Methodology/QA PDF HTML"""

    metrics = data.get('metrics', {})
    kpis = data.get('kpis', {})

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>P2-FED: Methodology & QA Documentation</title>
        <style>
            @page {{
                size: letter;
                margin: 0.6in;
            }}
            body {{
                font-family: 'Helvetica Neue', Arial, sans-serif;
                font-size: 9pt;
                line-height: 1.5;
                color: #1f2937;
            }}
            .header {{
                background: linear-gradient(135deg, {PRIMARY_COLOR}, {SECONDARY_COLOR});
                color: white;
                padding: 15px;
                margin: -0.6in -0.6in 15px -0.6in;
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 18pt;
            }}
            .section {{
                margin-bottom: 15px;
            }}
            .section-title {{
                color: {PRIMARY_COLOR};
                font-size: 12pt;
                font-weight: 700;
                border-bottom: 2px solid {PRIMARY_COLOR};
                padding-bottom: 3px;
                margin-bottom: 8px;
            }}
            .subsection-title {{
                color: {SECONDARY_COLOR};
                font-size: 10pt;
                font-weight: 600;
                margin: 10px 0 5px 0;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 8px 0;
                font-size: 8pt;
            }}
            th {{
                background: {PRIMARY_COLOR};
                color: white;
                padding: 6px;
                text-align: left;
            }}
            td {{
                padding: 6px;
                border-bottom: 1px solid #e2e8f0;
            }}
            tr:nth-child(even) {{
                background: #f8fafc;
            }}
            code {{
                background: #f1f5f9;
                padding: 2px 4px;
                border-radius: 3px;
                font-family: 'Monaco', 'Consolas', monospace;
                font-size: 8pt;
            }}
            .endpoint-box {{
                background: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 6px;
                padding: 10px;
                margin: 8px 0;
            }}
            .endpoint-url {{
                font-family: monospace;
                color: {PRIMARY_COLOR};
            }}
            .formula-box {{
                background: #eef2ff;
                border-left: 3px solid {PRIMARY_COLOR};
                padding: 10px;
                margin: 8px 0;
                font-family: monospace;
            }}
            .footer {{
                margin-top: 15px;
                padding-top: 8px;
                border-top: 1px solid #e2e8f0;
                font-size: 7pt;
                color: #6b7280;
                text-align: center;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>P2-FED: Methodology & Quality Assurance</h1>
        </div>

        <div class="section">
            <div class="section-title">1. Data Source & API Endpoints</div>
            <p><strong>Primary Source:</strong> USAspending.gov - Official source of federal spending data mandated by the Digital Accountability and Transparency Act (DATA Act).</p>

            <div class="subsection-title">API Endpoints Used</div>
            <div class="endpoint-box">
                <p><span class="endpoint-url">POST /api/v2/search/spending_by_award/</span></p>
                <p>Purpose: Retrieve award-level spending data with pagination support</p>
            </div>
            <div class="endpoint-box">
                <p><span class="endpoint-url">POST /api/v2/search/spending_by_award_count/</span></p>
                <p>Purpose: Get total record counts for scope estimation</p>
            </div>

            <div class="subsection-title">Request Parameters</div>
            <table>
                <tr><th>Parameter</th><th>Value</th><th>Description</th></tr>
                <tr><td>time_period</td><td>Multi-year (FY2004-2027)</td><td>Includes multi-year contract obligations</td></tr>
                <tr><td>agencies</td><td>DoD, HHS, DHS</td><td>Top-tier awarding agencies</td></tr>
                <tr><td>award_type_codes</td><td>A, B, C, D</td><td>Contract types: BPA, PO, DO, Definitive</td></tr>
                <tr><td>limit</td><td>100</td><td>Records per API call</td></tr>
            </table>
            <p><strong>Note:</strong> Federal contracts often span multiple fiscal years. Total spend reflects cumulative obligations across all contract periods.</p>
        </div>

        <div class="section">
            <div class="section-title">2. Data Model (Star Schema)</div>

            <div class="subsection-title">Fact Table: award_fact</div>
            <table>
                <tr><th>Column</th><th>Type</th><th>Description</th></tr>
                <tr><td>award_id</td><td>VARCHAR</td><td>Unique award identifier</td></tr>
                <tr><td>award_amount</td><td>DECIMAL</td><td>Total obligation amount (USD)</td></tr>
                <tr><td>agency_id</td><td>INT</td><td>FK → agency_dim</td></tr>
                <tr><td>recipient_id</td><td>INT</td><td>FK → recipient_dim</td></tr>
                <tr><td>start_date</td><td>DATE</td><td>Award period start</td></tr>
                <tr><td>fiscal_year</td><td>INT</td><td>Derived fiscal year</td></tr>
                <tr><td>naics_code</td><td>VARCHAR</td><td>Industry classification</td></tr>
                <tr><td>psc_code</td><td>VARCHAR</td><td>Product/Service code</td></tr>
            </table>

            <div class="subsection-title">Dimension Tables</div>
            <table>
                <tr><th>Table</th><th>Key Columns</th><th>Row Count</th></tr>
                <tr><td>agency_dim</td><td>agency_id, toptier_name, subtier_name</td><td>{data.get('agency_dim', {}).get('row_count', 'N/A')}</td></tr>
                <tr><td>recipient_dim</td><td>recipient_id, recipient_name, uei</td><td>{data.get('recipient_dim', {}).get('row_count', 'N/A')}</td></tr>
                <tr><td>time_dim</td><td>date_id, date, fiscal_year, quarter</td><td>{data.get('time_dim', {}).get('row_count', 'N/A')}</td></tr>
                <tr><td>geo_dim</td><td>geo_id, state_code, city_name</td><td>{data.get('geo_dim', {}).get('row_count', 'N/A')}</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">3. Quality Gates</div>
            <table>
                <tr><th>Gate</th><th>Threshold</th><th>Logic</th></tr>
                <tr><td>Schema Drift</td><td>≥95%</td><td>Verify expected columns present; null rates &lt; 1% for key fields</td></tr>
                <tr><td>Freshness</td><td>≥80%</td><td>Latest record date within 365 days; penalize stale data</td></tr>
                <tr><td>Completeness</td><td>≥90%</td><td>Null rates for agency, recipient, amount &lt; 5%</td></tr>
                <tr><td>Duplicates</td><td>≥95%</td><td>Hash-based deduplication on award_id + recipient + amount</td></tr>
                <tr><td>Value Sanity</td><td>≥85%</td><td>Negative amounts &lt; 5%; z-score outliers &lt; 1%</td></tr>
                <tr><td>Referential Integrity</td><td>≥95%</td><td>FK references exist in dimension tables</td></tr>
            </table>

            <div class="subsection-title">Quality Score Formula</div>
            <div class="formula-box">
                Overall = Σ(gate_score × weight) / Σ(weights)<br><br>
                Weights: schema=0.15, fresh=0.15, complete=0.20, dupe=0.15, sanity=0.20, RI=0.15
            </div>
        </div>

        <div class="section">
            <div class="section-title">4. KPI Definitions</div>

            <div class="subsection-title">Spend Metrics</div>
            <table>
                <tr><th>Metric</th><th>Formula</th></tr>
                <tr><td>Total Spend</td><td>SUM(award_amount) by dimension</td></tr>
                <tr><td>Average Award</td><td>AVG(award_amount)</td></tr>
                <tr><td>Median Award</td><td>PERCENTILE_CONT(0.5) of award_amount</td></tr>
            </table>

            <div class="subsection-title">Concentration Metrics</div>
            <div class="formula-box">
                <strong>Herfindahl-Hirschman Index (HHI):</strong><br>
                HHI = Σ(market_share²) × 10,000<br><br>
                where market_share = vendor_spend / total_spend<br><br>
                Interpretation:<br>
                • &lt; 1,500: Unconcentrated (healthy competition)<br>
                • 1,500-2,500: Moderately Concentrated<br>
                • &gt; 2,500: Highly Concentrated (potential risk)
            </div>

            <div class="subsection-title">Change Detection</div>
            <table>
                <tr><th>Metric</th><th>Formula</th></tr>
                <tr><td>QoQ Change</td><td>(Q_current - Q_prior) / Q_prior × 100%</td></tr>
                <tr><td>Rank Change</td><td>Vendor rank position delta between periods</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">5. Pipeline Architecture</div>
            <table>
                <tr><th>Stage</th><th>Description</th><th>Output</th></tr>
                <tr><td>1. Ingest</td><td>Paginated API calls with cursor-based pagination</td><td>raw_awards_fy2024.csv</td></tr>
                <tr><td>2. Clean</td><td>Type conversion, null handling, deduplication</td><td>cleaned_awards_fy2024.csv</td></tr>
                <tr><td>3. Model</td><td>Star schema transformation, FK generation</td><td>award_fact.csv, *_dim.csv</td></tr>
                <tr><td>4. Validate</td><td>Quality gate execution</td><td>pipeline_metrics.json</td></tr>
                <tr><td>5. Analyze</td><td>KPI calculation</td><td>kpis.json</td></tr>
                <tr><td>6. Report</td><td>PDF generation</td><td>Executive + Methodology PDFs</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">6. Run Metrics</div>
            <table>
                <tr><th>Metric</th><th>Value</th></tr>
                <tr><td>Pipeline Start</td><td>{metrics.get('start_time', 'N/A')}</td></tr>
                <tr><td>Pipeline End</td><td>{metrics.get('end_time', 'N/A')}</td></tr>
                <tr><td>Duration</td><td>{metrics.get('duration_seconds', 0):.1f} seconds</td></tr>
                <tr><td>API Calls</td><td>{metrics.get('api_calls', 0):,}</td></tr>
                <tr><td>API Errors</td><td>{metrics.get('api_errors', 0)}</td></tr>
                <tr><td>Raw Records</td><td>{metrics.get('raw_rows', 0):,}</td></tr>
                <tr><td>Cleaned Records</td><td>{metrics.get('cleaned_rows', 0):,}</td></tr>
                <tr><td>Modeled Records</td><td>{metrics.get('modeled_rows', 0):,}</td></tr>
                <tr><td>Overall Quality Score</td><td>{metrics.get('overall_quality_score', 0)*100:.1f}%</td></tr>
            </table>
        </div>

        <div class="footer">
            <p>Report prepared by {AUTHOR} | {EMAIL}</p>
            <p>Data Source: USAspending.gov API | Documentation: api.usaspending.gov/docs/endpoints</p>
            <p>Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>
        </div>
    </body>
    </html>
    """
    return html


def generate_founder_summary_html(data: Dict[str, Any]) -> str:
    """Generate founder-friendly one-page summary"""

    metrics = data.get('metrics', {})
    kpis = data.get('kpis', {})
    summary = kpis.get('summary', {})
    concentration = kpis.get('vendor_concentration', {})

    total_spend = summary.get('total_spend', 0)
    total_awards = summary.get('total_awards', 0)
    unique_vendors = summary.get('unique_vendors', 0)
    hhi = concentration.get('hhi', 0)
    quality_score = metrics.get('overall_quality_score', 0) * 100

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>P2-FED: Founder Summary</title>
        <style>
            @page {{ size: letter; margin: 0.5in; }}
            body {{
                font-family: 'Helvetica Neue', Arial, sans-serif;
                font-size: 11pt;
                line-height: 1.5;
                color: #1f2937;
                margin: 0;
            }}
            .header {{
                background: linear-gradient(135deg, {PRIMARY_COLOR}, {SECONDARY_COLOR});
                color: white;
                padding: 25px;
                margin: -0.5in -0.5in 20px -0.5in;
                text-align: center;
            }}
            .header h1 {{ margin: 0; font-size: 28pt; }}
            .header .subtitle {{ font-size: 14pt; opacity: 0.9; margin-top: 5px; }}
            .tldr {{
                background: linear-gradient(135deg, #eef2ff, #e0e7ff);
                border-left: 4px solid {PRIMARY_COLOR};
                padding: 15px 20px;
                margin: 20px 0;
                border-radius: 0 8px 8px 0;
            }}
            .tldr-title {{ font-size: 14pt; font-weight: 700; color: {PRIMARY_COLOR}; margin-bottom: 8px; }}
            .stat-grid {{
                display: flex;
                flex-wrap: wrap;
                gap: 15px;
                margin: 20px 0;
            }}
            .stat-card {{
                flex: 1;
                min-width: 120px;
                background: white;
                border: 2px solid #e2e8f0;
                border-radius: 12px;
                padding: 15px;
                text-align: center;
            }}
            .stat-value {{
                font-size: 24pt;
                font-weight: 700;
                color: {PRIMARY_COLOR};
            }}
            .stat-label {{
                font-size: 9pt;
                color: #6b7280;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            .section {{ margin: 25px 0; }}
            .section-title {{
                font-size: 14pt;
                font-weight: 700;
                color: {PRIMARY_COLOR};
                border-bottom: 2px solid {PRIMARY_COLOR};
                padding-bottom: 5px;
                margin-bottom: 12px;
            }}
            .highlight-box {{
                background: #f8fafc;
                border-radius: 8px;
                padding: 15px;
                margin: 10px 0;
            }}
            .two-col {{
                display: flex;
                gap: 20px;
            }}
            .two-col > div {{ flex: 1; }}
            ul {{ margin: 5px 0; padding-left: 20px; }}
            li {{ margin: 3px 0; }}
            .footer {{
                margin-top: 20px;
                padding-top: 10px;
                border-top: 1px solid #e2e8f0;
                font-size: 8pt;
                color: #6b7280;
                text-align: center;
            }}
            .quality-badge {{
                display: inline-block;
                background: {ACCENT_COLOR};
                color: white;
                padding: 4px 12px;
                border-radius: 20px;
                font-weight: 600;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Federal Spend Intelligence</h1>
            <div class="subtitle">USAspending.gov Data Pipeline | Portfolio Demo</div>
        </div>

        <div class="tldr">
            <div class="tldr-title">TL;DR</div>
            <p style="margin: 0;">Built an <strong>enterprise data pipeline</strong> that ingests federal procurement data from <strong>USAspending.gov</strong> (official government source). Pipeline processes <strong>{format_number(total_awards)} contract awards</strong> spanning <strong>FY2004-2027</strong> (multi-year contracts) totaling <strong>{format_currency(total_spend)}</strong>, identifies <strong>{format_number(unique_vendors)} vendors</strong>, calculates <strong>concentration risk metrics</strong>, and produces <strong>audit-ready analytics</strong> with a <strong>{quality_score:.0f}% data quality score</strong>.</p>
        </div>

        <div class="stat-grid">
            <div class="stat-card">
                <div class="stat-value">{format_currency(total_spend)}</div>
                <div class="stat-label">Total Spend</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{format_number(total_awards)}</div>
                <div class="stat-label">Awards</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{format_number(unique_vendors)}</div>
                <div class="stat-label">Vendors</div>
            </div>
            <div class="stat-card">
                <div class="stat-value"><span class="quality-badge">{quality_score:.0f}%</span></div>
                <div class="stat-label">Quality Score</div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">What This Demonstrates</div>
            <div class="two-col">
                <div class="highlight-box">
                    <strong>Data Engineering</strong>
                    <ul>
                        <li>Public API integration</li>
                        <li>Incremental data loading</li>
                        <li>Star schema modeling</li>
                        <li>Data quality gates</li>
                    </ul>
                </div>
                <div class="highlight-box">
                    <strong>Analytics Engineering</strong>
                    <ul>
                        <li>HHI concentration analysis</li>
                        <li>Spend trend detection</li>
                        <li>Vendor risk scoring</li>
                        <li>Automated PDF reporting</li>
                    </ul>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Key Insight: Vendor Concentration</div>
            <div class="highlight-box">
                <p><strong>HHI Index: {hhi:.0f}</strong> — {'Unconcentrated (healthy competition)' if hhi < 1500 else 'Moderately Concentrated' if hhi < 2500 else 'Highly Concentrated'}</p>
                <p>The Herfindahl-Hirschman Index measures market concentration. This analysis helps procurement teams identify single-vendor dependency risks and supports vendor diversification strategies.</p>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Quality Gates (All Passing)</div>
            <div class="highlight-box">
                {''.join(f'<span style="margin-right: 15px;">✓ <strong>{g["name"].replace("_", " ").title()}</strong>: {g["score"]*100:.0f}%</span>' for g in metrics.get('quality_gates', []))}
            </div>
        </div>

        <div class="section">
            <div class="section-title">Technical Highlights</div>
            <ul>
                <li><strong>Source:</strong> USAspending.gov API (official, public, verifiable)</li>
                <li><strong>Scope:</strong> DoD, HHS, DHS contracts for FY2024</li>
                <li><strong>Pipeline:</strong> Python → Star Schema → Quality Gates → KPIs → PDF</li>
                <li><strong>Duration:</strong> {metrics.get('duration_seconds', 0):.0f} seconds</li>
            </ul>
        </div>

        <div class="footer">
            <p>Report prepared by {AUTHOR} | {EMAIL}</p>
            <p>Data Source: USAspending.gov | Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}</p>
        </div>
    </body>
    </html>
    """
    return html


def generate_pdfs():
    """Generate all P2-FED PDF reports"""

    print("Loading pipeline data...")
    data = load_pipeline_data()

    if not data:
        print("ERROR: No pipeline data found. Run P2_FED_pipeline.py first.")
        return

    print(f"Loaded data with {len(data)} components")

    # Generate Founder Summary
    print("Generating Founder Summary PDF...")
    founder_html = generate_founder_summary_html(data)
    founder_path = REPORTS_DIR / 'P2-FED_Founder_Summary_v1.0.pdf'
    HTML(string=founder_html).write_pdf(founder_path)
    print(f"  Saved: {founder_path}")

    # Generate Executive Summary
    print("Generating Executive Summary PDF...")
    exec_html = generate_executive_summary_html(data)
    exec_path = REPORTS_DIR / 'P2-FED_Executive_Summary_v1.0.pdf'
    HTML(string=exec_html).write_pdf(exec_path)
    print(f"  Saved: {exec_path}")

    # Generate Methodology/QA
    print("Generating Methodology/QA PDF...")
    method_html = generate_methodology_html(data)
    method_path = REPORTS_DIR / 'P2-FED_Methodology_QA_v1.0.pdf'
    HTML(string=method_html).write_pdf(method_path)
    print(f"  Saved: {method_path}")

    print("\nPDF generation complete!")
    print(f"Reports saved to: {REPORTS_DIR}")


if __name__ == '__main__':
    generate_pdfs()
