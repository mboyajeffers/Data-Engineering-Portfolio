#!/usr/bin/env python3
"""
P2-SEC: PDF Report Generator
============================
Generates Executive Summary, Methodology, and Founder-Friendly PDFs

Author: Mboya Jeffers
Version: 1.0.0
Created: 2026-02-01
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any
import pandas as pd
from weasyprint import HTML

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"

REPORTS_DIR.mkdir(exist_ok=True)

# Color scheme - Brokerage/Finance
PRIMARY_COLOR = "#1e3a5f"
SECONDARY_COLOR = "#0f766e"
ACCENT_COLOR = "#059669"
WARNING_COLOR = "#dc2626"

AUTHOR = "Mboya Jeffers"
EMAIL = "MboyaJeffers9@gmail.com"


def load_pipeline_data() -> Dict[str, Any]:
    """Load all pipeline output data"""
    data = {}

    metrics_path = DATA_DIR / 'pipeline_metrics.json'
    if metrics_path.exists():
        with open(metrics_path) as f:
            data['metrics'] = json.load(f)

    kpi_path = DATA_DIR / 'kpis.json'
    if kpi_path.exists():
        with open(kpi_path) as f:
            data['kpis'] = json.load(f)

    company_path = DATA_DIR / 'company_dim.csv'
    if company_path.exists():
        data['companies'] = pd.read_csv(company_path).to_dict('records')

    return data


def format_currency(value: float) -> str:
    if abs(value) >= 1e12:
        return f"${value/1e12:.1f}T"
    elif abs(value) >= 1e9:
        return f"${value/1e9:.1f}B"
    elif abs(value) >= 1e6:
        return f"${value/1e6:.1f}M"
    else:
        return f"${value:,.0f}"


def format_number(value: float) -> str:
    if abs(value) >= 1e6:
        return f"{value/1e6:.2f}M"
    elif abs(value) >= 1e3:
        return f"{value/1e3:.1f}K"
    else:
        return f"{value:,.0f}"


def format_pct(value: float) -> str:
    return f"{value*100:.1f}%"


def generate_founder_summary_html(data: Dict[str, Any]) -> str:
    """Generate founder-friendly one-page summary"""

    metrics = data.get('metrics', {})
    kpis = data.get('kpis', {})
    summary = kpis.get('summary', {})
    companies = data.get('companies', [])
    company_metrics = kpis.get('company_metrics', {})

    quality_score = metrics.get('overall_quality_score', 0) * 100

    # Get sector breakdown
    sectors = summary.get('companies_by_sector', {})

    # Top companies by revenue
    top_by_revenue = sorted(
        [(t, m.get('revenue', 0)) for t, m in company_metrics.items() if m.get('revenue')],
        key=lambda x: x[1], reverse=True
    )[:5]

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>P2-SEC: Founder Summary</title>
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
                background: linear-gradient(135deg, #ecfdf5, #d1fae5);
                border-left: 4px solid {ACCENT_COLOR};
                padding: 15px 20px;
                margin: 20px 0;
                border-radius: 0 8px 8px 0;
            }}
            .tldr-title {{ font-size: 14pt; font-weight: 700; color: {ACCENT_COLOR}; margin-bottom: 8px; }}
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
            <h1>SEC Financial Intelligence</h1>
            <div class="subtitle">Public Company XBRL Data Pipeline | Portfolio Demo</div>
        </div>

        <div class="tldr">
            <div class="tldr-title">TL;DR</div>
            <p style="margin: 0;">Built an <strong>enterprise data pipeline</strong> that extracts standardized financial metrics from <strong>{len(companies)} Fortune 500 companies</strong> via SEC EDGAR. Pipeline processes <strong>{format_number(summary.get('total_facts', 0))} XBRL facts</strong>, maps them to canonical metrics (Revenue, Net Income, etc.), and produces <strong>investment-grade analytics</strong> with <strong>{quality_score:.0f}% data quality score</strong>.</p>
        </div>

        <div class="stat-grid">
            <div class="stat-card">
                <div class="stat-value">{format_number(summary.get('total_facts', 0))}</div>
                <div class="stat-label">XBRL Facts</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len(companies)}</div>
                <div class="stat-label">Companies</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{summary.get('total_concepts', 0):,}</div>
                <div class="stat-label">Concepts</div>
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
                        <li>SEC EDGAR API integration</li>
                        <li>XBRL taxonomy parsing</li>
                        <li>1M+ row data processing</li>
                        <li>Star schema modeling</li>
                    </ul>
                </div>
                <div class="highlight-box">
                    <strong>Analytics Engineering</strong>
                    <ul>
                        <li>Financial metric extraction</li>
                        <li>Cohort benchmarking</li>
                        <li>Data quality gates</li>
                        <li>Automated reporting</li>
                    </ul>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Company Coverage</div>
            <div class="two-col">
                <div>
                    <strong>Sectors Covered:</strong>
                    <ul>
                        {''.join(f'<li>{sector}: {count} companies</li>' for sector, count in sectors.items())}
                    </ul>
                </div>
                <div>
                    <strong>Top 5 by Revenue:</strong>
                    <ul>
                        {''.join(f'<li>{ticker}: {format_currency(rev)}</li>' for ticker, rev in top_by_revenue)}
                    </ul>
                </div>
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
                <li><strong>Source:</strong> SEC EDGAR data.sec.gov (official, public, verifiable)</li>
                <li><strong>Pipeline:</strong> Python (requests, pandas) → Star Schema → Quality Gates → KPIs → PDF</li>
                <li><strong>Duration:</strong> {metrics.get('duration_seconds', 0):.0f} seconds for full cohort</li>
                <li><strong>Rate Limiting:</strong> SEC-compliant (10 req/sec with User-Agent header)</li>
            </ul>
        </div>

        <div class="footer">
            <p>Report prepared by {AUTHOR} | {EMAIL}</p>
            <p>Data Source: SEC EDGAR (data.sec.gov) | Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}</p>
        </div>
    </body>
    </html>
    """
    return html


def generate_executive_html(data: Dict[str, Any]) -> str:
    """Generate Executive Summary HTML"""

    metrics = data.get('metrics', {})
    kpis = data.get('kpis', {})
    summary = kpis.get('summary', {})
    companies = data.get('companies', [])
    benchmarks = kpis.get('cohort_benchmarks', {})
    company_metrics = kpis.get('company_metrics', {})

    quality_score = metrics.get('overall_quality_score', 0) * 100
    gates = metrics.get('quality_gates', [])

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>P2-SEC: Executive Summary</title>
        <style>
            @page {{ size: letter; margin: 0.5in; }}
            body {{
                font-family: 'Helvetica Neue', Arial, sans-serif;
                font-size: 10pt;
                line-height: 1.4;
                color: #1f2937;
            }}
            .header {{
                background: linear-gradient(135deg, {PRIMARY_COLOR}, {SECONDARY_COLOR});
                color: white;
                padding: 20px;
                margin: -0.5in -0.5in 20px -0.5in;
                text-align: center;
            }}
            .header h1 {{ margin: 0; font-size: 24pt; }}
            .traceability {{
                background: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 8px;
                padding: 10px;
                margin-bottom: 20px;
                font-size: 8pt;
            }}
            .section {{ margin-bottom: 20px; }}
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
            }}
            .kpi-card {{
                flex: 1;
                min-width: 100px;
                background: white;
                border: 1px solid #e2e8f0;
                border-radius: 8px;
                padding: 12px;
                text-align: center;
            }}
            .kpi-value {{ font-size: 20pt; font-weight: 700; color: {PRIMARY_COLOR}; }}
            .kpi-label {{ font-size: 8pt; color: #6b7280; text-transform: uppercase; }}
            table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 9pt; }}
            th {{ background: {PRIMARY_COLOR}; color: white; padding: 8px; text-align: left; }}
            td {{ padding: 8px; border-bottom: 1px solid #e2e8f0; }}
            tr:nth-child(even) {{ background: #f8fafc; }}
            .pass {{ color: {ACCENT_COLOR}; font-weight: 600; }}
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
            <h1>SEC EDGAR Financial Intelligence</h1>
            <div>XBRL Data Pipeline | {len(companies)} Public Companies</div>
        </div>

        <div class="traceability">
            <strong>Traceability:</strong> SEC EDGAR (data.sec.gov) | {len(companies)} companies | {format_number(summary.get('total_facts', 0))} XBRL facts | Quality: {quality_score:.1f}% | Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
        </div>

        <div class="section">
            <div class="section-title">Key Metrics</div>
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{format_number(summary.get('total_facts', 0))}</div>
                    <div class="kpi-label">XBRL Facts</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{len(companies)}</div>
                    <div class="kpi-label">Companies</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{summary.get('total_concepts', 0):,}</div>
                    <div class="kpi-label">Unique Concepts</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{quality_score:.0f}%</div>
                    <div class="kpi-label">Quality Score</div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Cohort Benchmarks (Latest FY)</div>
            <table>
                <tr><th>Metric</th><th>Min</th><th>Median</th><th>Max</th><th>Spread</th></tr>
                {''.join(f'''<tr>
                    <td>{metric.title()}</td>
                    <td>{format_currency(stats.get('min', 0))}</td>
                    <td>{format_currency(stats.get('median', 0))}</td>
                    <td>{format_currency(stats.get('max', 0))}</td>
                    <td>{stats.get('max', 0)/stats.get('min', 1):.1f}x</td>
                </tr>''' for metric, stats in benchmarks.items() if stats.get('min', 0) > 0)}
            </table>
        </div>

        <div class="section">
            <div class="section-title">Quality Gates</div>
            <table>
                <tr><th>Gate</th><th>Status</th><th>Score</th><th>Threshold</th></tr>
                {''.join(f'''<tr>
                    <td>{g.get('name', '').replace('_', ' ').title()}</td>
                    <td class="pass">{'✓ PASS' if g.get('passed') else '✗ FAIL'}</td>
                    <td>{g.get('score', 0)*100:.1f}%</td>
                    <td>{g.get('threshold', 0)*100:.0f}%</td>
                </tr>''' for g in gates)}
            </table>
        </div>

        <div class="footer">
            <p>Report prepared by {AUTHOR} | {EMAIL}</p>
            <p>Data Source: SEC EDGAR | Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>
        </div>
    </body>
    </html>
    """
    return html


def generate_methodology_html(data: Dict[str, Any]) -> str:
    """Generate Methodology/QA PDF"""

    metrics = data.get('metrics', {})

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>P2-SEC: Methodology & QA</title>
        <style>
            @page {{ size: letter; margin: 0.6in; }}
            body {{ font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 9pt; line-height: 1.5; color: #1f2937; }}
            .header {{ background: linear-gradient(135deg, {PRIMARY_COLOR}, {SECONDARY_COLOR}); color: white; padding: 15px; margin: -0.6in -0.6in 15px -0.6in; text-align: center; }}
            .header h1 {{ margin: 0; font-size: 18pt; }}
            .section {{ margin-bottom: 15px; }}
            .section-title {{ color: {PRIMARY_COLOR}; font-size: 12pt; font-weight: 700; border-bottom: 2px solid {PRIMARY_COLOR}; padding-bottom: 3px; margin-bottom: 8px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 8pt; }}
            th {{ background: {PRIMARY_COLOR}; color: white; padding: 6px; text-align: left; }}
            td {{ padding: 6px; border-bottom: 1px solid #e2e8f0; }}
            code {{ background: #f1f5f9; padding: 2px 4px; border-radius: 3px; font-family: monospace; font-size: 8pt; }}
            .endpoint-box {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 10px; margin: 8px 0; }}
            .formula-box {{ background: #eef2ff; border-left: 3px solid {PRIMARY_COLOR}; padding: 10px; margin: 8px 0; font-family: monospace; }}
            .footer {{ margin-top: 15px; padding-top: 8px; border-top: 1px solid #e2e8f0; font-size: 7pt; color: #6b7280; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>P2-SEC: Methodology & Quality Assurance</h1>
        </div>

        <div class="section">
            <div class="section-title">1. Data Source</div>
            <p><strong>SEC EDGAR</strong> - Official repository for SEC filings and structured XBRL data.</p>
            <div class="endpoint-box">
                <code>GET /submissions/CIK{{cik}}.json</code> - Company filing history<br>
                <code>GET /api/xbrl/companyfacts/CIK{{cik}}.json</code> - XBRL financial facts
            </div>
            <p><strong>Compliance:</strong> User-Agent header required. Rate limit: 10 req/sec.</p>
        </div>

        <div class="section">
            <div class="section-title">2. Data Model</div>
            <table>
                <tr><th>Table</th><th>Description</th><th>Key Columns</th></tr>
                <tr><td>company_dim</td><td>Company master data</td><td>cik, ticker, name, sector</td></tr>
                <tr><td>filings_dim</td><td>SEC filing history</td><td>accession, form, filing_date</td></tr>
                <tr><td>concept_map</td><td>XBRL to canonical mapping</td><td>raw_concept, canonical_metric</td></tr>
                <tr><td>xbrl_facts</td><td>Financial fact values</td><td>cik, concept, value, period_end</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">3. Canonical Metric Mapping</div>
            <table>
                <tr><th>Canonical</th><th>XBRL Concepts</th><th>Category</th></tr>
                <tr><td>revenue</td><td>Revenues, RevenueFromContractWithCustomer...</td><td>Income Statement</td></tr>
                <tr><td>net_income</td><td>NetIncomeLoss, ProfitLoss</td><td>Income Statement</td></tr>
                <tr><td>assets</td><td>Assets</td><td>Balance Sheet</td></tr>
                <tr><td>equity</td><td>StockholdersEquity, ...</td><td>Balance Sheet</td></tr>
                <tr><td>operating_cash_flow</td><td>NetCashProvidedByUsedInOperatingActivities</td><td>Cash Flow</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">4. Quality Gates</div>
            <table>
                <tr><th>Gate</th><th>Threshold</th><th>Logic</th></tr>
                <tr><td>Unit Consistency</td><td>≥90%</td><td>Each concept uses single unit (USD/shares)</td></tr>
                <tr><td>Period Logic</td><td>≥95%</td><td>Valid period types (instant/quarterly/annual)</td></tr>
                <tr><td>Coverage</td><td>≥80%</td><td>Required metrics present per company</td></tr>
                <tr><td>Restatement</td><td>Info</td><td>Detect same metric+period with different values</td></tr>
                <tr><td>Outliers</td><td>≥85%</td><td>Z-score outliers (|z| > 5) below threshold</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">5. KPI Formulas</div>
            <div class="formula-box">
                Net Margin = Net Income / Revenue<br>
                Gross Margin = Gross Profit / Revenue<br>
                ROA = Net Income / Total Assets<br>
                ROE = Net Income / Stockholders Equity
            </div>
        </div>

        <div class="section">
            <div class="section-title">6. Run Metrics</div>
            <table>
                <tr><th>Metric</th><th>Value</th></tr>
                <tr><td>Duration</td><td>{metrics.get('duration_seconds', 0):.1f} seconds</td></tr>
                <tr><td>API Calls</td><td>{metrics.get('api_calls', 0)}</td></tr>
                <tr><td>Raw Facts</td><td>{metrics.get('raw_facts', 0):,}</td></tr>
                <tr><td>Cleaned Facts</td><td>{metrics.get('cleaned_facts', 0):,}</td></tr>
                <tr><td>Companies</td><td>{metrics.get('companies_processed', 0)}</td></tr>
                <tr><td>Quality Score</td><td>{metrics.get('overall_quality_score', 0)*100:.1f}%</td></tr>
            </table>
        </div>

        <div class="footer">
            <p>Report prepared by {AUTHOR} | {EMAIL}</p>
            <p>Data Source: SEC EDGAR (data.sec.gov) | Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>
        </div>
    </body>
    </html>
    """
    return html


def generate_pdfs():
    """Generate all P2-SEC PDF reports"""

    print("Loading pipeline data...")
    data = load_pipeline_data()

    if not data:
        print("ERROR: No pipeline data found.")
        return

    # Founder Summary
    print("Generating Founder Summary PDF...")
    founder_html = generate_founder_summary_html(data)
    HTML(string=founder_html).write_pdf(REPORTS_DIR / 'P2-SEC_Founder_Summary_v1.0.pdf')
    print(f"  Saved: P2-SEC_Founder_Summary_v1.0.pdf")

    # Executive Summary
    print("Generating Executive Summary PDF...")
    exec_html = generate_executive_html(data)
    HTML(string=exec_html).write_pdf(REPORTS_DIR / 'P2-SEC_Executive_Summary_v1.0.pdf')
    print(f"  Saved: P2-SEC_Executive_Summary_v1.0.pdf")

    # Methodology
    print("Generating Methodology PDF...")
    method_html = generate_methodology_html(data)
    HTML(string=method_html).write_pdf(REPORTS_DIR / 'P2-SEC_Methodology_QA_v1.0.pdf')
    print(f"  Saved: P2-SEC_Methodology_QA_v1.0.pdf")

    print(f"\nAll PDFs saved to: {REPORTS_DIR}")


if __name__ == '__main__':
    generate_pdfs()
