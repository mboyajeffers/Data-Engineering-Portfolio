#!/usr/bin/env python3
"""
Report Generator - Creates PDF reports from verified KPI data
=============================================================
Generates Executive Summary and Technical Analysis PDFs for each company.

All data comes from real public APIs:
- SEC EDGAR XBRL: Company financial data
- Yahoo Finance: Stock price data

Author: Mboya Jeffers
Version: 2.0.0
Created: 2026-02-01
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT.parent  # Industry folders are one level up


def format_currency(value: float, decimals: int = 1) -> str:
    """Format large numbers as currency with B/M/K suffix"""
    if value is None:
        return "N/A"

    abs_val = abs(value)
    if abs_val >= 1e12:
        return f"${value/1e12:.{decimals}f}T"
    elif abs_val >= 1e9:
        return f"${value/1e9:.{decimals}f}B"
    elif abs_val >= 1e6:
        return f"${value/1e6:.{decimals}f}M"
    elif abs_val >= 1e3:
        return f"${value/1e3:.{decimals}f}K"
    else:
        return f"${value:.{decimals}f}"


def format_percent(value: float) -> str:
    """Format as percentage"""
    if value is None:
        return "N/A"
    return f"{value:+.1f}%" if value >= 0 else f"{value:.1f}%"


def generate_executive_html(ticker: str, kpis: Dict[str, Any], industry: str) -> str:
    """Generate Executive Summary HTML"""

    meta = kpis.get('metadata', {})
    perf = kpis.get('stock_performance', {})
    fin = kpis.get('financials', {})
    stock = kpis.get('stock_data_summary', {})

    company_name = meta.get('company_name', ticker)
    focus = meta.get('focus', 'Analysis')

    # Extract financial values (with None handling)
    revenue = fin.get('revenue', {}).get('value')
    net_income = fin.get('net_income', {}).get('value')
    total_assets = fin.get('total_assets', {}).get('value')
    equity = fin.get('stockholders_equity', {}).get('value')
    eps = fin.get('eps', {}).get('value')
    eps_str = f"${eps:.2f}" if eps is not None else "N/A"

    revenue_date = fin.get('revenue', {}).get('date', 'N/A')
    revenue_form = fin.get('revenue', {}).get('form', '10-K')
    revenue_accn = fin.get('revenue', {}).get('accession', 'N/A')

    # Stock performance
    total_return = perf.get('total_return', 0)
    ann_return = perf.get('annualized_return', 0)
    volatility = perf.get('volatility', 0)
    sharpe = perf.get('sharpe_ratio', 0)
    max_dd = perf.get('max_drawdown', 0)
    years = perf.get('years_analyzed', 0)

    html = f'''
<style>
body {{ font-family: Arial, sans-serif; font-size: 10pt; line-height: 1.3; }}
h2 {{ font-size: 12pt; color: #2c5282; margin: 12px 0 6px 0; border-bottom: 1px solid #2c5282; padding-bottom: 3px; }}
.kpi-grid {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 10px 0; }}
.kpi-box {{ flex: 1; min-width: 120px; background: #f7fafc; border-left: 3px solid #2c5282; padding: 8px; }}
.kpi-value {{ font-size: 16pt; font-weight: bold; color: #2c5282; }}
.kpi-label {{ font-size: 8pt; color: #718096; text-transform: uppercase; }}
table {{ width: 100%; border-collapse: collapse; font-size: 9pt; margin: 8px 0; }}
th, td {{ padding: 4px 8px; text-align: left; border: 1px solid #e2e8f0; }}
th {{ background: #edf2f7; }}
.positive {{ color: #38a169; }}
.negative {{ color: #e53e3e; }}
.source {{ background: #f0fff4; padding: 8px; margin: 10px 0; border-left: 3px solid #38a169; font-size: 9pt; }}
.footer {{ font-size: 8pt; color: #718096; margin-top: 15px; border-top: 1px solid #e2e8f0; padding-top: 8px; }}
</style>

<h2>Key Financial Metrics (SEC EDGAR)</h2>
<div class="kpi-grid">
    <div class="kpi-box">
        <div class="kpi-value">{format_currency(revenue) if revenue else "N/A"}</div>
        <div class="kpi-label">Revenue ({revenue_date[:4] if revenue_date != "N/A" else "N/A"})</div>
    </div>
    <div class="kpi-box">
        <div class="kpi-value">{format_currency(net_income) if net_income else "N/A"}</div>
        <div class="kpi-label">Net Income</div>
    </div>
    <div class="kpi-box">
        <div class="kpi-value">{format_currency(total_assets) if total_assets else "N/A"}</div>
        <div class="kpi-label">Total Assets</div>
    </div>
    <div class="kpi-box">
        <div class="kpi-value">{format_currency(equity) if equity else "N/A"}</div>
        <div class="kpi-label">Stockholders Equity</div>
    </div>
    <div class="kpi-box">
        <div class="kpi-value">{eps_str}</div>
        <div class="kpi-label">EPS (Basic)</div>
    </div>
</div>

<h2>Stock Performance ({years:.1f} Years)</h2>
<table>
<tr><th>Metric</th><th>Value</th><th>Interpretation</th></tr>
<tr><td>Total Return</td><td class="{"positive" if total_return >= 0 else "negative"}">{format_percent(total_return)}</td><td>Cumulative price appreciation</td></tr>
<tr><td>Annualized Return</td><td class="{"positive" if ann_return >= 0 else "negative"}">{format_percent(ann_return)}</td><td>Geometric mean annual return</td></tr>
<tr><td>Volatility</td><td>{volatility:.1f}%</td><td>Annualized standard deviation</td></tr>
<tr><td>Sharpe Ratio</td><td>{sharpe:.2f}</td><td>Risk-adjusted return (higher=better)</td></tr>
<tr><td>Max Drawdown</td><td class="negative">{max_dd:.1f}%</td><td>Largest peak-to-trough decline</td></tr>
</table>

<h2>Data Coverage</h2>
<table>
<tr><td><b>Stock Data Period:</b></td><td>{stock.get("start_date", "N/A")} to {stock.get("end_date", "N/A")}</td></tr>
<tr><td><b>Trading Days Analyzed:</b></td><td>{perf.get("trading_days", 0):,}</td></tr>
<tr><td><b>Financial Data:</b></td><td>SEC Form {revenue_form} (Filed {revenue_date})</td></tr>
<tr><td><b>SEC Accession:</b></td><td>{revenue_accn}</td></tr>
</table>

<div class="source">
<b>Data Sources (100% Verifiable):</b><br>
• <b>Financials:</b> SEC EDGAR XBRL - {meta.get("data_sources", {}).get("financials", "N/A")}<br>
• <b>Stock Prices:</b> {meta.get("data_sources", {}).get("stock_prices", "N/A")}<br>
<b>NO SYNTHETIC OR SIMULATED DATA</b>
</div>

<div class="footer">
<b>Report prepared by Mboya Jeffers</b> | MboyaJeffers9@gmail.com<br>
Ticker: {ticker} | CIK: {meta.get("cik", "N/A")} | Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}<br>
<b>All metrics verifiable via public APIs - Quality Standard Compliant</b>
</div>
'''
    return html


def generate_technical_html(ticker: str, kpis: Dict[str, Any], industry: str) -> str:
    """Generate Technical Analysis HTML"""

    meta = kpis.get('metadata', {})
    perf = kpis.get('stock_performance', {})
    fin = kpis.get('financials', {})
    stock = kpis.get('stock_data_summary', {})

    company_name = meta.get('company_name', ticker)
    focus = meta.get('focus', 'Analysis')

    # Financial metrics detail
    fin_rows = ""
    for metric, data in fin.items():
        if isinstance(data, dict) and 'value' in data:
            val = data['value']
            if metric in ['eps']:
                formatted = f"${val:.2f}"
            else:
                formatted = format_currency(val)
            fin_rows += f'''<tr>
                <td>{metric.replace("_", " ").title()}</td>
                <td>{formatted}</td>
                <td>{data.get("date", "N/A")}</td>
                <td>{data.get("form", "N/A")}</td>
                <td style="font-size:7pt">{data.get("accession", "N/A")}</td>
            </tr>'''

    html = f'''
<style>
body {{ font-family: Arial, sans-serif; font-size: 9pt; line-height: 1.3; }}
h2 {{ font-size: 11pt; color: #2c5282; margin: 10px 0 5px 0; border-bottom: 1px solid #2c5282; padding-bottom: 2px; }}
table {{ width: 100%; border-collapse: collapse; font-size: 8pt; margin: 6px 0; }}
th, td {{ padding: 3px 5px; text-align: left; border: 1px solid #e2e8f0; }}
th {{ background: #edf2f7; }}
.methodology {{ background: #fffaf0; padding: 8px; margin: 8px 0; border-left: 3px solid #ed8936; font-size: 8pt; }}
.source {{ background: #f0fff4; padding: 8px; margin: 8px 0; border-left: 3px solid #38a169; font-size: 8pt; }}
.footer {{ font-size: 7pt; color: #718096; margin-top: 10px; border-top: 1px solid #e2e8f0; padding-top: 5px; }}
</style>

<h2>1. Data Overview</h2>
<table>
<tr><td><b>Company:</b></td><td>{company_name} ({ticker})</td></tr>
<tr><td><b>CIK:</b></td><td>{meta.get("cik", "N/A")}</td></tr>
<tr><td><b>Analysis Period:</b></td><td>{stock.get("start_date", "N/A")} to {stock.get("end_date", "N/A")} (~{perf.get("years_analyzed", 0):.1f} years)</td></tr>
<tr><td><b>Trading Days:</b></td><td>{perf.get("trading_days", 0):,}</td></tr>
<tr><td><b>Latest Close:</b></td><td>${stock.get("latest_close", 0):.2f}</td></tr>
</table>

<h2>2. Methodology</h2>
<div class="methodology">
<b>Risk Metrics (CFA/Basel Standards):</b>
<ul style="margin: 3px 0; padding-left: 15px;">
<li><b>Sharpe Ratio:</b> (Annualized Return - Rf) / Volatility (Rf = 0%)</li>
<li><b>Sortino Ratio:</b> Annualized Return / Downside Deviation</li>
<li><b>VaR (95%, 99%):</b> Historical percentile of daily returns</li>
<li><b>Max Drawdown:</b> Largest peak-to-trough decline in cumulative returns</li>
<li><b>Volatility:</b> Standard deviation of daily returns × √252 (annualized)</li>
</ul>
</div>

<h2>3. Risk Metrics Summary</h2>
<table>
<tr><th>Metric</th><th>Value</th><th>Interpretation</th></tr>
<tr><td>Total Return ({perf.get("years_analyzed", 0):.1f}Y)</td><td>{perf.get("total_return", 0):+.1f}%</td><td>Cumulative appreciation</td></tr>
<tr><td>Annualized Return</td><td>{perf.get("annualized_return", 0):+.1f}%</td><td>Geometric mean annual</td></tr>
<tr><td>Volatility</td><td>{perf.get("volatility", 0):.1f}%</td><td>Annualized std dev</td></tr>
<tr><td>Sharpe Ratio</td><td>{perf.get("sharpe_ratio", 0):.2f}</td><td>Risk-adjusted return</td></tr>
<tr><td>Sortino Ratio</td><td>{perf.get("sortino_ratio", 0):.2f}</td><td>Downside risk-adjusted</td></tr>
<tr><td>Max Drawdown</td><td>{perf.get("max_drawdown", 0):.1f}%</td><td>Peak-to-trough</td></tr>
</table>

<h2>4. Return Distribution</h2>
<table>
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>Positive Days</td><td>{perf.get("positive_days_pct", 0):.1f}%</td></tr>
<tr><td>Best Day</td><td>{perf.get("best_day", 0):+.2f}%</td></tr>
<tr><td>Worst Day</td><td>{perf.get("worst_day", 0):.2f}%</td></tr>
<tr><td>VaR (95%)</td><td>{perf.get("var_95", 0):.2f}%</td></tr>
<tr><td>VaR (99%)</td><td>{perf.get("var_99", 0):.2f}%</td></tr>
</table>

<h2>5. SEC EDGAR Financial Data</h2>
<table>
<tr><th>Metric</th><th>Value</th><th>Period End</th><th>Form</th><th>Accession</th></tr>
{fin_rows}
</table>

<div class="source">
<b>Data Sources (100% Verifiable - NO SIMULATION):</b><br>
• <b>SEC EDGAR:</b> {meta.get("data_sources", {}).get("financials", "N/A")}<br>
• <b>Stock Data:</b> {meta.get("data_sources", {}).get("stock_prices", "N/A")}<br>
Anyone can verify these numbers by querying the APIs directly.
</div>

<div class="footer">
<b>Report prepared by Mboya Jeffers</b> | MboyaJeffers9@gmail.com<br>
Data Source: SEC EDGAR XBRL, Yahoo Finance | Report: {ticker}_{focus.replace(" ", "_")}<br>
<b>Quality Standard Compliant - All Data Verifiable</b> | Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}
</div>
'''
    return html


def save_report_html(html: str, output_path: Path, title: str, subtitle: str):
    """Save HTML report to file for PDF generation"""

    full_html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
</head>
<body>
{html}
</body>
</html>'''

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(full_html)


def generate_all_reports():
    """Generate all reports from KPI data"""

    print("=" * 70)
    print("Report Generator - Creating PDFs from Verified Data")
    print("=" * 70)

    # Load master KPIs
    kpis_file = DATA_DIR / "all_industry_kpis.json"
    if not kpis_file.exists():
        print("ERROR: Run industry_demos_pipeline.py first to generate KPIs")
        return

    with open(kpis_file) as f:
        all_kpis = json.load(f)

    # Industry folder mapping
    industry_folders = {
        "Finance": "Finance",
        "Brokerage": "Brokerage",
        "Media": "Media",
        "Ecommerce": "Ecommerce",
        "Gaming": "Gaming",
        "Crypto": "Crypto",
        "Solar": "Solar",
        "Oilgas": "Oilgas",
        "Betting": "Betting",
        "Compliance": "Compliance",
    }

    reports_generated = 0

    for industry, companies in all_kpis.items():
        print(f"\n[{industry}] Generating reports...")

        folder_name = industry_folders.get(industry, industry)
        output_dir = REPORTS_DIR / folder_name
        output_dir.mkdir(exist_ok=True)

        for ticker, kpis in companies.items():
            company_name = kpis.get('metadata', {}).get('company_name', ticker)
            focus = kpis.get('metadata', {}).get('focus', 'Analysis')

            # Clean focus for filename
            focus_clean = focus.replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")

            # Generate Executive Summary
            exec_html = generate_executive_html(ticker, kpis, industry)
            exec_path = output_dir / f"{focus_clean}_Executive.html"
            save_report_html(
                exec_html,
                exec_path,
                f"{company_name}: {focus}",
                "Executive Summary"
            )
            print(f"  Created: {exec_path.name}")

            # Generate Technical Analysis
            tech_html = generate_technical_html(ticker, kpis, industry)
            tech_path = output_dir / f"{focus_clean}_Technical.html"
            save_report_html(
                tech_html,
                tech_path,
                f"{company_name}: {focus}",
                "Technical Analysis"
            )
            print(f"  Created: {tech_path.name}")

            reports_generated += 2

    print(f"\n" + "=" * 70)
    print(f"Complete! Generated {reports_generated} HTML reports")
    print("Next: Upload to PROD-VM for PDF conversion")
    print("=" * 70)


if __name__ == '__main__':
    generate_all_reports()
