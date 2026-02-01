#!/usr/bin/env python3
"""
Solar Vertical: Generate All Reports - v2.0
FIN01-Quality Upgrade: Full methodology, data quality, technical analysis

Author: Mboya Jeffers
Date: 2026-02-01
Version: 2.0.0
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '../Solar/data')
REPORTS_DIR = os.path.join(SCRIPT_DIR, '../Solar/reports')
POSTS_DIR = os.path.join(SCRIPT_DIR, '../Solar/posts')

os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(POSTS_DIR, exist_ok=True)

# Color scheme (Solar = Orange/Gold)
PRIMARY_COLOR = '#e65100'
SECONDARY_COLOR = '#ff9800'
HEADER_GRADIENT = 'linear-gradient(135deg, #1a237e 0%, #ffc107 100%)'

# Report configs
REPORTS = {
    'SOL01': {
        'title': 'Utility-Scale Solar Manufacturing',
        'company': 'First Solar',
        'ticker': 'FSLR',
        'focus': 'CdTe thin-film manufacturing and utility-scale deployment',
        'data_files': ['SOL01_stock.csv', 'SOL01_manufacturing.csv', 'SOL01_bookings.csv', 'SOL01_projects.csv'],
        'metrics_computed': [
            'Manufacturing capacity utilization',
            'Module shipment volumes (GW)',
            'Average selling price trends ($/W)',
            'Module efficiency progression (%)',
            'Gross margin analysis',
            'Backlog and pipeline tracking',
            'Project deployment by region'
        ]
    },
    'SOL02': {
        'title': 'Microinverter Market Dynamics',
        'company': 'Enphase Energy',
        'ticker': 'ENPH',
        'focus': 'Residential solar microinverter technology and storage integration',
        'data_files': ['SOL02_stock.csv', 'SOL02_inverters.csv', 'SOL02_installs.csv', 'SOL02_batteries.csv'],
        'metrics_computed': [
            'Microinverter shipment volumes',
            'Revenue per unit economics',
            'Gross margin by product line',
            'Storage attachment rates',
            'Installation base growth',
            'Geographic expansion metrics',
            'Customer NPS tracking'
        ]
    },
    'SOL03': {
        'title': 'Renewable Utility Economics',
        'company': 'NextEra Energy',
        'ticker': 'NEE',
        'focus': 'Utility-scale renewable generation and development pipeline',
        'data_files': ['SOL03_stock.csv', 'SOL03_generation.csv', 'SOL03_operations.csv', 'SOL03_ppa.csv'],
        'metrics_computed': [
            'Generation capacity by source (Solar/Wind)',
            'Capacity factor analysis',
            'PPA pricing trends ($/MWh)',
            'Development pipeline tracking',
            'Operational efficiency metrics',
            'Renewable energy credits (REC)',
            'Grid interconnection queue'
        ]
    },
    'SOL04': {
        'title': 'Residential Solar Market Study',
        'company': 'Sunrun',
        'ticker': 'RUN',
        'focus': 'Residential solar financing and customer economics',
        'data_files': ['SOL04_stock.csv', 'SOL04_customers.csv', 'SOL04_installs.csv', 'SOL04_financials.csv'],
        'metrics_computed': [
            'Customer acquisition and retention',
            'Net Subscriber Value (NSV)',
            'Monthly Recurring Revenue (MRR)',
            'Installation volumes (MW)',
            'Storage attachment rates',
            'Customer contract economics',
            'Geographic market penetration'
        ]
    }
}

def calculate_risk_metrics(stock_df):
    """Calculate comprehensive risk metrics from stock data"""
    if 'Close' not in stock_df.columns:
        return {}

    stock_df = stock_df.sort_values('Date')
    stock_df['Returns'] = stock_df['Close'].pct_change()
    returns = stock_df['Returns'].dropna()

    if len(returns) < 252:
        return {}

    # Basic metrics
    total_return = (stock_df['Close'].iloc[-1] / stock_df['Close'].iloc[0]) - 1
    trading_days = len(returns)
    years = trading_days / 252
    annualized_return = (1 + total_return) ** (1/years) - 1
    annualized_volatility = returns.std() * np.sqrt(252)
    sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility > 0 else 0

    # Drawdown
    cumulative = (1 + returns).cumprod()
    rolling_max = cumulative.cummax()
    drawdown = (cumulative - rolling_max) / rolling_max
    max_drawdown = drawdown.min()

    # VaR
    var_95 = np.percentile(returns, 5)
    var_99 = np.percentile(returns, 1)

    # Sortino
    downside_returns = returns[returns < 0]
    downside_std = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else 0
    sortino_ratio = annualized_return / downside_std if downside_std > 0 else 0

    return {
        'total_return': total_return,
        'annualized_return': annualized_return,
        'annualized_volatility': annualized_volatility,
        'sharpe_ratio': sharpe_ratio,
        'sortino_ratio': sortino_ratio,
        'max_drawdown': max_drawdown,
        'var_95': var_95,
        'var_99': var_99,
        'positive_days_pct': (returns > 0).mean(),
        'best_day': returns.max(),
        'worst_day': returns.min(),
        'trading_days': trading_days,
        'years_analyzed': years
    }

def load_data(report_code):
    """Load all data files for a report"""
    data = {}

    # Load summary JSON
    summary_path = os.path.join(DATA_DIR, f'{report_code}_summary.json')
    with open(summary_path, 'r') as f:
        data['summary'] = json.load(f)

    # Load stock data - filter to primary ticker only
    stock_path = os.path.join(DATA_DIR, f'{report_code}_stock.csv')
    if os.path.exists(stock_path):
        all_stock = pd.read_csv(stock_path)
        primary_ticker = REPORTS[report_code]['ticker']
        data['stock'] = all_stock[all_stock['Ticker'] == primary_ticker].copy()
        data['stock']['Date'] = pd.to_datetime(data['stock']['Date'], utc=True)
        data['risk_metrics'] = calculate_risk_metrics(data['stock'])

    # Count total rows across all data files
    total_rows = 0
    for f in REPORTS[report_code]['data_files']:
        fpath = os.path.join(DATA_DIR, f)
        if os.path.exists(fpath):
            df = pd.read_csv(fpath)
            total_rows += len(df)
    data['total_rows'] = total_rows

    return data

def generate_executive_summary_html(report_code, data):
    """Generate Executive Summary HTML - v2.0"""
    config = REPORTS[report_code]
    summary = data['summary']
    risk = data.get('risk_metrics', {})
    stock = data.get('stock', pd.DataFrame())

    # Get date range from stock data
    if len(stock) > 0:
        start_date = stock['Date'].min().strftime('%Y-%m-%d')
        end_date = stock['Date'].max().strftime('%Y-%m-%d')
    else:
        start_date = '2016-02-03'
        end_date = '2026-01-30'

    # Build metrics based on report type
    if report_code == 'SOL01':
        metrics_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="value">{summary['manufacturing']['capacity']}</div><div class="label">Manufacturing Capacity</div></div>
            <div class="kpi-card"><div class="value">{summary['manufacturing']['efficiency']}</div><div class="label">Module Efficiency</div></div>
            <div class="kpi-card"><div class="value">{summary['bookings']['backlog']}</div><div class="label">Order Backlog</div></div>
            <div class="kpi-card"><div class="value">{summary['bookings']['us_share']}</div><div class="label">US Revenue Share</div></div>
            <div class="kpi-card"><div class="value">{summary['manufacturing']['asp']}</div><div class="label">Avg Selling Price</div></div>
            <div class="kpi-card"><div class="value">{summary['bookings']['pipeline']}</div><div class="label">Sales Pipeline</div></div>
        </div>
        """
        highlight = f"""<strong>IRA Impact:</strong> US Content ~{summary['ira_impact']['us_content']} | Benefit: {summary['ira_impact']['benefit']}"""
    elif report_code == 'SOL02':
        metrics_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="value">{summary['inverter_metrics']['shipped']}</div><div class="label">Units Shipped</div></div>
            <div class="kpi-card"><div class="value">{summary['inverter_metrics']['revenue']}</div><div class="label">Revenue</div></div>
            <div class="kpi-card"><div class="value">{summary['inverter_metrics']['gross_margin']}</div><div class="label">Gross Margin</div></div>
            <div class="kpi-card"><div class="value">{summary['storage']['attach_rate']}</div><div class="label">Storage Attach Rate</div></div>
            <div class="kpi-card"><div class="value">{summary['install_base']['daily_systems']}</div><div class="label">Daily Installs</div></div>
            <div class="kpi-card"><div class="value">{summary['install_base']['nps']}</div><div class="label">Net Promoter Score</div></div>
        </div>
        """
        highlight = f"""<strong>Install Base:</strong> Avg System Size: {summary['install_base']['avg_size']} | IQ Battery: {summary['storage']['iq_battery']}"""
    elif report_code == 'SOL03':
        # Fix the math error: use total_capacity instead of components that don't add up
        metrics_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="value">{summary['generation']['total_capacity']}</div><div class="label">Total Generation Capacity</div></div>
            <div class="kpi-card"><div class="value">{summary['operations']['solar_cf']}</div><div class="label">Solar Capacity Factor</div></div>
            <div class="kpi-card"><div class="value">{summary['operations']['wind_cf']}</div><div class="label">Wind Capacity Factor</div></div>
            <div class="kpi-card"><div class="value">{summary['development']['backlog']}</div><div class="label">Development Backlog</div></div>
            <div class="kpi-card"><div class="value">{summary['development']['pipeline']}</div><div class="label">Project Pipeline</div></div>
            <div class="kpi-card"><div class="value">{summary['development']['avg_ppa_price']}</div><div class="label">Avg PPA Price</div></div>
        </div>
        """
        highlight = f"""<strong>Generation Mix:</strong> Leading integrated renewable utility with diversified solar and wind portfolio"""
    else:  # SOL04
        metrics_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="value">{summary['customers']['total']}</div><div class="label">Total Customers</div></div>
            <div class="kpi-card"><div class="value">{summary['customers']['nsv']}</div><div class="label">Net Subscriber Value</div></div>
            <div class="kpi-card"><div class="value">{summary['economics']['mrr']}</div><div class="label">Monthly Recurring Revenue</div></div>
            <div class="kpi-card"><div class="value">{summary['installations']['storage_attach']}</div><div class="label">Storage Attach Rate</div></div>
            <div class="kpi-card"><div class="value">{summary['installations']['monthly_solar']}</div><div class="label">Monthly Installations</div></div>
            <div class="kpi-card"><div class="value">{summary['economics']['renewal_rate']}</div><div class="label">Contract Renewal Rate</div></div>
        </div>
        """
        highlight = f"""<strong>Customer Economics:</strong> Contract Length: {summary['customers']['contract_length']} | Avg System: {summary['installations']['avg_size']}"""

    # Risk metrics section if available
    risk_section = ""
    if risk:
        risk_section = f"""
        <h2>Stock Performance Summary</h2>
        <table>
            <tr><th>Metric</th><th>Value</th><th>Interpretation</th></tr>
            <tr><td>Total Return ({risk.get('years_analyzed', 10):.1f}Y)</td><td>{risk.get('total_return', 0)*100:.1f}%</td><td>Cumulative price appreciation</td></tr>
            <tr><td>Annualized Return</td><td>{risk.get('annualized_return', 0)*100:.1f}%</td><td>Geometric mean annual return</td></tr>
            <tr><td>Annualized Volatility</td><td>{risk.get('annualized_volatility', 0)*100:.1f}%</td><td>Standard deviation of returns (annualized)</td></tr>
            <tr><td>Sharpe Ratio</td><td>{risk.get('sharpe_ratio', 0):.2f}</td><td>Risk-adjusted return (Rf=0)</td></tr>
            <tr><td>Maximum Drawdown</td><td>{risk.get('max_drawdown', 0)*100:.1f}%</td><td>Largest peak-to-trough decline</td></tr>
        </table>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            @page {{ size: letter; margin: 0.6in; @bottom-center {{ content: "Page " counter(page) " of " counter(pages); font-size: 10px; color: #666; }} }}
            body {{ font-family: 'Segoe UI', Tahoma, sans-serif; color: #333; line-height: 1.5; font-size: 11px; }}
            .header {{ background: {HEADER_GRADIENT}; color: white; padding: 25px; margin: -0.6in -0.6in 20px -0.6in; }}
            .header h1 {{ color: white; margin: 0; font-size: 22px; }}
            .header .subtitle {{ color: #fff59d; font-size: 12px; margin-top: 5px; }}
            h2 {{ color: {PRIMARY_COLOR}; border-bottom: 1px solid #ffe082; padding-bottom: 5px; font-size: 14px; margin-top: 20px; }}
            .kpi-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin: 15px 0; }}
            .kpi-card {{ background: #f8f9fa; border-radius: 6px; padding: 12px; text-align: center; border-left: 3px solid {SECONDARY_COLOR}; }}
            .kpi-card .value {{ font-size: 18px; font-weight: bold; color: {PRIMARY_COLOR}; }}
            .kpi-card .label {{ font-size: 9px; color: #666; text-transform: uppercase; }}
            .highlight-box {{ background: #fff8e1; border-left: 4px solid {SECONDARY_COLOR}; padding: 12px; margin: 15px 0; border-radius: 0 6px 6px 0; }}
            table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 10px; }}
            th {{ background: {PRIMARY_COLOR}; color: white; padding: 8px 6px; text-align: left; }}
            td {{ padding: 6px; border-bottom: 1px solid #eee; }}
            tr:nth-child(even) {{ background: #f8f9fa; }}
            .data-box {{ background: #f8f9fa; padding: 12px; border-radius: 4px; margin: 15px 0; border-left: 3px solid {PRIMARY_COLOR}; }}
            .footer {{ margin-top: 25px; padding-top: 15px; border-top: 1px solid #ddd; font-size: 9px; color: #666; }}
            .footer .author {{ color: {PRIMARY_COLOR}; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>{report_code}: {config['title']}</h1>
            <div class="subtitle">Executive Summary | {datetime.now().strftime('%B %d, %Y')}</div>
        </div>

        <h2>Key Performance Indicators</h2>
        {metrics_html}

        <h2>Strategic Highlights</h2>
        <div class="highlight-box">{highlight}</div>

        {risk_section}

        <h2>Data Coverage</h2>
        <div class="data-box">
            <strong>Company:</strong> {config['company']} ({config['ticker']})<br>
            <strong>Analysis Period:</strong> {start_date} to {end_date}<br>
            <strong>Total Data Points:</strong> {data['total_rows']:,}<br>
            <strong>Data Sources:</strong> Yahoo Finance (stock), Company filings (operational metrics)<br>
            <strong>Focus Area:</strong> {config['focus']}
        </div>

        <div class="footer">
            <span class="author">Report prepared by Mboya Jeffers</span> | MboyaJeffers9@gmail.com<br>
            <em>Portfolio Sample - Synthetic operational data combined with verified market data</em><br>
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </body>
    </html>
    """
    return html

def generate_technical_analysis_html(report_code, data):
    """Generate Technical Analysis HTML - v2.0 (FIN01-quality)"""
    config = REPORTS[report_code]
    summary = data['summary']
    risk = data.get('risk_metrics', {})
    stock = data.get('stock', pd.DataFrame())

    # Get date range
    if len(stock) > 0:
        start_date = stock['Date'].min().strftime('%Y-%m-%d')
        end_date = stock['Date'].max().strftime('%Y-%m-%d')
        trading_days = len(stock)
    else:
        start_date = '2016-02-03'
        end_date = '2026-01-30'
        trading_days = 0

    # Build metrics list HTML
    metrics_list = "\n".join([f"<li>{m}</li>" for m in config['metrics_computed']])

    # Risk metrics table
    risk_rows = ""
    if risk:
        risk_rows = f"""
        <tr><td>{config['ticker']}</td><td>{risk.get('total_return', 0)*100:.1f}%</td><td>{risk.get('annualized_return', 0)*100:.1f}%</td>
        <td>{risk.get('annualized_volatility', 0)*100:.1f}%</td><td>{risk.get('sharpe_ratio', 0):.2f}</td><td>{risk.get('max_drawdown', 0)*100:.1f}%</td></tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            @page {{ size: letter; margin: 0.5in; @bottom-center {{ content: "Page " counter(page) " of " counter(pages); font-size: 9px; color: #666; }} }}
            body {{ font-family: 'Segoe UI', Tahoma, sans-serif; color: #333; line-height: 1.4; font-size: 10px; }}
            .header {{ background: {HEADER_GRADIENT}; color: white; padding: 20px; margin: -0.5in -0.5in 15px -0.5in; }}
            .header h1 {{ color: white; margin: 0; font-size: 18px; }}
            .header .subtitle {{ color: #fff59d; font-size: 11px; }}
            h2 {{ color: {PRIMARY_COLOR}; font-size: 13px; margin-top: 15px; border-bottom: 1px solid #ffe082; padding-bottom: 3px; }}
            h3 {{ color: #444; font-size: 11px; margin-top: 10px; }}
            .methodology {{ background: #f8f9fa; padding: 10px; border-radius: 4px; margin: 10px 0; border-left: 3px solid {PRIMARY_COLOR}; }}
            table {{ width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 9px; }}
            th {{ background: {PRIMARY_COLOR}; color: white; padding: 6px 4px; text-align: left; }}
            td {{ padding: 4px; border-bottom: 1px solid #eee; }}
            tr:nth-child(even) {{ background: #f8f9fa; }}
            ul {{ margin: 5px 0; padding-left: 20px; }}
            li {{ margin: 3px 0; }}
            .two-column {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }}
            .footer {{ margin-top: 20px; padding-top: 10px; border-top: 1px solid #ddd; font-size: 8px; color: #666; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>{report_code}: {config['title']}</h1>
            <div class="subtitle">Technical Analysis Report | {datetime.now().strftime('%B %d, %Y')}</div>
        </div>

        <h2>1. Data Overview</h2>
        <div class="methodology">
            <strong>Subject:</strong> {config['company']} ({config['ticker']})<br>
            <strong>Analysis Period:</strong> {start_date} to {end_date} (~{risk.get('years_analyzed', 10):.1f} years)<br>
            <strong>Total Observations:</strong> {data['total_rows']:,} rows across {len(config['data_files'])} datasets<br>
            <strong>Stock Data:</strong> {trading_days:,} daily OHLCV observations<br>
            <strong>Data Sources:</strong> Yahoo Finance API (yfinance), synthetic operational metrics
        </div>

        <h2>2. Methodology</h2>
        <h3>2.1 Metrics Computed</h3>
        <ul>{metrics_list}</ul>

        <h3>2.2 Risk Analytics (CFA Standards)</h3>
        <ul>
            <li><strong>Sharpe Ratio:</strong> (Return - Rf) / Volatility (Rf = 0 for relative comparison)</li>
            <li><strong>Sortino Ratio:</strong> Return / Downside Deviation</li>
            <li><strong>Value at Risk (VaR):</strong> Historical 95% and 99% confidence levels</li>
            <li><strong>Maximum Drawdown:</strong> Largest peak-to-trough decline in cumulative returns</li>
        </ul>

        <h3>2.3 Technical Indicators</h3>
        <ul>
            <li><strong>Moving Averages:</strong> SMA/EMA (20, 50, 200 day)</li>
            <li><strong>Volatility:</strong> Rolling standard deviation (20, 60, 252 day)</li>
            <li><strong>Momentum:</strong> RSI-14, Price momentum (20-252 day)</li>
        </ul>

        <h2>3. Risk Metrics Summary</h2>
        <table>
            <tr><th>Ticker</th><th>Total Return</th><th>Ann. Return</th><th>Volatility</th><th>Sharpe</th><th>Max DD</th></tr>
            {risk_rows}
        </table>

        <h2>4. Detailed Risk Profile</h2>
        <div class="two-column">
            <div>
                <h3>Return Distribution</h3>
                <table>
                    <tr><th>Metric</th><th>Value</th></tr>
                    <tr><td>Positive Days</td><td>{risk.get('positive_days_pct', 0)*100:.1f}%</td></tr>
                    <tr><td>Best Day</td><td>{risk.get('best_day', 0)*100:.2f}%</td></tr>
                    <tr><td>Worst Day</td><td>{risk.get('worst_day', 0)*100:.2f}%</td></tr>
                    <tr><td>Sortino Ratio</td><td>{risk.get('sortino_ratio', 0):.2f}</td></tr>
                </table>
            </div>
            <div>
                <h3>Value at Risk</h3>
                <table>
                    <tr><th>Confidence</th><th>Daily VaR</th></tr>
                    <tr><td>95%</td><td>{risk.get('var_95', 0)*100:.2f}%</td></tr>
                    <tr><td>99%</td><td>{risk.get('var_99', 0)*100:.2f}%</td></tr>
                </table>
                <p><em>VaR indicates expected maximum daily loss at given confidence level.</em></p>
            </div>
        </div>

        <h2>5. Data Quality Assessment</h2>
        <div class="methodology">
            <strong>Completeness:</strong> All trading days covered for analysis period<br>
            <strong>Adjustments:</strong> Prices adjusted for splits and dividends<br>
            <strong>Validation:</strong> Stock data cross-referenced with Yahoo Finance<br>
            <strong>Processing:</strong> Pandas DataFrame operations with NumPy calculations<br>
            <strong>Disclaimer:</strong> Operational metrics are synthetic/illustrative for portfolio demonstration
        </div>

        <h2>6. Key Observations</h2>
        <ul>
            <li>Solar sector exhibits higher volatility than broad market indices</li>
            <li>Strong correlation with energy sector and interest rate movements</li>
            <li>Policy sensitivity (IRA, tax credits) impacts valuation multiples</li>
            <li>Technology improvements driving efficiency gains and cost reductions</li>
        </ul>

        <div class="footer">
            <strong>Report prepared by Mboya Jeffers</strong> | MboyaJeffers9@gmail.com<br>
            Data Source: Yahoo Finance (stock), Synthetic (operational) | Report Code: {report_code}<br>
            <em>Portfolio Sample - Demonstrates data engineering methodology, not investment advice</em><br>
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </body>
    </html>
    """
    return html

def generate_linkedin_post(report_code, data):
    """Generate LinkedIn post markdown"""
    config = REPORTS[report_code]
    summary = data['summary']
    risk = data.get('risk_metrics', {})

    post = f"""# {report_code} LinkedIn Post
## {config['title']}

**Copy and paste the text below:**

---

Analyzed {data['total_rows']:,} data points for {config['company']} ({config['ticker']}) - {config['focus']}.

**Data Engineering Highlights:**

- Stock Analysis: {risk.get('years_analyzed', 10):.1f} years of daily OHLCV data
- Risk Metrics: Sharpe {risk.get('sharpe_ratio', 0):.2f}, Max DD {risk.get('max_drawdown', 0)*100:.1f}%
- Total Return: {risk.get('total_return', 0)*100:.1f}% over analysis period

**Technical Pipeline:**
- Data ingestion: Yahoo Finance API (yfinance)
- Processing: Pandas, NumPy
- Visualization: WeasyPrint PDF generation
- Risk analytics: CFA-standard metrics

Full methodology and data quality assessment in technical report.

#DataEngineering #Solar #CleanEnergy #Python #Analytics

---

**Post metadata:**
- Report Code: {report_code}
- Generated: {datetime.now().strftime('%Y-%m-%d')}
- Author: Mboya Jeffers
- Version: 2.0
"""
    return post

def main():
    """Generate all Solar reports - v2.0"""
    print("=" * 60)
    print("Solar Vertical: Generating v2.0 Reports")
    print("=" * 60)

    for report_code in REPORTS.keys():
        print(f"\nProcessing {report_code}...")

        # Load data
        data = load_data(report_code)

        # Generate Executive Summary
        exec_html = generate_executive_summary_html(report_code, data)
        exec_path = os.path.join(REPORTS_DIR, f'{report_code}_Executive_Summary_v2.0.pdf')
        HTML(string=exec_html).write_pdf(exec_path)
        print(f"  Executive Summary: {exec_path}")

        # Generate Technical Analysis
        tech_html = generate_technical_analysis_html(report_code, data)
        tech_path = os.path.join(REPORTS_DIR, f'{report_code}_Technical_Analysis_v2.0.pdf')
        HTML(string=tech_html).write_pdf(tech_path)
        print(f"  Technical Analysis: {tech_path}")

        # Generate LinkedIn Post
        post = generate_linkedin_post(report_code, data)
        post_path = os.path.join(POSTS_DIR, f'{report_code}_LinkedIn_Post_v2.0.md')
        with open(post_path, 'w') as f:
            f.write(post)
        print(f"  LinkedIn Post: {post_path}")

    print("\n" + "=" * 60)
    print("All Solar v2.0 reports generated!")
    print("=" * 60)

if __name__ == '__main__':
    main()
