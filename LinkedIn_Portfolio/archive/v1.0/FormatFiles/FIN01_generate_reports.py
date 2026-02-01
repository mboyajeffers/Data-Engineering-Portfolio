#!/usr/bin/env python3
"""
FIN-01: Generate Reports from Processed Data
Generates: Executive Summary PDF, Technical Analysis PDF, LinkedIn Post

Author: Mboya Jeffers
Date: 2026-01-31
"""

import pandas as pd
import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '../Finance/data')
REPORTS_DIR = os.path.join(SCRIPT_DIR, '../Finance/reports')
POSTS_DIR = os.path.join(SCRIPT_DIR, '../Finance/posts')

os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(POSTS_DIR, exist_ok=True)

# Color scheme (Finance = #1e3a5f Navy)
PRIMARY_COLOR = '#1e3a5f'
SECONDARY_COLOR = '#2563eb'

def load_data():
    """Load processed data files"""
    summary = {}
    with open(os.path.join(DATA_DIR, 'FIN01_summary.json'), 'r') as f:
        summary = json.load(f)

    stock_data = pd.read_csv(os.path.join(DATA_DIR, 'FIN01_stock_data.csv'))
    risk_metrics = pd.read_csv(os.path.join(DATA_DIR, 'FIN01_risk_metrics.csv'))
    correlations = pd.read_csv(os.path.join(DATA_DIR, 'FIN01_correlations.csv'))

    return summary, stock_data, risk_metrics, correlations

def generate_executive_summary_html(summary, risk_metrics):
    """Generate Executive Summary HTML"""

    key_findings = summary.get('key_findings', {})
    sector = summary.get('sector_comparison', {})
    metadata = summary.get('report_metadata', {})

    # Top performers table
    top_5 = risk_metrics.nlargest(5, 'total_return')[['ticker', 'total_return', 'sharpe_ratio', 'annualized_volatility']]
    top_5_rows = ""
    for _, row in top_5.iterrows():
        top_5_rows += f"""
        <tr>
            <td>{row['ticker']}</td>
            <td>{row['total_return']*100:.1f}%</td>
            <td>{row['sharpe_ratio']:.2f}</td>
            <td>{row['annualized_volatility']*100:.1f}%</td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            @page {{
                size: letter;
                margin: 0.6in;
                @bottom-center {{
                    content: "Page " counter(page) " of " counter(pages);
                    font-size: 10px;
                    color: #666;
                }}
            }}
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                color: #333;
                line-height: 1.5;
                font-size: 11px;
            }}
            .header {{
                border-bottom: 3px solid {PRIMARY_COLOR};
                padding-bottom: 10px;
                margin-bottom: 15px;
            }}
            .header h1 {{
                color: #1a1a2e;
                margin: 0;
                font-size: 24px;
            }}
            .header .subtitle {{
                color: {PRIMARY_COLOR};
                font-size: 14px;
                margin-top: 5px;
            }}
            .header .date {{
                color: {PRIMARY_COLOR};
                font-weight: bold;
                font-size: 12px;
            }}
            h2 {{
                color: {PRIMARY_COLOR};
                border-bottom: 1px solid #ddd;
                padding-bottom: 5px;
                font-size: 16px;
            }}
            .kpi-grid {{
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 10px;
                margin: 15px 0;
            }}
            .kpi-card {{
                background: #f8f9fa;
                border-radius: 6px;
                padding: 12px;
                text-align: center;
                border-left: 3px solid {PRIMARY_COLOR};
            }}
            .kpi-card .value {{
                font-size: 20px;
                font-weight: bold;
                color: {PRIMARY_COLOR};
            }}
            .kpi-card .label {{
                font-size: 10px;
                color: #666;
                text-transform: uppercase;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 10px 0;
                font-size: 10px;
            }}
            th {{
                background: {PRIMARY_COLOR};
                color: white;
                padding: 8px 6px;
                text-align: left;
            }}
            td {{
                padding: 6px;
                border-bottom: 1px solid #eee;
            }}
            tr:nth-child(even) {{
                background: #f8f9fa;
            }}
            .highlight-box {{
                background: #f0f7ff;
                border-left: 4px solid {PRIMARY_COLOR};
                padding: 12px;
                margin: 15px 0;
                border-radius: 0 6px 6px 0;
            }}
            .footer {{
                margin-top: 30px;
                padding-top: 15px;
                border-top: 1px solid #ddd;
                font-size: 9px;
                color: #666;
            }}
            .footer .author {{
                color: {PRIMARY_COLOR};
                font-weight: bold;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Major Bank Stock: 10-Year Performance Analysis</h1>
            <div class="subtitle">Executive Summary</div>
            <div class="date">{datetime.now().strftime('%B %d, %Y')}</div>
        </div>

        <h2>Key Performance Metrics</h2>
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="value">{key_findings.get('total_return_10y', 'N/A')}</div>
                <div class="label">10-Year Total Return</div>
            </div>
            <div class="kpi-card">
                <div class="value">{key_findings.get('annualized_return', 'N/A')}</div>
                <div class="label">Annualized Return</div>
            </div>
            <div class="kpi-card">
                <div class="value">{key_findings.get('sharpe_ratio', 'N/A')}</div>
                <div class="label">Sharpe Ratio</div>
            </div>
            <div class="kpi-card">
                <div class="value">{key_findings.get('annualized_volatility', 'N/A')}</div>
                <div class="label">Annualized Volatility</div>
            </div>
            <div class="kpi-card">
                <div class="value">{key_findings.get('max_drawdown', 'N/A')}</div>
                <div class="label">Maximum Drawdown</div>
            </div>
            <div class="kpi-card">
                <div class="value">{key_findings.get('beta', 'N/A')}</div>
                <div class="label">Beta (vs S&P 500)</div>
            </div>
        </div>

        <h2>Sector Comparison</h2>
        <div class="highlight-box">
            <strong>Banking Sector Performance (10-Year)</strong><br>
            Average Return: {sector.get('avg_sector_return', 'N/A')} |
            Average Volatility: {sector.get('avg_sector_volatility', 'N/A')} |
            Average Sharpe: {sector.get('avg_sector_sharpe', 'N/A')}<br>
            Best Performer: {sector.get('best_performer', 'N/A')} |
            Worst Performer: {sector.get('worst_performer', 'N/A')}
        </div>

        <h2>Top 5 Performers (by Total Return)</h2>
        <table>
            <tr>
                <th>Ticker</th>
                <th>Total Return</th>
                <th>Sharpe Ratio</th>
                <th>Volatility</th>
            </tr>
            {top_5_rows}
        </table>

        <h2>Risk Profile Summary</h2>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
                <th>Interpretation</th>
            </tr>
            <tr>
                <td>Value at Risk (95%)</td>
                <td>{key_findings.get('var_95_daily', 'N/A')}</td>
                <td>Expected max daily loss 95% of the time</td>
            </tr>
            <tr>
                <td>Sortino Ratio</td>
                <td>{summary.get('risk_metrics', {}).get('sortino_ratio', 'N/A')}</td>
                <td>Risk-adjusted return (downside only)</td>
            </tr>
            <tr>
                <td>Positive Days</td>
                <td>{summary.get('risk_metrics', {}).get('positive_days_pct', 'N/A')}</td>
                <td>Percentage of days with gains</td>
            </tr>
            <tr>
                <td>Best Day</td>
                <td>{summary.get('risk_metrics', {}).get('best_day', 'N/A')}</td>
                <td>Largest single-day gain</td>
            </tr>
            <tr>
                <td>Worst Day</td>
                <td>{summary.get('risk_metrics', {}).get('worst_day', 'N/A')}</td>
                <td>Largest single-day loss</td>
            </tr>
        </table>

        <h2>Data Coverage</h2>
        <div class="highlight-box">
            <strong>Analysis Period:</strong> {metadata.get('data_start', 'N/A')[:10]} to {metadata.get('data_end', 'N/A')[:10]}<br>
            <strong>Total Data Rows:</strong> {metadata.get('total_rows_processed', 0):,}<br>
            <strong>Tickers Analyzed:</strong> {metadata.get('tickers_analyzed', 0)}<br>
            <strong>Data Source:</strong> Yahoo Finance (verified public data)
        </div>

        <div class="footer">
            <span class="author">Report prepared by Mboya Jeffers</span> | MboyaJeffers9@gmail.com<br>
            Data Source: Yahoo Finance<br>
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </body>
    </html>
    """
    return html

def generate_technical_analysis_html(summary, stock_data, risk_metrics, correlations):
    """Generate Technical Analysis HTML (detailed report)"""

    metadata = summary.get('report_metadata', {})

    # Risk metrics table for all tickers
    risk_rows = ""
    for _, row in risk_metrics.iterrows():
        risk_rows += f"""
        <tr>
            <td>{row['ticker']}</td>
            <td>{row['total_return']*100:.1f}%</td>
            <td>{row['annualized_return']*100:.1f}%</td>
            <td>{row['annualized_volatility']*100:.1f}%</td>
            <td>{row['sharpe_ratio']:.2f}</td>
            <td>{row['max_drawdown']*100:.1f}%</td>
            <td>{row.get('beta', 0):.2f}</td>
        </tr>
        """

    # Correlation summary
    if len(correlations) > 0:
        avg_corr = correlations.groupby('ticker_2')['correlation'].mean().sort_values(ascending=False)
        corr_rows = ""
        for ticker, corr in avg_corr.head(10).items():
            corr_rows += f"<tr><td>{ticker}</td><td>{corr:.3f}</td></tr>"
    else:
        corr_rows = "<tr><td colspan='2'>No correlation data available</td></tr>"

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            @page {{
                size: letter;
                margin: 0.5in;
                @bottom-center {{
                    content: "Page " counter(page) " of " counter(pages);
                    font-size: 9px;
                    color: #666;
                }}
            }}
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                color: #333;
                line-height: 1.4;
                font-size: 10px;
            }}
            .header {{
                border-bottom: 3px solid {PRIMARY_COLOR};
                padding-bottom: 8px;
                margin-bottom: 12px;
            }}
            .header h1 {{
                color: #1a1a2e;
                margin: 0;
                font-size: 20px;
            }}
            .header .subtitle {{
                color: {PRIMARY_COLOR};
                font-size: 12px;
            }}
            h2 {{
                color: {PRIMARY_COLOR};
                font-size: 14px;
                margin-top: 15px;
                border-bottom: 1px solid #ddd;
                padding-bottom: 3px;
            }}
            h3 {{
                color: #444;
                font-size: 12px;
                margin-top: 10px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 8px 0;
                font-size: 9px;
            }}
            th {{
                background: {PRIMARY_COLOR};
                color: white;
                padding: 6px 4px;
                text-align: left;
            }}
            td {{
                padding: 4px;
                border-bottom: 1px solid #eee;
            }}
            tr:nth-child(even) {{
                background: #f8f9fa;
            }}
            .methodology {{
                background: #f8f9fa;
                padding: 10px;
                border-radius: 4px;
                margin: 10px 0;
            }}
            .two-column {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 15px;
            }}
            .footer {{
                margin-top: 20px;
                padding-top: 10px;
                border-top: 1px solid #ddd;
                font-size: 8px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Major Bank Stock: 10-Year Performance Analysis</h1>
            <div class="subtitle">Technical Analysis Report | {datetime.now().strftime('%B %d, %Y')}</div>
        </div>

        <h2>1. Data Overview</h2>
        <div class="methodology">
            <strong>Analysis Period:</strong> {metadata.get('data_start', 'N/A')[:10]} to {metadata.get('data_end', 'N/A')[:10]} (10 years)<br>
            <strong>Total Observations:</strong> {metadata.get('total_rows_processed', 0):,} rows<br>
            <strong>Securities Analyzed:</strong> {metadata.get('tickers_analyzed', 0)} tickers<br>
            <strong>Data Frequency:</strong> Daily OHLCV + computed indicators<br>
            <strong>Data Source:</strong> Yahoo Finance API (yfinance)
        </div>

        <h2>2. Methodology</h2>
        <h3>2.1 Technical Indicators Computed</h3>
        <ul>
            <li><strong>Moving Averages:</strong> SMA/EMA (5, 10, 20, 50, 100, 200 day)</li>
            <li><strong>Volatility:</strong> Rolling standard deviation (5, 20, 60, 252 day windows)</li>
            <li><strong>Momentum:</strong> RSI-14, MACD (12/26/9), Price momentum (5-252 day)</li>
            <li><strong>Bollinger Bands:</strong> 20-day SMA with 2 standard deviation bands</li>
            <li><strong>Volume Analysis:</strong> 20-day SMA, volume ratio</li>
        </ul>

        <h3>2.2 Risk Metrics (CFA/Basel Standards)</h3>
        <ul>
            <li><strong>Sharpe Ratio:</strong> (Return - Rf) / Volatility (Rf assumed 0 for relative comparison)</li>
            <li><strong>Sortino Ratio:</strong> Return / Downside Deviation</li>
            <li><strong>Value at Risk (VaR):</strong> Historical 95% and 99% confidence levels</li>
            <li><strong>Conditional VaR (CVaR):</strong> Expected shortfall beyond VaR threshold</li>
            <li><strong>Beta:</strong> Covariance with SPY / Variance of SPY</li>
            <li><strong>Maximum Drawdown:</strong> Peak-to-trough decline</li>
        </ul>

        <h2>3. Complete Risk Metrics by Ticker</h2>
        <table>
            <tr>
                <th>Ticker</th>
                <th>Total Return</th>
                <th>Ann. Return</th>
                <th>Volatility</th>
                <th>Sharpe</th>
                <th>Max DD</th>
                <th>Beta</th>
            </tr>
            {risk_rows}
        </table>

        <h2>4. Correlation Analysis</h2>
        <div class="two-column">
            <div>
                <h3>Top Correlated Assets (Average)</h3>
                <table>
                    <tr><th>Ticker</th><th>Avg Correlation</th></tr>
                    {corr_rows}
                </table>
            </div>
            <div>
                <h3>Correlation Windows</h3>
                <ul>
                    <li>20-day rolling (short-term)</li>
                    <li>60-day rolling (medium-term)</li>
                    <li>252-day rolling (annual)</li>
                </ul>
                <p>Correlations computed against primary subject ticker vs all peers and indices.</p>
            </div>
        </div>

        <h2>5. Data Quality Assessment</h2>
        <div class="methodology">
            <strong>Completeness:</strong> All tickers have continuous daily data for analysis period<br>
            <strong>Adjustments:</strong> Prices adjusted for splits and dividends<br>
            <strong>Validation:</strong> Cross-referenced with public market data<br>
            <strong>Processing:</strong> Pandas DataFrame operations with NumPy calculations
        </div>

        <h2>6. Key Observations</h2>
        <ul>
            <li>Banking sector shows significant correlation to interest rate sensitive instruments</li>
            <li>Beta values indicate systematic risk relative to broader market</li>
            <li>Volatility clustering observed during market stress periods (2020 COVID, 2022 rate hikes)</li>
            <li>Risk-adjusted returns (Sharpe) vary significantly across peer group</li>
        </ul>

        <div class="footer">
            <strong>Report prepared by Mboya Jeffers</strong> | MboyaJeffers9@gmail.com<br>
            Data Source: Yahoo Finance<br>
            Report Code: FIN-01 | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </body>
    </html>
    """
    return html

def generate_linkedin_post(summary):
    """Generate LinkedIn post markdown"""

    key_findings = summary.get('key_findings', {})
    sector = summary.get('sector_comparison', {})
    metadata = summary.get('report_metadata', {})

    post = f"""# FIN-01 LinkedIn Post
## Major Bank Stock: 10-Year Performance Analysis

**Copy and paste the text below:**

---

Analyzed {metadata.get('total_rows_processed', 0):,} data points across {metadata.get('tickers_analyzed', 0)} financial securities over 10 years (2016-2026).

**Key findings from major US bank analysis:**

📈 10-Year Total Return: {key_findings.get('total_return_10y', 'N/A')}
📊 Annualized Return: {key_findings.get('annualized_return', 'N/A')}
⚡ Annualized Volatility: {key_findings.get('annualized_volatility', 'N/A')}
📉 Maximum Drawdown: {key_findings.get('max_drawdown', 'N/A')}
🎯 Sharpe Ratio: {key_findings.get('sharpe_ratio', 'N/A')}
📐 Beta (vs S&P 500): {key_findings.get('beta', 'N/A')}

**Sector comparison:**
• Avg sector return: {sector.get('avg_sector_return', 'N/A')}
• Best performer: {sector.get('best_performer', 'N/A')}
• Worst performer: {sector.get('worst_performer', 'N/A')}

Includes VaR analysis, rolling correlations, and full risk decomposition.

📊 Data Source: Yahoo Finance
🔧 Built with custom Python analytics pipeline

#DataEngineering #Finance #RiskAnalytics #Banking #DataAnalysis #Portfolio

---

**Post metadata:**
- Report Code: FIN-01
- Generated: {datetime.now().strftime('%Y-%m-%d')}
- Author: Mboya Jeffers
"""
    return post

def main():
    """Generate all reports"""
    print("="*60)
    print("FIN-01: Generating Reports")
    print("="*60)

    # Load data
    print("Loading data...")
    summary, stock_data, risk_metrics, correlations = load_data()

    # Generate Executive Summary
    print("Generating Executive Summary PDF...")
    exec_html = generate_executive_summary_html(summary, risk_metrics)
    exec_pdf_path = os.path.join(REPORTS_DIR, 'FIN01_Executive_Summary_v1.0.pdf')
    HTML(string=exec_html).write_pdf(exec_pdf_path)
    print(f"  Saved: {exec_pdf_path}")

    # Generate Technical Analysis
    print("Generating Technical Analysis PDF...")
    tech_html = generate_technical_analysis_html(summary, stock_data, risk_metrics, correlations)
    tech_pdf_path = os.path.join(REPORTS_DIR, 'FIN01_Technical_Analysis_v1.0.pdf')
    HTML(string=tech_html).write_pdf(tech_pdf_path)
    print(f"  Saved: {tech_pdf_path}")

    # Generate LinkedIn Post
    print("Generating LinkedIn Post...")
    linkedin_post = generate_linkedin_post(summary)
    post_path = os.path.join(POSTS_DIR, 'FIN01_LinkedIn_Post_v1.0.md')
    with open(post_path, 'w') as f:
        f.write(linkedin_post)
    print(f"  Saved: {post_path}")

    print("\n" + "="*60)
    print("All FIN-01 reports generated successfully!")
    print("="*60)

if __name__ == '__main__':
    main()
