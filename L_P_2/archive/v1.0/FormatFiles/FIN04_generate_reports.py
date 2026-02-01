#!/usr/bin/env python3
"""
FIN-04: Generate Reports from Payment Network Data
Generates: Executive Summary PDF, Technical Analysis PDF, LinkedIn Post

Author: Mboya Jeffers
Date: 2026-01-31
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

REPORT_CODE = "FIN04"
REPORT_TITLE = "Payment Network Revenue Correlation"
DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Finance/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Finance/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Finance/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #6c2bd9; padding-bottom: 10px; margin-bottom: 20px; }
h2 { color: #6c2bd9; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #c4b5fd; padding-bottom: 5px; }
h3 { color: #8b5cf6; font-size: 13pt; margin-top: 15px; }
.header { background: linear-gradient(135deg, #4c1d95 0%, #6c2bd9 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #c4b5fd; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #6c2bd9; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #6c2bd9; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 10pt; }
th { background: #6c2bd9; color: white; padding: 10px; text-align: left; }
td { padding: 8px 10px; border-bottom: 1px solid #e0e0e0; }
tr:nth-child(even) { background: #f8f9fa; }
.highlight { background: #ede9fe; padding: 15px; border-radius: 5px; margin: 15px 0; }
.key-finding { background: #ddd6fe; padding: 15px; border-radius: 5px; margin: 15px 0; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; font-size: 9pt; color: #666; }
"""

def load_data():
    """Load processed data files"""
    metrics_df = pd.read_csv(f"{DATA_DIR}/{REPORT_CODE}_metrics.csv")
    tpv_df = pd.read_csv(f"{DATA_DIR}/{REPORT_CODE}_tpv_synthetic.csv")

    try:
        corr_df = pd.read_csv(f"{DATA_DIR}/{REPORT_CODE}_correlations.csv")
    except:
        corr_df = pd.DataFrame()

    with open(f"{DATA_DIR}/{REPORT_CODE}_summary.json", 'r') as f:
        summary = json.load(f)

    return metrics_df, tpv_df, corr_df, summary

def generate_executive_summary(metrics_df, tpv_df, corr_df, summary):
    """Generate executive summary PDF"""

    payment_tickers = ['V', 'MA', 'AXP', 'PYPL']
    payment_df = metrics_df[metrics_df['Ticker'].isin(payment_tickers)]

    html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body>
    <div class="header">
        <h1>{REPORT_CODE}: {REPORT_TITLE}</h1>
        <div class="subtitle">Executive Summary | Generated: {datetime.now().strftime('%B %d, %Y')}</div>
    </div>

    <h2>Key Findings</h2>
    <div class="grid">
        <div class="grid-item">
            <div class="metric-box">
                <div class="metric-value">{summary['market_leadership']['leader_tpv'][:12]}</div>
                <div class="metric-label">Leader TPV</div>
            </div>
        </div>
        <div class="grid-item">
            <div class="metric-box">
                <div class="metric-value">{summary['market_leadership']['duopoly_share']}</div>
                <div class="metric-label">V+MA Market Share</div>
            </div>
        </div>
        <div class="grid-item">
            <div class="metric-box">
                <div class="metric-value">{summary['performance_metrics']['leader_10y_return']}</div>
                <div class="metric-label">10-Year Return</div>
            </div>
        </div>
        <div class="grid-item">
            <div class="metric-box">
                <div class="metric-value">{summary['correlation_insights']['avg_consumer_correlation']}</div>
                <div class="metric-label">Consumer Correlation</div>
            </div>
        </div>
    </div>

    <div class="key-finding">
        <strong>Primary Insight:</strong> Payment networks exhibit a powerful duopoly structure with Visa and
        Mastercard controlling {summary['market_leadership']['duopoly_share']} of total payment volume.
        Strong correlation with consumer spending ({summary['correlation_insights']['avg_consumer_correlation']})
        makes these stocks effective consumer economy proxies.
    </div>

    <h2>Payment Network Performance Comparison</h2>
    <table>
        <tr>
            <th>Network</th>
            <th>10Y Return</th>
            <th>Ann. Vol</th>
            <th>Sharpe</th>
            <th>Max DD</th>
        </tr>
    """

    for ticker in payment_tickers:
        ticker_df = payment_df[payment_df['Ticker'] == ticker]
        if len(ticker_df) > 252:
            total_ret = (ticker_df['Close'].iloc[-1] / ticker_df['Close'].iloc[0] - 1) * 100
            vol = ticker_df['Daily_Return'].std() * np.sqrt(252) * 100
            ann_ret = ((1 + total_ret/100) ** (1/10) - 1) * 100
            sharpe = ann_ret / vol if vol > 0 else 0
            max_dd = ticker_df['Drawdown'].min() * 100

            html += f"""
            <tr>
                <td><strong>{ticker}</strong></td>
                <td>{total_ret:.1f}%</td>
                <td>{vol:.1f}%</td>
                <td>{sharpe:.2f}</td>
                <td>{max_dd:.1f}%</td>
            </tr>
            """

    html += f"""
    </table>

    <h2>Revenue Correlation Analysis</h2>
    <div class="highlight">
        <p><strong>Correlation Insights:</strong></p>
        <ul>
            <li>Consumer Spending Correlation: {summary['correlation_insights']['avg_consumer_correlation']}</li>
            <li>Bank Stock Correlation: {summary['correlation_insights']['avg_bank_correlation']}</li>
            <li>Total Observations: {summary['correlation_insights']['total_observations']:,}</li>
        </ul>
    </div>

    <h2>Revenue Drivers</h2>
    <ul>
        <li><strong>Cross-Border Premium:</strong> {summary['revenue_drivers']['cross_border_premium']}</li>
        <li><strong>Commercial Segment:</strong> {summary['revenue_drivers']['commercial_growth']}</li>
        <li><strong>Disruption Threats:</strong> {summary['revenue_drivers']['digital_disruption']}</li>
    </ul>

    <div class="footer">
        <p><strong>Data Sources:</strong> Yahoo Finance, Synthetic TPV Models | <strong>Period:</strong> {summary['report_metadata']['data_start'][:10]} to {summary['report_metadata']['data_end'][:10]}</p>
        <p><strong>Author:</strong> {summary['report_metadata']['author']} | {summary['report_metadata']['email']}</p>
        <p><em>Generated by Mboya Jeffers - Data Engineering Portfolio</em></p>
    </div>
    </body>
    </html>
    """

    return html

def generate_technical_analysis(metrics_df, tpv_df, corr_df, summary):
    """Generate technical analysis PDF"""

    html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body>
    <div class="header">
        <h1>{REPORT_CODE}: Technical Analysis</h1>
        <div class="subtitle">TPV Modeling & Correlation Analysis | {datetime.now().strftime('%B %d, %Y')}</div>
    </div>

    <h2>1. Data Overview</h2>
    <table>
        <tr><td><strong>Analysis Period</strong></td><td>{summary['report_metadata']['data_start'][:10]} to {summary['report_metadata']['data_end'][:10]}</td></tr>
        <tr><td><strong>Total Data Points</strong></td><td>{summary['report_metadata']['total_rows_processed']:,}</td></tr>
        <tr><td><strong>Securities Analyzed</strong></td><td>{summary['report_metadata']['tickers_analyzed']}</td></tr>
        <tr><td><strong>Primary Data Source</strong></td><td>Yahoo Finance (Daily OHLCV)</td></tr>
    </table>

    <h2>2. TPV Modeling Methodology</h2>
    <p>Total Payment Volume estimated using:</p>
    <ul>
        <li>Historical growth rates from quarterly earnings</li>
        <li>Seasonal adjustments for holiday spending patterns</li>
        <li>Consumer spending indices correlation</li>
        <li>Cross-border volume premiums</li>
    </ul>

    <div class="metric-box">
        TPV(t) = Base_TPV × (1 + r)^t × Seasonal_Factor × (1 + ε)
    </div>

    <h2>3. Transaction Type Breakdown</h2>
    """

    tpv_by_type = tpv_df.groupby('Transaction_Type')['TPV_Billions'].sum()

    html += """
    <table>
        <tr><th>Transaction Type</th><th>Total TPV ($B)</th><th>Weight</th></tr>
    """

    total_tpv = tpv_by_type.sum()
    for tx_type, tpv in tpv_by_type.items():
        html += f"<tr><td>{tx_type.replace('_', ' ')}</td><td>${tpv:,.0f}B</td><td>{tpv/total_tpv*100:.1f}%</td></tr>"

    html += """
    </table>

    <h2>4. Network-Level Analysis</h2>
    """

    tpv_by_network = tpv_df.groupby('Network')['TPV_Billions'].sum().sort_values(ascending=False)

    html += """
    <table>
        <tr><th>Network</th><th>Estimated TPV ($B)</th><th>Market Share</th></tr>
    """

    for network, tpv in tpv_by_network.items():
        html += f"<tr><td>{network}</td><td>${tpv:,.0f}B</td><td>{tpv/total_tpv*100:.1f}%</td></tr>"

    html += f"""
    </table>

    <h2>5. Correlation Analysis</h2>
    <p>Rolling correlations computed between payment networks and:</p>
    <ul>
        <li>Consumer discretionary stocks (proxy for spending)</li>
        <li>Bank stocks (card issuer economics)</li>
        <li>E-commerce platforms (digital payments growth)</li>
    </ul>

    <div class="highlight">
        <p><strong>Key Correlation Findings:</strong></p>
        <ul>
            <li>Payment networks show {summary['correlation_insights']['avg_consumer_correlation']} correlation with consumer stocks</li>
            <li>Higher correlation during economic expansions</li>
            <li>Cross-border volumes most cyclically sensitive</li>
        </ul>
    </div>

    <h2>6. Competitive Threats</h2>
    <ul>
        <li><strong>BNPL:</strong> Affirm, Klarna capturing younger demographics</li>
        <li><strong>RTP Networks:</strong> FedNow, Zelle reducing card dependence</li>
        <li><strong>Crypto/Stablecoins:</strong> Potential long-term disruption</li>
        <li><strong>Open Banking:</strong> Account-to-account payments gaining traction</li>
    </ul>

    <div class="footer">
        <p><strong>Report:</strong> {REPORT_CODE} | <strong>Version:</strong> 1.0</p>
        <p><strong>Author:</strong> {summary['report_metadata']['author']} | {summary['report_metadata']['email']}</p>
        <p><em>Generated by Mboya Jeffers - Data Engineering Portfolio</em></p>
    </div>
    </body>
    </html>
    """

    return html

def generate_linkedin_post(summary):
    """Generate LinkedIn post markdown"""

    post = f"""# {REPORT_CODE} LinkedIn Post
## {REPORT_TITLE}

**Copy and paste the text below:**

---

Deep dive into payment network economics: Analyzed {summary['report_metadata']['total_rows_processed']:,} data points across {summary['report_metadata']['tickers_analyzed']} securities over 10 years.

**Key findings:**

📊 Leader TPV: {summary['market_leadership']['leader_tpv'][:15]}
💳 Duopoly Market Share: {summary['market_leadership']['duopoly_share']}
📈 10-Year Return: {summary['performance_metrics']['leader_10y_return']}
🎯 Sharpe Ratio: {summary['performance_metrics']['leader_sharpe']}

**Correlation insights:**
• Consumer spending correlation: {summary['correlation_insights']['avg_consumer_correlation']}
• Bank stock correlation: {summary['correlation_insights']['avg_bank_correlation']}
• Cross-border premium: {summary['revenue_drivers']['cross_border_premium']}

Includes TPV modeling, transaction type breakdowns, and competitive threat analysis.

📊 Data Source: Yahoo Finance
🔧 Built with custom Python analytics pipeline

#DataEngineering #Payments #FinTech #Finance #DataAnalysis #Visa #Mastercard

---

**Post metadata:**
- Report Code: {REPORT_CODE}
- Generated: {datetime.now().strftime('%Y-%m-%d')}
- Author: {summary['report_metadata']['author']}
"""
    return post

def main():
    print("=" * 60)
    print(f"{REPORT_CODE}: Generating Reports")
    print("=" * 60)

    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)

    print("Loading data...")
    metrics_df, tpv_df, corr_df, summary = load_data()

    print("Generating Executive Summary PDF...")
    exec_html = generate_executive_summary(metrics_df, tpv_df, corr_df, summary)
    exec_path = f"{REPORT_DIR}/{REPORT_CODE}_Executive_Summary_v1.0.pdf"
    HTML(string=exec_html).write_pdf(exec_path, stylesheets=[CSS(string=CSS_STYLE)])
    print(f"  Saved: {exec_path}")

    print("Generating Technical Analysis PDF...")
    tech_html = generate_technical_analysis(metrics_df, tpv_df, corr_df, summary)
    tech_path = f"{REPORT_DIR}/{REPORT_CODE}_Technical_Analysis_v1.0.pdf"
    HTML(string=tech_html).write_pdf(tech_path, stylesheets=[CSS(string=CSS_STYLE)])
    print(f"  Saved: {tech_path}")

    print("Generating LinkedIn Post...")
    post_content = generate_linkedin_post(summary)
    post_path = f"{POST_DIR}/{REPORT_CODE}_LinkedIn_Post_v1.0.md"
    with open(post_path, 'w') as f:
        f.write(post_content)
    print(f"  Saved: {post_path}")

    print("\n" + "=" * 60)
    print(f"All {REPORT_CODE} reports generated successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
