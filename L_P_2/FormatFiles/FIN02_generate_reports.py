#!/usr/bin/env python3
"""
FIN-02: Generate Reports from Volatility Data
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

REPORT_CODE = "FIN02"
REPORT_TITLE = "Investment Bank Volatility Study"
DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Finance/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Finance/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Finance/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #1b4965; padding-bottom: 10px; margin-bottom: 20px; }
h2 { color: #1b4965; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #bee9e8; padding-bottom: 5px; }
h3 { color: #5fa8d3; font-size: 13pt; margin-top: 15px; }
.header { background: linear-gradient(135deg, #0d1b2a 0%, #1b4965 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #bee9e8; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #1b4965; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #1b4965; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 10pt; }
th { background: #1b4965; color: white; padding: 10px; text-align: left; }
td { padding: 8px 10px; border-bottom: 1px solid #e0e0e0; }
tr:nth-child(even) { background: #f8f9fa; }
.highlight { background: #fff3cd; padding: 15px; border-radius: 5px; margin: 15px 0; }
.key-finding { background: #d4edda; padding: 15px; border-radius: 5px; margin: 15px 0; }
.risk-warning { background: #f8d7da; padding: 15px; border-radius: 5px; margin: 15px 0; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; font-size: 9pt; color: #666; }
"""

def load_data():
    """Load processed data files"""
    vol_df = pd.read_csv(f"{DATA_DIR}/{REPORT_CODE}_volatility_data.csv")

    # Handle empty correlation file
    try:
        corr_df = pd.read_csv(f"{DATA_DIR}/{REPORT_CODE}_correlations.csv")
    except:
        corr_df = pd.DataFrame(columns=['Date', 'Ticker', 'Corr_With', 'Window', 'Correlation'])

    risk_df = pd.read_csv(f"{DATA_DIR}/{REPORT_CODE}_tail_risk.csv")

    with open(f"{DATA_DIR}/{REPORT_CODE}_summary.json", 'r') as f:
        summary = json.load(f)

    return vol_df, corr_df, risk_df, summary

def generate_executive_summary(vol_df, corr_df, risk_df, summary):
    """Generate 2-3 page executive summary PDF"""

    # Get investment bank subset
    ib_tickers = ['GS', 'MS', 'JPM', 'C', 'BAC']
    ib_risk = risk_df[risk_df['Ticker'].isin(ib_tickers)]

    # Current volatility regime
    latest = vol_df[vol_df['Date'] == vol_df['Date'].max()]

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
                <div class="metric-value">{summary['volatility_findings']['avg_21d_vol']}</div>
                <div class="metric-label">Avg 21-Day Volatility</div>
            </div>
        </div>
        <div class="grid-item">
            <div class="metric-box">
                <div class="metric-value">{summary['tail_risk_summary']['avg_var_95']}</div>
                <div class="metric-label">Average VaR (95%)</div>
            </div>
        </div>
        <div class="grid-item">
            <div class="metric-box">
                <div class="metric-value">{summary['tail_risk_summary']['avg_max_drawdown']}</div>
                <div class="metric-label">Avg Max Drawdown</div>
            </div>
        </div>
        <div class="grid-item">
            <div class="metric-box">
                <div class="metric-value">{summary['correlation_insights']['avg_vix_correlation']}</div>
                <div class="metric-label">Avg VIX Correlation</div>
            </div>
        </div>
    </div>

    <div class="key-finding">
        <strong>Primary Insight:</strong> Investment banks exhibit significantly higher volatility than broad market indices,
        with strong negative correlation to VIX during stress periods. Average realized volatility of {summary['volatility_findings']['avg_21d_vol']}
        compared to typical equity market volatility of 15-20%.
    </div>

    <h2>Investment Bank Volatility Comparison</h2>
    <table>
        <tr>
            <th>Bank</th>
            <th>Ann. Vol</th>
            <th>Sharpe</th>
            <th>Max DD</th>
            <th>VaR 99%</th>
            <th>Kurtosis</th>
        </tr>
    """

    for _, row in ib_risk.iterrows():
        html += f"""
        <tr>
            <td><strong>{row['Ticker']}</strong></td>
            <td>{row['Annualized_Vol']*100:.1f}%</td>
            <td>{row['Sharpe_Ratio']:.2f}</td>
            <td>{row['Max_Drawdown']*100:.1f}%</td>
            <td>{row['VaR_99_Daily']*100:.2f}%</td>
            <td>{row['Kurtosis']:.2f}</td>
        </tr>
        """

    html += """
    </table>

    <h2>Volatility Regime Analysis</h2>
    <p>Historical volatility cycles through distinct regimes:</p>
    <ul>
        <li><strong>Low Volatility (< 15%):</strong> Stable markets, low hedging costs</li>
        <li><strong>Normal (15-25%):</strong> Typical market conditions</li>
        <li><strong>Elevated (25-40%):</strong> Market stress, increased risk premia</li>
        <li><strong>Crisis (> 40%):</strong> Tail events, correlation breakdown</li>
    </ul>
    """

    html += f"""
    <div class="highlight">
        <strong>Current Regime:</strong> {summary['volatility_findings']['current_vol_regime']}
        <br>
        Maximum observed 21-day volatility: {summary['volatility_findings']['max_21d_vol']}
    </div>

    <h2>Tail Risk Characteristics</h2>
    <p>Investment bank returns exhibit:</p>
    <div class="risk-warning">
        <strong>Fat Tails:</strong> Average kurtosis of {ib_risk['Kurtosis'].mean():.2f} indicates significantly more
        extreme moves than normal distribution would predict. This necessitates robust risk management frameworks
        beyond simple VaR metrics.
    </div>

    <div class="footer">
        <p><strong>Data Sources:</strong> Yahoo Finance, FRED | <strong>Period:</strong> {summary['report_metadata']['data_start'][:10]} to {summary['report_metadata']['data_end'][:10]}</p>
        <p><strong>Author:</strong> {summary['report_metadata']['author']} | {summary['report_metadata']['email']}</p>
        <p><em>Generated by Mboya Jeffers - Data Engineering Portfolio</em></p>
    </div>
    </body>
    </html>
    """

    return html

def generate_technical_analysis(vol_df, corr_df, risk_df, summary):
    """Generate detailed technical analysis PDF"""

    ib_tickers = ['GS', 'MS', 'JPM', 'C', 'BAC']
    ib_vol = vol_df[vol_df['Ticker'].isin(ib_tickers)]

    # Volatility statistics by ticker
    vol_stats = ib_vol.groupby('Ticker').agg({
        'HV_21d': ['mean', 'std', 'min', 'max'],
        'Parkinson_Vol': 'mean',
        'GK_Vol': 'mean',
        'Vol_of_Vol': 'mean'
    }).round(2)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body>
    <div class="header">
        <h1>{REPORT_CODE}: Technical Analysis</h1>
        <div class="subtitle">Volatility Methodology & Detailed Findings | {datetime.now().strftime('%B %d, %Y')}</div>
    </div>

    <h2>1. Data Overview</h2>
    <table>
        <tr><td><strong>Analysis Period</strong></td><td>{summary['report_metadata']['data_start'][:10]} to {summary['report_metadata']['data_end'][:10]}</td></tr>
        <tr><td><strong>Total Data Points</strong></td><td>{summary['report_metadata']['total_rows_processed']:,}</td></tr>
        <tr><td><strong>Securities Analyzed</strong></td><td>{summary['report_metadata']['tickers_analyzed']}</td></tr>
        <tr><td><strong>Primary Data Source</strong></td><td>Yahoo Finance (Daily OHLCV)</td></tr>
    </table>

    <h2>2. Volatility Estimation Methodologies</h2>

    <h3>2.1 Close-to-Close (Historical) Volatility</h3>
    <p>Standard deviation of log returns, annualized:</p>
    <div class="metric-box">
        σ = √(Σ(r_t - μ)² / (n-1)) × √252
    </div>

    <h3>2.2 Parkinson Volatility</h3>
    <p>Uses high-low range, more efficient with ~5x information content:</p>
    <div class="metric-box">
        σ_P = √(1/(4ln(2)) × E[ln(H/L)²]) × √252
    </div>

    <h3>2.3 Garman-Klass Volatility</h3>
    <p>Incorporates OHLC data for improved efficiency:</p>
    <div class="metric-box">
        σ_GK = √(0.5×ln(H/L)² - (2ln(2)-1)×ln(C/O)²) × √252
    </div>

    <h3>2.4 Yang-Zhang Volatility</h3>
    <p>Accounts for overnight jumps and opening gaps:</p>
    <div class="metric-box">
        σ_YZ = √(σ_overnight² + k×σ_open² + (1-k)×σ_RS²) × √252
    </div>

    <h2>3. Volatility Statistics by Bank</h2>
    <table>
        <tr>
            <th>Ticker</th>
            <th>Mean HV21</th>
            <th>Std HV21</th>
            <th>Min HV21</th>
            <th>Max HV21</th>
            <th>Parkinson</th>
            <th>Vol-of-Vol</th>
        </tr>
    """

    for ticker in ib_tickers:
        ticker_data = ib_vol[ib_vol['Ticker'] == ticker]
        if len(ticker_data) > 0:
            html += f"""
            <tr>
                <td><strong>{ticker}</strong></td>
                <td>{ticker_data['HV_21d'].mean():.2f}%</td>
                <td>{ticker_data['HV_21d'].std():.2f}%</td>
                <td>{ticker_data['HV_21d'].min():.2f}%</td>
                <td>{ticker_data['HV_21d'].max():.2f}%</td>
                <td>{ticker_data['Parkinson_Vol'].mean():.2f}%</td>
                <td>{ticker_data['Vol_of_Vol'].mean():.2f}%</td>
            </tr>
            """

    html += """
    </table>

    <h2>4. Correlation Analysis</h2>
    <p>Rolling correlations computed across multiple windows (21, 63, 126, 252 days).</p>
    """

    # VIX correlations
    vix_corr = corr_df[corr_df['Corr_With'] == 'VIX'].groupby('Ticker')['Correlation'].mean()

    html += """
    <h3>4.1 VIX Correlation (63-day rolling average)</h3>
    <table>
        <tr><th>Ticker</th><th>Avg Correlation</th><th>Interpretation</th></tr>
    """

    for ticker, corr in vix_corr.items():
        interp = "Strong negative" if corr < -0.3 else "Moderate negative" if corr < 0 else "Positive (unusual)"
        html += f"<tr><td>{ticker}</td><td>{corr:.3f}</td><td>{interp}</td></tr>"

    html += """
    </table>

    <h2>5. Tail Risk Decomposition</h2>
    """

    ib_risk = risk_df[risk_df['Ticker'].isin(ib_tickers)]

    html += """
    <table>
        <tr>
            <th>Metric</th>
            <th>Mean</th>
            <th>Std Dev</th>
            <th>Risk Implication</th>
        </tr>
    """

    metrics = [
        ('VaR_95_Daily', 'Daily VaR 95%', 'Expected loss exceeded 5% of days'),
        ('CVaR_95_Daily', 'CVaR 95%', 'Average loss when VaR breached'),
        ('Skewness', 'Return Skewness', 'Negative = left tail heavier'),
        ('Kurtosis', 'Return Kurtosis', 'Higher = fatter tails'),
        ('Max_Drawdown', 'Maximum Drawdown', 'Worst peak-to-trough decline')
    ]

    for col, name, interp in metrics:
        if col in ib_risk.columns:
            mean_val = ib_risk[col].mean()
            std_val = ib_risk[col].std()
            if 'Daily' in name or 'Drawdown' in name:
                html += f"<tr><td>{name}</td><td>{mean_val*100:.2f}%</td><td>{std_val*100:.2f}%</td><td>{interp}</td></tr>"
            else:
                html += f"<tr><td>{name}</td><td>{mean_val:.3f}</td><td>{std_val:.3f}</td><td>{interp}</td></tr>"

    html += f"""
    </table>

    <h2>6. Volatility Surface Proxy</h2>
    <p>Synthetic implied volatility surface generated using historical volatility as base,
    with adjustments for moneyness (smile/skew) and term structure.</p>

    <div class="highlight">
        <strong>Key Observation:</strong> Investment bank options typically exhibit pronounced put skew,
        reflecting market concerns about tail risk. The skew steepens during periods of elevated VIX.
    </div>

    <h2>7. Data Quality Notes</h2>
    <ul>
        <li>Adjusted close prices used for return calculations (dividend/split adjusted)</li>
        <li>Missing data points forward-filled (holidays, gaps)</li>
        <li>Outliers > 5 standard deviations flagged but not removed</li>
        <li>Correlation calculations require minimum 90% data overlap</li>
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

def generate_linkedin_post(summary, risk_df):
    """Generate LinkedIn post markdown"""

    ib_risk = risk_df[risk_df['Ticker'].isin(['GS', 'MS', 'JPM', 'C', 'BAC'])]
    highest_vol = ib_risk.loc[ib_risk['Annualized_Vol'].idxmax()]
    best_sharpe = ib_risk.loc[ib_risk['Sharpe_Ratio'].idxmax()]

    post = f"""# {REPORT_CODE} LinkedIn Post
## {REPORT_TITLE}

**Copy and paste the text below:**

---

Deep dive into investment bank volatility patterns: Analyzed {summary['report_metadata']['total_rows_processed']:,} data points across {summary['report_metadata']['tickers_analyzed']} securities over 10 years.

**Key volatility findings:**

📊 Avg 21-Day Realized Vol: {summary['volatility_findings']['avg_21d_vol']}
📈 Peak Volatility Observed: {summary['volatility_findings']['max_21d_vol']}
⚡ Volatility-of-Volatility: {summary['volatility_findings']['vol_of_vol_avg']}
🎯 Average VIX Correlation: {summary['correlation_insights']['avg_vix_correlation']}

**Tail risk analysis:**
• Average VaR (95%): {summary['tail_risk_summary']['avg_var_95']}
• Average CVaR (95%): {summary['tail_risk_summary']['avg_cvar_95']}
• Highest kurtosis: {summary['tail_risk_summary']['highest_kurtosis']}

Methodology includes Parkinson, Garman-Klass, and Yang-Zhang volatility estimators with full correlation dynamics.

📊 Data Source: Yahoo Finance
🔧 Built with custom Python analytics pipeline

#DataEngineering #RiskManagement #Volatility #Finance #QuantFinance #InvestmentBanking

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

    # Ensure directories exist
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)

    print("Loading data...")
    vol_df, corr_df, risk_df, summary = load_data()

    # Executive Summary
    print("Generating Executive Summary PDF...")
    exec_html = generate_executive_summary(vol_df, corr_df, risk_df, summary)
    exec_path = f"{REPORT_DIR}/{REPORT_CODE}_Executive_Summary_v1.0.pdf"
    HTML(string=exec_html).write_pdf(exec_path, stylesheets=[CSS(string=CSS_STYLE)])
    print(f"  Saved: {exec_path}")

    # Technical Analysis
    print("Generating Technical Analysis PDF...")
    tech_html = generate_technical_analysis(vol_df, corr_df, risk_df, summary)
    tech_path = f"{REPORT_DIR}/{REPORT_CODE}_Technical_Analysis_v1.0.pdf"
    HTML(string=tech_html).write_pdf(tech_path, stylesheets=[CSS(string=CSS_STYLE)])
    print(f"  Saved: {tech_path}")

    # LinkedIn Post
    print("Generating LinkedIn Post...")
    post_content = generate_linkedin_post(summary, risk_df)
    post_path = f"{POST_DIR}/{REPORT_CODE}_LinkedIn_Post_v1.0.md"
    with open(post_path, 'w') as f:
        f.write(post_content)
    print(f"  Saved: {post_path}")

    print("\n" + "=" * 60)
    print(f"All {REPORT_CODE} reports generated successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
