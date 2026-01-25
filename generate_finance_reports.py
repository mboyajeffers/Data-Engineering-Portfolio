#!/usr/bin/env python3
"""
CMS LinkedIn Proof Package - Finance Reports
Author: Mboya Jeffers (MboyaJeffers9@gmail.com)
Generated: January 25, 2026
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Report generation imports
from weasyprint import HTML, CSS
from jinja2 import Template
import os

OUTPUT_DIR = "/Users/mboyajeffers/Downloads/LinkedIn_Proof_Package/Finance"
AUTHOR = "Mboya Jeffers"
EMAIL = "MboyaJeffers9@gmail.com"
DATE = datetime.now().strftime("%B %d, %Y")

# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_stock_data(tickers, period="6mo"):
    """Fetch OHLCV data for multiple tickers"""
    data = {}
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period=period)
            if not hist.empty:
                data[ticker] = hist
                print(f"  ✓ {ticker}: {len(hist)} days")
        except Exception as e:
            print(f"  ✗ {ticker}: {e}")
    return data

def fetch_fred_data():
    """Fetch FRED data via yfinance treasury proxies"""
    # Using yfinance for treasury/macro proxies
    macro_tickers = {
        '^TNX': '10Y Treasury Yield',
        '^IRX': '3M Treasury Yield',
        '^VIX': 'VIX Volatility Index',
        'SPY': 'S&P 500 ETF',
        'TLT': '20+ Year Treasury ETF',
        'GLD': 'Gold ETF',
        'UUP': 'US Dollar Index ETF'
    }
    return fetch_stock_data(list(macro_tickers.keys()), period="1y"), macro_tickers

# ============================================================================
# KPI CALCULATIONS
# ============================================================================

def compute_returns(prices):
    """Compute daily returns"""
    return prices.pct_change().dropna()

def compute_sharpe(returns, risk_free_rate=0.05):
    """Compute annualized Sharpe ratio"""
    excess_returns = returns - risk_free_rate/252
    if returns.std() == 0:
        return 0
    return np.sqrt(252) * excess_returns.mean() / returns.std()

def compute_sortino(returns, risk_free_rate=0.05):
    """Compute Sortino ratio (downside deviation)"""
    excess_returns = returns - risk_free_rate/252
    downside = returns[returns < 0]
    if len(downside) == 0 or downside.std() == 0:
        return 0
    return np.sqrt(252) * excess_returns.mean() / downside.std()

def compute_var(returns, confidence=0.95):
    """Compute historical VaR"""
    return np.percentile(returns, (1 - confidence) * 100)

def compute_max_drawdown(prices):
    """Compute maximum drawdown"""
    peak = prices.expanding(min_periods=1).max()
    drawdown = (prices - peak) / peak
    return drawdown.min()

def compute_beta(returns, benchmark_returns):
    """Compute beta vs benchmark"""
    aligned = pd.concat([returns, benchmark_returns], axis=1).dropna()
    if len(aligned) < 10:
        return np.nan
    cov = np.cov(aligned.iloc[:, 0], aligned.iloc[:, 1])
    if cov[1, 1] == 0:
        return np.nan
    return cov[0, 1] / cov[1, 1]

def compute_correlation_matrix(data_dict):
    """Compute correlation matrix for multiple assets"""
    returns_df = pd.DataFrame()
    for ticker, df in data_dict.items():
        if 'Close' in df.columns:
            returns_df[ticker] = compute_returns(df['Close'])
    return returns_df.corr()

def compute_portfolio_var(weights, returns_df, confidence=0.95):
    """Compute portfolio VaR"""
    portfolio_returns = (returns_df * weights).sum(axis=1)
    return compute_var(portfolio_returns, confidence)

# ============================================================================
# HTML REPORT TEMPLATES
# ============================================================================

BASE_CSS = """
@page { size: letter; margin: 0.75in; }
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.5; }
.header { border-bottom: 3px solid #2d9596; padding-bottom: 10px; margin-bottom: 20px; }
.header h1 { color: #1a1a2e; margin: 0; font-size: 28px; }
.header .subtitle { color: #666; font-size: 14px; margin-top: 5px; }
.header .date { color: #999; font-size: 12px; float: right; }
.kpi-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin: 20px 0; }
.kpi-card { background: #f8f9fa; border-radius: 8px; padding: 20px; text-align: center; border-left: 4px solid #2d9596; }
.kpi-card .label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 1px; }
.kpi-card .value { font-size: 28px; font-weight: bold; color: #1a1a2e; margin: 8px 0; }
.kpi-card .value.positive { color: #10b981; }
.kpi-card .value.negative { color: #ef4444; }
table { width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 13px; }
th { background: #2d9596; color: white; padding: 12px 10px; text-align: left; }
td { padding: 10px; border-bottom: 1px solid #eee; }
tr:nth-child(even) { background: #f8f9fa; }
.section { margin: 30px 0; }
.section h2 { color: #2d9596; font-size: 18px; border-bottom: 1px solid #eee; padding-bottom: 8px; }
.highlight-box { background: #f0fdf4; border-left: 4px solid #10b981; padding: 15px; margin: 15px 0; }
.highlight-box.warning { background: #fef3c7; border-color: #f59e0b; }
.methodology { background: #f8f9fa; padding: 15px; border-radius: 8px; font-size: 12px; color: #666; margin-top: 30px; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #eee; font-size: 11px; color: #999; }
.footer .author { color: #2d9596; font-weight: bold; }
.matrix { font-size: 11px; }
.matrix td, .matrix th { text-align: center; padding: 8px; }
.corr-high { background: #dcfce7; }
.corr-med { background: #fef9c3; }
.corr-low { background: #fee2e2; }
"""

REPORT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{{ title }}</title></head>
<body>
<div class="header">
    <span class="date">{{ date }}</span>
    <h1>{{ title }}</h1>
    <div class="subtitle">{{ subtitle }}</div>
</div>
{{ content }}
<div class="footer">
    Report prepared by <span class="author">{{ author }}</span> | {{ date }}<br>
    Data Source: {{ data_source }}
</div>
</body>
</html>
"""

# ============================================================================
# REPORT 1: PORTFOLIO RISK DASHBOARD
# ============================================================================

def generate_portfolio_risk_report(data):
    """Generate Portfolio Risk Dashboard PDF"""
    print("\n📊 Generating Portfolio Risk Dashboard...")

    # Portfolio tickers
    portfolio = ['NVDA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN']
    weights = np.array([0.25, 0.20, 0.20, 0.20, 0.15])

    # Compute individual metrics
    metrics = []
    returns_df = pd.DataFrame()
    spy_returns = compute_returns(data.get('SPY', pd.DataFrame()).get('Close', pd.Series()))

    for ticker in portfolio:
        if ticker in data:
            df = data[ticker]
            close = df['Close']
            returns = compute_returns(close)
            returns_df[ticker] = returns

            period_return = (close.iloc[-1] / close.iloc[0] - 1) * 100
            volatility = returns.std() * np.sqrt(252) * 100
            sharpe = compute_sharpe(returns)
            var_95 = compute_var(returns) * 100
            max_dd = compute_max_drawdown(close) * 100
            beta = compute_beta(returns, spy_returns)

            metrics.append({
                'ticker': ticker,
                'return': period_return,
                'volatility': volatility,
                'sharpe': sharpe,
                'var_95': var_95,
                'max_dd': max_dd,
                'beta': beta
            })

    # Portfolio metrics
    portfolio_returns = (returns_df * weights[:len(returns_df.columns)]).sum(axis=1)
    portfolio_return = portfolio_returns.sum() * 100
    portfolio_vol = portfolio_returns.std() * np.sqrt(252) * 100
    portfolio_sharpe = compute_sharpe(portfolio_returns)
    portfolio_var = compute_var(portfolio_returns) * 100

    # Correlation matrix
    corr_matrix = returns_df.corr()

    # Build HTML
    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Portfolio Return</div>
            <div class="value {'positive' if portfolio_return > 0 else 'negative'}">{portfolio_return:+.2f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Portfolio Volatility (Ann.)</div>
            <div class="value">{portfolio_vol:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Portfolio Sharpe Ratio</div>
            <div class="value">{portfolio_sharpe:.3f}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Portfolio VaR (95%)</div>
            <div class="value negative">{portfolio_var:.2f}%</div>
        </div>
    </div>

    <div class="section">
        <h2>Individual Asset Metrics</h2>
        <table>
            <tr>
                <th>Ticker</th>
                <th>Weight</th>
                <th>Return</th>
                <th>Volatility</th>
                <th>Sharpe</th>
                <th>VaR 95%</th>
                <th>Beta</th>
            </tr>
    """

    for i, m in enumerate(metrics):
        ret_class = 'positive' if m['return'] > 0 else 'negative'
        content += f"""
            <tr>
                <td><strong>{m['ticker']}</strong></td>
                <td>{weights[i]*100:.0f}%</td>
                <td class="{ret_class}">{m['return']:+.2f}%</td>
                <td>{m['volatility']:.1f}%</td>
                <td>{m['sharpe']:.3f}</td>
                <td>{m['var_95']:.2f}%</td>
                <td>{m['beta']:.2f}</td>
            </tr>
        """

    content += """
        </table>
    </div>

    <div class="section">
        <h2>Correlation Matrix</h2>
        <table class="matrix">
            <tr><th></th>
    """

    for ticker in corr_matrix.columns:
        content += f"<th>{ticker}</th>"
    content += "</tr>"

    for i, ticker in enumerate(corr_matrix.index):
        content += f"<tr><th>{ticker}</th>"
        for j, col in enumerate(corr_matrix.columns):
            val = corr_matrix.iloc[i, j]
            if i == j:
                css_class = ""
            elif abs(val) > 0.7:
                css_class = "corr-high"
            elif abs(val) > 0.4:
                css_class = "corr-med"
            else:
                css_class = "corr-low"
            content += f'<td class="{css_class}">{val:.2f}</td>'
        content += "</tr>"

    content += """
        </table>
    </div>

    <div class="highlight-box">
        <strong>Key Insights:</strong>
        <ul>
            <li>Portfolio diversification reduces individual stock risk through correlation benefits</li>
            <li>VaR indicates maximum expected daily loss at 95% confidence</li>
            <li>Beta values show sensitivity to broader market movements (SPY)</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Returns computed as daily log returns. Sharpe ratio uses 5% risk-free rate,
        annualized with √252. VaR computed using historical percentile method. Beta calculated via covariance
        with SPY returns. Correlation matrix uses Pearson correlation on daily returns.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Portfolio Risk Dashboard",
        subtitle="5-Stock Tech Portfolio | Risk Analytics",
        date=DATE,
        author=AUTHOR,
        data_source="Yahoo Finance",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Portfolio_Risk_Dashboard.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 2: MACRO ECONOMIC INDICATORS
# ============================================================================

def generate_macro_report(macro_data, macro_labels):
    """Generate Macro Economic Indicators PDF"""
    print("\n📊 Generating Macro Economic Indicators...")

    metrics = []
    for ticker, label in macro_labels.items():
        if ticker in macro_data:
            df = macro_data[ticker]
            close = df['Close']
            current = close.iloc[-1]
            prev_month = close.iloc[-21] if len(close) > 21 else close.iloc[0]
            prev_quarter = close.iloc[-63] if len(close) > 63 else close.iloc[0]

            if ticker in ['^TNX', '^IRX']:
                # Yields are already in percentage
                metrics.append({
                    'label': label,
                    'current': f"{current:.2f}%",
                    'mom': f"{current - prev_month:+.2f}%",
                    'qoq': f"{current - prev_quarter:+.2f}%",
                    'is_yield': True
                })
            else:
                mom_pct = (current / prev_month - 1) * 100
                qoq_pct = (current / prev_quarter - 1) * 100
                metrics.append({
                    'label': label,
                    'current': f"${current:.2f}" if ticker not in ['^VIX'] else f"{current:.1f}",
                    'mom': f"{mom_pct:+.1f}%",
                    'qoq': f"{qoq_pct:+.1f}%",
                    'is_yield': False
                })

    content = """
    <div class="section">
        <h2>Market Indicators Overview</h2>
        <table>
            <tr>
                <th>Indicator</th>
                <th>Current</th>
                <th>MoM Change</th>
                <th>QoQ Change</th>
            </tr>
    """

    for m in metrics:
        content += f"""
            <tr>
                <td><strong>{m['label']}</strong></td>
                <td>{m['current']}</td>
                <td>{m['mom']}</td>
                <td>{m['qoq']}</td>
            </tr>
        """

    content += """
        </table>
    </div>

    <div class="kpi-grid">
    """

    # Highlight key indicators
    for m in metrics[:4]:
        content += f"""
        <div class="kpi-card">
            <div class="label">{m['label']}</div>
            <div class="value">{m['current']}</div>
        </div>
        """

    content += """
    </div>

    <div class="highlight-box">
        <strong>Market Context:</strong>
        <ul>
            <li>Treasury yields indicate interest rate expectations and risk-free rates</li>
            <li>VIX measures implied volatility and market fear/greed sentiment</li>
            <li>Dollar strength (UUP) affects international earnings and commodity prices</li>
            <li>Gold (GLD) serves as inflation hedge and safe-haven indicator</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Data sourced from Yahoo Finance ETF proxies for macro indicators.
        MoM = Month-over-month (21 trading days). QoQ = Quarter-over-quarter (63 trading days).
        Treasury yields from ^TNX (10Y) and ^IRX (3M) indices.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Macro Economic Indicators",
        subtitle="Market Context Dashboard | Treasury, Volatility, Currency",
        date=DATE,
        author=AUTHOR,
        data_source="Yahoo Finance",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Macro_Economic_Indicators.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 3: SECTOR ROTATION ANALYSIS
# ============================================================================

def generate_sector_report(data):
    """Generate Sector Rotation Analysis PDF"""
    print("\n📊 Generating Sector Rotation Analysis...")

    sectors = {
        'XLK': ('Technology', '#3b82f6'),
        'XLF': ('Financials', '#10b981'),
        'XLE': ('Energy', '#f59e0b'),
        'XLV': ('Healthcare', '#ef4444'),
        'XLI': ('Industrials', '#8b5cf6'),
        'XLY': ('Consumer Disc.', '#ec4899'),
        'XLP': ('Consumer Staples', '#06b6d4'),
        'XLU': ('Utilities', '#84cc16')
    }

    metrics = []
    for ticker, (name, color) in sectors.items():
        if ticker in data:
            df = data[ticker]
            close = df['Close']
            returns = compute_returns(close)

            period_return = (close.iloc[-1] / close.iloc[0] - 1) * 100
            volatility = returns.std() * np.sqrt(252) * 100
            sharpe = compute_sharpe(returns)

            metrics.append({
                'ticker': ticker,
                'name': name,
                'return': period_return,
                'volatility': volatility,
                'sharpe': sharpe,
                'color': color
            })

    # Sort by return
    metrics.sort(key=lambda x: x['return'], reverse=True)

    content = """
    <div class="section">
        <h2>Sector Performance Ranking</h2>
        <table>
            <tr>
                <th>Rank</th>
                <th>Sector</th>
                <th>ETF</th>
                <th>Return</th>
                <th>Volatility</th>
                <th>Sharpe</th>
            </tr>
    """

    for i, m in enumerate(metrics):
        ret_class = 'positive' if m['return'] > 0 else 'negative'
        content += f"""
            <tr>
                <td>{i+1}</td>
                <td><strong>{m['name']}</strong></td>
                <td>{m['ticker']}</td>
                <td class="{ret_class}">{m['return']:+.2f}%</td>
                <td>{m['volatility']:.1f}%</td>
                <td>{m['sharpe']:.3f}</td>
            </tr>
        """

    content += """
        </table>
    </div>

    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Best Performing Sector</div>
            <div class="value positive">""" + metrics[0]['name'] + """</div>
        </div>
        <div class="kpi-card">
            <div class="label">Worst Performing Sector</div>
            <div class="value negative">""" + metrics[-1]['name'] + """</div>
        </div>
        <div class="kpi-card">
            <div class="label">Sector Spread</div>
            <div class="value">""" + f"{metrics[0]['return'] - metrics[-1]['return']:.1f}%" + """</div>
        </div>
        <div class="kpi-card">
            <div class="label">Avg Sector Volatility</div>
            <div class="value">""" + f"{np.mean([m['volatility'] for m in metrics]):.1f}%" + """</div>
        </div>
    </div>

    <div class="highlight-box">
        <strong>Sector Rotation Insights:</strong>
        <ul>
            <li>Wide sector spread indicates differentiated performance opportunities</li>
            <li>Defensive sectors (Utilities, Staples) typically outperform in risk-off environments</li>
            <li>Cyclical sectors (Tech, Discretionary) lead in risk-on markets</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Sector ETFs from State Street SPDR family. Returns computed over
        6-month period. Volatility annualized using √252. Sharpe ratio uses 5% risk-free rate.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Sector Rotation Analysis",
        subtitle="S&P 500 Sector ETF Performance Comparison",
        date=DATE,
        author=AUTHOR,
        data_source="Yahoo Finance",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Sector_Rotation_Analysis.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 4: MULTI-ASSET COMPARISON
# ============================================================================

def generate_multi_asset_report(data):
    """Generate Multi-Asset Comparison PDF"""
    print("\n📊 Generating Multi-Asset Comparison...")

    assets = {
        'SPY': ('S&P 500', 'Equity'),
        'QQQ': ('Nasdaq 100', 'Equity'),
        'BTC-USD': ('Bitcoin', 'Crypto'),
        'ETH-USD': ('Ethereum', 'Crypto'),
        'GLD': ('Gold', 'Commodity'),
        'TLT': ('20Y Treasury', 'Fixed Income'),
        'VNQ': ('Real Estate', 'REIT')
    }

    metrics = []
    returns_df = pd.DataFrame()

    for ticker, (name, asset_class) in assets.items():
        if ticker in data:
            df = data[ticker]
            close = df['Close']
            returns = compute_returns(close)
            returns_df[ticker] = returns

            period_return = (close.iloc[-1] / close.iloc[0] - 1) * 100
            volatility = returns.std() * np.sqrt(252) * 100
            sharpe = compute_sharpe(returns)
            max_dd = compute_max_drawdown(close) * 100

            metrics.append({
                'ticker': ticker,
                'name': name,
                'class': asset_class,
                'return': period_return,
                'volatility': volatility,
                'sharpe': sharpe,
                'max_dd': max_dd
            })

    # Correlation matrix
    corr_matrix = returns_df.corr()

    content = """
    <div class="section">
        <h2>Multi-Asset Performance Comparison</h2>
        <table>
            <tr>
                <th>Asset</th>
                <th>Class</th>
                <th>Return</th>
                <th>Volatility</th>
                <th>Sharpe</th>
                <th>Max Drawdown</th>
            </tr>
    """

    for m in metrics:
        ret_class = 'positive' if m['return'] > 0 else 'negative'
        content += f"""
            <tr>
                <td><strong>{m['name']}</strong> ({m['ticker']})</td>
                <td>{m['class']}</td>
                <td class="{ret_class}">{m['return']:+.2f}%</td>
                <td>{m['volatility']:.1f}%</td>
                <td>{m['sharpe']:.3f}</td>
                <td class="negative">{m['max_dd']:.2f}%</td>
            </tr>
        """

    content += """
        </table>
    </div>

    <div class="section">
        <h2>Cross-Asset Correlation Matrix</h2>
        <table class="matrix">
            <tr><th></th>
    """

    for ticker in corr_matrix.columns:
        content += f"<th>{ticker}</th>"
    content += "</tr>"

    for i, ticker in enumerate(corr_matrix.index):
        content += f"<tr><th>{ticker}</th>"
        for j, col in enumerate(corr_matrix.columns):
            val = corr_matrix.iloc[i, j]
            if i == j:
                css_class = ""
            elif abs(val) > 0.6:
                css_class = "corr-high"
            elif abs(val) > 0.3:
                css_class = "corr-med"
            else:
                css_class = "corr-low"
            content += f'<td class="{css_class}">{val:.2f}</td>'
        content += "</tr>"

    content += """
        </table>
    </div>

    <div class="highlight-box">
        <strong>Diversification Analysis:</strong>
        <ul>
            <li>Low correlation between asset classes provides diversification benefits</li>
            <li>Crypto assets typically show higher volatility but potential uncorrelated returns</li>
            <li>Gold and Treasuries often provide hedging during equity drawdowns</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Data from Yahoo Finance. Period returns over 6 months.
        Volatility and Sharpe annualized. Maximum drawdown computed from peak-to-trough.
        Correlation matrix uses Pearson correlation on daily returns.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Multi-Asset Comparison",
        subtitle="Cross-Asset Class Performance & Correlation Analysis",
        date=DATE,
        author=AUTHOR,
        data_source="Yahoo Finance",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Multi_Asset_Comparison.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("=" * 60)
    print("CMS LinkedIn Proof Package - Finance Reports")
    print(f"Author: {AUTHOR}")
    print(f"Date: {DATE}")
    print("=" * 60)

    # All tickers needed
    all_tickers = [
        # Portfolio
        'NVDA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'SPY',
        # Sectors
        'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLY', 'XLP', 'XLU',
        # Multi-asset
        'QQQ', 'BTC-USD', 'ETH-USD', 'GLD', 'TLT', 'VNQ',
        # Macro
        '^TNX', '^IRX', '^VIX', 'UUP'
    ]

    print("\n📥 Fetching market data...")
    data = fetch_stock_data(all_tickers, period="6mo")

    macro_labels = {
        '^TNX': '10Y Treasury Yield',
        '^IRX': '3M Treasury Yield',
        '^VIX': 'VIX Volatility Index',
        'SPY': 'S&P 500 ETF',
        'TLT': '20+ Year Treasury ETF',
        'GLD': 'Gold ETF',
        'UUP': 'US Dollar Index ETF'
    }

    print("\n📄 Generating Reports...")

    # Generate all reports
    generate_portfolio_risk_report(data)
    generate_macro_report(data, macro_labels)
    generate_sector_report(data)
    generate_multi_asset_report(data)

    print("\n" + "=" * 60)
    print("✅ Finance Reports Complete!")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
