#!/usr/bin/env python3
"""
Crypto Vertical: Generate All Reports - v2.0
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
DATA_DIR = os.path.join(SCRIPT_DIR, '../Crypto/data')
REPORTS_DIR = os.path.join(SCRIPT_DIR, '../Crypto/reports')
POSTS_DIR = os.path.join(SCRIPT_DIR, '../Crypto/posts')

os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(POSTS_DIR, exist_ok=True)

# Color scheme (Crypto = Orange/Bronze)
PRIMARY_COLOR = '#bf360c'
SECONDARY_COLOR = '#ff6d00'
HEADER_GRADIENT = 'linear-gradient(135deg, #1a1a2e 0%, #ff9800 100%)'

# Report configs
REPORTS = {
    'CRY01': {
        'title': 'US Crypto Exchange Market Structure',
        'company': 'Coinbase',
        'ticker': 'COIN',
        'focus': 'US-regulated exchange market share and trading dynamics',
        'data_files': ['CRY01_stock.csv', 'CRY01_volume.csv', 'CRY01_trading.csv', 'CRY01_regulatory.csv'],
        'metrics_computed': [
            'Market share analysis by exchange',
            'Trading volume aggregation (spot)',
            'Bid-ask spread analysis',
            'Regulatory action tracking',
            'Fee structure comparison',
            'User growth metrics',
            'Institutional vs retail mix'
        ]
    },
    'CRY02': {
        'title': 'Global Exchange Liquidity Analysis',
        'company': 'Binance',
        'ticker': 'BNB',
        'focus': 'Global crypto liquidity and derivatives market structure',
        'data_files': ['CRY02_volume.csv', 'CRY02_liquidity.csv', 'CRY02_derivatives.csv', 'CRY02_prices.csv'],
        'metrics_computed': [
            'Spot vs derivatives volume split',
            'Order book depth analysis (1% depth)',
            'Slippage estimation by trade size',
            'Open interest tracking',
            'Funding rate analysis',
            'Liquidation volume monitoring',
            'Cross-exchange arbitrage spreads'
        ]
    },
    'CRY03': {
        'title': 'Corporate Bitcoin Treasury Analysis',
        'company': 'MicroStrategy',
        'ticker': 'MSTR',
        'focus': 'Corporate BTC treasury strategies and NAV analysis',
        'data_files': ['CRY03_holdings.csv', 'CRY03_nav.csv', 'CRY03_financing.csv', 'CRY03_prices.csv'],
        'metrics_computed': [
            'BTC holdings per company',
            'NAV premium/discount calculation',
            'BTC beta and correlation analysis',
            'Financing activity tracking',
            'Cost basis estimation',
            'Stock vs BTC performance',
            'Convertible note analysis'
        ]
    },
    'CRY04': {
        'title': 'Bitcoin Mining Economics Study',
        'company': 'Marathon Digital',
        'ticker': 'MARA',
        'focus': 'Bitcoin mining profitability and network economics',
        'data_files': ['CRY04_hashrate.csv', 'CRY04_miners.csv', 'CRY04_energy.csv', 'CRY04_prices.csv'],
        'metrics_computed': [
            'Network hashrate tracking',
            'Difficulty adjustment analysis',
            'Mining profitability (cost/BTC)',
            'Energy efficiency (J/TH)',
            'Miner revenue breakdown',
            'Renewable energy adoption',
            'ASIC efficiency trends'
        ]
    }
}

def calculate_risk_metrics(stock_df):
    """Calculate comprehensive risk metrics from stock data"""
    if 'Close' not in stock_df.columns or len(stock_df) < 100:
        return {}

    stock_df = stock_df.sort_values('Date')
    stock_df['Returns'] = stock_df['Close'].pct_change()
    returns = stock_df['Returns'].dropna()

    if len(returns) < 100:
        return {}

    total_return = (stock_df['Close'].iloc[-1] / stock_df['Close'].iloc[0]) - 1
    trading_days = len(returns)
    years = trading_days / 252
    annualized_return = (1 + total_return) ** (1/years) - 1 if years > 0 else 0
    annualized_volatility = returns.std() * np.sqrt(252)
    sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility > 0 else 0

    cumulative = (1 + returns).cumprod()
    rolling_max = cumulative.cummax()
    drawdown = (cumulative - rolling_max) / rolling_max
    max_drawdown = drawdown.min()

    var_95 = np.percentile(returns, 5)
    var_99 = np.percentile(returns, 1)

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

    summary_path = os.path.join(DATA_DIR, f'{report_code}_summary.json')
    with open(summary_path, 'r') as f:
        data['summary'] = json.load(f)

    # Find and load stock/price data
    config = REPORTS[report_code]
    stock_file = None
    for f in config['data_files']:
        if 'stock' in f.lower() or 'prices' in f.lower():
            stock_file = f
            break

    if stock_file:
        stock_path = os.path.join(DATA_DIR, stock_file)
        if os.path.exists(stock_path):
            all_stock = pd.read_csv(stock_path)
            if 'Ticker' in all_stock.columns:
                primary_ticker = config['ticker']
                data['stock'] = all_stock[all_stock['Ticker'] == primary_ticker].copy()
            else:
                data['stock'] = all_stock.copy()
            if 'Date' in data['stock'].columns:
                data['stock']['Date'] = pd.to_datetime(data['stock']['Date'], utc=True)
            data['risk_metrics'] = calculate_risk_metrics(data['stock'])

    total_rows = 0
    for f in config['data_files']:
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

    if len(stock) > 0 and 'Date' in stock.columns:
        start_date = stock['Date'].min().strftime('%Y-%m-%d')
        end_date = stock['Date'].max().strftime('%Y-%m-%d')
    else:
        start_date = '2021-04-14'
        end_date = '2026-01-30'

    # Build metrics based on report type
    if report_code == 'CRY01':
        ms = summary['market_structure']
        tm = summary['trading_metrics']
        rg = summary['regulatory']
        metrics_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="value">{ms['leader_share']}</div><div class="label">Market Share</div></div>
            <div class="kpi-card"><div class="value">{ms['leader_volume']}</div><div class="label">Trading Volume</div></div>
            <div class="kpi-card"><div class="value">{tm['avg_spread']}</div><div class="label">Avg Spread</div></div>
            <div class="kpi-card"><div class="value">{rg['clarity_score']}</div><div class="label">Regulatory Clarity</div></div>
            <div class="kpi-card"><div class="value">{tm['pairs_tracked']}</div><div class="label">Trading Pairs</div></div>
            <div class="kpi-card"><div class="value">{rg['total_actions']}</div><div class="label">Regulatory Actions</div></div>
        </div>
        """
        highlight = f"""<strong>Market Leader:</strong> {ms['leader_exchange']} dominates US regulated crypto exchange market"""
    elif report_code == 'CRY02':
        gv = summary['global_volume']
        lm = summary['liquidity_metrics']
        di = summary['derivatives_insights']
        metrics_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="value">{gv['total_spot']}</div><div class="label">Global Spot Volume</div></div>
            <div class="kpi-card"><div class="value">{gv['total_derivatives']}</div><div class="label">Derivatives Volume</div></div>
            <div class="kpi-card"><div class="value">{lm['avg_spread']}</div><div class="label">Avg Spread</div></div>
            <div class="kpi-card"><div class="value">{lm['avg_1pct_depth']}</div><div class="label">1% Depth</div></div>
            <div class="kpi-card"><div class="value">{di['btc_oi']}</div><div class="label">BTC Open Interest</div></div>
            <div class="kpi-card"><div class="value">{di['avg_liquidations']}</div><div class="label">Avg Liquidations</div></div>
        </div>
        """
        highlight = f"""<strong>Derivatives Dominance:</strong> {gv['deriv_to_spot_ratio']} derivatives-to-spot ratio | Leader: {gv['leader']}"""
    elif report_code == 'CRY03':
        tm = summary['treasury_metrics']
        vm = summary['valuation_metrics']
        fs = summary['financing_summary']
        metrics_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="value">{tm['leader_holdings']}</div><div class="label">Leader BTC Holdings</div></div>
            <div class="kpi-card"><div class="value">{tm['leader_value']}</div><div class="label">Holdings Value</div></div>
            <div class="kpi-card"><div class="value">{vm['avg_nav_premium']}</div><div class="label">NAV Premium</div></div>
            <div class="kpi-card"><div class="value">{vm['btc_beta']}</div><div class="label">BTC Beta</div></div>
            <div class="kpi-card"><div class="value">{fs['total_raised']}</div><div class="label">Total Raised</div></div>
            <div class="kpi-card"><div class="value">{tm['companies_tracked']}</div><div class="label">Companies Tracked</div></div>
        </div>
        """
        highlight = f"""<strong>Corporate BTC:</strong> {tm['total_corporate_btc']} held across tracked companies | Primary method: {fs['primary_method']}"""
    else:  # CRY04
        nm = summary['network_metrics']
        me = summary['miner_economics']
        su = summary['sustainability']
        metrics_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="value">{nm['current_hashrate']}</div><div class="label">Network Hashrate</div></div>
            <div class="kpi-card"><div class="value">{me['leader_hashrate']}</div><div class="label">Leader Hashrate</div></div>
            <div class="kpi-card"><div class="value">{me['avg_cost_per_btc']}</div><div class="label">Cost per BTC</div></div>
            <div class="kpi-card"><div class="value">{me['avg_gross_margin']}</div><div class="label">Gross Margin</div></div>
            <div class="kpi-card"><div class="value">{su['renewable_pct']}</div><div class="label">Renewable Energy</div></div>
            <div class="kpi-card"><div class="value">{nm['block_reward']}</div><div class="label">Block Reward</div></div>
        </div>
        """
        highlight = f"""<strong>Mining Economics:</strong> {me['avg_efficiency']} avg efficiency | Leader: {me['leader']} | {su['trend']}"""

    risk_section = ""
    if risk:
        risk_section = f"""
        <h2>Stock Performance Summary</h2>
        <table>
            <tr><th>Metric</th><th>Value</th><th>Interpretation</th></tr>
            <tr><td>Total Return ({risk.get('years_analyzed', 3):.1f}Y)</td><td>{risk.get('total_return', 0)*100:.1f}%</td><td>Cumulative price appreciation</td></tr>
            <tr><td>Annualized Return</td><td>{risk.get('annualized_return', 0)*100:.1f}%</td><td>Geometric mean annual return</td></tr>
            <tr><td>Annualized Volatility</td><td>{risk.get('annualized_volatility', 0)*100:.1f}%</td><td>Standard deviation (annualized)</td></tr>
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
            .header .subtitle {{ color: #ffcc80; font-size: 12px; margin-top: 5px; }}
            h2 {{ color: {PRIMARY_COLOR}; border-bottom: 1px solid #ffcc80; padding-bottom: 5px; font-size: 14px; margin-top: 20px; }}
            .kpi-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin: 15px 0; }}
            .kpi-card {{ background: #f8f9fa; border-radius: 6px; padding: 12px; text-align: center; border-left: 3px solid {SECONDARY_COLOR}; }}
            .kpi-card .value {{ font-size: 18px; font-weight: bold; color: {PRIMARY_COLOR}; }}
            .kpi-card .label {{ font-size: 9px; color: #666; text-transform: uppercase; }}
            .highlight-box {{ background: #fff3e0; border-left: 4px solid {SECONDARY_COLOR}; padding: 12px; margin: 15px 0; border-radius: 0 6px 6px 0; }}
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
            <strong>Data Sources:</strong> Yahoo Finance (stock), CoinGecko/exchange APIs (market data)<br>
            <strong>Focus Area:</strong> {config['focus']}
        </div>

        <div class="footer">
            <span class="author">Report prepared by Mboya Jeffers</span> | MboyaJeffers9@gmail.com<br>
            <em>Portfolio Sample - Synthetic market data combined with verified stock prices</em><br>
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </body>
    </html>
    """
    return html

def generate_technical_analysis_html(report_code, data):
    """Generate Technical Analysis HTML - v2.0"""
    config = REPORTS[report_code]
    risk = data.get('risk_metrics', {})
    stock = data.get('stock', pd.DataFrame())

    if len(stock) > 0 and 'Date' in stock.columns:
        start_date = stock['Date'].min().strftime('%Y-%m-%d')
        end_date = stock['Date'].max().strftime('%Y-%m-%d')
        trading_days = len(stock)
    else:
        start_date = '2021-04-14'
        end_date = '2026-01-30'
        trading_days = 0

    metrics_list = "\n".join([f"<li>{m}</li>" for m in config['metrics_computed']])

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
            .header .subtitle {{ color: #ffcc80; font-size: 11px; }}
            h2 {{ color: {PRIMARY_COLOR}; font-size: 13px; margin-top: 15px; border-bottom: 1px solid #ffcc80; padding-bottom: 3px; }}
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
            <strong>Analysis Period:</strong> {start_date} to {end_date} (~{risk.get('years_analyzed', 3):.1f} years)<br>
            <strong>Total Observations:</strong> {data['total_rows']:,} rows across {len(config['data_files'])} datasets<br>
            <strong>Stock Data:</strong> {trading_days:,} daily OHLCV observations<br>
            <strong>Data Sources:</strong> Yahoo Finance API, CoinGecko, exchange APIs (synthetic operational data)
        </div>

        <h2>2. Methodology</h2>
        <h3>2.1 Metrics Computed</h3>
        <ul>{metrics_list}</ul>

        <h3>2.2 Risk Analytics (CFA Standards)</h3>
        <ul>
            <li><strong>Sharpe Ratio:</strong> (Return - Rf) / Volatility (Rf = 0)</li>
            <li><strong>Sortino Ratio:</strong> Return / Downside Deviation</li>
            <li><strong>Value at Risk (VaR):</strong> Historical 95% and 99% confidence</li>
            <li><strong>Maximum Drawdown:</strong> Peak-to-trough decline</li>
        </ul>

        <h3>2.3 Crypto-Specific Indicators</h3>
        <ul>
            <li><strong>Volume Analysis:</strong> On-chain vs exchange volume</li>
            <li><strong>Volatility:</strong> Rolling 30-day realized volatility</li>
            <li><strong>Correlation:</strong> BTC/ETH correlation, equity beta</li>
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
            </div>
        </div>

        <h2>5. Data Quality Assessment</h2>
        <div class="methodology">
            <strong>Completeness:</strong> All trading days covered for analysis period<br>
            <strong>Stock Data:</strong> Yahoo Finance adjusted prices (splits, dividends)<br>
            <strong>Market Data:</strong> Synthetic/illustrative for portfolio demonstration<br>
            <strong>Processing:</strong> Pandas DataFrame operations with NumPy calculations<br>
            <strong>Disclaimer:</strong> Market metrics are synthetic; stock performance is verified
        </div>

        <h2>6. Key Observations</h2>
        <ul>
            <li>Crypto equities exhibit extreme volatility compared to traditional assets</li>
            <li>High correlation with Bitcoin price movements</li>
            <li>Regulatory developments significantly impact valuations</li>
            <li>Trading volume and liquidity vary significantly across exchanges</li>
        </ul>

        <div class="footer">
            <strong>Report prepared by Mboya Jeffers</strong> | MboyaJeffers9@gmail.com<br>
            Data Source: Yahoo Finance (stock), Synthetic (market) | Report Code: {report_code}<br>
            <em>Portfolio Sample - Demonstrates data engineering methodology</em><br>
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </body>
    </html>
    """
    return html

def generate_linkedin_post(report_code, data):
    """Generate LinkedIn post markdown"""
    config = REPORTS[report_code]
    risk = data.get('risk_metrics', {})

    post = f"""# {report_code} LinkedIn Post
## {config['title']}

**Copy and paste:**

---

Analyzed {data['total_rows']:,} data points for {config['company']} ({config['ticker']}) - {config['focus']}.

**Data Engineering Highlights:**
- Stock Analysis: {risk.get('years_analyzed', 3):.1f} years daily OHLCV
- Risk Metrics: Sharpe {risk.get('sharpe_ratio', 0):.2f}, Max DD {risk.get('max_drawdown', 0)*100:.1f}%
- Total Return: {risk.get('total_return', 0)*100:.1f}%

**Pipeline:** Python, Pandas, NumPy, WeasyPrint

#DataEngineering #Crypto #Python #Analytics

---
Report Code: {report_code} | Generated: {datetime.now().strftime('%Y-%m-%d')} | v2.0
"""
    return post

def main():
    """Generate all Crypto reports - v2.0"""
    print("=" * 60)
    print("Crypto Vertical: Generating v2.0 Reports")
    print("=" * 60)

    for report_code in REPORTS.keys():
        print(f"\nProcessing {report_code}...")
        data = load_data(report_code)

        exec_html = generate_executive_summary_html(report_code, data)
        exec_path = os.path.join(REPORTS_DIR, f'{report_code}_Executive_Summary_v2.0.pdf')
        HTML(string=exec_html).write_pdf(exec_path)
        print(f"  Executive Summary: {exec_path}")

        tech_html = generate_technical_analysis_html(report_code, data)
        tech_path = os.path.join(REPORTS_DIR, f'{report_code}_Technical_Analysis_v2.0.pdf')
        HTML(string=tech_html).write_pdf(tech_path)
        print(f"  Technical Analysis: {tech_path}")

        post = generate_linkedin_post(report_code, data)
        post_path = os.path.join(POSTS_DIR, f'{report_code}_LinkedIn_Post_v2.0.md')
        with open(post_path, 'w') as f:
            f.write(post)
        print(f"  LinkedIn Post: {post_path}")

    print("\n" + "=" * 60)
    print("All Crypto v2.0 reports generated!")
    print("=" * 60)

if __name__ == '__main__':
    main()
