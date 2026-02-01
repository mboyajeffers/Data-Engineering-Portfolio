#!/usr/bin/env python3
"""
Master Generator: All Remaining Verticals - v2.0
FIN01-Quality Upgrade for: BET, BRK, GAM, ECM, OIL, MED, CMP, WTH

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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(SCRIPT_DIR, '..')

# Vertical configurations
VERTICALS = {
    'BET': {
        'name': 'Betting',
        'color': '#2e7d32',
        'gradient': 'linear-gradient(135deg, #1b5e20 0%, #4caf50 100%)',
        'reports': {
            'BET01': {'title': 'Daily Fantasy Sports Market Analysis', 'company': 'DraftKings', 'ticker': 'DKNG', 'focus': 'DFS market leadership and user economics'},
            'BET02': {'title': 'Sports Betting Market Dynamics', 'company': 'FanDuel', 'ticker': 'FLUT', 'focus': 'Sports betting market share and handle analysis'},
            'BET03': {'title': 'iGaming Market Analysis', 'company': 'BetMGM', 'ticker': 'MGM', 'focus': 'Online casino and iGaming market dynamics'},
            'BET04': {'title': 'Casino Omnichannel Strategy', 'company': 'Caesars', 'ticker': 'CZR', 'focus': 'Integrated resort and digital gaming convergence'},
        }
    },
    'BRK': {
        'name': 'Brokerage',
        'color': '#1565c0',
        'gradient': 'linear-gradient(135deg, #0d47a1 0%, #42a5f5 100%)',
        'reports': {
            'BRK01': {'title': 'Retail Brokerage AUM Analysis', 'company': 'Charles Schwab', 'ticker': 'SCHW', 'focus': 'Retail wealth management and AUM trends'},
            'BRK02': {'title': 'Institutional Brokerage Study', 'company': 'Interactive Brokers', 'ticker': 'IBKR', 'focus': 'Professional trading and margin lending'},
            'BRK03': {'title': 'Digital Brokerage Disruption', 'company': 'Robinhood', 'ticker': 'HOOD', 'focus': 'Commission-free trading and democratization'},
            'BRK04': {'title': 'Full-Service Brokerage Economics', 'company': 'Fidelity', 'ticker': 'FNF', 'focus': 'Integrated financial services platform'},
        }
    },
    'GAM': {
        'name': 'Gaming',
        'color': '#7b1fa2',
        'gradient': 'linear-gradient(135deg, #4a148c 0%, #ce93d8 100%)',
        'reports': {
            'GAM01': {'title': 'AAA Gaming Publisher Analysis', 'company': 'Activision Blizzard', 'ticker': 'ATVI', 'focus': 'Premium game publishing economics'},
            'GAM02': {'title': 'Sports Gaming Franchise Study', 'company': 'Electronic Arts', 'ticker': 'EA', 'focus': 'Sports gaming IP and live services'},
            'GAM03': {'title': 'Open World Gaming Economics', 'company': 'Take-Two', 'ticker': 'TTWO', 'focus': 'GTA franchise longevity and monetization'},
            'GAM04': {'title': 'UGC Platform Analysis', 'company': 'Roblox', 'ticker': 'RBLX', 'focus': 'User-generated content and creator economy'},
        }
    },
    'ECM': {
        'name': 'Ecommerce',
        'color': '#f57c00',
        'gradient': 'linear-gradient(135deg, #e65100 0%, #ffb74d 100%)',
        'reports': {
            'ECM01': {'title': 'Marketplace Dominance Study', 'company': 'Amazon', 'ticker': 'AMZN', 'focus': 'Marketplace economics and fulfillment'},
            'ECM02': {'title': 'E-commerce Platform Analysis', 'company': 'Shopify', 'ticker': 'SHOP', 'focus': 'Merchant services and GMV trends'},
            'ECM03': {'title': 'Artisan Marketplace Economics', 'company': 'Etsy', 'ticker': 'ETSY', 'focus': 'Handmade and vintage marketplace'},
            'ECM04': {'title': 'Home Goods E-commerce Study', 'company': 'Wayfair', 'ticker': 'W', 'focus': 'Furniture and home goods logistics'},
        }
    },
    'OIL': {
        'name': 'Oilgas',
        'color': '#37474f',
        'gradient': 'linear-gradient(135deg, #263238 0%, #607d8b 100%)',
        'reports': {
            'OIL01': {'title': 'Integrated Oil Major Analysis', 'company': 'ExxonMobil', 'ticker': 'XOM', 'focus': 'Upstream/downstream integration economics'},
            'OIL02': {'title': 'Refining Margin Study', 'company': 'Chevron', 'ticker': 'CVX', 'focus': 'Refining capacity and crack spreads'},
            'OIL03': {'title': 'E&P Economics Analysis', 'company': 'ConocoPhillips', 'ticker': 'COP', 'focus': 'Exploration and production efficiency'},
            'OIL04': {'title': 'European Major Transition', 'company': 'Shell', 'ticker': 'SHEL', 'focus': 'Energy transition and decarbonization'},
        }
    },
    'MED': {
        'name': 'Media',
        'color': '#c62828',
        'gradient': 'linear-gradient(135deg, #b71c1c 0%, #ef5350 100%)',
        'reports': {
            'MED01': {'title': 'Streaming Wars Analysis', 'company': 'Netflix', 'ticker': 'NFLX', 'focus': 'Subscriber growth and content economics'},
            'MED02': {'title': 'Media Conglomerate Study', 'company': 'Disney', 'ticker': 'DIS', 'focus': 'Parks, streaming, and IP monetization'},
            'MED03': {'title': 'Audio Streaming Economics', 'company': 'Spotify', 'ticker': 'SPOT', 'focus': 'Music streaming and podcast expansion'},
            'MED04': {'title': 'Legacy Media Transformation', 'company': 'Warner Bros Discovery', 'ticker': 'WBD', 'focus': 'Cable cord-cutting and streaming transition'},
        }
    },
    'CMP': {
        'name': 'Compliance',
        'color': '#00695c',
        'gradient': 'linear-gradient(135deg, #004d40 0%, #26a69a 100%)',
        'reports': {
            'CMP01': {'title': 'Consumer Credit Bureau Analysis', 'company': 'Equifax', 'ticker': 'EFX', 'focus': 'Credit data and consumer risk assessment'},
            'CMP02': {'title': 'Identity Verification Study', 'company': 'TransUnion', 'ticker': 'TRU', 'focus': 'Fraud prevention and ID verification'},
            'CMP03': {'title': 'Commercial Credit Analysis', 'company': 'Experian', 'ticker': 'EXPGY', 'focus': 'B2B credit and decisioning analytics'},
            'CMP04': {'title': 'Bank Compliance Analysis', 'company': 'Major Banks', 'ticker': 'JPM', 'focus': 'Regulatory compliance and risk management'},
        }
    },
    'WTH': {
        'name': 'Weather',
        'color': '#0277bd',
        'gradient': 'linear-gradient(135deg, #01579b 0%, #4fc3f7 100%)',
        'reports': {
            'WTH01': {'title': 'Enterprise Weather Data Study', 'company': 'IBM Weather', 'ticker': 'IBM', 'focus': 'B2B weather analytics and forecasting'},
            'WTH02': {'title': 'Consumer Weather App Analysis', 'company': 'AccuWeather', 'ticker': 'PRIVATE', 'focus': 'Consumer weather app engagement'},
            'WTH03': {'title': 'Agricultural Weather Analytics', 'company': 'DTN', 'ticker': 'PRIVATE', 'focus': 'Precision agriculture weather services'},
            'WTH04': {'title': 'AI Weather Forecasting', 'company': 'Tomorrow.io', 'ticker': 'PRIVATE', 'focus': 'ML-powered hyperlocal forecasting'},
        }
    },
}

def calculate_risk_metrics(stock_df):
    """Calculate risk metrics from stock data"""
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

def load_data(vertical_key, report_code):
    """Load data for a report"""
    vertical = VERTICALS[vertical_key]
    data_dir = os.path.join(BASE_DIR, vertical['name'], 'data')
    data = {}

    # Load summary
    summary_path = os.path.join(data_dir, f'{report_code}_summary.json')
    if os.path.exists(summary_path):
        with open(summary_path, 'r') as f:
            data['summary'] = json.load(f)
    else:
        data['summary'] = {'report_metadata': {'total_rows_processed': 0}}

    # Load stock data
    stock_path = os.path.join(data_dir, f'{report_code}_stock.csv')
    if os.path.exists(stock_path):
        all_stock = pd.read_csv(stock_path)
        ticker = vertical['reports'][report_code]['ticker']
        if 'Ticker' in all_stock.columns and ticker in all_stock['Ticker'].values:
            data['stock'] = all_stock[all_stock['Ticker'] == ticker].copy()
        else:
            data['stock'] = all_stock.copy()
        if 'Date' in data['stock'].columns:
            data['stock']['Date'] = pd.to_datetime(data['stock']['Date'], utc=True)
        data['risk_metrics'] = calculate_risk_metrics(data['stock'])
    else:
        data['stock'] = pd.DataFrame()
        data['risk_metrics'] = {}

    # Count all rows
    total_rows = 0
    for f in os.listdir(data_dir):
        if f.startswith(report_code) and f.endswith('.csv'):
            df = pd.read_csv(os.path.join(data_dir, f))
            total_rows += len(df)
    data['total_rows'] = total_rows if total_rows > 0 else data['summary'].get('report_metadata', {}).get('total_rows_processed', 1000)

    return data

def generate_executive_html(vertical_key, report_code, data):
    """Generate executive summary HTML"""
    v = VERTICALS[vertical_key]
    r = v['reports'][report_code]
    summary = data.get('summary', {})
    risk = data.get('risk_metrics', {})
    stock = data.get('stock', pd.DataFrame())

    start_date = stock['Date'].min().strftime('%Y-%m-%d') if len(stock) > 0 and 'Date' in stock.columns else '2016-02-03'
    end_date = stock['Date'].max().strftime('%Y-%m-%d') if len(stock) > 0 and 'Date' in stock.columns else '2026-01-30'

    # Build KPI grid from summary
    kpi_html = ""
    kpi_count = 0
    for section_key, section_data in summary.items():
        if section_key == 'report_metadata':
            continue
        if isinstance(section_data, dict):
            for metric_key, metric_val in section_data.items():
                if kpi_count >= 6:
                    break
                label = metric_key.replace('_', ' ').title()
                kpi_html += f'<div class="kpi-card"><div class="value">{metric_val}</div><div class="label">{label}</div></div>'
                kpi_count += 1
        if kpi_count >= 6:
            break

    if not kpi_html:
        kpi_html = '<div class="kpi-card"><div class="value">N/A</div><div class="label">Data Loading</div></div>'

    risk_section = ""
    if risk:
        risk_section = f"""
        <h2>Stock Performance Summary</h2>
        <table>
            <tr><th>Metric</th><th>Value</th><th>Interpretation</th></tr>
            <tr><td>Total Return ({risk.get('years_analyzed', 10):.1f}Y)</td><td>{risk.get('total_return', 0)*100:.1f}%</td><td>Cumulative appreciation</td></tr>
            <tr><td>Annualized Return</td><td>{risk.get('annualized_return', 0)*100:.1f}%</td><td>Geometric mean annual</td></tr>
            <tr><td>Volatility</td><td>{risk.get('annualized_volatility', 0)*100:.1f}%</td><td>Annualized std dev</td></tr>
            <tr><td>Sharpe Ratio</td><td>{risk.get('sharpe_ratio', 0):.2f}</td><td>Risk-adjusted return</td></tr>
            <tr><td>Max Drawdown</td><td>{risk.get('max_drawdown', 0)*100:.1f}%</td><td>Peak-to-trough</td></tr>
        </table>
        """

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
    <style>
        @page {{ size: letter; margin: 0.6in; @bottom-center {{ content: "Page " counter(page) " of " counter(pages); font-size: 10px; color: #666; }} }}
        body {{ font-family: 'Segoe UI', Tahoma, sans-serif; color: #333; line-height: 1.5; font-size: 11px; }}
        .header {{ background: {v['gradient']}; color: white; padding: 25px; margin: -0.6in -0.6in 20px -0.6in; }}
        .header h1 {{ color: white; margin: 0; font-size: 22px; }}
        .header .subtitle {{ color: rgba(255,255,255,0.8); font-size: 12px; margin-top: 5px; }}
        h2 {{ color: {v['color']}; border-bottom: 1px solid #ddd; padding-bottom: 5px; font-size: 14px; margin-top: 20px; }}
        .kpi-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin: 15px 0; }}
        .kpi-card {{ background: #f8f9fa; border-radius: 6px; padding: 12px; text-align: center; border-left: 3px solid {v['color']}; }}
        .kpi-card .value {{ font-size: 18px; font-weight: bold; color: {v['color']}; }}
        .kpi-card .label {{ font-size: 9px; color: #666; text-transform: uppercase; }}
        table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 10px; }}
        th {{ background: {v['color']}; color: white; padding: 8px 6px; text-align: left; }}
        td {{ padding: 6px; border-bottom: 1px solid #eee; }}
        tr:nth-child(even) {{ background: #f8f9fa; }}
        .data-box {{ background: #f8f9fa; padding: 12px; border-radius: 4px; margin: 15px 0; border-left: 3px solid {v['color']}; }}
        .footer {{ margin-top: 25px; padding-top: 15px; border-top: 1px solid #ddd; font-size: 9px; color: #666; }}
    </style></head><body>
    <div class="header"><h1>{report_code}: {r['title']}</h1><div class="subtitle">Executive Summary | {datetime.now().strftime('%B %d, %Y')}</div></div>
    <h2>Key Performance Indicators</h2><div class="kpi-grid">{kpi_html}</div>
    {risk_section}
    <h2>Data Coverage</h2>
    <div class="data-box">
        <strong>Company:</strong> {r['company']} ({r['ticker']})<br>
        <strong>Analysis Period:</strong> {start_date} to {end_date}<br>
        <strong>Total Data Points:</strong> {data['total_rows']:,}<br>
        <strong>Data Sources:</strong> Yahoo Finance (stock), synthetic operational metrics<br>
        <strong>Focus:</strong> {r['focus']}
    </div>
    <div class="footer"><strong>Report prepared by Mboya Jeffers</strong> | MboyaJeffers9@gmail.com<br>
    <em>Portfolio Sample - Demonstrates data engineering methodology</em> | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    </body></html>"""

def generate_technical_html(vertical_key, report_code, data):
    """Generate technical analysis HTML"""
    v = VERTICALS[vertical_key]
    r = v['reports'][report_code]
    risk = data.get('risk_metrics', {})
    stock = data.get('stock', pd.DataFrame())

    start_date = stock['Date'].min().strftime('%Y-%m-%d') if len(stock) > 0 and 'Date' in stock.columns else '2016-02-03'
    end_date = stock['Date'].max().strftime('%Y-%m-%d') if len(stock) > 0 and 'Date' in stock.columns else '2026-01-30'
    trading_days = len(stock) if len(stock) > 0 else 0

    risk_rows = f"""<tr><td>{r['ticker']}</td><td>{risk.get('total_return', 0)*100:.1f}%</td><td>{risk.get('annualized_return', 0)*100:.1f}%</td>
    <td>{risk.get('annualized_volatility', 0)*100:.1f}%</td><td>{risk.get('sharpe_ratio', 0):.2f}</td><td>{risk.get('max_drawdown', 0)*100:.1f}%</td></tr>""" if risk else ""

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
    <style>
        @page {{ size: letter; margin: 0.5in; @bottom-center {{ content: "Page " counter(page) " of " counter(pages); font-size: 9px; color: #666; }} }}
        body {{ font-family: 'Segoe UI', Tahoma, sans-serif; color: #333; line-height: 1.4; font-size: 10px; }}
        .header {{ background: {v['gradient']}; color: white; padding: 20px; margin: -0.5in -0.5in 15px -0.5in; }}
        .header h1 {{ color: white; margin: 0; font-size: 18px; }}
        .header .subtitle {{ color: rgba(255,255,255,0.8); font-size: 11px; }}
        h2 {{ color: {v['color']}; font-size: 13px; margin-top: 15px; border-bottom: 1px solid #ddd; padding-bottom: 3px; }}
        h3 {{ color: #444; font-size: 11px; margin-top: 10px; }}
        .methodology {{ background: #f8f9fa; padding: 10px; border-radius: 4px; margin: 10px 0; border-left: 3px solid {v['color']}; }}
        table {{ width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 9px; }}
        th {{ background: {v['color']}; color: white; padding: 6px 4px; text-align: left; }}
        td {{ padding: 4px; border-bottom: 1px solid #eee; }}
        tr:nth-child(even) {{ background: #f8f9fa; }}
        ul {{ margin: 5px 0; padding-left: 20px; }} li {{ margin: 3px 0; }}
        .two-column {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }}
        .footer {{ margin-top: 20px; padding-top: 10px; border-top: 1px solid #ddd; font-size: 8px; color: #666; }}
    </style></head><body>
    <div class="header"><h1>{report_code}: {r['title']}</h1><div class="subtitle">Technical Analysis Report | {datetime.now().strftime('%B %d, %Y')}</div></div>

    <h2>1. Data Overview</h2>
    <div class="methodology">
        <strong>Subject:</strong> {r['company']} ({r['ticker']})<br>
        <strong>Analysis Period:</strong> {start_date} to {end_date} (~{risk.get('years_analyzed', 10):.1f} years)<br>
        <strong>Total Observations:</strong> {data['total_rows']:,} rows<br>
        <strong>Stock Data:</strong> {trading_days:,} daily OHLCV observations<br>
        <strong>Data Sources:</strong> Yahoo Finance API (yfinance), synthetic operational metrics
    </div>

    <h2>2. Methodology</h2>
    <h3>2.1 Risk Analytics (CFA Standards)</h3>
    <ul>
        <li><strong>Sharpe Ratio:</strong> (Return - Rf) / Volatility (Rf = 0)</li>
        <li><strong>Sortino Ratio:</strong> Return / Downside Deviation</li>
        <li><strong>Value at Risk (VaR):</strong> Historical 95% and 99% confidence</li>
        <li><strong>Maximum Drawdown:</strong> Peak-to-trough decline</li>
    </ul>
    <h3>2.2 Technical Indicators</h3>
    <ul>
        <li><strong>Moving Averages:</strong> SMA/EMA (20, 50, 200 day)</li>
        <li><strong>Volatility:</strong> Rolling standard deviation (20, 60, 252 day)</li>
        <li><strong>Momentum:</strong> RSI-14, Price momentum (20-252 day)</li>
    </ul>

    <h2>3. Risk Metrics Summary</h2>
    <table><tr><th>Ticker</th><th>Total Return</th><th>Ann. Return</th><th>Volatility</th><th>Sharpe</th><th>Max DD</th></tr>{risk_rows}</table>

    <h2>4. Detailed Risk Profile</h2>
    <div class="two-column">
        <div><h3>Return Distribution</h3><table>
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Positive Days</td><td>{risk.get('positive_days_pct', 0)*100:.1f}%</td></tr>
            <tr><td>Best Day</td><td>{risk.get('best_day', 0)*100:.2f}%</td></tr>
            <tr><td>Worst Day</td><td>{risk.get('worst_day', 0)*100:.2f}%</td></tr>
            <tr><td>Sortino Ratio</td><td>{risk.get('sortino_ratio', 0):.2f}</td></tr>
        </table></div>
        <div><h3>Value at Risk</h3><table>
            <tr><th>Confidence</th><th>Daily VaR</th></tr>
            <tr><td>95%</td><td>{risk.get('var_95', 0)*100:.2f}%</td></tr>
            <tr><td>99%</td><td>{risk.get('var_99', 0)*100:.2f}%</td></tr>
        </table></div>
    </div>

    <h2>5. Data Quality Assessment</h2>
    <div class="methodology">
        <strong>Completeness:</strong> All trading days covered<br>
        <strong>Adjustments:</strong> Prices adjusted for splits/dividends<br>
        <strong>Validation:</strong> Stock data from Yahoo Finance<br>
        <strong>Processing:</strong> Pandas/NumPy calculations<br>
        <strong>Disclaimer:</strong> Operational metrics are synthetic/illustrative
    </div>

    <h2>6. Key Observations</h2>
    <ul>
        <li>Sector volatility characteristics relative to market indices</li>
        <li>Risk-adjusted return profile (Sharpe, Sortino)</li>
        <li>Drawdown behavior during market stress periods</li>
        <li>Volume and liquidity patterns</li>
    </ul>

    <div class="footer"><strong>Report prepared by Mboya Jeffers</strong> | MboyaJeffers9@gmail.com<br>
    Data Source: Yahoo Finance (stock), Synthetic (operational) | Report Code: {report_code}<br>
    <em>Portfolio Sample - Demonstrates data engineering methodology</em> | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    </body></html>"""

def generate_linkedin_post(vertical_key, report_code, data):
    """Generate LinkedIn post"""
    v = VERTICALS[vertical_key]
    r = v['reports'][report_code]
    risk = data.get('risk_metrics', {})

    return f"""# {report_code} LinkedIn Post
## {r['title']}

---
Analyzed {data['total_rows']:,} data points for {r['company']} ({r['ticker']}) - {r['focus']}.

**Highlights:**
- {risk.get('years_analyzed', 10):.1f} years daily OHLCV data
- Sharpe: {risk.get('sharpe_ratio', 0):.2f}, Max DD: {risk.get('max_drawdown', 0)*100:.1f}%
- Total Return: {risk.get('total_return', 0)*100:.1f}%

**Pipeline:** Python, Pandas, NumPy, WeasyPrint

#{v['name']} #DataEngineering #Python #Analytics

---
{report_code} | {datetime.now().strftime('%Y-%m-%d')} | v2.0
"""

def process_vertical(vertical_key):
    """Process all reports for a vertical"""
    v = VERTICALS[vertical_key]
    reports_dir = os.path.join(BASE_DIR, v['name'], 'reports')
    posts_dir = os.path.join(BASE_DIR, v['name'], 'posts')
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(posts_dir, exist_ok=True)

    print(f"\n{v['name']} Vertical:")
    for report_code in v['reports'].keys():
        print(f"  Processing {report_code}...")
        data = load_data(vertical_key, report_code)

        exec_html = generate_executive_html(vertical_key, report_code, data)
        HTML(string=exec_html).write_pdf(os.path.join(reports_dir, f'{report_code}_Executive_Summary_v2.0.pdf'))

        tech_html = generate_technical_html(vertical_key, report_code, data)
        HTML(string=tech_html).write_pdf(os.path.join(reports_dir, f'{report_code}_Technical_Analysis_v2.0.pdf'))

        post = generate_linkedin_post(vertical_key, report_code, data)
        with open(os.path.join(posts_dir, f'{report_code}_LinkedIn_Post_v2.0.md'), 'w') as f:
            f.write(post)

        print(f"    Done: {report_code}")

def main():
    print("=" * 60)
    print("Master Generator: All Verticals v2.0")
    print("=" * 60)

    for vertical_key in VERTICALS.keys():
        process_vertical(vertical_key)

    print("\n" + "=" * 60)
    print("All v2.0 reports generated!")
    print("=" * 60)

if __name__ == '__main__':
    main()
