#!/usr/bin/env python3
"""
Crypto Vertical: Generate All Reports
Author: Mboya Jeffers
"""

import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Crypto/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Crypto/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Crypto/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #f7931a; padding-bottom: 10px; }
h2 { color: #f7931a; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #ffd93d; padding-bottom: 5px; }
.header { background: linear-gradient(135deg, #1a1a2e 0%, #f7931a 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #ffd93d; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #f7931a; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #f7931a; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 10pt; }
th { background: #f7931a; color: white; padding: 10px; text-align: left; }
td { padding: 8px 10px; border-bottom: 1px solid #e0e0e0; }
tr:nth-child(even) { background: #f8f9fa; }
.highlight { background: #fff8e1; padding: 15px; border-radius: 5px; margin: 15px 0; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; font-size: 9pt; color: #666; }
"""

def gen(code, title, summary, content):
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: {title}</h1><div class="subtitle">Executive Summary | {datetime.now().strftime('%B %d, %Y')}</div></div>
    {content}
    <div class="footer"><p><strong>Author:</strong> {summary['report_metadata']['author']} | <em>Mboya Jeffers - Data Engineering Portfolio</em></p></div>
    </body></html>"""
    return html

def cry01():
    code, title = "CRY01", "US Crypto Exchange Market Structure"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Market Structure</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['market_structure']['leader_share']}</div><div class="metric-label">Leader Market Share</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['market_structure']['leader_volume']}</div><div class="metric-label">Leader Volume</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['trading_metrics']['avg_spread']}</div><div class="metric-label">Avg Spread</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['regulatory']['clarity_score']}</div><div class="metric-label">Regulatory Clarity</div></div></div>
    </div>
    <h2>Trading Metrics</h2>
    <p>Leader: {s['market_structure']['leader_exchange']} | Daily Volume: {s['trading_metrics']['avg_daily_volume']} | Pairs: {s['trading_metrics']['pairs_tracked']}</p>
    <h2>Regulatory Landscape</h2>
    <p>Total Actions: {s['regulatory']['total_actions']} | Outlook: {s['regulatory']['outlook']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>US Exchange Landscape</h2><p>Coinbase dominates regulated US market with {s['market_structure']['leader_share']} share.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nUS crypto exchange analysis: {s['report_metadata']['total_rows_processed']:,} data points.\n\n📊 Leader: {s['market_structure']['leader_exchange']}\n💰 Volume: {s['market_structure']['leader_volume']}\n📈 Market Share: {s['market_structure']['leader_share']}\n⚖️ Regulatory Score: {s['regulatory']['clarity_score']}\n\n#Crypto #Coinbase #DataEngineering #FinTech\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def cry02():
    code, title = "CRY02", "Global Exchange Liquidity Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Global Volume</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['global_volume']['total_spot']}</div><div class="metric-label">Total Spot Volume</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['global_volume']['total_derivatives']}</div><div class="metric-label">Total Derivatives</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['global_volume']['deriv_to_spot_ratio']}</div><div class="metric-label">Deriv/Spot Ratio</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['liquidity_metrics']['avg_spread']}</div><div class="metric-label">Avg Spread</div></div></div>
    </div>
    <h2>Liquidity Depth</h2>
    <p>1% Depth: {s['liquidity_metrics']['avg_1pct_depth']} | Slippage (1M): {s['liquidity_metrics']['slippage_1M']}</p>
    <h2>Derivatives</h2>
    <p>BTC OI: {s['derivatives_insights']['btc_oi']} | Funding: {s['derivatives_insights']['avg_funding']} | Liquidations: {s['derivatives_insights']['avg_liquidations']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Global Dominance</h2><p>{s['global_volume']['leader']} leads with {s['global_volume']['leader_spot_volume']} spot volume.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nGlobal crypto liquidity: {s['report_metadata']['total_rows_processed']:,} records.\n\n📊 Leader: {s['global_volume']['leader']}\n💰 Spot: {s['global_volume']['total_spot']}\n📈 Derivatives: {s['global_volume']['total_derivatives']}\n🔄 Ratio: {s['global_volume']['deriv_to_spot_ratio']}\n\n#Crypto #Binance #Liquidity #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def cry03():
    code, title = "CRY03", "Corporate Bitcoin Treasury Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Treasury Holdings</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['treasury_metrics']['leader_holdings']}</div><div class="metric-label">Leader Holdings</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['treasury_metrics']['leader_value']}</div><div class="metric-label">Holdings Value</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['valuation_metrics']['avg_nav_premium']}</div><div class="metric-label">Avg NAV Premium</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['valuation_metrics']['btc_beta']}</div><div class="metric-label">BTC Beta</div></div></div>
    </div>
    <h2>Financing</h2>
    <p>Total Raised: {s['financing_summary']['total_raised']} | BTC Purchased: {s['financing_summary']['total_btc_purchased']} | Method: {s['financing_summary']['primary_method']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>MSTR Strategy</h2><p>{s['treasury_metrics']['leader']} pioneered corporate BTC treasury with {s['treasury_metrics']['leader_holdings']}.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nCorporate BTC treasury analysis: {s['report_metadata']['total_rows_processed']:,} records.\n\n📊 Leader: {s['treasury_metrics']['leader']}\n💰 Holdings: {s['treasury_metrics']['leader_holdings']}\n📈 Value: {s['treasury_metrics']['leader_value']}\n🎯 NAV Premium: {s['valuation_metrics']['avg_nav_premium']}\n\n#Bitcoin #MicroStrategy #CorporateTreasury #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def cry04():
    code, title = "CRY04", "Bitcoin Mining Economics Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Network Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['network_metrics']['current_hashrate']}</div><div class="metric-label">Network Hashrate</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['network_metrics']['block_reward']}</div><div class="metric-label">Block Reward</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['miner_economics']['avg_cost_per_btc']}</div><div class="metric-label">Avg Cost/BTC</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['miner_economics']['avg_gross_margin']}</div><div class="metric-label">Avg Gross Margin</div></div></div>
    </div>
    <h2>Miner Economics</h2>
    <p>Leader: {s['miner_economics']['leader']} @ {s['miner_economics']['leader_hashrate']} | Efficiency: {s['miner_economics']['avg_efficiency']}</p>
    <h2>Sustainability</h2>
    <p>Renewable: {s['sustainability']['renewable_pct']} | Power: {s['sustainability']['network_power']} | Trend: {s['sustainability']['trend']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Mining Economics</h2><p>Post-halving block reward: {s['network_metrics']['block_reward']} with {s['network_metrics']['daily_issuance']} daily.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nBitcoin mining economics: {s['report_metadata']['total_rows_processed']:,} data points.\n\n⛏️ Hashrate: {s['network_metrics']['current_hashrate']}\n💰 Cost/BTC: {s['miner_economics']['avg_cost_per_btc']}\n📈 Margin: {s['miner_economics']['avg_gross_margin']}\n🌱 Renewable: {s['sustainability']['renewable_pct']}\n\n#Bitcoin #Mining #Marathon #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def main():
    print("=" * 60)
    print("Crypto Vertical: Generating All Reports")
    print("=" * 60)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)

    print("\nGenerating reports...")
    cry01()
    cry02()
    cry03()
    cry04()

    print("\n" + "=" * 60)
    print("All Crypto reports generated successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
