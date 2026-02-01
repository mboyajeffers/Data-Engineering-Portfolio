#!/usr/bin/env python3
"""
Brokerage Vertical: Generate All Reports
Author: Mboya Jeffers
"""

import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Brokerage/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Brokerage/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Brokerage/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #2e7d32; padding-bottom: 10px; }
h2 { color: #2e7d32; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #81c784; padding-bottom: 5px; }
.header { background: linear-gradient(135deg, #1b5e20 0%, #4caf50 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #c8e6c9; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #2e7d32; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #2e7d32; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 10pt; }
th { background: #2e7d32; color: white; padding: 10px; text-align: left; }
td { padding: 8px 10px; border-bottom: 1px solid #e0e0e0; }
tr:nth-child(even) { background: #f8f9fa; }
.highlight { background: #e8f5e9; padding: 15px; border-radius: 5px; margin: 15px 0; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; font-size: 9pt; color: #666; }
"""

def gen(code, title, summary, content):
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: {title}</h1><div class="subtitle">Executive Summary | {datetime.now().strftime('%B %d, %Y')}</div></div>
    {content}
    <div class="footer"><p><strong>Author:</strong> {summary['report_metadata']['author']} | <em>Mboya Jeffers - Data Engineering Portfolio</em></p></div>
    </body></html>"""
    return html

def brk01():
    code, title = "BRK01", "Retail Brokerage Market Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Market Position</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['market_position']['leader_aum']}</div><div class="metric-label">Leader AUM</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['market_position']['total_market_aum']}</div><div class="metric-label">Total Market AUM</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['market_position']['accounts']}</div><div class="metric-label">Client Accounts</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['trading_metrics']['margin_balances']}</div><div class="metric-label">Margin Balances</div></div></div>
    </div>
    <h2>Trading Metrics</h2>
    <div class="highlight">
    <p><strong>Market Leader:</strong> {s['market_position']['leader']}</p>
    <p><strong>Daily Equity Trades:</strong> {s['trading_metrics']['avg_daily_equity_trades']}</p>
    <p><strong>Options Volume:</strong> {s['trading_metrics']['avg_options_volume']}</p>
    </div>
    <h2>Revenue Insights</h2>
    <p>Net Interest Income: {s['revenue_insights']['nii_share']} of revenue | Asset Management: {s['revenue_insights']['asset_mgmt_share']} | Rate Sensitivity: {s['revenue_insights']['rate_sensitivity']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Retail Brokerage Landscape</h2>
    <p>{s['market_position']['leader']} leads with {s['market_position']['leader_aum']} AUM across {s['market_position']['accounts']} accounts.</p>
    <p>Zero-commission trading has shifted revenue to NII ({s['revenue_insights']['nii_share']}) making firms highly rate-sensitive.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

Retail brokerage market analysis: {s['report_metadata']['total_rows_processed']:,} data points analyzed.

📊 Market Leader: {s['market_position']['leader']}
💰 Leader AUM: {s['market_position']['leader_aum']}
📈 Total Market: {s['market_position']['total_market_aum']}
🏦 Client Accounts: {s['market_position']['accounts']}

Key insight: Net interest income now drives {s['revenue_insights']['nii_share']} of revenue, making brokerages highly rate-sensitive.

#Brokerage #Schwab #Fidelity #DataEngineering #FinTech

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def brk02():
    code, title = "BRK02", "Global Trading Platform Analytics"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Global Reach</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['global_reach']['total_accounts']}</div><div class="metric-label">Total Accounts</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['global_reach']['total_equity']}</div><div class="metric-label">Client Equity</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['global_reach']['avg_darts']}</div><div class="metric-label">Avg DARTs</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['global_reach']['regions']}</div><div class="metric-label">Regions</div></div></div>
    </div>
    <h2>Product Mix</h2>
    <div class="highlight">
    <p><strong>Top Product:</strong> {s['product_mix']['top_product']}</p>
    <p><strong>Options Share:</strong> {s['product_mix']['options_share']}</p>
    </div>
    <h2>Margin Business</h2>
    <p>Margin Loans: {s['margin_business']['margin_loans']} | Securities Lending: {s['margin_business']['sec_lending']} | Avg Rate: {s['margin_business']['avg_rate']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>IBKR Global Platform</h2>
    <p>Interactive Brokers operates across {s['global_reach']['regions']} regions with {s['global_reach']['total_equity']} in client equity.</p>
    <p>Professional traders drive {s['global_reach']['avg_darts']} daily average revenue trades.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

Global trading platform analytics: {s['report_metadata']['total_rows_processed']:,} records analyzed.

🌍 Regions: {s['global_reach']['regions']}
💰 Client Equity: {s['global_reach']['total_equity']}
📊 Accounts: {s['global_reach']['total_accounts']}
📈 Avg DARTs: {s['global_reach']['avg_darts']}

IBKR's professional focus drives superior margin rates ({s['margin_business']['avg_rate']}) and {s['margin_business']['margin_loans']} in margin loans.

#IBKR #GlobalTrading #DataEngineering #FinTech

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def brk03():
    code, title = "BRK03", "Millennial Trading Platform Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>User Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['mau']}</div><div class="metric-label">Monthly Active Users</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['funded_accounts']}</div><div class="metric-label">Funded Accounts</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['auc']}</div><div class="metric-label">Assets Under Custody</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['arpu']}</div><div class="metric-label">ARPU</div></div></div>
    </div>
    <h2>Trading Patterns</h2>
    <div class="highlight">
    <p><strong>Avg Equity Volume:</strong> {s['trading_patterns']['avg_equity_volume']}</p>
    <p><strong>Options Dominance:</strong> {s['trading_patterns']['options_dominance']}</p>
    <p><strong>Avg Trade Size:</strong> {s['trading_patterns']['avg_trade_size']}</p>
    </div>
    <h2>Revenue Mix</h2>
    <p>PFOF: {s['revenue_mix']['pfof_share']} | NII: {s['revenue_mix']['nii_share']} | Gold Subscriptions: {s['revenue_mix']['gold_share']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Robinhood Platform</h2>
    <p>Robinhood revolutionized retail trading with {s['user_metrics']['mau']} monthly active users.</p>
    <p>Options trading dominance reflects younger demographic preference for leverage. PFOF remains {s['revenue_mix']['pfof_share']} of revenue.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

Millennial trading platform study: {s['report_metadata']['total_rows_processed']:,} data points analyzed.

📱 MAU: {s['user_metrics']['mau']}
💰 AUC: {s['user_metrics']['auc']}
📊 Funded Accounts: {s['user_metrics']['funded_accounts']}
💵 ARPU: {s['user_metrics']['arpu']}

Robinhood's options dominance and {s['trading_patterns']['avg_trade_size']} avg trade size reflect Gen Z/Millennial trading behavior.

#Robinhood #FinTech #Millennials #DataEngineering

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def brk04():
    code, title = "BRK04", "Full-Service Brokerage Competitive Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Competitive Position</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['competitive_position']['leader_aum']}</div><div class="metric-label">Leader AUM</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['competitive_position']['total_industry_aum']}</div><div class="metric-label">Industry AUM</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['competitive_position']['avg_retention']}</div><div class="metric-label">Avg Retention</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['flow_metrics']['organic_growth']}</div><div class="metric-label">Organic Growth</div></div></div>
    </div>
    <h2>Service Mix</h2>
    <div class="highlight">
    <p><strong>Leader:</strong> {s['competitive_position']['leader']}</p>
    <p><strong>Top Service:</strong> {s['service_mix']['top_service']}</p>
    <p><strong>Wealth Mgmt Share:</strong> {s['service_mix']['wealth_mgmt_share']}</p>
    </div>
    <h2>Flow Metrics</h2>
    <p>Avg Net New Assets: {s['flow_metrics']['avg_nna']} | Organic Growth: {s['flow_metrics']['organic_growth']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Full-Service Landscape</h2>
    <p>{s['competitive_position']['leader']} leads full-service brokerage with {s['competitive_position']['leader_aum']} AUM.</p>
    <p>Wealth management represents {s['service_mix']['wealth_mgmt_share']} of services with {s['competitive_position']['avg_retention']} client retention.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

Full-service brokerage competitive analysis: {s['report_metadata']['total_rows_processed']:,} records processed.

🏆 Leader: {s['competitive_position']['leader']}
💰 Leader AUM: {s['competitive_position']['leader_aum']}
📊 Industry AUM: {s['competitive_position']['total_industry_aum']}
🎯 Retention: {s['competitive_position']['avg_retention']}

Wealth management drives {s['service_mix']['wealth_mgmt_share']} of service revenue with strong {s['flow_metrics']['organic_growth']} organic growth.

#WealthManagement #Fidelity #Brokerage #DataEngineering

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def main():
    print("=" * 60)
    print("Brokerage Vertical: Generating All Reports")
    print("=" * 60)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)

    print("\nGenerating reports...")
    brk01()
    brk02()
    brk03()
    brk04()

    print("\n" + "=" * 60)
    print("All Brokerage reports generated successfully!")
    print("  - 8 PDFs (4 Executive + 4 Technical)")
    print("  - 4 LinkedIn Posts")
    print("=" * 60)

if __name__ == "__main__":
    main()
