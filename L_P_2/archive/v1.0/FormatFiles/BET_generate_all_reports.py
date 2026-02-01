#!/usr/bin/env python3
"""
Betting Vertical: Generate All Reports
Author: Mboya Jeffers
"""

import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Betting/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Betting/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Betting/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #e65100; padding-bottom: 10px; }
h2 { color: #e65100; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #ffb74d; padding-bottom: 5px; }
.header { background: linear-gradient(135deg, #1a1a2e 0%, #ff6f00 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #ffe0b2; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #e65100; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #e65100; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 10pt; }
th { background: #e65100; color: white; padding: 10px; text-align: left; }
td { padding: 8px 10px; border-bottom: 1px solid #e0e0e0; }
tr:nth-child(even) { background: #f8f9fa; }
.highlight { background: #fff3e0; padding: 15px; border-radius: 5px; margin: 15px 0; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; font-size: 9pt; color: #666; }
"""

def gen(code, title, summary, content):
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: {title}</h1><div class="subtitle">Executive Summary | {datetime.now().strftime('%B %d, %Y')}</div></div>
    {content}
    <div class="footer"><p><strong>Author:</strong> {summary['report_metadata']['author']} | <em>Mboya Jeffers - Data Engineering Portfolio</em></p></div>
    </body></html>"""
    return html

def bet01():
    code, title = "BET01", "Daily Fantasy Sports Market Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>User Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['mup']}</div><div class="metric-label">Monthly Unique Payers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['arpmup']}</div><div class="metric-label">ARPMUP</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['handle']}</div><div class="metric-label">Sportsbook Handle</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['mup_growth']}</div><div class="metric-label">YoY Growth</div></div></div>
    </div>
    <h2>Market Position</h2>
    <div class="highlight">
    <p><strong>Top State:</strong> {s['market_position']['top_state']}</p>
    <p><strong>States Live:</strong> {s['market_position']['states_live']}</p>
    <p><strong>Avg Market Share:</strong> {s['market_position']['avg_share']}</p>
    </div>
    <h2>Product Mix</h2>
    <p>DFS Contests: {s['product_mix']['dfs_contests']} | Parlay Share: {s['product_mix']['parlay_share']} | SGP Growth: {s['product_mix']['sgp_growth']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>DraftKings Platform</h2>
    <p>DraftKings leads DFS with {s['user_metrics']['mup']} monthly payers generating {s['user_metrics']['arpmup']} ARPMUP.</p>
    <p>Same-game parlays and live betting drive {s['user_metrics']['mup_growth']} growth.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

DFS market analysis: {s['report_metadata']['total_rows_processed']:,} data points analyzed.

📊 Monthly Payers: {s['user_metrics']['mup']}
💰 Handle: {s['user_metrics']['handle']}
📈 Growth: {s['user_metrics']['mup_growth']}
🗽 Top State: {s['market_position']['top_state']}

Same-game parlays driving accelerated growth in the sports betting market.

#DraftKings #SportsBetting #DFS #DataEngineering #FinTech

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def bet02():
    code, title = "BET02", "Sports Betting Market Dynamics"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Market Structure</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['market_structure']['leader_share']}</div><div class="metric-label">Leader Share</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['market_structure']['top_two_share']}</div><div class="metric-label">Duopoly Share</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['market_structure']['avg_hold']}</div><div class="metric-label">Avg Hold Rate</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['economics']['ltv_cac']}</div><div class="metric-label">LTV/CAC</div></div></div>
    </div>
    <h2>Betting Behavior</h2>
    <div class="highlight">
    <p><strong>Market Leader:</strong> {s['market_structure']['leader']}</p>
    <p><strong>Daily Bets:</strong> {s['betting_behavior']['daily_bets']}</p>
    <p><strong>Mobile Share:</strong> {s['betting_behavior']['mobile_share']}</p>
    <p><strong>Live Betting:</strong> {s['betting_behavior']['live_share']}</p>
    </div>
    <h2>Unit Economics</h2>
    <p>CAC: {s['economics']['avg_cac']} | LTV: {s['economics']['avg_ltv']} | Avg Bet: {s['betting_behavior']['avg_bet']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>FanDuel Dominance</h2>
    <p>{s['market_structure']['leader']} commands {s['market_structure']['leader_share']} of the US sports betting market.</p>
    <p>FanDuel + DraftKings duopoly controls {s['market_structure']['top_two_share']} with {s['economics']['ltv_cac']} LTV/CAC.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

Sports betting market dynamics: {s['report_metadata']['total_rows_processed']:,} records analyzed.

🏆 Leader: {s['market_structure']['leader']} ({s['market_structure']['leader_share']})
📊 Duopoly: {s['market_structure']['top_two_share']}
📱 Mobile: {s['betting_behavior']['mobile_share']}
💰 LTV/CAC: {s['economics']['ltv_cac']}

FanDuel + DraftKings duopoly continues to dominate US sports betting.

#FanDuel #Flutter #SportsBetting #DataEngineering

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def bet03():
    code, title = "BET03", "Casino iGaming Platform Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>iGaming Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['igaming_metrics']['ngr']}</div><div class="metric-label">Net Gaming Revenue</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['igaming_metrics']['active_users']}</div><div class="metric-label">Active Users</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['igaming_metrics']['arpu']}</div><div class="metric-label">ARPU</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['igaming_metrics']['slots_share']}</div><div class="metric-label">Slots Share</div></div></div>
    </div>
    <h2>Game Performance</h2>
    <div class="highlight">
    <p><strong>Daily Slots Sessions:</strong> {s['game_performance']['daily_slots']}</p>
    <p><strong>Avg Session:</strong> {s['game_performance']['avg_session']}</p>
    <p><strong>RTP:</strong> {s['game_performance']['rtp']}</p>
    <p><strong>Live Dealer:</strong> {s['game_performance']['live_dealer_growth']}</p>
    </div>
    <h2>Market Position</h2>
    <p>Top State: {s['market_position']['top_state']} | States Live: {s['market_position']['states_live']} | Avg Share: {s['market_position']['avg_share']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>BetMGM iGaming</h2>
    <p>BetMGM's iGaming platform generates {s['igaming_metrics']['ngr']} NGR with {s['igaming_metrics']['active_users']} active users.</p>
    <p>Slots dominate at {s['igaming_metrics']['slots_share']} with strong live dealer growth.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

Casino iGaming platform study: {s['report_metadata']['total_rows_processed']:,} data points analyzed.

🎰 NGR: {s['igaming_metrics']['ngr']}
👥 Active Users: {s['igaming_metrics']['active_users']}
💰 ARPU: {s['igaming_metrics']['arpu']}
🎲 Slots: {s['igaming_metrics']['slots_share']}

BetMGM leads regulated iGaming with strong live dealer growth.

#BetMGM #iGaming #OnlineCasino #DataEngineering

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def bet04():
    code, title = "BET04", "Traditional Casino Digital Integration"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Digital Integration</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['digital_integration']['digital_revenue']}</div><div class="metric-label">Digital Revenue</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['digital_integration']['digital_share']}</div><div class="metric-label">Digital % of Total</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['digital_integration']['rewards_members']}</div><div class="metric-label">Rewards Members</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['loyalty_metrics']['digital_engagement']}</div><div class="metric-label">Digital Engagement</div></div></div>
    </div>
    <h2>Property Performance</h2>
    <div class="highlight">
    <p><strong>Top Region:</strong> {s['property_performance']['top_region']}</p>
    <p><strong>Avg Occupancy:</strong> {s['property_performance']['avg_occupancy']}</p>
    <p><strong>RevPAR:</strong> {s['property_performance']['avg_revpar']}</p>
    </div>
    <h2>Loyalty Metrics</h2>
    <p>Active Members: {s['loyalty_metrics']['active_members']} | Daily Enrollments: {s['loyalty_metrics']['daily_enrollments']} | Cross-Channel: {s['digital_integration']['cross_channel']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Caesars Omnichannel</h2>
    <p>Caesars integrates digital with {s['digital_integration']['rewards_members']} rewards members.</p>
    <p>Digital now represents {s['digital_integration']['digital_share']} of total revenue with {s['loyalty_metrics']['digital_engagement']} engagement.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

Casino digital integration study: {s['report_metadata']['total_rows_processed']:,} records processed.

🏨 Rewards Members: {s['digital_integration']['rewards_members']}
💻 Digital Revenue: {s['digital_integration']['digital_revenue']}
📊 Digital Share: {s['digital_integration']['digital_share']}
🎯 Engagement: {s['loyalty_metrics']['digital_engagement']}

Caesars leads omnichannel casino-to-digital integration strategy.

#Caesars #Casino #Omnichannel #DataEngineering

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def main():
    print("=" * 60)
    print("Betting Vertical: Generating All Reports")
    print("=" * 60)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)

    print("\nGenerating reports...")
    bet01()
    bet02()
    bet03()
    bet04()

    print("\n" + "=" * 60)
    print("All Betting reports generated successfully!")
    print("  - 8 PDFs (4 Executive + 4 Technical)")
    print("  - 4 LinkedIn Posts")
    print("=" * 60)

if __name__ == "__main__":
    main()
