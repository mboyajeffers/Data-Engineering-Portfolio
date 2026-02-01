#!/usr/bin/env python3
"""
Gaming Vertical: Generate All Reports
Author: Mboya Jeffers
"""

import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Gaming/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Gaming/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Gaming/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #7b1fa2; padding-bottom: 10px; }
h2 { color: #7b1fa2; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #ce93d8; padding-bottom: 5px; }
.header { background: linear-gradient(135deg, #1a1a2e 0%, #9c27b0 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #e1bee7; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #7b1fa2; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #7b1fa2; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 10pt; }
th { background: #7b1fa2; color: white; padding: 10px; text-align: left; }
td { padding: 8px 10px; border-bottom: 1px solid #e0e0e0; }
tr:nth-child(even) { background: #f8f9fa; }
.highlight { background: #f3e5f5; padding: 15px; border-radius: 5px; margin: 15px 0; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; font-size: 9pt; color: #666; }
"""

def gen(code, title, summary, content):
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: {title}</h1><div class="subtitle">Executive Summary | {datetime.now().strftime('%B %d, %Y')}</div></div>
    {content}
    <div class="footer"><p><strong>Author:</strong> {summary['report_metadata']['author']} | <em>Mboya Jeffers - Data Engineering Portfolio</em></p></div>
    </body></html>"""
    return html

def gam01():
    code, title = "GAM01", "AAA Gaming Publisher Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Franchise Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['franchise_metrics']['top_revenue']}</div><div class="metric-label">Top Franchise Revenue</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['franchise_metrics']['total_mau']}</div><div class="metric-label">Total MAU</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['engagement']['daily_hours']}</div><div class="metric-label">Daily Hours</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['franchise_metrics']['mtx_share']}</div><div class="metric-label">MTX Share</div></div></div>
    </div>
    <h2>Platform Performance</h2>
    <div class="highlight">
    <p><strong>Top Franchise:</strong> {s['franchise_metrics']['top_franchise']}</p>
    <p><strong>Top Platform:</strong> {s['platform_split']['top_platform']}</p>
    <p><strong>Console Share:</strong> {s['platform_split']['console_share']}</p>
    </div>
    <h2>Engagement</h2>
    <p>Total MAU: {s['engagement']['total_mau']} | DAU/MAU: {s['engagement']['dau_mau']} | Mobile Growth: {s['platform_split']['mobile_growth']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Activision Analysis</h2>
    <p>{s['franchise_metrics']['top_franchise']} drives {s['franchise_metrics']['top_revenue']} revenue with {s['franchise_metrics']['mtx_share']} from microtransactions.</p>
    <p>Microsoft acquisition positions Activision for Game Pass integration.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

AAA gaming publisher analysis: {s['report_metadata']['total_rows_processed']:,} data points analyzed.

🎮 Top Franchise: {s['franchise_metrics']['top_franchise']}
💰 Revenue: {s['franchise_metrics']['top_revenue']}
👥 Total MAU: {s['engagement']['total_mau']}
⏱️ Daily Hours: {s['engagement']['daily_hours']}

Call of Duty and Candy Crush drive Activision's {s['franchise_metrics']['mtx_share']} MTX revenue share.

#Gaming #Activision #CallOfDuty #DataEngineering

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def gam02():
    code, title = "GAM02", "Sports Gaming Franchise Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>Franchise Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['franchise_metrics']['top_revenue']}</div><div class="metric-label">Top Franchise Revenue</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['franchise_metrics']['ultimate_team_share']}</div><div class="metric-label">Ultimate Team Share</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['live_services']['daily_transactions']}</div><div class="metric-label">Daily FUT Transactions</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['esports']['monthly_viewers']}</div><div class="metric-label">Esports Viewers</div></div></div>
    </div>
    <h2>Live Services</h2>
    <div class="highlight">
    <p><strong>Top Franchise:</strong> {s['franchise_metrics']['top_franchise']}</p>
    <p><strong>Weekly Players:</strong> {s['franchise_metrics']['total_weekly_players']}</p>
    <p><strong>Conversion Rate:</strong> {s['live_services']['conversion_rate']}</p>
    </div>
    <h2>Esports</h2>
    <p>Watch Hours: {s['esports']['watch_hours']} | Prize Pools: {s['esports']['prize_pools']} | Avg Spend: {s['live_services']['avg_spend']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>EA Sports Analysis</h2>
    <p>{s['franchise_metrics']['top_franchise']} generates {s['franchise_metrics']['top_revenue']} with {s['franchise_metrics']['ultimate_team_share']} from Ultimate Team.</p>
    <p>FUT drives {s['live_services']['daily_transactions']} daily transactions at {s['live_services']['conversion_rate']} conversion.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

Sports gaming analysis: {s['report_metadata']['total_rows_processed']:,} records analyzed.

⚽ Top Franchise: {s['franchise_metrics']['top_franchise']}
💰 Revenue: {s['franchise_metrics']['top_revenue']}
🎮 Weekly Players: {s['franchise_metrics']['total_weekly_players']}
📺 Esports: {s['esports']['monthly_viewers']}

Ultimate Team drives {s['franchise_metrics']['ultimate_team_share']} of EA Sports revenue.

#EA #FIFA #UltimateTeam #DataEngineering

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def gam03():
    code, title = "GAM03", "Open World Gaming Economics"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>GTA Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['gta_metrics']['total_units']}</div><div class="metric-label">Total Units Sold</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['gta_metrics']['online_revenue']}</div><div class="metric-label">GTA Online Revenue</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['gta_metrics']['active_players']}</div><div class="metric-label">Active Players</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['2k_metrics']['nba_dau']}</div><div class="metric-label">NBA 2K DAU</div></div></div>
    </div>
    <h2>Franchise Value</h2>
    <div class="highlight">
    <p><strong>GTA V Units:</strong> {s['gta_metrics']['total_units']}</p>
    <p><strong>Shark Cards:</strong> {s['gta_metrics']['shark_cards']}</p>
    <p><strong>RDR2 Units:</strong> {s['rdr_metrics']['cumulative_units']}</p>
    </div>
    <h2>2K Sports</h2>
    <p>NBA 2K DAU: {s['2k_metrics']['nba_dau']} | VC Revenue: {s['2k_metrics']['vc_revenue']} | MyTeam: {s['2k_metrics']['myteam_engagement']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Take-Two Analysis</h2>
    <p>GTA V is the best-selling game of all time with {s['gta_metrics']['total_units']} units and {s['gta_metrics']['online_revenue']} quarterly online revenue.</p>
    <p>NBA 2K drives {s['2k_metrics']['vc_revenue']} average daily VC revenue.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

Open world gaming economics: {s['report_metadata']['total_rows_processed']:,} data points analyzed.

🎮 GTA V Units: {s['gta_metrics']['total_units']}
💰 Online Rev: {s['gta_metrics']['online_revenue']}
🤠 RDR2 Units: {s['rdr_metrics']['cumulative_units']}
🏀 NBA 2K DAU: {s['2k_metrics']['nba_dau']}

GTA V remains the most successful entertainment product ever released.

#TakeTwo #GTA #NBA2K #DataEngineering

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def gam04():
    code, title = "GAM04", "UGC Gaming Platform Economics"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)

    content = f"""
    <h2>User Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['dau']}</div><div class="metric-label">Daily Active Users</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['bookings']}</div><div class="metric-label">Bookings</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['user_metrics']['hours_engaged']}</div><div class="metric-label">Hours Engaged</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['creator_economy']['monthly_payouts']}</div><div class="metric-label">Creator Payouts</div></div></div>
    </div>
    <h2>Creator Economy</h2>
    <div class="highlight">
    <p><strong>Active Developers:</strong> {s['creator_economy']['active_developers']}</p>
    <p><strong>Experiences:</strong> {s['creator_economy']['experiences']}</p>
    <p><strong>Monthly Payouts:</strong> {s['creator_economy']['monthly_payouts']}</p>
    </div>
    <h2>Engagement</h2>
    <p>Concurrent Peak: {s['engagement']['concurrent_peak']} | Daily Games: {s['engagement']['daily_games']} | Social: {s['engagement']['social_focus']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data Pipeline</h2><p>Rows Processed: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Roblox Analysis</h2>
    <p>Roblox has {s['user_metrics']['dau']} DAU generating {s['user_metrics']['bookings']} in bookings.</p>
    <p>Creator economy pays {s['creator_economy']['monthly_payouts']} monthly to {s['creator_economy']['active_developers']} developers.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post
## {title}

---

UGC gaming platform analysis: {s['report_metadata']['total_rows_processed']:,} records processed.

👥 DAU: {s['user_metrics']['dau']}
💰 Bookings: {s['user_metrics']['bookings']}
👨‍💻 Developers: {s['creator_economy']['active_developers']}
💸 Creator Payouts: {s['creator_economy']['monthly_payouts']}

Roblox's creator economy pays {s['creator_economy']['monthly_payouts']} monthly to developers.

#Roblox #UGC #CreatorEconomy #DataEngineering

---
*Generated by Mboya Jeffers - Data Engineering Portfolio*
"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def main():
    print("=" * 60)
    print("Gaming Vertical: Generating All Reports")
    print("=" * 60)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)

    print("\nGenerating reports...")
    gam01()
    gam02()
    gam03()
    gam04()

    print("\n" + "=" * 60)
    print("All Gaming reports generated successfully!")
    print("  - 8 PDFs (4 Executive + 4 Technical)")
    print("  - 4 LinkedIn Posts")
    print("=" * 60)

if __name__ == "__main__":
    main()
