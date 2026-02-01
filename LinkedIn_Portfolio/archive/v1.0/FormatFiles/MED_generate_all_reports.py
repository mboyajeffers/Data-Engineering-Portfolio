#!/usr/bin/env python3
"""
Media Vertical: Generate All Reports
Author: Mboya Jeffers
"""

import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Media/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Media/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Media/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #e50914; padding-bottom: 10px; }
h2 { color: #e50914; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #f5c6cb; padding-bottom: 5px; }
.header { background: linear-gradient(135deg, #141414 0%, #e50914 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #f5c6cb; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #e50914; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #e50914; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 10pt; }
th { background: #e50914; color: white; padding: 10px; text-align: left; }
td { padding: 8px 10px; border-bottom: 1px solid #e0e0e0; }
tr:nth-child(even) { background: #f8f9fa; }
.highlight { background: #fce4ec; padding: 15px; border-radius: 5px; margin: 15px 0; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; font-size: 9pt; color: #666; }
"""

def generate_report(code, title, summary, extra_content=""):
    html = f"""
    <!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header">
        <h1>{code}: {title}</h1>
        <div class="subtitle">Executive Summary | {datetime.now().strftime('%B %d, %Y')}</div>
    </div>
    {extra_content}
    <div class="footer">
        <p><strong>Author:</strong> {summary['report_metadata']['author']} | <em>Mboya Jeffers - Data Engineering Portfolio</em></p>
    </div>
    </body></html>
    """
    return html

def generate_med01():
    code, title = "MED01", "Streaming Leader Content Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f:
        s = json.load(f)

    content = f"""
    <h2>Subscriber Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['subscriber_metrics']['leader_subs']}</div><div class="metric-label">Leader Subscribers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['subscriber_metrics']['total_market_subs']}</div><div class="metric-label">Total Market</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['subscriber_metrics']['avg_arpu']}</div><div class="metric-label">Average ARPU</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['subscriber_metrics']['avg_churn']}</div><div class="metric-label">Average Churn</div></div></div>
    </div>
    <h2>Content Performance</h2>
    <div class="highlight">
        <ul>
            <li><strong>Titles Analyzed:</strong> {s['content_metrics']['total_titles_analyzed']}</li>
            <li><strong>Avg Budget:</strong> {s['content_metrics']['avg_budget']}</li>
            <li><strong>Avg Hours Viewed:</strong> {s['content_metrics']['avg_hours_viewed']}</li>
            <li><strong>Top Genre:</strong> {s['content_metrics']['top_genre']}</li>
        </ul>
    </div>
    <h2>Engagement</h2>
    <p>Weekly hours per user: {s['engagement_insights']['avg_weekly_hours']} | Mobile: {s['engagement_insights']['mobile_viewing_share']} | TV: {s['engagement_insights']['tv_viewing_share']}</p>
    """

    HTML(string=generate_report(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""
    <!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1><div class="subtitle">Content Analytics Methodology</div></div>
    <h2>Data Overview</h2>
    <p>Total rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Methodology</h2>
    <p>Subscriber data modeled with regional breakdowns, seasonal adjustments, and growth trajectories.</p>
    <p>Content performance metrics include budget, viewership, completion rates, and ratings.</p>
    <div class="footer"><p><strong>Author:</strong> {s['report_metadata']['author']}</p></div>
    </body></html>
    """
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nStreaming industry analysis: {s['report_metadata']['total_rows_processed']:,} data points.\n\n📊 Leader Subs: {s['subscriber_metrics']['leader_subs']}\n📈 Total Market: {s['subscriber_metrics']['total_market_subs']}\n💰 Avg ARPU: {s['subscriber_metrics']['avg_arpu']}\n🎬 Titles Analyzed: {s['content_metrics']['total_titles_analyzed']}\n\nTop genre: {s['content_metrics']['top_genre']}\n\n#DataEngineering #Streaming #Netflix #Media #Entertainment\n\n---\n- Report: {code}"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f:
        f.write(post)
    print(f"  {code}: Complete")

def generate_med02():
    code, title = "MED02", "Entertainment Conglomerate Streaming Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f:
        s = json.load(f)

    content = f"""
    <h2>Bundle Economics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['bundle_metrics']['total_subs']}</div><div class="metric-label">Total Subscribers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['bundle_metrics']['avg_arpu']}</div><div class="metric-label">Average ARPU</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['bundle_metrics']['bundle_penetration']}</div><div class="metric-label">Bundle Penetration</div></div></div>
    </div>
    <h2>Franchise Performance</h2>
    <p>Top franchise: {s['franchise_performance']['top_franchise']} | Total content hours: {s['franchise_performance']['total_content_hours']}</p>
    <h2>Parks/Streaming Synergy</h2>
    <p>Cross-promo lift: {s['synergy_insights']['avg_cross_promo_lift']}</p>
    """
    HTML(string=generate_report(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Bundle Strategy</h2><p>Disney Bundle combines Disney+, Hulu, ESPN+ to maximize ARPU and reduce churn.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nDisney streaming analysis: {s['report_metadata']['total_rows_processed']:,} records.\n\n📊 Subs: {s['bundle_metrics']['total_subs']}\n💰 ARPU: {s['bundle_metrics']['avg_arpu']}\n📦 Bundle Penetration: {s['bundle_metrics']['bundle_penetration']}\n🎬 Top Franchise: {s['franchise_performance']['top_franchise']}\n\n#DataEngineering #Disney #Streaming #Entertainment\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f:
        f.write(post)
    print(f"  {code}: Complete")

def generate_med03():
    code, title = "MED03", "Audio Streaming Market Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f:
        s = json.load(f)

    content = f"""
    <h2>Listener Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['listener_metrics']['leader_mau']}</div><div class="metric-label">Leader MAU</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['listener_metrics']['total_market_mau']}</div><div class="metric-label">Total Market</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['listener_metrics']['avg_premium_conversion']}</div><div class="metric-label">Premium Conversion</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['listener_metrics']['avg_arpu']}</div><div class="metric-label">Avg ARPU</div></div></div>
    </div>
    <h2>Podcast Growth</h2>
    <p>Total listens: {s['podcast_metrics']['total_listens']} | Top category: {s['podcast_metrics']['top_category']} | Avg CPM: {s['podcast_metrics']['avg_ad_cpm']}</p>
    <h2>Artist Economics</h2>
    <p>Payout per 1000 streams: {s['artist_economics']['avg_payout_per_stream']}</p>
    """
    HTML(string=generate_report(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Methodology</h2><p>MAU modeling across platforms and regions. Podcast and artist economics included.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nAudio streaming deep dive: {s['report_metadata']['total_rows_processed']:,} records.\n\n🎵 Leader MAU: {s['listener_metrics']['leader_mau']}\n📈 Total Market: {s['listener_metrics']['total_market_mau']}\n💎 Premium Conversion: {s['listener_metrics']['avg_premium_conversion']}\n🎙️ Podcast Listens: {s['podcast_metrics']['total_listens']}\n\n#DataEngineering #Spotify #AudioStreaming #Podcasts\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f:
        f.write(post)
    print(f"  {code}: Complete")

def generate_med04():
    code, title = "MED04", "Media Consolidation Impact Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f:
        s = json.load(f)

    content = f"""
    <h2>Merger Synergies</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['synergy_metrics']['total_realized']}</div><div class="metric-label">Synergies Realized</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['synergy_metrics']['realization_rate']}</div><div class="metric-label">Realization Rate</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['debt_profile']['total_debt']}</div><div class="metric-label">Total Debt</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['debt_profile']['leverage_ratio']}</div><div class="metric-label">Leverage Ratio</div></div></div>
    </div>
    <h2>Streaming Performance</h2>
    <p>Combined subs: {s['streaming_metrics']['combined_subs']} | ARPU: {s['streaming_metrics']['arpu']} | Churn: {s['streaming_metrics']['churn']}</p>
    <h2>Content Library</h2>
    <p>Library value: {s['content_library']['total_value']} | Annual spend: {s['content_library']['annual_spend']}</p>
    """
    HTML(string=generate_report(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Merger Analysis</h2><p>Cost synergies: {s['synergy_metrics']['cost_synergies']} | FCF: {s['debt_profile']['fcf']}</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])

    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nWBD merger analysis: {s['report_metadata']['total_rows_processed']:,} records.\n\n💰 Synergies: {s['synergy_metrics']['total_realized']}\n📊 Realization: {s['synergy_metrics']['realization_rate']}\n💳 Debt: {s['debt_profile']['total_debt']}\n📺 Combined Subs: {s['streaming_metrics']['combined_subs']}\n\n#DataEngineering #MediaConsolidation #WBD #Streaming\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f:
        f.write(post)
    print(f"  {code}: Complete")

def main():
    print("=" * 60)
    print("Media Vertical: Generating All Reports")
    print("=" * 60)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)

    print("\nGenerating reports...")
    generate_med01()
    generate_med02()
    generate_med03()
    generate_med04()

    print("\n" + "=" * 60)
    print("All Media reports generated successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
