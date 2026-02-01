#!/usr/bin/env python3
"""
Weather Vertical: Generate All Reports
Author: Mboya Jeffers
"""

import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Weather/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Weather/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Weather/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #00bcd4; padding-bottom: 10px; }
h2 { color: #00acc1; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #80deea; padding-bottom: 5px; }
.header { background: linear-gradient(135deg, #01579b 0%, #00bcd4 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #b2ebf2; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #00bcd4; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #00acc1; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
.highlight { background: #e0f7fa; padding: 15px; border-radius: 5px; margin: 15px 0; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; font-size: 9pt; color: #666; }
"""

def gen(code, title, summary, content):
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: {title}</h1><div class="subtitle">Executive Summary | {datetime.now().strftime('%B %d, %Y')}</div></div>
    {content}
    <div class="footer"><p><strong>Author:</strong> {summary['report_metadata']['author']} | <em>Mboya Jeffers - Data Engineering Portfolio</em></p></div>
    </body></html>"""
    return html

def wth01():
    code, title = "WTH01", "Enterprise Weather Data Platform"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Platform</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['platform']['daily_api_calls']}</div><div class="metric-label">Daily API Calls</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['platform']['enterprise_customers']}</div><div class="metric-label">Enterprise Customers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['platform']['uptime']}</div><div class="metric-label">Uptime</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['products']['avg_accuracy']}</div><div class="metric-label">Accuracy</div></div></div>
    </div>
    <h2>Products</h2><div class="highlight"><p><strong>Top:</strong> {s['products']['top_product']} | <strong>Verticals:</strong> {s['verticals']['top_vertical']}</p></div>
    <h2>Usage</h2><p>Daily Requests: {s['verticals']['daily_requests']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>IBM Weather</h2><p>IBM Weather handles {s['platform']['daily_api_calls']} daily API calls at {s['platform']['uptime']} uptime.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nIBM Weather analysis: {s['report_metadata']['total_rows_processed']:,} data points.\n\n🌤️ API Calls: {s['platform']['daily_api_calls']}\n🏢 Customers: {s['platform']['enterprise_customers']}\n⏱️ Uptime: {s['platform']['uptime']}\n🎯 Accuracy: {s['products']['avg_accuracy']}\n\n#IBMWeather #WeatherData #Enterprise #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def wth02():
    code, title = "WTH02", "Consumer Weather Platform Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Audience</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['audience']['monthly_users']}</div><div class="metric-label">Monthly Users</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['audience']['dau_mau']}</div><div class="metric-label">DAU/MAU</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['advertising']['monthly_revenue']}</div><div class="metric-label">Ad Revenue</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['products']['accuracy']}</div><div class="metric-label">Accuracy</div></div></div>
    </div>
    <h2>Advertising</h2><div class="highlight"><p><strong>CPM:</strong> {s['advertising']['avg_cpm']} | <strong>Fill Rate:</strong> {s['advertising']['fill_rate']}</p></div>
    <h2>Products</h2><p>MinuteCast: {s['products']['minutecast']} | RealFeel: {s['products']['realfeel']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>AccuWeather</h2><p>AccuWeather reaches {s['audience']['monthly_users']} monthly users with {s['products']['accuracy']} accuracy.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nAccuWeather analysis: {s['report_metadata']['total_rows_processed']:,} records.\n\n👥 Users: {s['audience']['monthly_users']}\n📊 DAU/MAU: {s['audience']['dau_mau']}\n💰 Revenue: {s['advertising']['monthly_revenue']}\n🎯 Accuracy: {s['products']['accuracy']}\n\n#AccuWeather #Consumer #Weather #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def wth03():
    code, title = "WTH03", "Agricultural Weather Intelligence"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Platform</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['platform']['subscribers']}</div><div class="metric-label">Subscribers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['platform']['acres']}</div><div class="metric-label">Acres Covered</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['platform']['arr']}</div><div class="metric-label">ARR</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['accuracy']['frost_alert']}</div><div class="metric-label">Frost Alert Accuracy</div></div></div>
    </div>
    <h2>Accuracy</h2><div class="highlight"><p><strong>Spray Window:</strong> {s['accuracy']['spray_window']} | <strong>Top Crop:</strong> {s['crops']['top_crop']}</p></div>
    <h2>Alerts</h2><p>Daily Alerts: {s['crops']['daily_alerts']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>DTN</h2><p>DTN serves {s['platform']['subscribers']} subscribers covering {s['platform']['acres']} acres.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nDTN agriculture analysis: {s['report_metadata']['total_rows_processed']:,} data points.\n\n🌾 Subscribers: {s['platform']['subscribers']}\n🏡 Acres: {s['platform']['acres']}\n💰 ARR: {s['platform']['arr']}\n❄️ Frost Alert: {s['accuracy']['frost_alert']}\n\n#DTN #Agriculture #Weather #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def wth04():
    code, title = "WTH04", "AI Weather Prediction Platform"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>AI Platform</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['ai_platform']['accuracy']}</div><div class="metric-label">AI Accuracy</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['ai_platform']['horizon']}</div><div class="metric-label">Prediction Horizon</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['ai_platform']['arr']}</div><div class="metric-label">ARR</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['radar_network']['active_radars']}</div><div class="metric-label">Active Radars</div></div></div>
    </div>
    <h2>Radar Network</h2><div class="highlight"><p><strong>Daily Data:</strong> {s['radar_network']['daily_data']} | <strong>Verticals:</strong> {s['enterprise']['total_verticals']}</p></div>
    <h2>Enterprise</h2><p>Aviation: {s['enterprise']['aviation']} | Logistics: {s['enterprise']['logistics']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Tomorrow.io</h2><p>Tomorrow.io achieves {s['ai_platform']['accuracy']} AI accuracy with {s['ai_platform']['horizon']} prediction horizon.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nTomorrow.io AI weather: {s['report_metadata']['total_rows_processed']:,} records.\n\n🤖 Accuracy: {s['ai_platform']['accuracy']}\n⏱️ Horizon: {s['ai_platform']['horizon']}\n📡 Radars: {s['radar_network']['active_radars']}\n💰 ARR: {s['ai_platform']['arr']}\n\n#TomorrowIO #AIWeather #ML #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def main():
    print("=" * 60)
    print("Weather Vertical: Generating All Reports")
    print("=" * 60)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)
    print("\nGenerating reports...")
    wth01()
    wth02()
    wth03()
    wth04()
    print("\n" + "=" * 60)
    print("All Weather reports generated!")
    print("=" * 60)

if __name__ == "__main__": main()
