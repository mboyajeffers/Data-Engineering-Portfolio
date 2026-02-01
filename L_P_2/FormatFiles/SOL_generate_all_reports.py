#!/usr/bin/env python3
"""
Solar Vertical: Generate All Reports
Author: Mboya Jeffers
"""

import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Solar/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Solar/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Solar/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #ffc107; padding-bottom: 10px; }
h2 { color: #ff9800; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #ffe082; padding-bottom: 5px; }
.header { background: linear-gradient(135deg, #1a237e 0%, #ffc107 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #fff59d; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #ffc107; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #ff9800; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
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

def sol01():
    code, title = "SOL01", "Utility-Scale Solar Manufacturing"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Manufacturing</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['manufacturing']['capacity']}</div><div class="metric-label">Capacity</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['manufacturing']['efficiency']}</div><div class="metric-label">Efficiency</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['bookings']['backlog']}</div><div class="metric-label">Backlog</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['bookings']['us_share']}</div><div class="metric-label">US Share</div></div></div>
    </div>
    <h2>Bookings</h2><div class="highlight"><p><strong>Pipeline:</strong> {s['bookings']['pipeline']} | <strong>ASP:</strong> {s['manufacturing']['asp']}</p></div>
    <h2>IRA Impact</h2><p>US Content: {s['ira_impact']['us_content']} | Benefit: {s['ira_impact']['benefit']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>First Solar</h2><p>FSLR has {s['manufacturing']['capacity']} manufacturing capacity with {s['bookings']['backlog']} backlog.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nFirst Solar analysis: {s['report_metadata']['total_rows_processed']:,} data points.\n\n☀️ Capacity: {s['manufacturing']['capacity']}\n📊 Efficiency: {s['manufacturing']['efficiency']}\n📈 Backlog: {s['bookings']['backlog']}\n🇺🇸 US Share: {s['bookings']['us_share']}\n\n#FirstSolar #Solar #CleanEnergy #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def sol02():
    code, title = "SOL02", "Microinverter Market Dynamics"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Inverters</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['inverter_metrics']['shipped']}</div><div class="metric-label">Shipped</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['inverter_metrics']['revenue']}</div><div class="metric-label">Revenue</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['inverter_metrics']['gross_margin']}</div><div class="metric-label">Gross Margin</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['storage']['attach_rate']}</div><div class="metric-label">Storage Attach</div></div></div>
    </div>
    <h2>Install Base</h2><div class="highlight"><p><strong>Daily Systems:</strong> {s['install_base']['daily_systems']} | <strong>Avg Size:</strong> {s['install_base']['avg_size']}</p></div>
    <h2>Storage</h2><p>IQ Battery: {s['storage']['iq_battery']} | NPS: {s['install_base']['nps']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Enphase</h2><p>ENPH ships {s['inverter_metrics']['shipped']} microinverters with {s['inverter_metrics']['gross_margin']} margins.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nEnphase analysis: {s['report_metadata']['total_rows_processed']:,} records.\n\n⚡ Shipped: {s['inverter_metrics']['shipped']}\n💰 Revenue: {s['inverter_metrics']['revenue']}\n📊 Margin: {s['inverter_metrics']['gross_margin']}\n🔋 Storage: {s['storage']['attach_rate']}\n\n#Enphase #Solar #Microinverter #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def sol03():
    code, title = "SOL03", "Renewable Utility Economics"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Generation</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['generation']['renewable']}</div><div class="metric-label">Renewable</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['generation']['solar']}</div><div class="metric-label">Solar</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['generation']['wind']}</div><div class="metric-label">Wind</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['development']['backlog']}</div><div class="metric-label">Backlog</div></div></div>
    </div>
    <h2>Development</h2><div class="highlight"><p><strong>Pipeline:</strong> {s['development']['pipeline']} | <strong>PPA Price:</strong> {s['development']['avg_ppa_price']}</p></div>
    <h2>Operations</h2><p>Solar CF: {s['operations']['solar_cf']} | Wind CF: {s['operations']['wind_cf']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>NextEra</h2><p>NEE has {s['generation']['renewable']} renewable capacity with {s['development']['backlog']} backlog.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nNextEra analysis: {s['report_metadata']['total_rows_processed']:,} data points.\n\n🌱 Renewable: {s['generation']['renewable']}\n☀️ Solar: {s['generation']['solar']}\n💨 Wind: {s['generation']['wind']}\n📈 Pipeline: {s['development']['pipeline']}\n\n#NextEra #Renewables #CleanEnergy #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def sol04():
    code, title = "SOL04", "Residential Solar Market Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Customers</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['customers']['total']}</div><div class="metric-label">Total</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['customers']['nsv']}</div><div class="metric-label">Net Subscriber Value</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['installations']['monthly_solar']}</div><div class="metric-label">Monthly MW</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['installations']['storage_attach']}</div><div class="metric-label">Storage Attach</div></div></div>
    </div>
    <h2>Economics</h2><div class="highlight"><p><strong>MRR:</strong> {s['economics']['mrr']} | <strong>Renewal:</strong> {s['economics']['renewal_rate']}</p></div>
    <h2>Contracts</h2><p>Contract Length: {s['customers']['contract_length']} | Avg Size: {s['installations']['avg_size']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Sunrun</h2><p>RUN has {s['customers']['total']} customers with {s['customers']['nsv']} NSV.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nSunrun analysis: {s['report_metadata']['total_rows_processed']:,} records.\n\n👥 Customers: {s['customers']['total']}\n💰 NSV: {s['customers']['nsv']}\n☀️ Monthly: {s['installations']['monthly_solar']}\n🔋 Storage: {s['installations']['storage_attach']}\n\n#Sunrun #ResidentialSolar #CleanEnergy #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def main():
    print("=" * 60)
    print("Solar Vertical: Generating All Reports")
    print("=" * 60)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)
    print("\nGenerating reports...")
    sol01()
    sol02()
    sol03()
    sol04()
    print("\n" + "=" * 60)
    print("All Solar reports generated!")
    print("=" * 60)

if __name__ == "__main__": main()
