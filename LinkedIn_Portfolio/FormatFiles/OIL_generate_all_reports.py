#!/usr/bin/env python3
"""
Oil & Gas Vertical: Generate All Reports
Author: Mboya Jeffers
"""

import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../OilGas/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../OilGas/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../OilGas/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #1565c0; padding-bottom: 10px; }
h2 { color: #1565c0; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #64b5f6; padding-bottom: 5px; }
.header { background: linear-gradient(135deg, #0d47a1 0%, #1976d2 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #bbdefb; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #1565c0; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #1565c0; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
.highlight { background: #e3f2fd; padding: 15px; border-radius: 5px; margin: 15px 0; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; font-size: 9pt; color: #666; }
"""

def gen(code, title, summary, content):
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: {title}</h1><div class="subtitle">Executive Summary | {datetime.now().strftime('%B %d, %Y')}</div></div>
    {content}
    <div class="footer"><p><strong>Author:</strong> {summary['report_metadata']['author']} | <em>Mboya Jeffers - Data Engineering Portfolio</em></p></div>
    </body></html>"""
    return html

def oil01():
    code, title = "OIL01", "Integrated Oil Major Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Production</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['production']['total_boe']}</div><div class="metric-label">Total Production</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['financials']['revenue']}</div><div class="metric-label">Revenue</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['financials']['net_income']}</div><div class="metric-label">Net Income</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['production']['guyana']}</div><div class="metric-label">Guyana</div></div></div>
    </div>
    <h2>Operations</h2>
    <div class="highlight"><p><strong>Permian:</strong> {s['production']['permian_share']} | <strong>Reserves:</strong> {s['reserves']['proved']}</p></div>
    <h2>Economics</h2><p>Breakeven: {s['reserves']['breakeven']} | CAPEX: {s['financials']['capex']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>ExxonMobil</h2><p>XOM produces {s['production']['total_boe']} with Guyana driving growth at {s['production']['guyana']}.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nExxonMobil analysis: {s['report_metadata']['total_rows_processed']:,} data points.\n\n⛽ Production: {s['production']['total_boe']}\n💰 Revenue: {s['financials']['revenue']}\n🏝️ Guyana: {s['production']['guyana']}\n📊 Breakeven: {s['reserves']['breakeven']}\n\n#ExxonMobil #OilGas #Energy #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def oil02():
    code, title = "OIL02", "Downstream Refining Economics"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Refining</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['refining']['capacity']}</div><div class="metric-label">Capacity</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['refining']['utilization']}</div><div class="metric-label">Utilization</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['refining']['crack_spread']}</div><div class="metric-label">Crack Spread</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['retail']['stations']}</div><div class="metric-label">Stations</div></div></div>
    </div>
    <h2>Products</h2><div class="highlight"><p><strong>Top:</strong> {s['products']['top_product']} | <strong>Gasoline:</strong> {s['products']['gasoline_share']}</p></div>
    <h2>Retail</h2><p>Daily Gallons: {s['retail']['daily_gallons']} | EV: {s['retail']['ev_transition']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Chevron Downstream</h2><p>Chevron operates {s['refining']['capacity']} refining at {s['refining']['utilization']} utilization.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nChevron refining: {s['report_metadata']['total_rows_processed']:,} records.\n\n🏭 Capacity: {s['refining']['capacity']}\n📊 Utilization: {s['refining']['utilization']}\n💰 Crack Spread: {s['refining']['crack_spread']}\n⛽ Stations: {s['retail']['stations']}\n\n#Chevron #Refining #OilGas #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def oil03():
    code, title = "OIL03", "E&P Pure-Play Analysis"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Production</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['production']['total']}</div><div class="metric-label">Total Production</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['well_economics']['monthly_wells']}</div><div class="metric-label">Monthly Wells</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['well_economics']['avg_cost']}</div><div class="metric-label">Avg Well Cost</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['returns']['fcf_yield']}</div><div class="metric-label">FCF Yield</div></div></div>
    </div>
    <h2>Basins</h2><div class="highlight"><p><strong>Top:</strong> {s['production']['top_basin']} | <strong>Permian:</strong> {s['production']['permian_share']}</p></div>
    <h2>Returns</h2><p>IP30: {s['well_economics']['avg_ip30']} | Shareholder Returns: {s['returns']['shareholder_returns']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>ConocoPhillips</h2><p>COP produces {s['production']['total']} focused on {s['production']['top_basin']}.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nConocoPhillips E&P: {s['report_metadata']['total_rows_processed']:,} data points.\n\n⛽ Production: {s['production']['total']}\n🏔️ Top Basin: {s['production']['top_basin']}\n💰 FCF Yield: {s['returns']['fcf_yield']}\n🛢️ Well Cost: {s['well_economics']['avg_cost']}\n\n#ConocoPhillips #EP #OilGas #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def oil04():
    code, title = "OIL04", "Energy Transition Strategy"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Transition</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['transition']['renewables_capacity']}</div><div class="metric-label">Renewables</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['transition']['ev_chargers']}</div><div class="metric-label">EV Chargers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['transition']['carbon_reduction']}</div><div class="metric-label">Carbon Reduction</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['segments']['lng_position']}</div><div class="metric-label">LNG Capacity</div></div></div>
    </div>
    <h2>Segments</h2><div class="highlight"><p><strong>Top:</strong> {s['segments']['top_segment']} | <strong>Low-Carbon CAPEX:</strong> {s['transition']['low_carbon_capex']}</p></div>
    <h2>LNG Trading</h2><p>Daily Cargoes: {s['lng_trading']['daily_cargoes']} | Margin: {s['lng_trading']['avg_margin']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body><div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Shell Transition</h2><p>Shell has {s['transition']['renewables_capacity']} renewables and {s['transition']['ev_chargers']} EV chargers.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nShell energy transition: {s['report_metadata']['total_rows_processed']:,} records.\n\n🌱 Renewables: {s['transition']['renewables_capacity']}\n⚡ EV Chargers: {s['transition']['ev_chargers']}\n📉 Carbon: {s['transition']['carbon_reduction']}\n🚢 LNG: {s['segments']['lng_position']}\n\n#Shell #EnergyTransition #LNG #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def main():
    print("=" * 60)
    print("Oil & Gas Vertical: Generating All Reports")
    print("=" * 60)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)
    print("\nGenerating reports...")
    oil01()
    oil02()
    oil03()
    oil04()
    print("\n" + "=" * 60)
    print("All Oil & Gas reports generated!")
    print("=" * 60)

if __name__ == "__main__": main()
