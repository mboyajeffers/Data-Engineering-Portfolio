#!/usr/bin/env python3
"""
Ecommerce Vertical: Generate All Reports
Author: Mboya Jeffers
"""

import json
import os
from datetime import datetime
from weasyprint import HTML, CSS

DATA_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Ecommerce/data"
REPORT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Ecommerce/reports"
POST_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Ecommerce/posts"

CSS_STYLE = """
@page { margin: 0.75in; size: letter; }
body { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { color: #0d1b2a; font-size: 24pt; border-bottom: 3px solid #ff9900; padding-bottom: 10px; }
h2 { color: #ff9900; font-size: 16pt; margin-top: 25px; border-bottom: 1px solid #ffcc80; padding-bottom: 5px; }
.header { background: linear-gradient(135deg, #232f3e 0%, #ff9900 100%); color: white; padding: 30px; margin: -0.75in -0.75in 30px -0.75in; }
.header h1 { color: white; border-bottom: none; margin: 0; }
.header .subtitle { color: #ffe0b2; font-size: 14pt; margin-top: 10px; }
.metric-box { background: #f8f9fa; border-left: 4px solid #ff9900; padding: 15px; margin: 15px 0; }
.metric-value { font-size: 28pt; font-weight: bold; color: #ff9900; }
.metric-label { font-size: 10pt; color: #666; text-transform: uppercase; }
.grid { display: flex; flex-wrap: wrap; gap: 20px; }
.grid-item { flex: 1; min-width: 200px; }
table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 10pt; }
th { background: #ff9900; color: white; padding: 10px; text-align: left; }
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

def ecm01():
    code, title = "ECM01", "E-commerce Marketplace Analytics"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>GMV Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['gmv_metrics']['total_gmv']}</div><div class="metric-label">Total GMV</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['gmv_metrics']['active_customers']}</div><div class="metric-label">Active Customers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['gmv_metrics']['prime_members']}</div><div class="metric-label">Prime Members</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['fulfillment']['daily_packages']}</div><div class="metric-label">Daily Packages</div></div></div>
    </div>
    <h2>Marketplace</h2>
    <div class="highlight"><p><strong>3P Share:</strong> {s['gmv_metrics']['3p_share']} | <strong>Top Category:</strong> {s['category_performance']['top_category']}</p></div>
    <h2>Fulfillment</h2>
    <p>Same Day: {s['fulfillment']['same_day']} | One Day: {s['fulfillment']['one_day']} | AOV: {s['category_performance']['avg_aov']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Amazon Dominance</h2><p>Amazon commands {s['gmv_metrics']['total_gmv']} GMV with {s['gmv_metrics']['prime_members']} Prime members.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nAmazon marketplace: {s['report_metadata']['total_rows_processed']:,} data points.\n\n📦 GMV: {s['gmv_metrics']['total_gmv']}\n👥 Customers: {s['gmv_metrics']['active_customers']}\n⭐ Prime: {s['gmv_metrics']['prime_members']}\n🚚 Packages: {s['fulfillment']['daily_packages']}\n\n#Amazon #Ecommerce #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def ecm02():
    code, title = "ECM02", "E-commerce Platform Economics"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Platform Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['platform_metrics']['merchants']}</div><div class="metric-label">Merchants</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['platform_metrics']['gmv']}</div><div class="metric-label">GMV</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['platform_metrics']['take_rate']}</div><div class="metric-label">Take Rate</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['checkout']['conversion']}</div><div class="metric-label">Conversion</div></div></div>
    </div>
    <h2>Products</h2>
    <div class="highlight"><p><strong>Top Product:</strong> {s['product_mix']['top_product']} | <strong>Shop Pay:</strong> {s['product_mix']['shop_pay_growth']}</p></div>
    <h2>Checkout</h2>
    <p>Daily Sessions: {s['checkout']['daily_sessions']} | Mobile: {s['checkout']['mobile_share']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Shopify Platform</h2><p>Shopify powers {s['platform_metrics']['merchants']} merchants with {s['platform_metrics']['take_rate']} take rate.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nShopify platform: {s['report_metadata']['total_rows_processed']:,} records.\n\n🏪 Merchants: {s['platform_metrics']['merchants']}\n💰 GMV: {s['platform_metrics']['gmv']}\n📊 Take Rate: {s['platform_metrics']['take_rate']}\n📱 Mobile: {s['checkout']['mobile_share']}\n\n#Shopify #Ecommerce #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def ecm03():
    code, title = "ECM03", "Handmade Marketplace Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Marketplace Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['marketplace_metrics']['gms']}</div><div class="metric-label">GMS</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['marketplace_metrics']['active_buyers']}</div><div class="metric-label">Active Buyers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['marketplace_metrics']['active_sellers']}</div><div class="metric-label">Active Sellers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['marketplace_metrics']['take_rate']}</div><div class="metric-label">Take Rate</div></div></div>
    </div>
    <h2>Categories</h2>
    <div class="highlight"><p><strong>Top:</strong> {s['category_performance']['top_category']} | <strong>Avg Price:</strong> {s['category_performance']['avg_item_price']}</p></div>
    <h2>Sellers</h2>
    <p>Star Sellers: {s['seller_economics']['star_sellers']} | Repeat Buyers: {s['seller_economics']['repeat_buyers']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Etsy Marketplace</h2><p>Etsy's {s['marketplace_metrics']['active_sellers']} sellers drive {s['marketplace_metrics']['gms']} GMS.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nEtsy marketplace: {s['report_metadata']['total_rows_processed']:,} data points.\n\n🛍️ GMS: {s['marketplace_metrics']['gms']}\n👥 Buyers: {s['marketplace_metrics']['active_buyers']}\n🎨 Sellers: {s['marketplace_metrics']['active_sellers']}\n💰 Take Rate: {s['marketplace_metrics']['take_rate']}\n\n#Etsy #Handmade #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def ecm04():
    code, title = "ECM04", "Home Furnishings E-commerce Study"
    with open(f"{DATA_DIR}/{code}_summary.json") as f: s = json.load(f)
    content = f"""
    <h2>Revenue Metrics</h2>
    <div class="grid">
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['revenue_metrics']['net_revenue']}</div><div class="metric-label">Net Revenue</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['revenue_metrics']['active_customers']}</div><div class="metric-label">Active Customers</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['revenue_metrics']['ltm_rev_per_cust']}</div><div class="metric-label">LTM Rev/Customer</div></div></div>
        <div class="grid-item"><div class="metric-box"><div class="metric-value">{s['logistics']['two_day']}</div><div class="metric-label">2-Day Delivery</div></div></div>
    </div>
    <h2>Categories</h2>
    <div class="highlight"><p><strong>Top:</strong> {s['category_performance']['top_category']} | <strong>AOV:</strong> {s['category_performance']['avg_aov']}</p></div>
    <h2>Logistics</h2>
    <p>Daily Orders: {s['logistics']['daily_orders']} | CastleGate: {s['logistics']['castlegate']}</p>
    """
    HTML(string=gen(code, title, s, content)).write_pdf(f"{REPORT_DIR}/{code}_Executive_Summary_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    tech = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body>
    <div class="header"><h1>{code}: Technical Analysis</h1></div>
    <h2>Data</h2><p>Rows: {s['report_metadata']['total_rows_processed']:,}</p>
    <h2>Wayfair</h2><p>Wayfair generates {s['revenue_metrics']['net_revenue']} with {s['logistics']['two_day']} 2-day delivery.</p>
    <div class="footer"><p>{s['report_metadata']['author']}</p></div></body></html>"""
    HTML(string=tech).write_pdf(f"{REPORT_DIR}/{code}_Technical_Analysis_v1.0.pdf", stylesheets=[CSS(string=CSS_STYLE)])
    post = f"""# {code} LinkedIn Post\n## {title}\n\n---\n\nWayfair home furnishings: {s['report_metadata']['total_rows_processed']:,} records.\n\n🏠 Revenue: {s['revenue_metrics']['net_revenue']}\n👥 Customers: {s['revenue_metrics']['active_customers']}\n📦 Orders: {s['logistics']['daily_orders']}\n🚚 2-Day: {s['logistics']['two_day']}\n\n#Wayfair #HomeFurnishings #DataEngineering\n\n---"""
    with open(f"{POST_DIR}/{code}_LinkedIn_Post_v1.0.md", 'w') as f: f.write(post)
    print(f"  {code}: Complete")

def main():
    print("=" * 60)
    print("Ecommerce Vertical: Generating All Reports")
    print("=" * 60)
    os.makedirs(REPORT_DIR, exist_ok=True)
    os.makedirs(POST_DIR, exist_ok=True)
    print("\nGenerating reports...")
    ecm01()
    ecm02()
    ecm03()
    ecm04()
    print("\n" + "=" * 60)
    print("All Ecommerce reports generated!")
    print("=" * 60)

if __name__ == "__main__": main()
