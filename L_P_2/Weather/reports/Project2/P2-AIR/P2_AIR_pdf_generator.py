#!/usr/bin/env python3
"""
P2-AIR: PDF Report Generator
============================
Generates Founder Summary, Executive Summary, and Methodology PDFs

Author: Mboya Jeffers
Version: 1.0.0
Created: 2026-02-01
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any
import pandas as pd
from weasyprint import HTML

PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

# Color scheme - Weather/Operations
PRIMARY_COLOR = "#0284c7"  # Sky blue
SECONDARY_COLOR = "#0f766e"  # Teal
ACCENT_COLOR = "#059669"  # Green
WARNING_COLOR = "#dc2626"  # Red

AUTHOR = "Mboya Jeffers"
EMAIL = "MboyaJeffers9@gmail.com"


def load_pipeline_data() -> Dict[str, Any]:
    data = {}

    metrics_path = DATA_DIR / 'pipeline_metrics.json'
    if metrics_path.exists():
        with open(metrics_path) as f:
            data['metrics'] = json.load(f)

    kpi_path = DATA_DIR / 'kpis.json'
    if kpi_path.exists():
        with open(kpi_path) as f:
            data['kpis'] = json.load(f)

    return data


def format_pct(value: float) -> str:
    return f"{value*100:.1f}%"


def format_number(value: float) -> str:
    if abs(value) >= 1e6:
        return f"{value/1e6:.2f}M"
    elif abs(value) >= 1e3:
        return f"{value/1e3:.1f}K"
    return f"{value:,.0f}"


def generate_founder_summary_html(data: Dict[str, Any]) -> str:
    metrics = data.get('metrics', {})
    kpis = data.get('kpis', {})
    reliability = kpis.get('reliability', {})
    carrier_rankings = kpis.get('carrier_rankings', {})
    airport_rankings = kpis.get('airport_rankings', {})
    weather_attr = kpis.get('weather_attribution', {})
    summary = kpis.get('summary', {})

    quality_score = metrics.get('overall_quality_score', 0) * 100
    on_time_rate = reliability.get('on_time_rate', 0) * 100
    cancel_rate = reliability.get('cancellation_rate', 0) * 100
    weather_delta = weather_attr.get('weather_delay_delta', 0)

    # Simulated data disclaimer
    DATA_DISCLAIMER = "DEMONSTRATION: Flight data is synthetic (BTS format) for portfolio demonstration. Weather data is real (NOAA CDO API)."

    # Top/bottom carriers
    carriers = carrier_rankings.get('by_carrier', [])
    top_carrier = carriers[0] if carriers else {}
    worst_carrier = carriers[-1] if carriers else {}

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>P2-AIR: Founder Summary</title>
        <style>
            @page {{ size: letter; margin: 0.5in; }}
            body {{
                font-family: 'Helvetica Neue', Arial, sans-serif;
                font-size: 11pt;
                line-height: 1.5;
                color: #1f2937;
                margin: 0;
            }}
            .header {{
                background: linear-gradient(135deg, {PRIMARY_COLOR}, {SECONDARY_COLOR});
                color: white;
                padding: 25px;
                margin: -0.5in -0.5in 20px -0.5in;
                text-align: center;
            }}
            .header h1 {{ margin: 0; font-size: 28pt; }}
            .header .subtitle {{ font-size: 14pt; opacity: 0.9; margin-top: 5px; }}
            .tldr {{
                background: linear-gradient(135deg, #ecfeff, #cffafe);
                border-left: 4px solid {PRIMARY_COLOR};
                padding: 15px 20px;
                margin: 20px 0;
                border-radius: 0 8px 8px 0;
            }}
            .tldr-title {{ font-size: 14pt; font-weight: 700; color: {PRIMARY_COLOR}; margin-bottom: 8px; }}
            .stat-grid {{
                display: flex;
                flex-wrap: wrap;
                gap: 15px;
                margin: 20px 0;
            }}
            .stat-card {{
                flex: 1;
                min-width: 100px;
                background: white;
                border: 2px solid #e2e8f0;
                border-radius: 12px;
                padding: 15px;
                text-align: center;
            }}
            .stat-value {{
                font-size: 22pt;
                font-weight: 700;
                color: {PRIMARY_COLOR};
            }}
            .stat-label {{
                font-size: 9pt;
                color: #6b7280;
                text-transform: uppercase;
            }}
            .section {{ margin: 25px 0; }}
            .section-title {{
                font-size: 14pt;
                font-weight: 700;
                color: {PRIMARY_COLOR};
                border-bottom: 2px solid {PRIMARY_COLOR};
                padding-bottom: 5px;
                margin-bottom: 12px;
            }}
            .highlight-box {{
                background: #f8fafc;
                border-radius: 8px;
                padding: 15px;
                margin: 10px 0;
            }}
            .two-col {{ display: flex; gap: 20px; }}
            .two-col > div {{ flex: 1; }}
            ul {{ margin: 5px 0; padding-left: 20px; }}
            li {{ margin: 3px 0; }}
            .insight {{
                background: linear-gradient(135deg, #fef3c7, #fde68a);
                border-left: 4px solid #f59e0b;
                padding: 12px;
                margin: 15px 0;
                border-radius: 0 8px 8px 0;
            }}
            .footer {{
                margin-top: 20px;
                padding-top: 10px;
                border-top: 1px solid #e2e8f0;
                font-size: 8pt;
                color: #6b7280;
                text-align: center;
            }}
            .quality-badge {{
                display: inline-block;
                background: {ACCENT_COLOR};
                color: white;
                padding: 4px 12px;
                border-radius: 20px;
                font-weight: 600;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Airline Reliability Intelligence</h1>
            <div class="subtitle">On-Time Performance + Weather Attribution | Portfolio Demo</div>
        </div>

        <div class="tldr">
            <div class="tldr-title">TL;DR</div>
            <p style="margin: 0;">Built an <strong>ops analytics pipeline</strong> that combines <strong>{format_number(summary.get('total_flights', 0))} flight records</strong> (synthetic, BTS format) with <strong>real NOAA weather data</strong> to measure airline reliability and quantify weather impact. Key finding: <strong>severe weather correlates with +{weather_delta:.1f} minute</strong> additional delay. Pipeline achieves <strong>{quality_score:.0f}% data quality score</strong> with full weather attribution.</p>
            <p style="margin: 8px 0 0 0; font-size: 9pt; color: #6b7280;"><em>{DATA_DISCLAIMER}</em></p>
        </div>

        <div class="stat-grid">
            <div class="stat-card">
                <div class="stat-value">{format_number(summary.get('total_flights', 0))}</div>
                <div class="stat-label">Flights</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{on_time_rate:.1f}%</div>
                <div class="stat-label">On-Time Rate</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{cancel_rate:.2f}%</div>
                <div class="stat-label">Cancel Rate</div>
            </div>
            <div class="stat-card">
                <div class="stat-value"><span class="quality-badge">{quality_score:.0f}%</span></div>
                <div class="stat-label">Quality Score</div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">What This Demonstrates</div>
            <div class="two-col">
                <div class="highlight-box">
                    <strong>Data Engineering</strong>
                    <ul>
                        <li>Multi-source data integration</li>
                        <li>NOAA API integration</li>
                        <li>100K+ row processing</li>
                        <li>Weather-flight joins</li>
                    </ul>
                </div>
                <div class="highlight-box">
                    <strong>Analytics Engineering</strong>
                    <ul>
                        <li>Reliability scoring</li>
                        <li>Weather attribution</li>
                        <li>Carrier rankings</li>
                        <li>Anomaly detection</li>
                    </ul>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Key Insight: Weather Attribution</div>
            <div class="insight">
                <p><strong>⛈️ Weather Impact Quantified</strong></p>
                <p>Flights on <strong>severe weather days</strong> (high precip/wind/fog) experience <strong>+{weather_delta:.0f} minute</strong> additional delay compared to normal conditions. Cancel rates increase from <strong>{weather_attr.get('normal_cancel_rate', 0)*100:.1f}%</strong> to <strong>{weather_attr.get('severe_cancel_rate', 0)*100:.1f}%</strong>.</p>
                <p style="font-size: 9pt; color: #6b7280; margin-top: 8px;"><em>Note: Correlation observed; causation requires further analysis.</em></p>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Carrier Leaderboard</div>
            <div class="highlight-box">
                <p>🥇 <strong>Best:</strong> {top_carrier.get('carrier', 'N/A')} — {top_carrier.get('on_time_rate', 0)*100:.1f}% on-time</p>
                <p>⚠️ <strong>Needs Improvement:</strong> {worst_carrier.get('carrier', 'N/A')} — {worst_carrier.get('on_time_rate', 0)*100:.1f}% on-time</p>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Technical Highlights</div>
            <ul>
                <li><strong>Flight Data:</strong> BTS-format on-time performance records</li>
                <li><strong>Weather Data:</strong> NOAA CDO API (GHCND daily summaries)</li>
                <li><strong>Airports:</strong> {summary.get('airports', 0)} major US hubs with weather stations</li>
                <li><strong>Pipeline:</strong> Python → Star Schema → Weather Join → Quality Gates → KPIs</li>
                <li><strong>Duration:</strong> {metrics.get('duration_seconds', 0):.0f} seconds</li>
            </ul>
        </div>

        <div class="footer">
            <p>Report prepared by {AUTHOR} | {EMAIL}</p>
            <p>Data Sources: Synthetic flight data (BTS format) + NOAA CDO API (real weather)</p>
            <p style="font-size: 7pt; color: #9ca3af;">PORTFOLIO DEMONSTRATION - Flight data is synthetic for demo purposes</p>
            <p>Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}</p>
        </div>
    </body>
    </html>
    """
    return html


def generate_executive_html(data: Dict[str, Any]) -> str:
    metrics = data.get('metrics', {})
    kpis = data.get('kpis', {})
    reliability = kpis.get('reliability', {})
    carrier_rankings = kpis.get('carrier_rankings', {})
    weather_attr = kpis.get('weather_attribution', {})
    summary = kpis.get('summary', {})
    gates = metrics.get('quality_gates', [])

    quality_score = metrics.get('overall_quality_score', 0) * 100
    carriers = carrier_rankings.get('by_carrier', [])

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>P2-AIR: Executive Summary</title>
        <style>
            @page {{ size: letter; margin: 0.5in; }}
            body {{ font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 10pt; line-height: 1.4; color: #1f2937; }}
            .header {{ background: linear-gradient(135deg, {PRIMARY_COLOR}, {SECONDARY_COLOR}); color: white; padding: 20px; margin: -0.5in -0.5in 20px -0.5in; text-align: center; }}
            .header h1 {{ margin: 0; font-size: 24pt; }}
            .traceability {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px; margin-bottom: 20px; font-size: 8pt; }}
            .section {{ margin-bottom: 20px; }}
            .section-title {{ color: {PRIMARY_COLOR}; font-size: 14pt; font-weight: 700; border-bottom: 2px solid {PRIMARY_COLOR}; padding-bottom: 5px; margin-bottom: 10px; }}
            .kpi-grid {{ display: flex; flex-wrap: wrap; gap: 12px; }}
            .kpi-card {{ flex: 1; min-width: 90px; background: white; border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px; text-align: center; }}
            .kpi-value {{ font-size: 18pt; font-weight: 700; color: {PRIMARY_COLOR}; }}
            .kpi-label {{ font-size: 8pt; color: #6b7280; text-transform: uppercase; }}
            table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 9pt; }}
            th {{ background: {PRIMARY_COLOR}; color: white; padding: 8px; text-align: left; }}
            td {{ padding: 8px; border-bottom: 1px solid #e2e8f0; }}
            tr:nth-child(even) {{ background: #f8fafc; }}
            .pass {{ color: {ACCENT_COLOR}; font-weight: 600; }}
            .footer {{ margin-top: 20px; padding-top: 10px; border-top: 1px solid #e2e8f0; font-size: 8pt; color: #6b7280; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Airline On-Time Reliability Report</h1>
            <div>{summary.get('date_range', 'January 2024')} | {summary.get('airports', 10)} Airports | {summary.get('carriers', 8)} Carriers</div>
        </div>

        <div class="traceability">
            <strong>Traceability:</strong> Synthetic flights (BTS format) + NOAA CDO (real) | {format_number(metrics.get('flight_records', 0))} flights | {metrics.get('weather_records', 0):,} weather records | Quality: {quality_score:.1f}% | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
            <br><span style="color: #dc2626; font-weight: 600;">DEMO DATA: Flight records are synthetic for portfolio demonstration</span>
        </div>

        <div class="section">
            <div class="section-title">Reliability Metrics</div>
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value">{reliability.get('on_time_rate', 0)*100:.1f}%</div>
                    <div class="kpi-label">On-Time Rate</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{reliability.get('avg_dep_delay', 0):.1f}</div>
                    <div class="kpi-label">Avg Delay (min)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{reliability.get('p90_dep_delay', 0):.0f}</div>
                    <div class="kpi-label">P90 Delay (min)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value">{reliability.get('cancellation_rate', 0)*100:.2f}%</div>
                    <div class="kpi-label">Cancel Rate</div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Carrier Performance</div>
            <table>
                <tr><th>Rank</th><th>Carrier</th><th>On-Time</th><th>Avg Delay</th><th>Cancel Rate</th><th>Flights</th></tr>
                {''.join(f'''<tr>
                    <td>{c.get('rank', '')}</td>
                    <td>{c.get('carrier', '')}</td>
                    <td>{c.get('on_time_rate', 0)*100:.1f}%</td>
                    <td>{c.get('avg_delay', 0):.1f} min</td>
                    <td>{c.get('cancel_rate', 0)*100:.2f}%</td>
                    <td>{int(c.get('flight_count', 0)):,}</td>
                </tr>''' for c in carriers[:8])}
            </table>
        </div>

        <div class="section">
            <div class="section-title">Weather Attribution</div>
            <table>
                <tr><th>Condition</th><th>Flights</th><th>Avg Delay</th><th>Cancel Rate</th></tr>
                <tr>
                    <td>Normal Weather</td>
                    <td>{weather_attr.get('normal_weather_flights', 0):,}</td>
                    <td>{weather_attr.get('normal_avg_delay', 0):.1f} min</td>
                    <td>{weather_attr.get('normal_cancel_rate', 0)*100:.2f}%</td>
                </tr>
                <tr>
                    <td>Severe Weather</td>
                    <td>{weather_attr.get('severe_weather_flights', 0):,}</td>
                    <td>{weather_attr.get('severe_avg_delay', 0):.1f} min</td>
                    <td>{weather_attr.get('severe_cancel_rate', 0)*100:.2f}%</td>
                </tr>
                <tr style="background: #fef3c7;">
                    <td><strong>Delta (Weather Impact)</strong></td>
                    <td>-</td>
                    <td><strong>+{weather_attr.get('weather_delay_delta', 0):.1f} min</strong></td>
                    <td>-</td>
                </tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">Quality Gates</div>
            <table>
                <tr><th>Gate</th><th>Status</th><th>Score</th><th>Threshold</th></tr>
                {''.join(f'''<tr>
                    <td>{g.get('name', '').replace('_', ' ').title()}</td>
                    <td class="pass">{'✓ PASS' if g.get('passed') else '✗ FAIL'}</td>
                    <td>{g.get('score', 0)*100:.1f}%</td>
                    <td>{g.get('threshold', 0)*100:.0f}%</td>
                </tr>''' for g in gates)}
            </table>
        </div>

        <div class="footer">
            <p>Report prepared by {AUTHOR} | {EMAIL}</p>
            <p>Data Sources: Synthetic flights (BTS format) + NOAA CDO API | Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>
        </div>
    </body>
    </html>
    """
    return html


def generate_methodology_html(data: Dict[str, Any]) -> str:
    metrics = data.get('metrics', {})

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>P2-AIR: Methodology & QA</title>
        <style>
            @page {{ size: letter; margin: 0.6in; }}
            body {{ font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 9pt; line-height: 1.5; color: #1f2937; }}
            .header {{ background: linear-gradient(135deg, {PRIMARY_COLOR}, {SECONDARY_COLOR}); color: white; padding: 15px; margin: -0.6in -0.6in 15px -0.6in; text-align: center; }}
            .header h1 {{ margin: 0; font-size: 18pt; }}
            .section {{ margin-bottom: 15px; }}
            .section-title {{ color: {PRIMARY_COLOR}; font-size: 12pt; font-weight: 700; border-bottom: 2px solid {PRIMARY_COLOR}; padding-bottom: 3px; margin-bottom: 8px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 8pt; }}
            th {{ background: {PRIMARY_COLOR}; color: white; padding: 6px; text-align: left; }}
            td {{ padding: 6px; border-bottom: 1px solid #e2e8f0; }}
            .formula-box {{ background: #ecfeff; border-left: 3px solid {PRIMARY_COLOR}; padding: 10px; margin: 8px 0; font-family: monospace; }}
            .note-box {{ background: #fef3c7; border-left: 3px solid #f59e0b; padding: 10px; margin: 8px 0; }}
            .footer {{ margin-top: 15px; padding-top: 8px; border-top: 1px solid #e2e8f0; font-size: 7pt; color: #6b7280; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>P2-AIR: Methodology & Quality Assurance</h1>
        </div>

        <div class="section">
            <div class="section-title">1. Data Sources</div>
            <div class="note-box" style="background: #fef2f2; border-left-color: #dc2626;">
                <strong>IMPORTANT:</strong> Flight data is <strong>SYNTHETIC</strong> (generated in BTS TranStats format) for portfolio demonstration purposes. Weather data is <strong>REAL</strong> from NOAA CDO API.
            </div>
            <table>
                <tr><th>Source</th><th>Type</th><th>Data Status</th></tr>
                <tr><td>BTS TranStats Format</td><td>Flight Performance</td><td><strong>SYNTHETIC</strong> (demo)</td></tr>
                <tr><td>NOAA CDO API v2</td><td>Weather</td><td><strong>REAL</strong> (live API)</td></tr>
            </table>
            <p><strong>Weather Data:</strong> GHCND (Global Historical Climatology Network Daily) dataset with PRCP, TMAX, TMIN, AWND, fog, thunder indicators.</p>
        </div>

        <div class="section">
            <div class="section-title">2. Data Model</div>
            <table>
                <tr><th>Table</th><th>Description</th><th>Key Columns</th></tr>
                <tr><td>flight_fact</td><td>Flight records</td><td>flight_date, carrier, origin, dest, delays</td></tr>
                <tr><td>weather_fact</td><td>Daily weather by airport</td><td>airport, date, precip, temp, wind</td></tr>
                <tr><td>airport_dim</td><td>Airport reference</td><td>code, name, city, state, lat/lon</td></tr>
                <tr><td>carrier_dim</td><td>Carrier reference</td><td>carrier_code, carrier_name</td></tr>
                <tr><td>flight_weather_fact</td><td>Joined fact</td><td>All flight + origin/dest weather</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">3. Weather Attribution Logic</div>
            <div class="formula-box">
                <strong>Severe Weather Flag:</strong><br>
                severe_weather = (precipitation > 25mm) OR (wind > 25mph) OR (fog=1) OR (thunder=1)<br><br>
                <strong>Weather Impact:</strong><br>
                delta = avg_delay(severe) - avg_delay(normal)
            </div>
            <div class="note-box">
                <strong>⚠️ Attribution Note:</strong> Weather correlation does not imply causation. Delays may result from cascading effects, crew availability, or other operational factors coinciding with weather events.
            </div>
        </div>

        <div class="section">
            <div class="section-title">4. Quality Gates</div>
            <table>
                <tr><th>Gate</th><th>Threshold</th><th>Logic</th></tr>
                <tr><td>Timezone</td><td>≥95%</td><td>Valid date/time parsing</td></tr>
                <tr><td>Join Coverage</td><td>≥80%</td><td>Flights matched with weather data</td></tr>
                <tr><td>Outliers</td><td>≥95%</td><td>No impossible delays (>24h or <-60min)</td></tr>
                <tr><td>Delay Codes</td><td>≥75%</td><td>Delayed flights have reason codes</td></tr>
                <tr><td>Completeness</td><td>≥95%</td><td>Required fields populated</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">5. KPI Definitions</div>
            <table>
                <tr><th>Metric</th><th>Formula</th></tr>
                <tr><td>On-Time Rate</td><td>% flights with arr_delay ≤ 15 min</td></tr>
                <tr><td>Avg Delay</td><td>MEAN(dep_delay) for non-cancelled flights</td></tr>
                <tr><td>P90 Delay</td><td>90th percentile of dep_delay</td></tr>
                <tr><td>Cancel Rate</td><td>% flights with cancelled=1</td></tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">6. Run Metrics</div>
            <table>
                <tr><th>Metric</th><th>Value</th></tr>
                <tr><td>Duration</td><td>{metrics.get('duration_seconds', 0):.1f} seconds</td></tr>
                <tr><td>Flight Records</td><td>{metrics.get('flight_records', 0):,}</td></tr>
                <tr><td>Weather Records</td><td>{metrics.get('weather_records', 0):,}</td></tr>
                <tr><td>Joined Records</td><td>{metrics.get('joined_records', 0):,}</td></tr>
                <tr><td>NOAA API Calls</td><td>{metrics.get('api_calls', 0)}</td></tr>
                <tr><td>Quality Score</td><td>{metrics.get('overall_quality_score', 0)*100:.1f}%</td></tr>
            </table>
        </div>

        <div class="footer">
            <p>Report prepared by {AUTHOR} | {EMAIL}</p>
            <p>Data Sources: Synthetic flights (BTS format) + NOAA CDO API (real) | Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>
            <p style="font-size: 7pt; color: #dc2626;">PORTFOLIO DEMONSTRATION - Flight data is synthetic</p>
        </div>
    </body>
    </html>
    """
    return html


def generate_pdfs():
    print("Loading pipeline data...")
    data = load_pipeline_data()

    if not data:
        print("ERROR: No pipeline data found.")
        return

    print("Generating Founder Summary PDF...")
    HTML(string=generate_founder_summary_html(data)).write_pdf(REPORTS_DIR / 'P2-AIR_Founder_Summary_v1.0.pdf')
    print("  Saved: P2-AIR_Founder_Summary_v1.0.pdf")

    print("Generating Executive Summary PDF...")
    HTML(string=generate_executive_html(data)).write_pdf(REPORTS_DIR / 'P2-AIR_Executive_Summary_v1.0.pdf')
    print("  Saved: P2-AIR_Executive_Summary_v1.0.pdf")

    print("Generating Methodology PDF...")
    HTML(string=generate_methodology_html(data)).write_pdf(REPORTS_DIR / 'P2-AIR_Methodology_QA_v1.0.pdf')
    print("  Saved: P2-AIR_Methodology_QA_v1.0.pdf")

    print(f"\nAll PDFs saved to: {REPORTS_DIR}")


if __name__ == '__main__':
    generate_pdfs()
