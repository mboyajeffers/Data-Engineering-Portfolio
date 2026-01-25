#!/usr/bin/env python3
"""
CMS LinkedIn Proof Package - Weather Reports
Author: Mboya Jeffers (MboyaJeffers9@gmail.com)
Generated: January 25, 2026

Data Source: Open-Meteo API (free, global, no auth required)
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from weasyprint import HTML, CSS
from jinja2 import Template
import os

OUTPUT_DIR = "/Users/mboyajeffers/Downloads/LinkedIn_Proof_Package/Weather"
AUTHOR = "Mboya Jeffers"
EMAIL = "MboyaJeffers9@gmail.com"
DATE = datetime.now().strftime("%B %d, %Y")

# Open-Meteo API
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# Target cities
LOCATIONS = {
    'Chicago, IL': {'lat': 41.8781, 'lon': -87.6298},
    'Houston, TX': {'lat': 29.7604, 'lon': -95.3698},
    'Seattle, WA': {'lat': 47.6062, 'lon': -122.3321},
    'Miami, FL': {'lat': 25.7617, 'lon': -80.1918},
    'Denver, CO': {'lat': 39.7392, 'lon': -104.9903}
}

# ============================================================================
# CSS
# ============================================================================

BASE_CSS = """
@page { size: letter; margin: 0.75in; }
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.5; }
.header { border-bottom: 3px solid #0ea5e9; padding-bottom: 10px; margin-bottom: 20px; }
.header h1 { color: #1a1a2e; margin: 0; font-size: 28px; }
.header .subtitle { color: #666; font-size: 14px; margin-top: 5px; }
.header .date { color: #999; font-size: 12px; float: right; }
.kpi-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin: 20px 0; }
.kpi-card { background: #f0f9ff; border-radius: 8px; padding: 20px; text-align: center; border-left: 4px solid #0ea5e9; }
.kpi-card .label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 1px; }
.kpi-card .value { font-size: 28px; font-weight: bold; color: #1a1a2e; margin: 8px 0; }
.kpi-card .value.hot { color: #ef4444; }
.kpi-card .value.cold { color: #3b82f6; }
.kpi-card .value.moderate { color: #10b981; }
table { width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 13px; }
th { background: #0ea5e9; color: white; padding: 12px 10px; text-align: left; }
td { padding: 10px; border-bottom: 1px solid #eee; }
tr:nth-child(even) { background: #f0f9ff; }
.section { margin: 30px 0; }
.section h2 { color: #0ea5e9; font-size: 18px; border-bottom: 1px solid #eee; padding-bottom: 8px; }
.highlight-box { background: #ecfeff; border-left: 4px solid #06b6d4; padding: 15px; margin: 15px 0; }
.highlight-box.warning { background: #fef3c7; border-color: #f59e0b; }
.methodology { background: #f8f9fa; padding: 15px; border-radius: 8px; font-size: 12px; color: #666; margin-top: 30px; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #eee; font-size: 11px; color: #999; }
.footer .author { color: #0ea5e9; font-weight: bold; }
"""

REPORT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{{ title }}</title></head>
<body>
<div class="header">
    <span class="date">{{ date }}</span>
    <h1>{{ title }}</h1>
    <div class="subtitle">{{ subtitle }}</div>
</div>
{{ content }}
<div class="footer">
    Report prepared by <span class="author">{{ author }}</span> | {{ date }}<br>
    Data Source: {{ data_source }}
</div>
</body>
</html>
"""

# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_open_meteo_data(lat, lon, start_date, end_date):
    """Fetch weather data from Open-Meteo Archive API"""
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,rain_sum,snowfall_sum,windspeed_10m_max,windgusts_10m_max,relative_humidity_2m_mean',
        'timezone': 'auto'
    }

    try:
        response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'daily' in data:
            df = pd.DataFrame(data['daily'])
            df['time'] = pd.to_datetime(df['time'])
            df.set_index('time', inplace=True)
            return df
    except Exception as e:
        print(f"    Error: {e}")
    return None

def fetch_all_locations():
    """Fetch weather data for all locations"""
    end_date = datetime.now() - timedelta(days=5)
    start_date = end_date - timedelta(days=90)

    all_data = {}
    for city, coords in LOCATIONS.items():
        print(f"  Fetching {city}...")
        df = fetch_open_meteo_data(coords['lat'], coords['lon'], start_date, end_date)
        if df is not None:
            all_data[city] = df
            print(f"    ✓ {len(df)} days of data")
        else:
            print(f"    ✗ Failed")

    return all_data

# ============================================================================
# KPI CALCULATIONS
# ============================================================================

def compute_weather_kpis(df):
    """Compute weather-specific KPIs"""
    kpis = {}

    # Temperature
    kpis['temp_avg'] = df['temperature_2m_mean'].mean()
    kpis['temp_max'] = df['temperature_2m_max'].max()
    kpis['temp_min'] = df['temperature_2m_min'].min()
    kpis['temp_range'] = kpis['temp_max'] - kpis['temp_min']

    # Precipitation
    kpis['precip_total'] = df['precipitation_sum'].sum()
    kpis['precip_days'] = (df['precipitation_sum'] > 0.1).sum()
    kpis['precip_avg_day'] = df['precipitation_sum'].mean()

    # Snow (if available)
    if 'snowfall_sum' in df.columns:
        kpis['snow_total'] = df['snowfall_sum'].sum()
        kpis['snow_days'] = (df['snowfall_sum'] > 0).sum()

    # Wind
    kpis['wind_avg'] = df['windspeed_10m_max'].mean()
    kpis['wind_max'] = df['windspeed_10m_max'].max()
    if 'windgusts_10m_max' in df.columns:
        kpis['gust_max'] = df['windgusts_10m_max'].max()

    # Humidity
    if 'relative_humidity_2m_mean' in df.columns:
        kpis['humidity_avg'] = df['relative_humidity_2m_mean'].mean()

    # Degree days (base 65°F / 18°C)
    base_temp = 18  # Celsius
    df_temp = df['temperature_2m_mean']
    kpis['hdd'] = (base_temp - df_temp).clip(lower=0).sum()  # Heating degree days
    kpis['cdd'] = (df_temp - base_temp).clip(lower=0).sum()  # Cooling degree days

    # Extreme events
    kpis['hot_days'] = (df['temperature_2m_max'] > 35).sum()  # >95°F
    kpis['cold_days'] = (df['temperature_2m_min'] < 0).sum()  # <32°F
    kpis['frost_days'] = (df['temperature_2m_min'] < 0).sum()
    kpis['heavy_precip_days'] = (df['precipitation_sum'] > 25).sum()  # >1 inch

    return kpis

# ============================================================================
# REPORT 1: CITY CLIMATE PROFILE
# ============================================================================

def generate_climate_profile_report(data, city='Chicago, IL'):
    """Generate City Climate Profile PDF"""
    print(f"\n🌡️ Generating Climate Profile for {city}...")

    if city not in data:
        return

    df = data[city]
    kpis = compute_weather_kpis(df)

    # Temperature classification
    if kpis['temp_avg'] > 25:
        temp_class = ('Hot', 'hot')
    elif kpis['temp_avg'] < 10:
        temp_class = ('Cold', 'cold')
    else:
        temp_class = ('Moderate', 'moderate')

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Average Temperature</div>
            <div class="value {temp_class[1]}">{kpis['temp_avg']:.1f}°C</div>
        </div>
        <div class="kpi-card">
            <div class="label">Temperature Range</div>
            <div class="value">{kpis['temp_min']:.1f}° to {kpis['temp_max']:.1f}°C</div>
        </div>
        <div class="kpi-card">
            <div class="label">Total Precipitation</div>
            <div class="value">{kpis['precip_total']:.1f} mm</div>
        </div>
        <div class="kpi-card">
            <div class="label">Precipitation Days</div>
            <div class="value">{kpis['precip_days']} days</div>
        </div>
    </div>

    <div class="section">
        <h2>Temperature Analysis</h2>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
                <th>Interpretation</th>
            </tr>
            <tr>
                <td>Mean Temperature</td>
                <td><strong>{kpis['temp_avg']:.1f}°C</strong> ({kpis['temp_avg'] * 9/5 + 32:.1f}°F)</td>
                <td>{temp_class[0]} climate</td>
            </tr>
            <tr>
                <td>Maximum Recorded</td>
                <td>{kpis['temp_max']:.1f}°C ({kpis['temp_max'] * 9/5 + 32:.1f}°F)</td>
                <td>Peak temperature in period</td>
            </tr>
            <tr>
                <td>Minimum Recorded</td>
                <td>{kpis['temp_min']:.1f}°C ({kpis['temp_min'] * 9/5 + 32:.1f}°F)</td>
                <td>Lowest temperature in period</td>
            </tr>
            <tr>
                <td>Hot Days (>35°C)</td>
                <td>{kpis['hot_days']} days</td>
                <td>{'High heat stress risk' if kpis['hot_days'] > 10 else 'Low heat stress'}</td>
            </tr>
            <tr>
                <td>Frost Days (<0°C)</td>
                <td>{kpis['frost_days']} days</td>
                <td>{'Significant frost risk' if kpis['frost_days'] > 20 else 'Low frost risk'}</td>
            </tr>
        </table>
    </div>

    <div class="section">
        <h2>Precipitation & Wind</h2>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
                <th>Interpretation</th>
            </tr>
            <tr>
                <td>Total Precipitation</td>
                <td>{kpis['precip_total']:.1f} mm ({kpis['precip_total']/25.4:.2f} in)</td>
                <td>90-day cumulative</td>
            </tr>
            <tr>
                <td>Rainy Days</td>
                <td>{kpis['precip_days']} days</td>
                <td>{kpis['precip_days']/len(df)*100:.0f}% of period</td>
            </tr>
            <tr>
                <td>Heavy Rain Days (>25mm)</td>
                <td>{kpis['heavy_precip_days']} days</td>
                <td>{'Flood risk present' if kpis['heavy_precip_days'] > 3 else 'Low flood risk'}</td>
            </tr>
            <tr>
                <td>Average Wind Speed</td>
                <td>{kpis['wind_avg']:.1f} km/h</td>
                <td>Daily max average</td>
            </tr>
            <tr>
                <td>Peak Wind Speed</td>
                <td>{kpis['wind_max']:.1f} km/h</td>
                <td>Maximum recorded</td>
            </tr>
        </table>
    </div>

    <div class="section">
        <h2>Degree Day Analysis</h2>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
                <th>Use Case</th>
            </tr>
            <tr>
                <td>Heating Degree Days (HDD)</td>
                <td><strong>{kpis['hdd']:.0f}</strong></td>
                <td>Energy demand for heating</td>
            </tr>
            <tr>
                <td>Cooling Degree Days (CDD)</td>
                <td><strong>{kpis['cdd']:.0f}</strong></td>
                <td>Energy demand for cooling</td>
            </tr>
            <tr>
                <td>Dominant Load</td>
                <td><strong>{'Heating' if kpis['hdd'] > kpis['cdd'] else 'Cooling'}</strong></td>
                <td>Primary energy use pattern</td>
            </tr>
        </table>
    </div>

    <div class="highlight-box">
        <strong>Climate Summary for {city}:</strong>
        <ul>
            <li>Climate type: {temp_class[0]} with {'high' if kpis['precip_days'] > 30 else 'moderate'} precipitation</li>
            <li>Primary energy load: {'Heating' if kpis['hdd'] > kpis['cdd'] else 'Cooling'} ({max(kpis['hdd'], kpis['cdd']):.0f} degree days)</li>
            <li>Extreme weather days: {kpis['hot_days'] + kpis['frost_days']} in 90-day period</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Data from Open-Meteo Archive API (ERA5 reanalysis).
        Degree days use 18°C (65°F) base temperature. HDD = heating demand, CDD = cooling demand.
        Hot days >35°C, Frost days <0°C, Heavy precipitation >25mm/day.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="City Climate Profile",
        subtitle=f"{city} | 90-Day Weather Analysis",
        date=DATE,
        author=AUTHOR,
        data_source="Open-Meteo API (ERA5)",
        content=content
    )

    safe_city = city.replace(', ', '_').replace(' ', '_')
    output_path = os.path.join(OUTPUT_DIR, f"CMS_Climate_Profile_{safe_city}.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 2: DEGREE DAY ANALYSIS
# ============================================================================

def generate_degree_day_report(data):
    """Generate Degree Day Analysis PDF for energy demand"""
    print("\n🌡️ Generating Degree Day Analysis...")

    results = []
    for city, df in data.items():
        kpis = compute_weather_kpis(df)
        results.append({
            'city': city,
            'hdd': kpis['hdd'],
            'cdd': kpis['cdd'],
            'total_dd': kpis['hdd'] + kpis['cdd'],
            'dominant': 'Heating' if kpis['hdd'] > kpis['cdd'] else 'Cooling',
            'temp_avg': kpis['temp_avg']
        })

    results.sort(key=lambda x: x['total_dd'], reverse=True)

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Highest Energy Demand</div>
            <div class="value" style="font-size: 18px;">{results[0]['city']}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Total Degree Days</div>
            <div class="value">{results[0]['total_dd']:.0f}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Lowest Energy Demand</div>
            <div class="value" style="font-size: 18px;">{results[-1]['city']}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Cities Analyzed</div>
            <div class="value">{len(results)}</div>
        </div>
    </div>

    <div class="section">
        <h2>Energy Demand Comparison</h2>
        <table>
            <tr>
                <th>City</th>
                <th>HDD (Heating)</th>
                <th>CDD (Cooling)</th>
                <th>Total DD</th>
                <th>Dominant Load</th>
            </tr>
    """

    for r in results:
        content += f"""
            <tr>
                <td><strong>{r['city']}</strong></td>
                <td class="cold">{r['hdd']:.0f}</td>
                <td class="hot">{r['cdd']:.0f}</td>
                <td><strong>{r['total_dd']:.0f}</strong></td>
                <td>{r['dominant']}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Energy Planning Implications</h2>
        <table>
            <tr>
                <th>City</th>
                <th>Avg Temp</th>
                <th>HVAC Sizing</th>
                <th>Peak Season</th>
            </tr>
    """

    for r in results:
        hvac = 'Heat pump optimal' if abs(r['hdd'] - r['cdd']) < 200 else f'{r["dominant"]}-dominant system'
        peak = 'Winter' if r['hdd'] > r['cdd'] else 'Summer'
        content += f"""
            <tr>
                <td><strong>{r['city']}</strong></td>
                <td>{r['temp_avg']:.1f}°C</td>
                <td>{hvac}</td>
                <td>{peak}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="highlight-box">
        <strong>Key Insights:</strong>
        <ul>
            <li>Total degree days indicate overall HVAC energy consumption potential</li>
            <li>HDD/CDD balance determines optimal equipment type (heat pump vs single-purpose)</li>
            <li>Cities with balanced HDD/CDD benefit most from reversible heat pumps</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Degree days calculated using 18°C (65°F) base temperature.
        HDD = Σmax(0, 18 - Tavg) for heating demand.
        CDD = Σmax(0, Tavg - 18) for cooling demand.
        90-day analysis period from Open-Meteo ERA5 data.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Degree Day Analysis",
        subtitle="Multi-City Energy Demand Comparison",
        date=DATE,
        author=AUTHOR,
        data_source="Open-Meteo API (ERA5)",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Degree_Day_Analysis.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 3: EXTREME EVENT DETECTION
# ============================================================================

def generate_extreme_event_report(data, city='Chicago, IL'):
    """Generate Extreme Event Detection PDF"""
    print(f"\n🌡️ Generating Extreme Event Detection for {city}...")

    if city not in data:
        return

    df = data[city]

    # Define thresholds
    thresholds = {
        'Heat Wave': {'col': 'temperature_2m_max', 'op': '>', 'val': 35, 'unit': '°C'},
        'Cold Snap': {'col': 'temperature_2m_min', 'op': '<', 'val': -5, 'unit': '°C'},
        'Frost': {'col': 'temperature_2m_min', 'op': '<', 'val': 0, 'unit': '°C'},
        'Heavy Rain': {'col': 'precipitation_sum', 'op': '>', 'val': 25, 'unit': 'mm'},
        'High Wind': {'col': 'windspeed_10m_max', 'op': '>', 'val': 50, 'unit': 'km/h'},
    }

    events = []
    for event_name, config in thresholds.items():
        if config['col'] in df.columns:
            if config['op'] == '>':
                count = (df[config['col']] > config['val']).sum()
            else:
                count = (df[config['col']] < config['val']).sum()
            events.append({
                'event': event_name,
                'threshold': f"{config['op']} {config['val']}{config['unit']}",
                'count': count,
                'pct': count / len(df) * 100
            })

    total_extreme = sum(e['count'] for e in events)

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Total Extreme Days</div>
            <div class="value {'hot' if total_extreme > 20 else 'moderate'}">{total_extreme}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Analysis Period</div>
            <div class="value">{len(df)} days</div>
        </div>
        <div class="kpi-card">
            <div class="label">Extreme Day Rate</div>
            <div class="value">{total_extreme/len(df)*100:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Risk Level</div>
            <div class="value {'hot' if total_extreme > 20 else 'moderate' if total_extreme > 10 else 'cold'}">{'High' if total_extreme > 20 else 'Moderate' if total_extreme > 10 else 'Low'}</div>
        </div>
    </div>

    <div class="section">
        <h2>Extreme Event Summary</h2>
        <table>
            <tr>
                <th>Event Type</th>
                <th>Threshold</th>
                <th>Days Detected</th>
                <th>Frequency</th>
                <th>Risk Assessment</th>
            </tr>
    """

    for e in events:
        risk = 'High' if e['pct'] > 10 else 'Moderate' if e['pct'] > 5 else 'Low'
        risk_class = 'hot' if risk == 'High' else 'moderate' if risk == 'Moderate' else 'cold'
        content += f"""
            <tr>
                <td><strong>{e['event']}</strong></td>
                <td>{e['threshold']}</td>
                <td>{e['count']}</td>
                <td>{e['pct']:.1f}%</td>
                <td class="{risk_class}">{risk}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Threshold Configuration</h2>
        <p>The following thresholds are used for extreme event detection. These can be customized based on industry requirements:</p>
        <table>
            <tr>
                <th>Event</th>
                <th>Default Threshold</th>
                <th>Industry Use Case</th>
            </tr>
            <tr>
                <td>Heat Wave</td>
                <td>&gt; 35°C (95°F)</td>
                <td>Worker safety, HVAC load, agriculture</td>
            </tr>
            <tr>
                <td>Cold Snap</td>
                <td>&lt; -5°C (23°F)</td>
                <td>Pipe freeze risk, transportation</td>
            </tr>
            <tr>
                <td>Frost</td>
                <td>&lt; 0°C (32°F)</td>
                <td>Agriculture, construction delays</td>
            </tr>
            <tr>
                <td>Heavy Rain</td>
                <td>&gt; 25mm (~1 inch)</td>
                <td>Flood risk, drainage capacity</td>
            </tr>
            <tr>
                <td>High Wind</td>
                <td>&gt; 50 km/h (31 mph)</td>
                <td>Construction, aviation, outdoor events</td>
            </tr>
        </table>
    </div>

    <div class="highlight-box {'warning' if total_extreme > 15 else ''}">
        <strong>Risk Assessment for {city}:</strong>
        <ul>
            <li>Overall extreme weather risk: {'High' if total_extreme > 20 else 'Moderate' if total_extreme > 10 else 'Low'}</li>
            <li>Most frequent extreme event: {max(events, key=lambda x: x['count'])['event']} ({max(events, key=lambda x: x['count'])['count']} days)</li>
            <li>Recommended mitigation: {'Enhanced monitoring and contingency planning' if total_extreme > 15 else 'Standard operational procedures'}</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Daily weather data from Open-Meteo ERA5 reanalysis.
        Events counted when threshold breached on any given day.
        Thresholds aligned with NOAA/WMO extreme weather definitions.
        Risk levels: High (&gt;10%), Moderate (5-10%), Low (&lt;5%).
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Extreme Event Detection",
        subtitle=f"{city} | Weather Risk Analysis",
        date=DATE,
        author=AUTHOR,
        data_source="Open-Meteo API (ERA5)",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Extreme_Event_Detection.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 4: MULTI-CITY COMPARISON
# ============================================================================

def generate_multi_city_report(data):
    """Generate Multi-City Comparison PDF"""
    print("\n🌡️ Generating Multi-City Comparison...")

    results = []
    for city, df in data.items():
        kpis = compute_weather_kpis(df)
        results.append({
            'city': city,
            'temp_avg': kpis['temp_avg'],
            'temp_range': kpis['temp_range'],
            'precip_total': kpis['precip_total'],
            'precip_days': kpis['precip_days'],
            'hdd': kpis['hdd'],
            'cdd': kpis['cdd'],
            'hot_days': kpis['hot_days'],
            'frost_days': kpis['frost_days']
        })

    # Sort by temperature
    results.sort(key=lambda x: x['temp_avg'], reverse=True)

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Warmest City</div>
            <div class="value hot" style="font-size: 18px;">{results[0]['city']}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Coldest City</div>
            <div class="value cold" style="font-size: 18px;">{results[-1]['city']}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Wettest City</div>
            <div class="value" style="font-size: 18px;">{max(results, key=lambda x: x['precip_total'])['city']}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Cities Compared</div>
            <div class="value">{len(results)}</div>
        </div>
    </div>

    <div class="section">
        <h2>Temperature Comparison</h2>
        <table>
            <tr>
                <th>City</th>
                <th>Avg Temp</th>
                <th>Temp Range</th>
                <th>Hot Days</th>
                <th>Frost Days</th>
            </tr>
    """

    for r in results:
        temp_class = 'hot' if r['temp_avg'] > 20 else 'cold' if r['temp_avg'] < 10 else ''
        content += f"""
            <tr>
                <td><strong>{r['city']}</strong></td>
                <td class="{temp_class}">{r['temp_avg']:.1f}°C</td>
                <td>{r['temp_range']:.1f}°C</td>
                <td>{r['hot_days']}</td>
                <td>{r['frost_days']}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Precipitation Comparison</h2>
        <table>
            <tr>
                <th>City</th>
                <th>Total Precip</th>
                <th>Rainy Days</th>
                <th>Rain Frequency</th>
            </tr>
    """

    for r in sorted(results, key=lambda x: x['precip_total'], reverse=True):
        content += f"""
            <tr>
                <td><strong>{r['city']}</strong></td>
                <td>{r['precip_total']:.1f} mm</td>
                <td>{r['precip_days']} days</td>
                <td>{r['precip_days']/90*100:.0f}%</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Energy Demand Comparison</h2>
        <table>
            <tr>
                <th>City</th>
                <th>HDD (Heating)</th>
                <th>CDD (Cooling)</th>
                <th>Dominant Season</th>
            </tr>
    """

    for r in results:
        dominant = 'Winter/Heating' if r['hdd'] > r['cdd'] else 'Summer/Cooling'
        content += f"""
            <tr>
                <td><strong>{r['city']}</strong></td>
                <td>{r['hdd']:.0f}</td>
                <td>{r['cdd']:.0f}</td>
                <td>{dominant}</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="highlight-box">
        <strong>Regional Climate Insights:</strong>
        <ul>
            <li>Temperature spread across cities: {results[0]['temp_avg'] - results[-1]['temp_avg']:.1f}°C</li>
            <li>Most variable climate: {max(results, key=lambda x: x['temp_range'])['city']} ({max(results, key=lambda x: x['temp_range'])['temp_range']:.0f}°C range)</li>
            <li>Highest energy demand: {max(results, key=lambda x: x['hdd'] + x['cdd'])['city']}</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> 90-day parallel analysis across all cities using Open-Meteo ERA5 data.
        Degree days computed with 18°C base. Hot days &gt;35°C, Frost days &lt;0°C.
        All cities analyzed over identical time period for direct comparison.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Multi-City Weather Comparison",
        subtitle="Regional Climate Analysis | 5 US Cities",
        date=DATE,
        author=AUTHOR,
        data_source="Open-Meteo API (ERA5)",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Multi_City_Comparison.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("CMS LinkedIn Proof Package - Weather Reports")
    print(f"Author: {AUTHOR}")
    print(f"Date: {DATE}")
    print("=" * 60)

    print("\n📥 Fetching Open-Meteo data...")
    data = fetch_all_locations()

    if not data:
        print("✗ Failed to fetch data")
        return

    print(f"\n✓ Retrieved data for {len(data)} cities")

    print("\n📄 Generating Reports...")
    primary_city = 'Chicago, IL'

    generate_climate_profile_report(data, primary_city)
    generate_degree_day_report(data)
    generate_extreme_event_report(data, primary_city)
    generate_multi_city_report(data)

    print("\n" + "=" * 60)
    print("✅ Weather Reports Complete!")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
