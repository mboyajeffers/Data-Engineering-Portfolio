#!/usr/bin/env python3
"""
CMS LinkedIn Proof Package - Solar Reports
Author: Mboya Jeffers (MboyaJeffers9@gmail.com)
Generated: January 25, 2026

Data Source: NASA POWER API (free, global coverage)
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

OUTPUT_DIR = "/Users/mboyajeffers/Downloads/LinkedIn_Proof_Package/Solar"
AUTHOR = "Mboya Jeffers"
EMAIL = "MboyaJeffers9@gmail.com"
DATE = datetime.now().strftime("%B %d, %Y")

# NASA POWER API Base URL
NASA_POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

# Target locations (top US solar markets)
LOCATIONS = {
    'Phoenix, AZ': {'lat': 33.4484, 'lon': -112.0740},
    'Las Vegas, NV': {'lat': 36.1699, 'lon': -115.1398},
    'Austin, TX': {'lat': 30.2672, 'lon': -97.7431},
    'Denver, CO': {'lat': 39.7392, 'lon': -104.9903},
    'Los Angeles, CA': {'lat': 34.0522, 'lon': -118.2437}
}

# ============================================================================
# CSS (same as finance)
# ============================================================================

BASE_CSS = """
@page { size: letter; margin: 0.75in; }
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.5; }
.header { border-bottom: 3px solid #f59e0b; padding-bottom: 10px; margin-bottom: 20px; }
.header h1 { color: #1a1a2e; margin: 0; font-size: 28px; }
.header .subtitle { color: #666; font-size: 14px; margin-top: 5px; }
.header .date { color: #999; font-size: 12px; float: right; }
.kpi-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px; margin: 20px 0; }
.kpi-card { background: #fffbeb; border-radius: 8px; padding: 20px; text-align: center; border-left: 4px solid #f59e0b; }
.kpi-card .label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 1px; }
.kpi-card .value { font-size: 28px; font-weight: bold; color: #1a1a2e; margin: 8px 0; }
.kpi-card .value.good { color: #10b981; }
.kpi-card .value.moderate { color: #f59e0b; }
.kpi-card .value.poor { color: #ef4444; }
table { width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 13px; }
th { background: #f59e0b; color: white; padding: 12px 10px; text-align: left; }
td { padding: 10px; border-bottom: 1px solid #eee; }
tr:nth-child(even) { background: #fffbeb; }
.section { margin: 30px 0; }
.section h2 { color: #f59e0b; font-size: 18px; border-bottom: 1px solid #eee; padding-bottom: 8px; }
.highlight-box { background: #fef3c7; border-left: 4px solid #f59e0b; padding: 15px; margin: 15px 0; }
.methodology { background: #f8f9fa; padding: 15px; border-radius: 8px; font-size: 12px; color: #666; margin-top: 30px; }
.footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #eee; font-size: 11px; color: #999; }
.footer .author { color: #f59e0b; font-weight: bold; }
.seasonal-table td { text-align: center; }
.bar { height: 20px; background: #f59e0b; border-radius: 3px; }
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

def fetch_nasa_power_data(lat, lon, start_date, end_date):
    """Fetch solar irradiance data from NASA POWER API"""
    params = {
        'parameters': 'ALLSKY_SFC_SW_DWN,CLRSKY_SFC_SW_DWN,ALLSKY_SFC_SW_DNI,T2M,RH2M,WS10M',
        'community': 'RE',
        'longitude': lon,
        'latitude': lat,
        'start': start_date.strftime('%Y%m%d'),
        'end': end_date.strftime('%Y%m%d'),
        'format': 'JSON'
    }

    try:
        response = requests.get(NASA_POWER_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if 'properties' in data and 'parameter' in data['properties']:
            params_data = data['properties']['parameter']
            df = pd.DataFrame(params_data)
            df.index = pd.to_datetime(df.index, format='%Y%m%d')
            # Replace -999 (missing data indicator) with NaN
            df = df.replace(-999, np.nan)
            return df
    except Exception as e:
        print(f"    Error fetching data: {e}")
    return None

def fetch_all_locations():
    """Fetch data for all locations"""
    end_date = datetime.now() - timedelta(days=7)  # NASA POWER has ~1 week lag
    start_date = end_date - timedelta(days=365)

    all_data = {}
    for city, coords in LOCATIONS.items():
        print(f"  Fetching {city}...")
        df = fetch_nasa_power_data(coords['lat'], coords['lon'], start_date, end_date)
        if df is not None:
            all_data[city] = df
            print(f"    ✓ {len(df)} days of data")
        else:
            print(f"    ✗ Failed to fetch data")

    return all_data

# ============================================================================
# KPI CALCULATIONS
# ============================================================================

def compute_solar_kpis(df, system_capacity_kw=100):
    """Compute solar-specific KPIs"""
    kpis = {}

    # GHI - Global Horizontal Irradiance (kWh/m²/day)
    ghi = df['ALLSKY_SFC_SW_DWN']
    kpis['avg_ghi'] = ghi.mean()
    kpis['max_ghi'] = ghi.max()
    kpis['min_ghi'] = ghi.min()

    # DNI - Direct Normal Irradiance
    if 'ALLSKY_SFC_SW_DNI' in df.columns:
        dni = df['ALLSKY_SFC_SW_DNI']
        kpis['avg_dni'] = dni.mean()

    # Clear Sky GHI (theoretical maximum)
    clear_sky = df['CLRSKY_SFC_SW_DWN']
    kpis['avg_clear_sky'] = clear_sky.mean()

    # Performance Ratio (actual vs clear sky)
    kpis['performance_ratio'] = (ghi.mean() / clear_sky.mean()) * 100 if clear_sky.mean() > 0 else 0

    # Peak Sun Hours (PSH) - hours of 1 kW/m² equivalent
    kpis['avg_psh'] = ghi.mean()  # GHI in kWh/m²/day ≈ PSH

    # Estimated annual generation (kWh) for a system
    # Using standard formula: GHI × System Size × Performance Factor × 365
    performance_factor = 0.80  # Typical system efficiency
    kpis['estimated_annual_kwh'] = ghi.mean() * system_capacity_kw * performance_factor * 365

    # Capacity Factor estimate
    # CF = Actual Generation / (Capacity × 8760 hours)
    kpis['capacity_factor'] = (kpis['estimated_annual_kwh'] / (system_capacity_kw * 8760)) * 100

    # Variability (coefficient of variation)
    kpis['ghi_variability'] = (ghi.std() / ghi.mean()) * 100 if ghi.mean() > 0 else 0

    # Temperature (affects panel efficiency)
    if 'T2M' in df.columns:
        temp = df['T2M']
        kpis['avg_temp'] = temp.mean()
        kpis['max_temp'] = temp.max()
        kpis['hot_days'] = (temp > 35).sum()  # Days above 35°C

    return kpis

def compute_monthly_irradiance(df):
    """Compute monthly average GHI"""
    monthly = df['ALLSKY_SFC_SW_DWN'].resample('M').mean()
    return monthly

# ============================================================================
# REPORT 1: SITE IRRADIANCE ANALYSIS
# ============================================================================

def generate_site_irradiance_report(data, city='Phoenix, AZ'):
    """Generate Site Irradiance Analysis PDF"""
    print(f"\n☀️ Generating Site Irradiance Analysis for {city}...")

    if city not in data:
        print(f"  ✗ No data for {city}")
        return

    df = data[city]
    kpis = compute_solar_kpis(df)
    monthly = compute_monthly_irradiance(df)

    # Rating based on GHI
    if kpis['avg_ghi'] >= 5.5:
        rating = ('Excellent', 'good')
    elif kpis['avg_ghi'] >= 4.5:
        rating = ('Good', 'moderate')
    else:
        rating = ('Moderate', 'poor')

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Avg Daily GHI</div>
            <div class="value {rating[1]}">{kpis['avg_ghi']:.2f} kWh/m²</div>
        </div>
        <div class="kpi-card">
            <div class="label">Peak Sun Hours</div>
            <div class="value">{kpis['avg_psh']:.1f} hrs/day</div>
        </div>
        <div class="kpi-card">
            <div class="label">Performance Ratio</div>
            <div class="value">{kpis['performance_ratio']:.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Site Rating</div>
            <div class="value {rating[1]}">{rating[0]}</div>
        </div>
    </div>

    <div class="section">
        <h2>Irradiance Statistics</h2>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
                <th>Unit</th>
                <th>Interpretation</th>
            </tr>
            <tr>
                <td>Average GHI</td>
                <td><strong>{kpis['avg_ghi']:.2f}</strong></td>
                <td>kWh/m²/day</td>
                <td>{'Excellent solar resource' if kpis['avg_ghi'] > 5.5 else 'Good solar resource'}</td>
            </tr>
            <tr>
                <td>Maximum GHI</td>
                <td>{kpis['max_ghi']:.2f}</td>
                <td>kWh/m²/day</td>
                <td>Peak summer day</td>
            </tr>
            <tr>
                <td>Minimum GHI</td>
                <td>{kpis['min_ghi']:.2f}</td>
                <td>kWh/m²/day</td>
                <td>Worst recorded day</td>
            </tr>
            <tr>
                <td>Clear Sky GHI</td>
                <td>{kpis['avg_clear_sky']:.2f}</td>
                <td>kWh/m²/day</td>
                <td>Theoretical maximum</td>
            </tr>
            <tr>
                <td>GHI Variability</td>
                <td>{kpis['ghi_variability']:.1f}</td>
                <td>% CV</td>
                <td>{'Low variability' if kpis['ghi_variability'] < 30 else 'Moderate variability'}</td>
            </tr>
            <tr>
                <td>Avg Temperature</td>
                <td>{kpis.get('avg_temp', 0):.1f}</td>
                <td>°C</td>
                <td>{'Hot climate - derating needed' if kpis.get('avg_temp', 0) > 25 else 'Favorable temperature'}</td>
            </tr>
        </table>
    </div>

    <div class="section">
        <h2>Monthly Irradiance Pattern</h2>
        <table class="seasonal-table">
            <tr>
                <th>Month</th>
                <th>Avg GHI (kWh/m²/day)</th>
                <th>Visual</th>
            </tr>
    """

    max_ghi = monthly.max()
    for date, ghi in monthly.items():
        bar_width = (ghi / max_ghi) * 100 if max_ghi > 0 else 0
        content += f"""
            <tr>
                <td>{date.strftime('%b %Y')}</td>
                <td>{ghi:.2f}</td>
                <td><div class="bar" style="width: {bar_width}%"></div></td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="highlight-box">
        <strong>Site Assessment:</strong>
        <ul>
            <li>Location: {city} ({LOCATIONS[city]['lat']:.4f}°N, {LOCATIONS[city]['lon']:.4f}°W)</li>
            <li>Annual GHI: ~{kpis['avg_ghi'] * 365:.0f} kWh/m²/year</li>
            <li>Estimated capacity factor for fixed-tilt system: {kpis['capacity_factor']:.1f}%</li>
            <li>This site ranks in the {'top tier' if kpis['avg_ghi'] > 5.5 else 'good tier'} of US solar resources</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Irradiance data from NASA POWER API (CERES/MERRA-2).
        GHI = Global Horizontal Irradiance (total solar energy on horizontal surface).
        Performance Ratio = Actual GHI / Clear Sky GHI. Variability measured as coefficient of variation.
        Temperature affects panel efficiency (~0.4% loss per °C above 25°C).
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title=f"Site Irradiance Analysis",
        subtitle=f"{city} | Solar Resource Assessment",
        date=DATE,
        author=AUTHOR,
        data_source="NASA POWER API (CERES/MERRA-2)",
        content=content
    )

    safe_city = city.replace(', ', '_').replace(' ', '_')
    output_path = os.path.join(OUTPUT_DIR, f"CMS_Site_Irradiance_{safe_city}.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 2: CAPACITY FACTOR ESTIMATION
# ============================================================================

def generate_capacity_factor_report(data, city='Phoenix, AZ'):
    """Generate Capacity Factor Estimation PDF"""
    print(f"\n☀️ Generating Capacity Factor Estimation for {city}...")

    if city not in data:
        print(f"  ✗ No data for {city}")
        return

    df = data[city]

    # Model different system configurations
    configs = [
        {'name': 'Residential (5 kW)', 'capacity': 5, 'tilt': 'Fixed', 'efficiency': 0.78},
        {'name': 'Commercial (100 kW)', 'capacity': 100, 'tilt': 'Fixed', 'efficiency': 0.80},
        {'name': 'Utility (1 MW)', 'capacity': 1000, 'tilt': 'Tracking', 'efficiency': 0.85},
    ]

    ghi_avg = df['ALLSKY_SFC_SW_DWN'].mean()
    tracking_boost = 1.25  # Single-axis tracking typically adds 25%

    results = []
    for cfg in configs:
        boost = tracking_boost if cfg['tilt'] == 'Tracking' else 1.0
        effective_ghi = ghi_avg * boost
        annual_kwh = effective_ghi * cfg['capacity'] * cfg['efficiency'] * 365
        capacity_factor = (annual_kwh / (cfg['capacity'] * 8760)) * 100

        results.append({
            'name': cfg['name'],
            'capacity': cfg['capacity'],
            'tilt': cfg['tilt'],
            'efficiency': cfg['efficiency'] * 100,
            'annual_kwh': annual_kwh,
            'capacity_factor': capacity_factor
        })

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Base GHI</div>
            <div class="value">{ghi_avg:.2f} kWh/m²/day</div>
        </div>
        <div class="kpi-card">
            <div class="label">Best Capacity Factor</div>
            <div class="value good">{max(r['capacity_factor'] for r in results):.1f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Tracking Boost</div>
            <div class="value">+25%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Analysis Location</div>
            <div class="value" style="font-size: 18px;">{city}</div>
        </div>
    </div>

    <div class="section">
        <h2>System Configuration Comparison</h2>
        <table>
            <tr>
                <th>Configuration</th>
                <th>Capacity</th>
                <th>Mount Type</th>
                <th>System Eff.</th>
                <th>Annual Gen.</th>
                <th>Capacity Factor</th>
            </tr>
    """

    for r in results:
        cf_class = 'good' if r['capacity_factor'] > 20 else 'moderate'
        content += f"""
            <tr>
                <td><strong>{r['name']}</strong></td>
                <td>{r['capacity']:,} kW</td>
                <td>{r['tilt']}</td>
                <td>{r['efficiency']:.0f}%</td>
                <td>{r['annual_kwh']:,.0f} kWh</td>
                <td class="{cf_class}">{r['capacity_factor']:.1f}%</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Capacity Factor Benchmarks</h2>
        <table>
            <tr>
                <th>Rating</th>
                <th>Capacity Factor Range</th>
                <th>Typical Locations</th>
            </tr>
            <tr>
                <td><strong>Excellent</strong></td>
                <td>> 25%</td>
                <td>Desert Southwest with tracking</td>
            </tr>
            <tr>
                <td><strong>Good</strong></td>
                <td>20-25%</td>
                <td>Southwest US, fixed-tilt</td>
            </tr>
            <tr>
                <td><strong>Average</strong></td>
                <td>15-20%</td>
                <td>Most of continental US</td>
            </tr>
            <tr>
                <td><strong>Below Average</strong></td>
                <td>< 15%</td>
                <td>Pacific Northwest, Northeast</td>
            </tr>
        </table>
    </div>

    <div class="highlight-box">
        <strong>Key Findings:</strong>
        <ul>
            <li>Single-axis tracking increases capacity factor by ~25% vs fixed-tilt</li>
            <li>{city} supports capacity factors of {results[-1]['capacity_factor']:.1f}% for utility-scale</li>
            <li>Temperature derating reduces effective output in hot climates</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Capacity Factor = Annual Generation / (Capacity × 8760 hours).
        System efficiency accounts for inverter losses, wiring, soiling, and temperature derating.
        Tracking boost of 25% is typical for single-axis horizontal trackers.
        Data from NASA POWER API covering 12-month period.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Capacity Factor Estimation",
        subtitle=f"{city} | System Performance Modeling",
        date=DATE,
        author=AUTHOR,
        data_source="NASA POWER API",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Capacity_Factor_Estimation.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 3: SEASONAL PERFORMANCE FORECAST
# ============================================================================

def generate_seasonal_forecast_report(data, city='Phoenix, AZ'):
    """Generate Seasonal Performance Forecast PDF"""
    print(f"\n☀️ Generating Seasonal Performance Forecast for {city}...")

    if city not in data:
        return

    df = data[city]
    monthly = df['ALLSKY_SFC_SW_DWN'].resample('M').agg(['mean', 'std', 'min', 'max'])

    # Seasonal grouping
    def get_season(month):
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'

    df['Season'] = df.index.month.map(get_season)
    seasonal = df.groupby('Season')['ALLSKY_SFC_SW_DWN'].agg(['mean', 'std'])
    seasonal = seasonal.reindex(['Winter', 'Spring', 'Summer', 'Fall'])

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Summer Avg GHI</div>
            <div class="value good">{seasonal.loc['Summer', 'mean']:.2f} kWh/m²</div>
        </div>
        <div class="kpi-card">
            <div class="label">Winter Avg GHI</div>
            <div class="value moderate">{seasonal.loc['Winter', 'mean']:.2f} kWh/m²</div>
        </div>
        <div class="kpi-card">
            <div class="label">Seasonal Variation</div>
            <div class="value">{((seasonal.loc['Summer', 'mean'] / seasonal.loc['Winter', 'mean']) - 1) * 100:.0f}%</div>
        </div>
        <div class="kpi-card">
            <div class="label">Peak Month</div>
            <div class="value" style="font-size: 18px;">{monthly['mean'].idxmax().strftime('%B')}</div>
        </div>
    </div>

    <div class="section">
        <h2>Seasonal Irradiance Summary</h2>
        <table>
            <tr>
                <th>Season</th>
                <th>Avg GHI (kWh/m²/day)</th>
                <th>Std Dev</th>
                <th>Relative to Annual</th>
            </tr>
    """

    annual_mean = df['ALLSKY_SFC_SW_DWN'].mean()
    for season in ['Winter', 'Spring', 'Summer', 'Fall']:
        if season in seasonal.index:
            mean = seasonal.loc[season, 'mean']
            std = seasonal.loc[season, 'std']
            rel = ((mean / annual_mean) - 1) * 100
            rel_class = 'good' if rel > 0 else 'moderate'
            content += f"""
                <tr>
                    <td><strong>{season}</strong></td>
                    <td>{mean:.2f}</td>
                    <td>±{std:.2f}</td>
                    <td class="{rel_class}">{rel:+.1f}%</td>
                </tr>
            """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Monthly Forecast Detail</h2>
        <table>
            <tr>
                <th>Month</th>
                <th>Avg GHI</th>
                <th>Min</th>
                <th>Max</th>
                <th>Variability</th>
            </tr>
    """

    for date, row in monthly.iterrows():
        variability = (row['std'] / row['mean']) * 100 if row['mean'] > 0 else 0
        content += f"""
            <tr>
                <td>{date.strftime('%b %Y')}</td>
                <td>{row['mean']:.2f}</td>
                <td>{row['min']:.2f}</td>
                <td>{row['max']:.2f}</td>
                <td>{variability:.1f}%</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="highlight-box">
        <strong>Operational Planning Insights:</strong>
        <ul>
            <li>Summer production is {((seasonal.loc['Summer', 'mean'] / seasonal.loc['Winter', 'mean']) - 1) * 100:.0f}% higher than winter</li>
            <li>Plan maintenance during lower-production winter months</li>
            <li>Peak output months: {monthly['mean'].nlargest(3).index.strftime('%B').tolist()}</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> Monthly aggregation of daily GHI values from NASA POWER.
        Seasonal groupings: Winter (Dec-Feb), Spring (Mar-May), Summer (Jun-Aug), Fall (Sep-Nov).
        Variability measured as coefficient of variation within each month.
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Seasonal Performance Forecast",
        subtitle=f"{city} | 12-Month Irradiance Patterns",
        date=DATE,
        author=AUTHOR,
        data_source="NASA POWER API",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Seasonal_Performance_Forecast.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# REPORT 4: MULTI-SITE COMPARISON
# ============================================================================

def generate_multi_site_report(data):
    """Generate Multi-Site Comparison PDF"""
    print("\n☀️ Generating Multi-Site Comparison...")

    site_metrics = []
    for city, df in data.items():
        kpis = compute_solar_kpis(df)
        site_metrics.append({
            'city': city,
            'lat': LOCATIONS[city]['lat'],
            'lon': LOCATIONS[city]['lon'],
            'avg_ghi': kpis['avg_ghi'],
            'capacity_factor': kpis['capacity_factor'],
            'variability': kpis['ghi_variability'],
            'avg_temp': kpis.get('avg_temp', 0),
            'performance_ratio': kpis['performance_ratio']
        })

    # Sort by GHI
    site_metrics.sort(key=lambda x: x['avg_ghi'], reverse=True)

    content = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">Best Site</div>
            <div class="value good" style="font-size: 18px;">{site_metrics[0]['city']}</div>
        </div>
        <div class="kpi-card">
            <div class="label">Best GHI</div>
            <div class="value good">{site_metrics[0]['avg_ghi']:.2f} kWh/m²</div>
        </div>
        <div class="kpi-card">
            <div class="label">Sites Analyzed</div>
            <div class="value">{len(site_metrics)}</div>
        </div>
        <div class="kpi-card">
            <div class="label">GHI Spread</div>
            <div class="value">{site_metrics[0]['avg_ghi'] - site_metrics[-1]['avg_ghi']:.2f}</div>
        </div>
    </div>

    <div class="section">
        <h2>Site Ranking by Solar Resource</h2>
        <table>
            <tr>
                <th>Rank</th>
                <th>Location</th>
                <th>Coordinates</th>
                <th>Avg GHI</th>
                <th>Cap. Factor</th>
                <th>Variability</th>
            </tr>
    """

    for i, s in enumerate(site_metrics):
        ghi_class = 'good' if s['avg_ghi'] > 5.5 else 'moderate' if s['avg_ghi'] > 4.5 else ''
        content += f"""
            <tr>
                <td>{i+1}</td>
                <td><strong>{s['city']}</strong></td>
                <td>{s['lat']:.2f}°N, {abs(s['lon']):.2f}°W</td>
                <td class="{ghi_class}">{s['avg_ghi']:.2f}</td>
                <td>{s['capacity_factor']:.1f}%</td>
                <td>{s['variability']:.1f}%</td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="section">
        <h2>Site Suitability Assessment</h2>
        <table>
            <tr>
                <th>Location</th>
                <th>Resource Quality</th>
                <th>Temperature Impact</th>
                <th>Overall Rating</th>
            </tr>
    """

    for s in site_metrics:
        resource = 'Excellent' if s['avg_ghi'] > 5.5 else 'Good' if s['avg_ghi'] > 4.5 else 'Moderate'
        temp_impact = 'High Derating' if s['avg_temp'] > 28 else 'Moderate' if s['avg_temp'] > 22 else 'Low'
        overall = 'Tier 1' if s['avg_ghi'] > 5.5 and s['avg_temp'] < 30 else 'Tier 2' if s['avg_ghi'] > 4.5 else 'Tier 3'
        content += f"""
            <tr>
                <td><strong>{s['city']}</strong></td>
                <td>{resource}</td>
                <td>{temp_impact}</td>
                <td><strong>{overall}</strong></td>
            </tr>
        """

    content += f"""
        </table>
    </div>

    <div class="highlight-box">
        <strong>Portfolio Optimization Insights:</strong>
        <ul>
            <li>Geographic diversification reduces weather-related production risk</li>
            <li>Sites with lower correlation in daily output provide better portfolio stability</li>
            <li>Temperature derating is significant in desert locations ({site_metrics[0]['city']})</li>
        </ul>
    </div>

    <div class="methodology">
        <strong>Methodology:</strong> All sites analyzed with identical 12-month dataset from NASA POWER.
        GHI = Global Horizontal Irradiance. Capacity Factor assumes 100kW fixed-tilt system with 80% efficiency.
        Temperature impacts panel efficiency (~0.4% loss per °C above 25°C).
    </div>
    """

    template = Template(REPORT_TEMPLATE)
    html = template.render(
        title="Multi-Site Comparison",
        subtitle="Solar Resource Portfolio Analysis | 5 US Markets",
        date=DATE,
        author=AUTHOR,
        data_source="NASA POWER API",
        content=content
    )

    output_path = os.path.join(OUTPUT_DIR, "CMS_Multi_Site_Comparison.pdf")
    HTML(string=html).write_pdf(output_path, stylesheets=[CSS(string=BASE_CSS)])
    print(f"  ✓ Saved: {output_path}")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("=" * 60)
    print("CMS LinkedIn Proof Package - Solar Reports")
    print(f"Author: {AUTHOR}")
    print(f"Date: {DATE}")
    print("=" * 60)

    print("\n📥 Fetching NASA POWER data for all locations...")
    data = fetch_all_locations()

    if not data:
        print("✗ Failed to fetch any data. Check internet connection.")
        return

    print(f"\n✓ Retrieved data for {len(data)} locations")

    print("\n📄 Generating Reports...")
    primary_city = 'Phoenix, AZ'

    generate_site_irradiance_report(data, primary_city)
    generate_capacity_factor_report(data, primary_city)
    generate_seasonal_forecast_report(data, primary_city)
    generate_multi_site_report(data)

    print("\n" + "=" * 60)
    print("✅ Solar Reports Complete!")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()
