#!/usr/bin/env python3
"""
WTH-02: Consumer Weather Platform Study (AccuWeather)
Author: Mboya Jeffers
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "WTH02"
REPORT_TITLE = "Consumer Weather Platform Study"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Weather/data"

def generate_audience_data():
    print("\nGenerating audience metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for i, d in enumerate(dates):
        growth = 1.05 ** i
        data.append({'Date': d, 'Monthly_Unique_Users_M': np.random.uniform(1000, 1500) * growth,
                    'App_Downloads_M': np.random.uniform(10, 30) * growth,
                    'DAU_MAU_Ratio': np.random.uniform(0.3, 0.45),
                    'Avg_Session_Min': np.random.uniform(2, 5),
                    'Push_Enabled_Pct': np.random.uniform(40, 60)})
    return pd.DataFrame(data)

def generate_advertising_data():
    print("\nGenerating advertising data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    data = []
    for d in dates:
        q4_mult = 1.3 if d.month in [10, 11, 12] else 1.0
        data.append({'Date': d, 'Ad_Revenue_M': np.random.uniform(15, 40) * q4_mult,
                    'CPM': np.random.uniform(5, 15),
                    'Ad_Fill_Rate_Pct': np.random.uniform(85, 98),
                    'Video_Ads_Pct': np.random.uniform(20, 40),
                    'Programmatic_Pct': np.random.uniform(70, 90)})
    return pd.DataFrame(data)

def generate_weather_data():
    print("\nGenerating weather event data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        storm_season = d.month in [4, 5, 6, 9, 10]
        mult = 2.0 if storm_season else 1.0
        data.append({'Date': d, 'Severe_Weather_Alerts_K': np.random.uniform(10, 100) * mult,
                    'MinuteCast_Usage_M': np.random.uniform(5, 20),
                    'Forecast_Accuracy_Pct': np.random.uniform(80, 95),
                    'RealFeel_Queries_M': np.random.uniform(20, 60),
                    'Traffic_Spike_Events': np.random.randint(0, 10) * mult})
    return pd.DataFrame(data)

def generate_summary(aud_df, ad_df, wth_df):
    latest = aud_df[aud_df['Date'] == aud_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(aud_df)+len(ad_df)+len(wth_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'audience': {'monthly_users': f"{latest['Monthly_Unique_Users_M']:.0f}M",
            'dau_mau': f"{latest['DAU_MAU_Ratio']*100:.0f}%",
            'app_downloads': f"{latest['App_Downloads_M']:.0f}M"},
        'advertising': {'monthly_revenue': f"${ad_df['Ad_Revenue_M'].mean():.0f}M",
            'avg_cpm': f"${ad_df['CPM'].mean():.0f}",
            'fill_rate': f"{ad_df['Ad_Fill_Rate_Pct'].mean():.0f}%"},
        'products': {'minutecast': 'Core differentiator',
            'realfeel': 'Proprietary index',
            'accuracy': f"{wth_df['Forecast_Accuracy_Pct'].mean():.0f}%"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    aud_df, ad_df, wth_df = generate_audience_data(), generate_advertising_data(), generate_weather_data()
    total = len(aud_df) + len(ad_df) + len(wth_df)
    summary = generate_summary(aud_df, ad_df, wth_df)
    aud_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_audience.csv", index=False)
    ad_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_advertising.csv", index=False)
    wth_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_weather.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
