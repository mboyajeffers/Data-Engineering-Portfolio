#!/usr/bin/env python3
"""
WTH-04: AI Weather Prediction Platform (Tomorrow.io)
Author: Mboya Jeffers
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "WTH04"
REPORT_TITLE = "AI Weather Prediction Platform"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Weather/data"

def generate_ai_metrics():
    print("\nGenerating AI platform metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for i, d in enumerate(dates):
        growth = 1.15 ** i
        data.append({'Date': d, 'Model_Accuracy_Pct': 80 + i * 0.3,
                    'Prediction_Horizon_Hours': 24 + i * 0.5,
                    'API_Customers_K': np.random.uniform(1, 10) * growth,
                    'ARR_M': np.random.uniform(20, 80) * growth,
                    'ML_Training_Compute_Hours_K': np.random.uniform(100, 500) * growth})
    return pd.DataFrame(data)

def generate_radar_data():
    print("\nGenerating radar network data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    data = []
    for i, d in enumerate(dates):
        growth = 1.1 ** i
        data.append({'Date': d, 'Active_Radars': int(5 + i * 0.5),
                    'Coverage_Area_KM2_M': np.random.uniform(1, 5) * growth,
                    'Data_Points_Per_Day_B': np.random.uniform(10, 50) * growth,
                    'Latency_Seconds': np.random.uniform(30, 120),
                    'Uptime_Pct': np.random.uniform(99, 99.9)})
    return pd.DataFrame(data)

def generate_enterprise_data():
    print("\nGenerating enterprise customer data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        data.append({'Date': d, 'Aviation_Customers': np.random.randint(10, 50),
                    'Logistics_Customers': np.random.randint(20, 80),
                    'Sports_Customers': np.random.randint(5, 30),
                    'Insurance_Customers': np.random.randint(10, 40),
                    'Govt_Contracts': np.random.randint(5, 20)})
    return pd.DataFrame(data)

def generate_summary(ai_df, radar_df, ent_df):
    latest = ai_df[ai_df['Date'] == ai_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(ai_df)+len(radar_df)+len(ent_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'ai_platform': {'accuracy': f"{latest['Model_Accuracy_Pct']:.0f}%",
            'horizon': f"{latest['Prediction_Horizon_Hours']:.0f} hours",
            'arr': f"${latest['ARR_M']:.0f}M"},
        'radar_network': {'active_radars': f"{radar_df['Active_Radars'].max():.0f}",
            'daily_data': f"{radar_df['Data_Points_Per_Day_B'].mean():.0f}B points"},
        'enterprise': {'aviation': f"{ent_df['Aviation_Customers'].mean():.0f}",
            'logistics': f"{ent_df['Logistics_Customers'].mean():.0f}",
            'total_verticals': '5+'}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ai_df, radar_df, ent_df = generate_ai_metrics(), generate_radar_data(), generate_enterprise_data()
    total = len(ai_df) + len(radar_df) + len(ent_df)
    summary = generate_summary(ai_df, radar_df, ent_df)
    ai_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_ai.csv", index=False)
    radar_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_radar.csv", index=False)
    ent_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_enterprise.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
