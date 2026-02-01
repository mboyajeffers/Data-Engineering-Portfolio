#!/usr/bin/env python3
"""
WTH-03: Agricultural Weather Intelligence (DTN)
Author: Mboya Jeffers
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "WTH03"
REPORT_TITLE = "Agricultural Weather Intelligence"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Weather/data"

def generate_ag_data():
    print("\nGenerating agricultural metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for i, d in enumerate(dates):
        growth = 1.08 ** i
        data.append({'Date': d, 'Farm_Subscribers_K': np.random.uniform(200, 400) * growth,
                    'Acres_Covered_M': np.random.uniform(100, 200) * growth,
                    'ARR_M': np.random.uniform(100, 200) * growth,
                    'Churn_Rate_Pct': np.random.uniform(5, 12),
                    'NPS_Score': np.random.uniform(40, 60)})
    return pd.DataFrame(data)

def generate_crop_data():
    print("\nGenerating crop-specific data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    crops = ['Corn', 'Soybeans', 'Wheat', 'Cotton', 'Specialty']
    data = []
    for d in dates:
        for c in crops:
            mult = {'Corn': 1.5, 'Soybeans': 1.3, 'Wheat': 0.8, 'Cotton': 0.6, 'Specialty': 0.4}[c]
            data.append({'Date': d, 'Crop': c, 'Subscribers_K': np.random.uniform(20, 80) * mult,
                        'Alerts_Sent_K': np.random.uniform(50, 200) * mult,
                        'Spray_Window_Accuracy_Pct': np.random.uniform(85, 95),
                        'Frost_Alert_Accuracy_Pct': np.random.uniform(90, 98)})
    return pd.DataFrame(data)

def generate_commodity_data():
    print("\nGenerating commodity data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        harvest = d.month in [9, 10, 11]
        planting = d.month in [4, 5, 6]
        mult = 1.5 if harvest or planting else 1.0
        data.append({'Date': d, 'Corn_Price': np.random.uniform(4, 8),
                    'Soy_Price': np.random.uniform(10, 16),
                    'Wheat_Price': np.random.uniform(5, 9),
                    'Weather_Impact_Score': np.random.uniform(1, 10) * mult,
                    'Market_Alerts_K': np.random.uniform(5, 20) * mult})
    return pd.DataFrame(data)

def generate_summary(ag_df, crop_df, comm_df):
    latest = ag_df[ag_df['Date'] == ag_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(ag_df)+len(crop_df)+len(comm_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'platform': {'subscribers': f"{latest['Farm_Subscribers_K']:.0f}K",
            'acres': f"{latest['Acres_Covered_M']:.0f}M",
            'arr': f"${latest['ARR_M']:.0f}M"},
        'accuracy': {'spray_window': f"{crop_df['Spray_Window_Accuracy_Pct'].mean():.0f}%",
            'frost_alert': f"{crop_df['Frost_Alert_Accuracy_Pct'].mean():.0f}%"},
        'crops': {'top_crop': crop_df.groupby('Crop')['Subscribers_K'].sum().idxmax(),
            'daily_alerts': f"{crop_df['Alerts_Sent_K'].mean():.0f}K"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ag_df, crop_df, comm_df = generate_ag_data(), generate_crop_data(), generate_commodity_data()
    total = len(ag_df) + len(crop_df) + len(comm_df)
    summary = generate_summary(ag_df, crop_df, comm_df)
    ag_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_agriculture.csv", index=False)
    crop_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_crops.csv", index=False)
    comm_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_commodities.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
