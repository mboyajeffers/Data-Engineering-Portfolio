#!/usr/bin/env python3
"""
WTH-01: Enterprise Weather Data Platform (IBM Weather)
Author: Mboya Jeffers
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "WTH01"
REPORT_TITLE = "Enterprise Weather Data Platform"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Weather/data"

def generate_api_data():
    print("\nGenerating API usage metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    base_calls = 10
    for i, d in enumerate(dates):
        growth = 1.12 ** i
        data.append({'Date': d, 'API_Calls_B': base_calls * growth,
                    'Enterprise_Customers_K': np.random.uniform(5, 15) * growth,
                    'Avg_Revenue_Per_Customer_K': np.random.uniform(50, 200),
                    'Uptime_Pct': np.random.uniform(99.9, 99.99),
                    'Latency_MS': np.random.uniform(50, 150)})
    return pd.DataFrame(data)

def generate_data_product_data():
    print("\nGenerating data product metrics...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    products = ['Current Conditions', 'Forecasts', 'Historical', 'Alerts', 'Imagery', 'Aviation']
    data = []
    for d in dates:
        for p in products:
            mult = {'Current Conditions': 1.5, 'Forecasts': 1.3, 'Historical': 0.8, 'Alerts': 1.0, 'Imagery': 0.6, 'Aviation': 0.4}[p]
            data.append({'Date': d, 'Product': p, 'Requests_B': np.random.uniform(0.5, 3) * mult,
                        'Revenue_M': np.random.uniform(5, 30) * mult,
                        'Accuracy_Pct': np.random.uniform(85, 98)})
    return pd.DataFrame(data)

def generate_industry_data():
    print("\nGenerating industry vertical data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        data.append({'Date': d, 'Energy_Requests_M': np.random.uniform(100, 300),
                    'Retail_Requests_M': np.random.uniform(50, 150),
                    'Insurance_Requests_M': np.random.uniform(30, 100),
                    'Aviation_Requests_M': np.random.uniform(40, 120),
                    'Agriculture_Requests_M': np.random.uniform(20, 80)})
    return pd.DataFrame(data)

def generate_summary(api_df, prod_df, ind_df):
    latest = api_df[api_df['Date'] == api_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(api_df)+len(prod_df)+len(ind_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'platform': {'daily_api_calls': f"{latest['API_Calls_B']:.0f}B",
            'enterprise_customers': f"{latest['Enterprise_Customers_K']:.0f}K",
            'uptime': f"{latest['Uptime_Pct']:.2f}%"},
        'products': {'top_product': prod_df.groupby('Product')['Revenue_M'].sum().idxmax(),
            'avg_accuracy': f"{prod_df['Accuracy_Pct'].mean():.0f}%"},
        'verticals': {'top_vertical': 'Energy',
            'daily_requests': f"{ind_df[['Energy_Requests_M','Retail_Requests_M','Insurance_Requests_M','Aviation_Requests_M','Agriculture_Requests_M']].sum(axis=1).mean():.0f}M"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    api_df, prod_df, ind_df = generate_api_data(), generate_data_product_data(), generate_industry_data()
    total = len(api_df) + len(prod_df) + len(ind_df)
    summary = generate_summary(api_df, prod_df, ind_df)
    api_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_api.csv", index=False)
    prod_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_products.csv", index=False)
    ind_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_industries.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
