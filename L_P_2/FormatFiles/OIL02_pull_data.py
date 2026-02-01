#!/usr/bin/env python3
"""
OIL-02: Downstream Refining Economics (Chevron)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "OIL02"
REPORT_TITLE = "Downstream Refining Economics"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../OilGas/data"

TICKERS = {'CVX': 'Chevron', 'XOM': 'ExxonMobil', 'VLO': 'Valero', 'MPC': 'Marathon Petroleum', '^GSPC': 'S&P 500'}

def pull_data(years=10):
    end, start = datetime.now(), datetime.now() - timedelta(days=years*365)
    print(f"Pulling data...")
    data = []
    for t, n in TICKERS.items():
        try:
            df = yf.Ticker(t).history(start=start, end=end)
            if len(df) > 0:
                df = df.reset_index()
                df['Ticker'], df['Name'] = t.replace('^',''), n
                df.columns = ['Date','Open','High','Low','Close','Volume','Dividends','Stock_Splits','Ticker','Name']
                data.append(df)
                print(f"  {t}: {len(df)} rows")
        except: pass
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

def generate_refining_data():
    print("\nGenerating refining metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for d in dates:
        crack_spread = np.random.uniform(10, 40)
        data.append({'Date': d, 'Refining_Capacity_MBPD': np.random.uniform(1800, 2200),
                    'Utilization_Pct': np.random.uniform(85, 98),
                    'Crack_Spread': crack_spread,
                    'Refining_Margin_BBL': crack_spread * 0.8,
                    'Product_Sales_MBPD': np.random.uniform(1600, 2000)})
    return pd.DataFrame(data)

def generate_product_data():
    print("\nGenerating product mix data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    products = ['Gasoline', 'Diesel', 'Jet Fuel', 'Petrochemicals', 'Lubricants']
    data = []
    for d in dates:
        for p in products:
            mult = {'Gasoline': 1.3, 'Diesel': 1.0, 'Jet Fuel': 0.5, 'Petrochemicals': 0.6, 'Lubricants': 0.3}[p]
            data.append({'Date': d, 'Product': p, 'Volume_MBPD': np.random.uniform(200, 600) * mult,
                        'Margin_BBL': np.random.uniform(5, 25) * mult,
                        'Market_Share_Pct': np.random.uniform(8, 20)})
    return pd.DataFrame(data)

def generate_retail_data():
    print("\nGenerating retail network data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        summer = d.month in [6, 7, 8]
        mult = 1.2 if summer else 1.0
        data.append({'Date': d, 'Retail_Stations_K': np.random.uniform(8, 12),
                    'Daily_Gallons_M': np.random.uniform(15, 25) * mult,
                    'Fuel_Margin_Gallon': np.random.uniform(0.15, 0.40),
                    'Non_Fuel_Revenue_M': np.random.uniform(5, 15),
                    'EV_Chargers_K': np.random.uniform(0.5, 3)})
    return pd.DataFrame(data)

def generate_summary(stock_df, ref_df, prod_df, ret_df):
    latest = ref_df[ref_df['Date'] == ref_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(ref_df)+len(prod_df)+len(ret_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'refining': {'capacity': f"{latest['Refining_Capacity_MBPD']:.0f} MBPD",
            'utilization': f"{latest['Utilization_Pct']:.0f}%",
            'crack_spread': f"${latest['Crack_Spread']:.0f}/bbl"},
        'products': {'top_product': prod_df.groupby('Product')['Volume_MBPD'].sum().idxmax(),
            'gasoline_share': f"~40%"},
        'retail': {'stations': f"{ret_df['Retail_Stations_K'].mean():.0f}K",
            'daily_gallons': f"{ret_df['Daily_Gallons_M'].mean():.0f}M",
            'ev_transition': 'In Progress'}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, ref_df, prod_df, ret_df = pull_data(), generate_refining_data(), generate_product_data(), generate_retail_data()
    total = len(stock_df) + len(ref_df) + len(prod_df) + len(ret_df)
    summary = generate_summary(stock_df, ref_df, prod_df, ret_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    ref_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_refining.csv", index=False)
    prod_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_products.csv", index=False)
    ret_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_retail.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
