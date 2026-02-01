#!/usr/bin/env python3
"""
ECM-01: E-commerce Marketplace Analytics (Amazon)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "ECM01"
REPORT_TITLE = "E-commerce Marketplace Analytics"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Ecommerce/data"

TICKERS = {'AMZN': 'Amazon', 'WMT': 'Walmart', 'TGT': 'Target', 'COST': 'Costco', '^GSPC': 'S&P 500'}

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

def generate_gmv_data():
    print("\nGenerating GMV metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    base_gmv = 100
    for i, d in enumerate(dates):
        growth = 1.05 ** i
        q4_mult = 1.4 if d.month in [10, 11, 12] else 1.0
        data.append({'Date': d, 'GMV_B': base_gmv * growth * q4_mult * (1+np.random.normal(0,0.05)),
                    '3P_Seller_GMV_B': base_gmv * growth * 0.6 * q4_mult,
                    '1P_GMV_B': base_gmv * growth * 0.4 * q4_mult,
                    'Active_Customers_M': np.random.uniform(200, 350) * growth,
                    'Prime_Members_M': np.random.uniform(150, 250)})
    return pd.DataFrame(data)

def generate_category_data():
    print("\nGenerating category data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    categories = ['Electronics', 'Apparel', 'Home & Kitchen', 'Grocery', 'Media', 'Health & Beauty']
    data = []
    for d in dates:
        for c in categories:
            mult = {'Electronics': 1.2, 'Apparel': 0.9, 'Home & Kitchen': 1.0, 'Grocery': 0.8, 'Media': 0.6, 'Health & Beauty': 0.7}[c]
            data.append({'Date': d, 'Category': c, 'GMV_B': np.random.uniform(5, 20) * mult,
                        'Unit_Sales_M': np.random.uniform(50, 200) * mult,
                        'Avg_Order_Value': np.random.uniform(40, 150),
                        'Return_Rate_Pct': np.random.uniform(5, 20)})
    return pd.DataFrame(data)

def generate_fulfillment_data():
    print("\nGenerating fulfillment data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        prime_day = (d.month == 7 and d.day in [12, 13])
        bf_cm = (d.month == 11 and d.day > 20)
        mult = 3.0 if prime_day else (2.5 if bf_cm else 1.0)
        data.append({'Date': d, 'Packages_Shipped_M': np.random.uniform(10, 30) * mult,
                    'Same_Day_Pct': np.random.uniform(15, 30),
                    'One_Day_Pct': np.random.uniform(25, 40),
                    'FBA_Units_M': np.random.uniform(5, 15) * mult,
                    'Delivery_Speed_Hours': np.random.uniform(24, 72)})
    return pd.DataFrame(data)

def generate_summary(stock_df, gmv_df, cat_df, ful_df):
    latest = gmv_df[gmv_df['Date'] == gmv_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(gmv_df)+len(cat_df)+len(ful_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'gmv_metrics': {'total_gmv': f"${latest['GMV_B']:.0f}B", '3p_share': f"~60%",
            'active_customers': f"{latest['Active_Customers_M']:.0f}M", 'prime_members': f"{latest['Prime_Members_M']:.0f}M"},
        'category_performance': {'top_category': cat_df.groupby('Category')['GMV_B'].sum().idxmax(),
            'avg_aov': f"${cat_df['Avg_Order_Value'].mean():.0f}"},
        'fulfillment': {'daily_packages': f"{ful_df['Packages_Shipped_M'].mean():.0f}M",
            'same_day': f"{ful_df['Same_Day_Pct'].mean():.0f}%",
            'one_day': f"{ful_df['One_Day_Pct'].mean():.0f}%"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, gmv_df, cat_df, ful_df = pull_data(), generate_gmv_data(), generate_category_data(), generate_fulfillment_data()
    total = len(stock_df) + len(gmv_df) + len(cat_df) + len(ful_df)
    summary = generate_summary(stock_df, gmv_df, cat_df, ful_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    gmv_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_gmv.csv", index=False)
    cat_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_categories.csv", index=False)
    ful_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_fulfillment.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
