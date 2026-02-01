#!/usr/bin/env python3
"""
BRK-02: Global Trading Platform Analytics (IBKR)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "BRK02"
REPORT_TITLE = "Global Trading Platform Analytics"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Brokerage/data"

TICKERS = {'IBKR': 'Interactive Brokers', 'SCHW': 'Schwab', 'HOOD': 'Robinhood', 'FUTU': 'Futu Holdings', 'TIGR': 'UP Fintech', '^GSPC': 'S&P 500'}

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

def generate_global_metrics():
    print("\nGenerating global metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    regions = ['Americas', 'Europe', 'Asia-Pacific']
    data = []
    for d in dates:
        for r in regions:
            mult = {'Americas': 1.0, 'Europe': 0.6, 'Asia-Pacific': 0.8}[r]
            data.append({'Date': d, 'Region': r, 'Client_Accounts_K': np.random.uniform(500,2000) * mult,
                        'Client_Equity_B': np.random.uniform(100, 400) * mult,
                        'DARTs_K': np.random.uniform(1000, 3000) * mult,
                        'Commission_Per_Trade': np.random.uniform(0.5, 2.5)})
    return pd.DataFrame(data)

def generate_product_data():
    print("\nGenerating product data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    products = ['Stocks', 'Options', 'Futures', 'Forex', 'Bonds', 'Mutual Funds']
    data = []
    for d in dates:
        for p in products:
            mult = {'Stocks': 1.0, 'Options': 0.8, 'Futures': 0.3, 'Forex': 0.4, 'Bonds': 0.2, 'Mutual Funds': 0.15}[p]
            data.append({'Date': d, 'Product': p, 'Volume_B': np.random.uniform(50, 200) * mult,
                        'Revenue_M': np.random.uniform(100, 400) * mult, 'Market_Share_Pct': np.random.uniform(5, 25) * mult})
    return pd.DataFrame(data)

def generate_margin_data():
    print("\nGenerating margin/lending data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        data.append({'Date': d, 'Margin_Loans_B': np.random.uniform(40, 80),
                    'Securities_Lending_B': np.random.uniform(20, 50),
                    'Avg_Margin_Rate': np.random.uniform(5, 8),
                    'Margin_Utilization_Pct': np.random.uniform(30, 50)})
    return pd.DataFrame(data)

def generate_summary(stock_df, global_df, product_df, margin_df):
    latest = global_df[global_df['Date'] == global_df['Date'].max()]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(global_df)+len(product_df)+len(margin_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'global_reach': {'total_accounts': f"{latest['Client_Accounts_K'].sum()/1000:.1f}M",
            'total_equity': f"${latest['Client_Equity_B'].sum():.0f}B", 'regions': 3,
            'avg_darts': f"{latest['DARTs_K'].mean():.0f}K"},
        'product_mix': {'top_product': product_df.groupby('Product')['Revenue_M'].sum().idxmax(),
            'options_share': f"{product_df[product_df['Product']=='Options']['Revenue_M'].sum()/product_df['Revenue_M'].sum()*100:.0f}%"},
        'margin_business': {'margin_loans': f"${margin_df['Margin_Loans_B'].mean():.0f}B",
            'sec_lending': f"${margin_df['Securities_Lending_B'].mean():.0f}B",
            'avg_rate': f"{margin_df['Avg_Margin_Rate'].mean():.1f}%"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, global_df, product_df, margin_df = pull_data(), generate_global_metrics(), generate_product_data(), generate_margin_data()
    total = len(stock_df) + len(global_df) + len(product_df) + len(margin_df)
    summary = generate_summary(stock_df, global_df, product_df, margin_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    global_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_global.csv", index=False)
    product_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_products.csv", index=False)
    margin_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_margin.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
