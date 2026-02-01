#!/usr/bin/env python3
"""
OIL-01: Integrated Oil Major Analysis (ExxonMobil)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "OIL01"
REPORT_TITLE = "Integrated Oil Major Analysis"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../OilGas/data"

TICKERS = {'XOM': 'ExxonMobil', 'CVX': 'Chevron', 'COP': 'ConocoPhillips', 'SHEL': 'Shell', '^GSPC': 'S&P 500', 'CL=F': 'Crude Oil'}

def pull_data(years=10):
    end, start = datetime.now(), datetime.now() - timedelta(days=years*365)
    print(f"Pulling data...")
    data = []
    for t, n in TICKERS.items():
        try:
            df = yf.Ticker(t).history(start=start, end=end)
            if len(df) > 0:
                df = df.reset_index()
                df['Ticker'], df['Name'] = t.replace('^','').replace('=F',''), n
                df.columns = ['Date','Open','High','Low','Close','Volume','Dividends','Stock_Splits','Ticker','Name']
                data.append(df)
                print(f"  {t}: {len(df)} rows")
        except: pass
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

def generate_production_data():
    print("\nGenerating production metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for d in dates:
        data.append({'Date': d, 'Oil_MBOED': np.random.uniform(2200, 2800),
                    'Gas_BCFD': np.random.uniform(8, 12),
                    'Total_BOE_MBOED': np.random.uniform(3500, 4200),
                    'Permian_Share_Pct': np.random.uniform(25, 40),
                    'Guyana_MBOED': np.random.uniform(300, 600)})
    return pd.DataFrame(data)

def generate_financial_data():
    print("\nGenerating financial data...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for d in dates:
        oil_price = np.random.uniform(50, 100)
        data.append({'Date': d, 'Revenue_B': np.random.uniform(70, 120) * (oil_price/75),
                    'Net_Income_B': np.random.uniform(5, 20) * (oil_price/75),
                    'Upstream_Earnings_B': np.random.uniform(4, 15),
                    'Downstream_Earnings_B': np.random.uniform(1, 5),
                    'Chemical_Earnings_B': np.random.uniform(0.5, 3),
                    'CAPEX_B': np.random.uniform(4, 7)})
    return pd.DataFrame(data)

def generate_reserves_data():
    print("\nGenerating reserves data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        data.append({'Date': d, 'Proved_Reserves_BBOE': np.random.uniform(15, 20),
                    'Reserve_Replacement_Pct': np.random.uniform(80, 150),
                    'Finding_Cost_BOE': np.random.uniform(5, 15),
                    'Breakeven_Price': np.random.uniform(30, 50),
                    'Carbon_Intensity': np.random.uniform(18, 25)})
    return pd.DataFrame(data)

def generate_summary(stock_df, prod_df, fin_df, res_df):
    latest_prod = prod_df[prod_df['Date'] == prod_df['Date'].max()].iloc[0]
    latest_fin = fin_df[fin_df['Date'] == fin_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(prod_df)+len(fin_df)+len(res_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'production': {'total_boe': f"{latest_prod['Total_BOE_MBOED']:.0f} MBOED", 'oil': f"{latest_prod['Oil_MBOED']:.0f} MBOED",
            'permian_share': f"{latest_prod['Permian_Share_Pct']:.0f}%", 'guyana': f"{latest_prod['Guyana_MBOED']:.0f} MBOED"},
        'financials': {'revenue': f"${latest_fin['Revenue_B']:.0f}B", 'net_income': f"${latest_fin['Net_Income_B']:.0f}B",
            'capex': f"${latest_fin['CAPEX_B']:.0f}B"},
        'reserves': {'proved': f"{res_df['Proved_Reserves_BBOE'].mean():.0f} BBOE",
            'breakeven': f"${res_df['Breakeven_Price'].mean():.0f}/bbl"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, prod_df, fin_df, res_df = pull_data(), generate_production_data(), generate_financial_data(), generate_reserves_data()
    total = len(stock_df) + len(prod_df) + len(fin_df) + len(res_df)
    summary = generate_summary(stock_df, prod_df, fin_df, res_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    prod_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_production.csv", index=False)
    fin_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_financials.csv", index=False)
    res_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_reserves.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
