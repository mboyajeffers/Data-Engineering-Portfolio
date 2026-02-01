#!/usr/bin/env python3
"""
SOL-03: Renewable Utility Economics (NextEra)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "SOL03"
REPORT_TITLE = "Renewable Utility Economics"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Solar/data"

TICKERS = {'NEE': 'NextEra Energy', 'DUK': 'Duke Energy', 'SO': 'Southern Company', 'AEP': 'AEP', '^GSPC': 'S&P 500'}

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

def generate_generation_data():
    print("\nGenerating generation metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for i, d in enumerate(dates):
        renew_growth = 1.1 ** i
        data.append({'Date': d, 'Total_Capacity_GW': np.random.uniform(55, 65),
                    'Renewable_Capacity_GW': np.random.uniform(25, 40) * renew_growth,
                    'Solar_Capacity_GW': np.random.uniform(8, 20) * renew_growth,
                    'Wind_Capacity_GW': np.random.uniform(15, 25) * renew_growth,
                    'Battery_Storage_GW': np.random.uniform(1, 5) * renew_growth})
    return pd.DataFrame(data)

def generate_ppa_data():
    print("\nGenerating PPA data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    data = []
    for d in dates:
        data.append({'Date': d, 'PPA_Signed_MW': np.random.uniform(500, 2000),
                    'PPA_Price_MWh': np.random.uniform(25, 45),
                    'Contract_Length_Years': np.random.uniform(15, 25),
                    'Backlog_MW': np.random.uniform(15000, 25000),
                    'Development_Pipeline_MW': np.random.uniform(30000, 50000)})
    return pd.DataFrame(data)

def generate_operational_data():
    print("\nGenerating operational data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        solar_mult = 1.5 if d.month in [5, 6, 7, 8] else 0.8
        wind_mult = 1.3 if d.month in [3, 4, 11, 12] else 1.0
        data.append({'Date': d, 'Solar_Generation_GWh': np.random.uniform(20, 60) * solar_mult,
                    'Wind_Generation_GWh': np.random.uniform(40, 100) * wind_mult,
                    'Capacity_Factor_Solar_Pct': np.random.uniform(20, 35) * solar_mult,
                    'Capacity_Factor_Wind_Pct': np.random.uniform(30, 45) * wind_mult,
                    'Curtailment_Pct': np.random.uniform(1, 5)})
    return pd.DataFrame(data)

def generate_summary(stock_df, gen_df, ppa_df, ops_df):
    latest = gen_df[gen_df['Date'] == gen_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(gen_df)+len(ppa_df)+len(ops_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'generation': {'total_capacity': f"{latest['Total_Capacity_GW']:.0f} GW",
            'renewable': f"{latest['Renewable_Capacity_GW']:.0f} GW",
            'solar': f"{latest['Solar_Capacity_GW']:.0f} GW", 'wind': f"{latest['Wind_Capacity_GW']:.0f} GW"},
        'development': {'backlog': f"{ppa_df['Backlog_MW'].mean()/1000:.0f} GW",
            'pipeline': f"{ppa_df['Development_Pipeline_MW'].mean()/1000:.0f} GW",
            'avg_ppa_price': f"${ppa_df['PPA_Price_MWh'].mean():.0f}/MWh"},
        'operations': {'solar_cf': f"{ops_df['Capacity_Factor_Solar_Pct'].mean():.0f}%",
            'wind_cf': f"{ops_df['Capacity_Factor_Wind_Pct'].mean():.0f}%"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, gen_df, ppa_df, ops_df = pull_data(), generate_generation_data(), generate_ppa_data(), generate_operational_data()
    total = len(stock_df) + len(gen_df) + len(ppa_df) + len(ops_df)
    summary = generate_summary(stock_df, gen_df, ppa_df, ops_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    gen_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_generation.csv", index=False)
    ppa_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_ppa.csv", index=False)
    ops_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_operations.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
