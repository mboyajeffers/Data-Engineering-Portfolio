#!/usr/bin/env python3
"""
GAM-03: Open World Gaming Economics (Take-Two)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "GAM03"
REPORT_TITLE = "Open World Gaming Economics"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Gaming/data"

TICKERS = {'TTWO': 'Take-Two Interactive', 'EA': 'Electronic Arts', 'UBSFY': 'Ubisoft', 'NTDOY': 'Nintendo', '^GSPC': 'S&P 500'}

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

def generate_gta_data():
    print("\nGenerating GTA Online metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    base_rev = 200
    for i, d in enumerate(dates):
        growth = 0.98 ** i  # Slow decay
        content_boost = 1.3 if d.month in [6, 12] else 1.0  # DLC releases
        data.append({'Date': d, 'GTA_Online_Rev_M': base_rev * growth * content_boost * (1+np.random.normal(0,0.1)),
                    'Shark_Card_Sales_M': np.random.uniform(50, 150) * growth,
                    'Active_Players_M': np.random.uniform(100, 180) * growth,
                    'Total_Units_Sold_M': 200 + i * 2,  # Cumulative
                    'Heists_Completed_M': np.random.uniform(5, 20)})
    return pd.DataFrame(data)

def generate_rdr_data():
    print("\nGenerating Red Dead data...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for i, d in enumerate(dates):
        launch_decay = 0.9 ** max(0, i-8)
        data.append({'Date': d, 'RDR2_Rev_M': np.random.uniform(30, 100) * launch_decay,
                    'RDO_Players_M': np.random.uniform(5, 30) * launch_decay,
                    'Gold_Bar_Sales_M': np.random.uniform(5, 30) * launch_decay,
                    'Cumulative_Units_M': min(60, 20 + i * 1.5)})
    return pd.DataFrame(data)

def generate_2k_data():
    print("\nGenerating 2K Sports data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        nba_season = d.month in [10,11,12,1,2,3,4,5,6]
        mult = 1.5 if nba_season else 0.7
        data.append({'Date': d, 'NBA2K_DAU_M': np.random.uniform(3, 10) * mult,
                    'MyTeam_Transactions_K': np.random.uniform(100, 500) * mult,
                    'VC_Sales_M': np.random.uniform(5, 20) * mult,
                    'WWE2K_Players_K': np.random.uniform(100, 500),
                    'Borderlands_DAU_K': np.random.uniform(50, 300)})
    return pd.DataFrame(data)

def generate_summary(stock_df, gta_df, rdr_df, twok_df):
    latest_gta = gta_df[gta_df['Date'] == gta_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(gta_df)+len(rdr_df)+len(twok_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'gta_metrics': {'total_units': f"{latest_gta['Total_Units_Sold_M']:.0f}M", 'online_revenue': f"${latest_gta['GTA_Online_Rev_M']:.0f}M/Q",
            'active_players': f"{latest_gta['Active_Players_M']:.0f}M", 'shark_cards': f"${latest_gta['Shark_Card_Sales_M']:.0f}M/Q"},
        'rdr_metrics': {'cumulative_units': f"{rdr_df['Cumulative_Units_M'].max():.0f}M",
            'online_status': 'Sunset', 'franchise_value': 'Strong IP'},
        '2k_metrics': {'nba_dau': f"{twok_df['NBA2K_DAU_M'].mean():.0f}M",
            'vc_revenue': f"${twok_df['VC_Sales_M'].mean():.0f}M/day avg",
            'myteam_engagement': 'High'}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, gta_df, rdr_df, twok_df = pull_data(), generate_gta_data(), generate_rdr_data(), generate_2k_data()
    total = len(stock_df) + len(gta_df) + len(rdr_df) + len(twok_df)
    summary = generate_summary(stock_df, gta_df, rdr_df, twok_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    gta_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_gta.csv", index=False)
    rdr_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_rdr.csv", index=False)
    twok_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_2k.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
