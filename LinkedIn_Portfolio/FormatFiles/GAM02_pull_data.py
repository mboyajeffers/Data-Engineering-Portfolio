#!/usr/bin/env python3
"""
GAM-02: Sports Gaming Franchise Study (Electronic Arts)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "GAM02"
REPORT_TITLE = "Sports Gaming Franchise Study"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Gaming/data"

TICKERS = {'EA': 'Electronic Arts', 'TTWO': 'Take-Two', 'MSFT': 'Microsoft', 'SNE': 'Sony', '^GSPC': 'S&P 500'}

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

def generate_sports_data():
    print("\nGenerating sports franchise data...")
    dates = pd.date_range(end=datetime.now(), periods=20, freq='Q')
    sports = ['FIFA/EA Sports FC', 'Madden NFL', 'NHL', 'UFC', 'F1', 'PGA Tour']
    data = []
    base_rev = {'FIFA/EA Sports FC': 1500, 'Madden NFL': 800, 'NHL': 200, 'UFC': 150, 'F1': 100, 'PGA Tour': 50}
    for d in dates:
        for s in sports:
            season_mult = 1.3 if (s == 'Madden NFL' and d.month in [8,9]) or (s == 'FIFA/EA Sports FC' and d.month in [9,10]) else 1.0
            data.append({'Date': d, 'Franchise': s, 'Revenue_M': base_rev[s] * season_mult * (1+np.random.normal(0,0.08)),
                        'Unit_Sales_M': np.random.uniform(1, 15) * (base_rev[s]/1500),
                        'Ultimate_Team_Rev_M': base_rev[s] * 0.5 * (1+np.random.normal(0,0.1)),
                        'Weekly_Players_M': np.random.uniform(5, 50) * (base_rev[s]/1500)})
    return pd.DataFrame(data)

def generate_live_service_data():
    print("\nGenerating live service data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        weekend = d.dayofweek >= 5
        mult = 1.5 if weekend else 1.0
        data.append({'Date': d, 'FUT_Transactions_K': np.random.uniform(500, 2000) * mult,
                    'Pack_Opens_M': np.random.uniform(5, 20) * mult,
                    'Matches_Played_M': np.random.uniform(10, 50) * mult,
                    'Avg_Spend_Per_User': np.random.uniform(5, 25),
                    'Conversion_Rate_Pct': np.random.uniform(3, 8)})
    return pd.DataFrame(data)

def generate_esports_data():
    print("\nGenerating esports data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    data = []
    for d in dates:
        data.append({'Date': d, 'Tournament_Viewers_M': np.random.uniform(10, 50),
                    'Prize_Pool_M': np.random.uniform(1, 5),
                    'Partner_Revenue_M': np.random.uniform(5, 20),
                    'Pro_Players_Registered': np.random.uniform(50, 200) * 1000,
                    'Watch_Hours_M': np.random.uniform(50, 200)})
    return pd.DataFrame(data)

def generate_summary(stock_df, sports_df, live_df, esports_df):
    latest = sports_df[sports_df['Date'] == sports_df['Date'].max()]
    top_sport = latest.loc[latest['Revenue_M'].idxmax()]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(sports_df)+len(live_df)+len(esports_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'franchise_metrics': {'top_franchise': top_sport['Franchise'], 'top_revenue': f"${top_sport['Revenue_M']:.0f}M",
            'ultimate_team_share': '~50%', 'total_weekly_players': f"{latest['Weekly_Players_M'].sum():.0f}M"},
        'live_services': {'daily_transactions': f"{live_df['FUT_Transactions_K'].mean():.0f}K",
            'conversion_rate': f"{live_df['Conversion_Rate_Pct'].mean():.1f}%",
            'avg_spend': f"${live_df['Avg_Spend_Per_User'].mean():.0f}"},
        'esports': {'monthly_viewers': f"{esports_df['Tournament_Viewers_M'].mean():.0f}M",
            'watch_hours': f"{esports_df['Watch_Hours_M'].mean():.0f}M/month",
            'prize_pools': f"${esports_df['Prize_Pool_M'].sum():.0f}M total"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, sports_df, live_df, esports_df = pull_data(), generate_sports_data(), generate_live_service_data(), generate_esports_data()
    total = len(stock_df) + len(sports_df) + len(live_df) + len(esports_df)
    summary = generate_summary(stock_df, sports_df, live_df, esports_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    sports_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_sports.csv", index=False)
    live_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_liveservice.csv", index=False)
    esports_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_esports.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
