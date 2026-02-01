#!/usr/bin/env python3
"""
BET-02: Sports Betting Market Dynamics (FanDuel/Flutter)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "BET02"
REPORT_TITLE = "Sports Betting Market Dynamics"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Betting/data"

TICKERS = {'FLUT': 'Flutter Entertainment', 'DKNG': 'DraftKings', 'MGM': 'MGM Resorts', 'PENN': 'Penn Entertainment', '^GSPC': 'S&P 500'}

def pull_data(years=5):
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

def generate_market_data():
    print("\nGenerating market share data...")
    dates = pd.date_range(end=datetime.now(), periods=20, freq='Q')
    operators = ['FanDuel', 'DraftKings', 'BetMGM', 'Caesars', 'Others']
    data = []
    base_share = {'FanDuel': 40, 'DraftKings': 30, 'BetMGM': 12, 'Caesars': 8, 'Others': 10}
    for d in dates:
        for op in operators:
            data.append({'Date': d, 'Operator': op,
                        'Market_Share_Pct': base_share[op] + np.random.normal(0, 2),
                        'Handle_B': np.random.uniform(1, 10) * (base_share[op]/40),
                        'GGR_M': np.random.uniform(50, 300) * (base_share[op]/40),
                        'Hold_Pct': np.random.uniform(7, 12)})
    return pd.DataFrame(data)

def generate_betting_data():
    print("\nGenerating betting behavior data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        nfl_season = d.month in [9,10,11,12,1,2]
        march_madness = d.month == 3 and d.day > 15
        mult = 2.0 if nfl_season else (1.8 if march_madness else 1.0)
        data.append({'Date': d, 'Total_Bets_M': np.random.uniform(5, 20) * mult,
                    'Avg_Bet_Size': np.random.uniform(25, 75),
                    'Parlay_Volume_Pct': np.random.uniform(20, 40),
                    'Live_Betting_Pct': np.random.uniform(25, 45),
                    'Mobile_Pct': np.random.uniform(85, 95)})
    return pd.DataFrame(data)

def generate_promo_data():
    print("\nGenerating promotional spend data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    data = []
    for d in dates:
        nfl_month = d.month in [8,9,10,11,12,1,2]
        promo_mult = 1.5 if nfl_month else 1.0
        data.append({'Date': d, 'Promo_Spend_M': np.random.uniform(100, 300) * promo_mult,
                    'Promo_as_Pct_GGR': np.random.uniform(20, 50),
                    'CAC': np.random.uniform(300, 600),
                    'LTV': np.random.uniform(800, 1500),
                    'LTV_CAC_Ratio': np.random.uniform(1.5, 3.5)})
    return pd.DataFrame(data)

def generate_summary(stock_df, market_df, betting_df, promo_df):
    latest = market_df[market_df['Date'] == market_df['Date'].max()]
    leader = latest.loc[latest['Market_Share_Pct'].idxmax()]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(market_df)+len(betting_df)+len(promo_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'market_structure': {'leader': leader['Operator'], 'leader_share': f"{leader['Market_Share_Pct']:.0f}%",
            'top_two_share': f"{latest[latest['Operator'].isin(['FanDuel','DraftKings'])]['Market_Share_Pct'].sum():.0f}%",
            'avg_hold': f"{latest['Hold_Pct'].mean():.1f}%"},
        'betting_behavior': {'daily_bets': f"{betting_df['Total_Bets_M'].mean():.1f}M",
            'avg_bet': f"${betting_df['Avg_Bet_Size'].mean():.0f}",
            'mobile_share': f"{betting_df['Mobile_Pct'].mean():.0f}%", 'live_share': f"{betting_df['Live_Betting_Pct'].mean():.0f}%"},
        'economics': {'avg_cac': f"${promo_df['CAC'].mean():.0f}", 'avg_ltv': f"${promo_df['LTV'].mean():.0f}",
            'ltv_cac': f"{promo_df['LTV_CAC_Ratio'].mean():.1f}x"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, market_df, betting_df, promo_df = pull_data(), generate_market_data(), generate_betting_data(), generate_promo_data()
    total = len(stock_df) + len(market_df) + len(betting_df) + len(promo_df)
    summary = generate_summary(stock_df, market_df, betting_df, promo_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    market_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_market.csv", index=False)
    betting_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_betting.csv", index=False)
    promo_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_promos.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
