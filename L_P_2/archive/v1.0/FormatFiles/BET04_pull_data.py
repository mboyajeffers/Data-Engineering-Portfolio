#!/usr/bin/env python3
"""
BET-04: Traditional Casino Digital Integration (Caesars)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "BET04"
REPORT_TITLE = "Traditional Casino Digital Integration"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Betting/data"

TICKERS = {'CZR': 'Caesars Entertainment', 'MGM': 'MGM Resorts', 'WYNN': 'Wynn Resorts', 'PENN': 'Penn Entertainment', '^GSPC': 'S&P 500'}

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

def generate_omnichannel_data():
    print("\nGenerating omnichannel metrics...")
    dates = pd.date_range(end=datetime.now(), periods=20, freq='Q')
    data = []
    for i, d in enumerate(dates):
        digital_growth = 1.15 ** i
        data.append({'Date': d, 'Digital_Revenue_M': np.random.uniform(100, 300) * digital_growth,
                    'Brick_Mortar_Revenue_B': np.random.uniform(2, 4),
                    'Digital_Pct_Total': np.random.uniform(5, 20) * (1 + i*0.02),
                    'Cross_Channel_Users_K': np.random.uniform(500, 1500) * digital_growth,
                    'Rewards_Members_M': np.random.uniform(50, 70)})
    return pd.DataFrame(data)

def generate_property_data():
    print("\nGenerating property performance data...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    regions = ['Las Vegas', 'Atlantic City', 'Regional', 'Tribal', 'International']
    data = []
    for d in dates:
        for r in regions:
            mult = {'Las Vegas': 1.5, 'Atlantic City': 0.8, 'Regional': 1.0, 'Tribal': 0.6, 'International': 0.4}[r]
            data.append({'Date': d, 'Region': r, 'GGR_M': np.random.uniform(200, 800) * mult,
                        'Occupancy_Pct': np.random.uniform(70, 95),
                        'RevPAR': np.random.uniform(100, 300) * mult,
                        'F_B_Revenue_M': np.random.uniform(50, 200) * mult})
    return pd.DataFrame(data)

def generate_loyalty_data():
    print("\nGenerating loyalty program data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        weekend = d.dayofweek >= 5
        mult = 1.3 if weekend else 1.0
        data.append({'Date': d, 'New_Enrollments_K': np.random.uniform(5, 20) * mult,
                    'Active_Members_M': np.random.uniform(55, 65),
                    'Tier_Credits_Earned_B': np.random.uniform(0.5, 2) * mult,
                    'Reward_Redemptions_M': np.random.uniform(1, 5) * mult,
                    'Digital_Engagement_Pct': np.random.uniform(30, 50)})
    return pd.DataFrame(data)

def generate_summary(stock_df, omni_df, prop_df, loyalty_df):
    latest = omni_df[omni_df['Date'] == omni_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(omni_df)+len(prop_df)+len(loyalty_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'digital_integration': {'digital_revenue': f"${latest['Digital_Revenue_M']:.0f}M",
            'digital_share': f"{latest['Digital_Pct_Total']:.1f}%",
            'cross_channel': f"{latest['Cross_Channel_Users_K']:.0f}K users",
            'rewards_members': f"{latest['Rewards_Members_M']:.0f}M"},
        'property_performance': {'top_region': prop_df.groupby('Region')['GGR_M'].sum().idxmax(),
            'avg_occupancy': f"{prop_df['Occupancy_Pct'].mean():.0f}%",
            'avg_revpar': f"${prop_df['RevPAR'].mean():.0f}"},
        'loyalty_metrics': {'active_members': f"{loyalty_df['Active_Members_M'].mean():.0f}M",
            'daily_enrollments': f"{loyalty_df['New_Enrollments_K'].mean():.0f}K",
            'digital_engagement': f"{loyalty_df['Digital_Engagement_Pct'].mean():.0f}%"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, omni_df, prop_df, loyalty_df = pull_data(), generate_omnichannel_data(), generate_property_data(), generate_loyalty_data()
    total = len(stock_df) + len(omni_df) + len(prop_df) + len(loyalty_df)
    summary = generate_summary(stock_df, omni_df, prop_df, loyalty_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    omni_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_omnichannel.csv", index=False)
    prop_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_properties.csv", index=False)
    loyalty_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_loyalty.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
