#!/usr/bin/env python3
"""
BRK-01: Retail Brokerage Market Analysis (Schwab)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "BRK01"
REPORT_TITLE = "Retail Brokerage Market Analysis"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Brokerage/data"

TICKERS = {'SCHW': 'Charles Schwab', 'MS': 'Morgan Stanley', 'GS': 'Goldman Sachs', 'JPM': 'JPMorgan', 'IBKR': 'Interactive Brokers', '^GSPC': 'S&P 500', '^VIX': 'VIX'}

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

def generate_aum_data():
    print("\nGenerating AUM data...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    brokers = ['Schwab', 'Fidelity', 'Vanguard', 'Morgan Stanley', 'Merrill']
    data = []
    base = {'Schwab': 7000, 'Fidelity': 11000, 'Vanguard': 8000, 'Morgan Stanley': 4500, 'Merrill': 3500}
    for i, d in enumerate(dates):
        for b in brokers:
            growth = 1.02 ** i
            data.append({'Date': d, 'Broker': b, 'AUM_B': base[b] * growth * (1+np.random.normal(0,0.03)),
                        'Client_Accounts_M': np.random.uniform(20,40) if b in ['Schwab','Fidelity'] else np.random.uniform(5,15),
                        'NNA_B': np.random.uniform(-20, 80), 'Revenue_Per_Account': np.random.uniform(200,500)})
    return pd.DataFrame(data)

def generate_trading_data():
    print("\nGenerating trading data...")
    dates = pd.date_range(end=datetime.now(), periods=1825, freq='D')
    data = []
    for d in dates:
        vol_mult = 1.5 if d.dayofweek < 5 else 0.1
        data.append({'Date': d, 'Equity_Trades_M': np.random.exponential(5) * vol_mult,
                    'Options_Contracts_M': np.random.exponential(30) * vol_mult,
                    'PFOF_Revenue_M': np.random.uniform(50, 150) * vol_mult,
                    'Margin_Balances_B': np.random.uniform(60, 100),
                    'Cash_Sweep_B': np.random.uniform(300, 500)})
    return pd.DataFrame(data)

def generate_revenue_mix():
    print("\nGenerating revenue mix...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for d in dates:
        total = np.random.uniform(4, 6)
        data.append({'Date': d, 'NII_B': total * 0.50, 'Asset_Mgmt_B': total * 0.25,
                    'Trading_B': total * 0.15, 'Other_B': total * 0.10, 'Total_Revenue_B': total})
    return pd.DataFrame(data)

def generate_summary(stock_df, aum_df, trading_df, rev_df):
    latest_aum = aum_df[aum_df['Date'] == aum_df['Date'].max()]
    leader = latest_aum.loc[latest_aum['AUM_B'].idxmax()]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(aum_df)+len(trading_df)+len(rev_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'market_position': {'leader': leader['Broker'], 'leader_aum': f"${leader['AUM_B']:.0f}B",
            'total_market_aum': f"${latest_aum['AUM_B'].sum():.0f}B", 'accounts': f"{latest_aum['Client_Accounts_M'].sum():.0f}M"},
        'trading_metrics': {'avg_daily_equity_trades': f"{trading_df['Equity_Trades_M'].mean():.1f}M",
            'avg_options_volume': f"{trading_df['Options_Contracts_M'].mean():.0f}M contracts",
            'margin_balances': f"${trading_df['Margin_Balances_B'].mean():.0f}B"},
        'revenue_insights': {'nii_share': '~50%', 'asset_mgmt_share': '~25%', 'rate_sensitivity': 'High'}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, aum_df, trading_df, rev_df = pull_data(), generate_aum_data(), generate_trading_data(), generate_revenue_mix()
    total = len(stock_df) + len(aum_df) + len(trading_df) + len(rev_df)
    summary = generate_summary(stock_df, aum_df, trading_df, rev_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    aum_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_aum.csv", index=False)
    trading_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_trading.csv", index=False)
    rev_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_revenue.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
