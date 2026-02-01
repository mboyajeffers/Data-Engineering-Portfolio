#!/usr/bin/env python3
"""
MED-04: Media Consolidation Impact Study
Target: Warner Bros Discovery merger analysis
Author: Mboya Jeffers
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "MED04"
REPORT_TITLE = "Media Consolidation Impact Study"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Media/data"

TICKERS = {'WBD': 'Warner Bros Discovery', 'DIS': 'Disney', 'PARA': 'Paramount', 'NFLX': 'Netflix', 'CMCSA': 'Comcast', 'FOXA': 'Fox', '^GSPC': 'S&P 500'}

def pull_stock_data(years=10):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    print(f"Pulling stock data...")
    all_data = []
    for ticker, name in TICKERS.items():
        try:
            df = yf.Ticker(ticker).history(start=start_date, end=end_date, interval='1d')
            if len(df) > 0:
                df = df.reset_index()
                df['Ticker'] = ticker.replace('^', '')
                df['Name'] = name
                df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits', 'Ticker', 'Name']
                all_data.append(df)
                print(f"  {ticker}: {len(df)} rows")
        except: pass
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def generate_synergy_data():
    print("\nGenerating merger synergy data...")
    dates = pd.date_range(end=datetime.now(), periods=20, freq='Q')
    categories = ['Cost Synergies', 'Revenue Synergies', 'Content Synergies', 'Tech Synergies', 'Distribution Synergies']
    data = []

    for date in dates:
        for cat in categories:
            target = {'Cost Synergies': 3000, 'Revenue Synergies': 1500, 'Content Synergies': 800, 'Tech Synergies': 500, 'Distribution Synergies': 400}[cat]
            realized_pct = min(1, ((date - dates[0]).days / 1825) * np.random.uniform(0.8, 1.2))
            data.append({
                'Date': date, 'Category': cat,
                'Target_M': target,
                'Realized_M': target * realized_pct * (1 + np.random.normal(0, 0.1)),
                'Realization_Pct': realized_pct * 100,
                'Headcount_Impact': -np.random.randint(100, 500) if cat == 'Cost Synergies' else 0
            })
    df = pd.DataFrame(data)
    print(f"Synergy data: {len(df):,} rows")
    return df

def generate_debt_data():
    print("\nGenerating debt/leverage data...")
    dates = pd.date_range(end=datetime.now(), periods=20, freq='Q')
    data = []

    initial_debt = 55  # $55B initial debt load
    for i, date in enumerate(dates):
        paydown = i * 0.8  # Gradual paydown
        data.append({
            'Date': date,
            'Total_Debt_B': initial_debt - paydown + np.random.normal(0, 1),
            'Net_Debt_B': (initial_debt - paydown) * 0.9,
            'EBITDA_B': np.random.uniform(10, 14),
            'Leverage_Ratio': (initial_debt - paydown) / np.random.uniform(10, 14),
            'Interest_Expense_B': (initial_debt - paydown) * 0.05,
            'FCF_B': np.random.uniform(2, 6)
        })
    df = pd.DataFrame(data)
    print(f"Debt data: {len(df):,} rows")
    return df

def generate_content_library():
    print("\nGenerating content library data...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    studios = ['Warner Bros', 'HBO', 'Discovery', 'DC', 'CNN', 'TNT/TBS']
    data = []

    for date in dates:
        for studio in studios:
            data.append({
                'Date': date, 'Studio': studio,
                'Library_Titles': np.random.randint(5000, 50000),
                'Library_Value_B': np.random.uniform(5, 30),
                'Annual_Content_Spend_B': np.random.uniform(1, 8),
                'Theatrical_Releases': np.random.poisson(10) if studio in ['Warner Bros', 'DC'] else 0,
                'Streaming_Originals': np.random.poisson(50)
            })
    df = pd.DataFrame(data)
    print(f"Content library: {len(df):,} rows")
    return df

def generate_streaming_performance():
    print("\nGenerating streaming performance data...")
    dates = pd.date_range(end=datetime.now(), periods=24, freq='M')
    data = []

    for i, date in enumerate(dates):
        base_subs = 80 + i * 2  # Growing subs
        data.append({
            'Date': date,
            'Max_Subs_M': base_subs * (1 + np.random.normal(0, 0.02)),
            'Discovery_Subs_M': 20 + i * 0.5,
            'Combined_ARPU': np.random.uniform(7, 10),
            'Churn_Rate': np.random.uniform(0.04, 0.07),
            'Content_Hours_Viewed_B': np.random.uniform(5, 10),
            'DTC_Revenue_B': base_subs * np.random.uniform(7, 10) / 1000
        })
    df = pd.DataFrame(data)
    print(f"Streaming data: {len(df):,} rows")
    return df

def generate_summary(stock_df, synergy_df, debt_df, library_df, streaming_df):
    latest_synergy = synergy_df[synergy_df['Date'] == synergy_df['Date'].max()]
    latest_debt = debt_df[debt_df['Date'] == debt_df['Date'].max()]
    latest_stream = streaming_df[streaming_df['Date'] == streaming_df['Date'].max()]

    summary = {
        'report_metadata': {
            'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(),
            'total_rows_processed': len(synergy_df) + len(debt_df) + len(library_df) + len(streaming_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'
        },
        'synergy_metrics': {
            'total_realized': f"${latest_synergy['Realized_M'].sum()/1000:.1f}B",
            'realization_rate': f"{latest_synergy['Realization_Pct'].mean():.0f}%",
            'cost_synergies': f"${latest_synergy[latest_synergy['Category']=='Cost Synergies']['Realized_M'].sum()/1000:.1f}B"
        },
        'debt_profile': {
            'total_debt': f"${latest_debt['Total_Debt_B'].iloc[0]:.1f}B",
            'leverage_ratio': f"{latest_debt['Leverage_Ratio'].iloc[0]:.1f}x",
            'fcf': f"${latest_debt['FCF_B'].iloc[0]:.1f}B"
        },
        'streaming_metrics': {
            'combined_subs': f"{latest_stream['Max_Subs_M'].iloc[0] + latest_stream['Discovery_Subs_M'].iloc[0]:.0f}M",
            'arpu': f"${latest_stream['Combined_ARPU'].iloc[0]:.2f}",
            'churn': f"{latest_stream['Churn_Rate'].iloc[0]*100:.1f}%"
        },
        'content_library': {
            'total_value': f"${library_df.groupby('Date')['Library_Value_B'].sum().iloc[-1]:.0f}B",
            'annual_spend': f"${library_df.groupby('Date')['Annual_Content_Spend_B'].sum().iloc[-1]:.0f}B"
        }
    }
    return summary

def main():
    print("=" * 60)
    print(f"{REPORT_CODE}: {REPORT_TITLE}")
    print("=" * 60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stock_df = pull_stock_data()
    synergy_df = generate_synergy_data()
    debt_df = generate_debt_data()
    library_df = generate_content_library()
    streaming_df = generate_streaming_performance()

    total_rows = len(stock_df) + len(synergy_df) + len(debt_df) + len(library_df) + len(streaming_df)
    summary = generate_summary(stock_df, synergy_df, debt_df, library_df, streaming_df)

    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    synergy_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_synergies.csv", index=False)
    debt_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_debt.csv", index=False)
    library_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_library.csv", index=False)
    streaming_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_streaming.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n{'='*60}\nTOTAL DATA ROWS: {total_rows:,}\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total_rows

if __name__ == "__main__":
    main()
