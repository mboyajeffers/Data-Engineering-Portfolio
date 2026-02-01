#!/usr/bin/env python3
"""
MED-01: Streaming Leader Content Analysis
Data Sources: Yahoo Finance, Synthetic streaming metrics
Target Rows: 300K-500K

Author: Mboya Jeffers
Date: 2026-01-31
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "MED01"
REPORT_TITLE = "Streaming Leader Content Analysis"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Media/data"

TICKERS = {
    'NFLX': 'Netflix',
    'DIS': 'Walt Disney',
    'WBD': 'Warner Bros Discovery',
    'PARA': 'Paramount Global',
    'CMCSA': 'Comcast',
    'AMZN': 'Amazon',
    'AAPL': 'Apple',
    'GOOGL': 'Alphabet',
    '^GSPC': 'S&P 500',
}

CONTENT_GENRES = ['Drama', 'Comedy', 'Action', 'Documentary', 'Reality', 'Kids', 'Anime', 'International']
REGIONS = ['North America', 'EMEA', 'LATAM', 'APAC']

def pull_stock_data(years=10):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    print(f"Pulling stock data for {len(TICKERS)} tickers...")

    all_data = []
    for ticker, name in TICKERS.items():
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(start=start_date, end=end_date, interval='1d')
            if len(df) > 0:
                df = df.reset_index()
                df['Ticker'] = ticker.replace('^', '')
                df['Name'] = name
                df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits', 'Ticker', 'Name']
                all_data.append(df)
                print(f"  {ticker}: {len(df)} rows")
        except Exception as e:
            print(f"  {ticker}: ERROR")

    combined = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    print(f"\nTotal stock data: {len(combined):,} rows")
    return combined

def generate_subscriber_data(years=10):
    print("\nGenerating subscriber data...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    dates = pd.date_range(start=start_date, end=end_date, freq='Q')

    streamers = ['Netflix', 'Disney+', 'Amazon Prime', 'HBO Max', 'Paramount+', 'Peacock', 'Apple TV+']
    sub_data = []

    base_subs = {'Netflix': 150, 'Disney+': 80, 'Amazon Prime': 200, 'HBO Max': 60, 'Paramount+': 40, 'Peacock': 30, 'Apple TV+': 25}

    for i, date in enumerate(dates):
        for streamer in streamers:
            base = base_subs.get(streamer, 50)
            growth = (1.05 + np.random.uniform(-0.02, 0.03)) ** (i/4)
            subs = base * growth * (1 + np.random.normal(0, 0.02))

            for region in REGIONS:
                region_share = {'North America': 0.35, 'EMEA': 0.30, 'LATAM': 0.15, 'APAC': 0.20}[region]
                sub_data.append({
                    'Date': date,
                    'Streamer': streamer,
                    'Region': region,
                    'Subscribers_M': subs * region_share * (1 + np.random.normal(0, 0.05)),
                    'ARPU': np.random.uniform(8, 16) if region == 'North America' else np.random.uniform(4, 10),
                    'Churn_Rate': np.random.uniform(0.02, 0.06)
                })

    df = pd.DataFrame(sub_data)
    print(f"Subscriber data: {len(df):,} rows")
    return df

def generate_content_data(years=10):
    print("\nGenerating content performance data...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    dates = pd.date_range(start=start_date, end=end_date, freq='M')

    content_data = []
    for date in dates:
        for genre in CONTENT_GENRES:
            # Monthly content releases and performance
            titles_released = np.random.poisson(15)
            for _ in range(titles_released):
                content_data.append({
                    'Date': date,
                    'Genre': genre,
                    'Content_Type': np.random.choice(['Series', 'Film', 'Documentary'], p=[0.5, 0.35, 0.15]),
                    'Budget_M': np.random.exponential(30) + 5,
                    'Hours_Viewed_M': np.random.exponential(50),
                    'Completion_Rate': np.random.uniform(0.3, 0.9),
                    'Rating': np.random.uniform(5, 9),
                    'Awards_Noms': np.random.poisson(0.5)
                })

    df = pd.DataFrame(content_data)
    print(f"Content data: {len(df):,} rows")
    return df

def generate_engagement_data(years=10):
    print("\nGenerating engagement metrics...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    dates = pd.date_range(start=start_date, end=end_date, freq='W')

    streamers = ['Netflix', 'Disney+', 'Amazon Prime', 'HBO Max']
    engagement = []

    for date in dates:
        for streamer in streamers:
            engagement.append({
                'Date': date,
                'Streamer': streamer,
                'Weekly_Hours_Per_User': np.random.uniform(5, 15),
                'Sessions_Per_Week': np.random.uniform(3, 8),
                'Avg_Session_Length_Min': np.random.uniform(30, 90),
                'Mobile_Share': np.random.uniform(0.25, 0.45),
                'TV_Share': np.random.uniform(0.45, 0.65),
                'Search_Volume_Index': np.random.uniform(50, 100)
            })

    df = pd.DataFrame(engagement)
    print(f"Engagement data: {len(df):,} rows")
    return df

def generate_summary(stock_df, sub_df, content_df, engage_df):
    print("\nGenerating summary...")

    latest_subs = sub_df[sub_df['Date'] == sub_df['Date'].max()].groupby('Streamer')['Subscribers_M'].sum()
    total_content = len(content_df)
    avg_budget = content_df['Budget_M'].mean()

    summary = {
        'report_metadata': {
            'report_code': REPORT_CODE,
            'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(),
            'data_start': str(sub_df['Date'].min()),
            'data_end': str(sub_df['Date'].max()),
            'total_rows_processed': len(sub_df) + len(content_df) + len(engage_df),
            'author': 'Mboya Jeffers',
            'email': 'MboyaJeffers9@gmail.com'
        },
        'subscriber_metrics': {
            'leader_subs': f"{latest_subs.max():.0f}M",
            'leader_name': latest_subs.idxmax(),
            'total_market_subs': f"{latest_subs.sum():.0f}M",
            'avg_arpu': f"${sub_df['ARPU'].mean():.2f}",
            'avg_churn': f"{sub_df['Churn_Rate'].mean()*100:.1f}%"
        },
        'content_metrics': {
            'total_titles_analyzed': f"{total_content:,}",
            'avg_budget': f"${avg_budget:.1f}M",
            'avg_hours_viewed': f"{content_df['Hours_Viewed_M'].mean():.1f}M",
            'avg_completion_rate': f"{content_df['Completion_Rate'].mean()*100:.1f}%",
            'top_genre': content_df.groupby('Genre')['Hours_Viewed_M'].sum().idxmax()
        },
        'engagement_insights': {
            'avg_weekly_hours': f"{engage_df['Weekly_Hours_Per_User'].mean():.1f}",
            'mobile_viewing_share': f"{engage_df['Mobile_Share'].mean()*100:.0f}%",
            'tv_viewing_share': f"{engage_df['TV_Share'].mean()*100:.0f}%"
        }
    }
    return summary

def main():
    print("=" * 60)
    print(f"{REPORT_CODE}: {REPORT_TITLE}")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stock_df = pull_stock_data(years=10)
    sub_df = generate_subscriber_data(years=10)
    content_df = generate_content_data(years=10)
    engage_df = generate_engagement_data(years=10)

    total_rows = len(stock_df) + len(sub_df) + len(content_df) + len(engage_df)
    print(f"\nTotal data rows: {total_rows:,}")

    summary = generate_summary(stock_df, sub_df, content_df, engage_df)

    print("\nSaving outputs...")
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    sub_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_subscribers.csv", index=False)
    content_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_content.csv", index=False)
    engage_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_engagement.csv", index=False)

    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n{'='*60}\nTOTAL DATA ROWS: {total_rows:,}\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total_rows

if __name__ == "__main__":
    main()
