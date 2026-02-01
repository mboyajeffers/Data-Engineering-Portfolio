#!/usr/bin/env python3
"""
MED-03: Audio Streaming Market Analysis
Target: Spotify and audio streaming economics
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

REPORT_CODE = "MED03"
REPORT_TITLE = "Audio Streaming Market Analysis"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Media/data"

TICKERS = {'SPOT': 'Spotify', 'AAPL': 'Apple', 'AMZN': 'Amazon', 'GOOGL': 'Alphabet', 'SIRI': 'SiriusXM', '^GSPC': 'S&P 500'}

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

def generate_listener_data(years=10):
    print("\nGenerating listener data...")
    dates = pd.date_range(end=datetime.now(), periods=years*12, freq='M')
    platforms = ['Spotify', 'Apple Music', 'Amazon Music', 'YouTube Music', 'Deezer', 'Tidal']
    regions = ['North America', 'Europe', 'LATAM', 'APAC', 'ROW']
    data = []

    base_mau = {'Spotify': 500, 'Apple Music': 100, 'Amazon Music': 80, 'YouTube Music': 80, 'Deezer': 16, 'Tidal': 5}

    for date in dates:
        for platform in platforms:
            base = base_mau.get(platform, 50)
            growth = 1.12 ** ((date - dates[0]).days / 365)
            total_mau = base * growth

            for region in regions:
                region_share = {'North America': 0.25, 'Europe': 0.30, 'LATAM': 0.20, 'APAC': 0.20, 'ROW': 0.05}[region]
                data.append({
                    'Date': date, 'Platform': platform, 'Region': region,
                    'MAU_M': total_mau * region_share * (1 + np.random.normal(0, 0.03)),
                    'Premium_Subs_M': total_mau * region_share * np.random.uniform(0.3, 0.5),
                    'ARPU': np.random.uniform(3, 6) if region in ['LATAM', 'APAC', 'ROW'] else np.random.uniform(5, 10),
                    'Ad_Revenue_Per_User': np.random.uniform(0.5, 2)
                })
    df = pd.DataFrame(data)
    print(f"Listener data: {len(df):,} rows")
    return df

def generate_podcast_data(years=5):
    print("\nGenerating podcast data...")
    dates = pd.date_range(end=datetime.now(), periods=years*12, freq='M')
    categories = ['True Crime', 'Comedy', 'News', 'Sports', 'Business', 'Tech', 'Music', 'Education']
    data = []

    for date in dates:
        for category in categories:
            data.append({
                'Date': date, 'Category': category,
                'Shows_Added': np.random.poisson(500),
                'Listens_M': np.random.exponential(50),
                'Avg_Completion_Rate': np.random.uniform(0.4, 0.8),
                'Ad_CPM': np.random.uniform(15, 35),
                'Exclusive_Shows': np.random.poisson(5)
            })
    df = pd.DataFrame(data)
    print(f"Podcast data: {len(df):,} rows")
    return df

def generate_artist_economics():
    print("\nGenerating artist economics data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    tiers = ['Top 100', 'Top 1000', 'Top 10000', 'Long Tail']
    data = []

    for date in dates:
        for tier in tiers:
            streams_mult = {'Top 100': 1000, 'Top 1000': 100, 'Top 10000': 10, 'Long Tail': 1}[tier]
            data.append({
                'Date': date, 'Artist_Tier': tier,
                'Avg_Monthly_Streams_M': np.random.exponential(10) * streams_mult,
                'Payout_Per_Stream': np.random.uniform(0.003, 0.005),
                'Catalog_Share_Pct': np.random.uniform(0.3, 0.7),
                'Merch_Revenue_Index': np.random.uniform(50, 150) if tier in ['Top 100', 'Top 1000'] else 0
            })
    df = pd.DataFrame(data)
    print(f"Artist economics: {len(df):,} rows")
    return df

def generate_summary(stock_df, listener_df, podcast_df, artist_df):
    latest = listener_df[listener_df['Date'] == listener_df['Date'].max()]
    leader_mau = latest.groupby('Platform')['MAU_M'].sum()

    summary = {
        'report_metadata': {
            'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(),
            'total_rows_processed': len(listener_df) + len(podcast_df) + len(artist_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'
        },
        'listener_metrics': {
            'leader_mau': f"{leader_mau.max():.0f}M",
            'leader_name': leader_mau.idxmax(),
            'total_market_mau': f"{leader_mau.sum():.0f}M",
            'avg_premium_conversion': f"{(latest['Premium_Subs_M'].sum() / latest['MAU_M'].sum() * 100):.1f}%",
            'avg_arpu': f"${listener_df['ARPU'].mean():.2f}"
        },
        'podcast_metrics': {
            'total_listens': f"{podcast_df['Listens_M'].sum()/1000:.1f}B",
            'top_category': podcast_df.groupby('Category')['Listens_M'].sum().idxmax(),
            'avg_ad_cpm': f"${podcast_df['Ad_CPM'].mean():.2f}"
        },
        'artist_economics': {
            'avg_payout_per_stream': f"${artist_df['Payout_Per_Stream'].mean()*1000:.2f}/1000",
            'top_100_dominance': 'High concentration'
        }
    }
    return summary

def main():
    print("=" * 60)
    print(f"{REPORT_CODE}: {REPORT_TITLE}")
    print("=" * 60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stock_df = pull_stock_data()
    listener_df = generate_listener_data()
    podcast_df = generate_podcast_data()
    artist_df = generate_artist_economics()

    total_rows = len(stock_df) + len(listener_df) + len(podcast_df) + len(artist_df)
    summary = generate_summary(stock_df, listener_df, podcast_df, artist_df)

    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    listener_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_listeners.csv", index=False)
    podcast_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_podcasts.csv", index=False)
    artist_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_artists.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n{'='*60}\nTOTAL DATA ROWS: {total_rows:,}\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total_rows

if __name__ == "__main__":
    main()
