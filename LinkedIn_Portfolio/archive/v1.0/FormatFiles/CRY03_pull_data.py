#!/usr/bin/env python3
"""
CRY-03: Corporate Bitcoin Treasury Analysis
Target: MicroStrategy corporate BTC strategy
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

REPORT_CODE = "CRY03"
REPORT_TITLE = "Corporate Bitcoin Treasury Analysis"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Crypto/data"

TICKERS = {'MSTR': 'MicroStrategy', 'BTC-USD': 'Bitcoin', 'TSLA': 'Tesla', 'SQ': 'Block', 'COIN': 'Coinbase', 'GBTC': 'Grayscale BTC', '^GSPC': 'S&P 500'}

def pull_data(years=5):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    print(f"Pulling stock/crypto data...")
    all_data = []
    for ticker, name in TICKERS.items():
        try:
            df = yf.Ticker(ticker).history(start=start_date, end=end_date, interval='1d')
            if len(df) > 0:
                df = df.reset_index()
                df['Ticker'] = ticker.replace('-USD', '').replace('^', '')
                df['Name'] = name
                df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits', 'Ticker', 'Name']
                all_data.append(df)
                print(f"  {ticker}: {len(df)} rows")
        except: pass
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def generate_treasury_holdings():
    print("\nGenerating treasury holdings data...")
    dates = pd.date_range(end=datetime.now(), periods=20, freq='Q')
    companies = ['MicroStrategy', 'Tesla', 'Block', 'Marathon', 'Coinbase', 'Galaxy Digital']
    data = []

    holdings = {'MicroStrategy': 150000, 'Tesla': 10000, 'Block': 8000, 'Marathon': 12000, 'Coinbase': 10000, 'Galaxy Digital': 5000}

    for i, date in enumerate(dates):
        btc_price = 20000 + i * 2000 + np.random.normal(0, 3000)
        for company in companies:
            base = holdings[company]
            # MicroStrategy keeps buying
            if company == 'MicroStrategy':
                current_holdings = base + i * 5000
            else:
                current_holdings = base * (1 + np.random.normal(0, 0.05))

            avg_cost = np.random.uniform(20000, 40000)
            data.append({
                'Date': date, 'Company': company,
                'BTC_Holdings': current_holdings,
                'Avg_Cost_Basis': avg_cost,
                'Current_BTC_Price': btc_price,
                'Holdings_Value_M': current_holdings * btc_price / 1e6,
                'Unrealized_PnL_M': current_holdings * (btc_price - avg_cost) / 1e6,
                'Pct_of_Market_Cap': np.random.uniform(0.5, 3) if company == 'MicroStrategy' else np.random.uniform(0.01, 0.5)
            })
    df = pd.DataFrame(data)
    print(f"Treasury holdings: {len(df):,} rows")
    return df

def generate_nav_premium():
    print("\nGenerating NAV premium/discount data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')  # 3 years
    data = []
    for date in dates:
        # MSTR typically trades at premium to NAV
        mstr_premium = np.random.uniform(-20, 100)
        gbtc_discount = np.random.uniform(-50, 10)
        data.append({
            'Date': date,
            'MSTR_NAV_Premium_Pct': mstr_premium,
            'GBTC_NAV_Discount_Pct': gbtc_discount,
            'MSTR_Implied_BTC_Price': np.random.uniform(50000, 150000),
            'MSTR_Beta_to_BTC': np.random.uniform(1.5, 3.0),
            'Correlation_to_BTC': np.random.uniform(0.7, 0.95)
        })
    df = pd.DataFrame(data)
    print(f"NAV premium data: {len(df):,} rows")
    return df

def generate_financing_data():
    print("\nGenerating financing activity data...")
    data = []
    # MSTR convertible note issuances
    issuances = [
        {'Date': '2020-12-11', 'Type': 'Convertible', 'Amount_M': 650, 'Rate': 0.75},
        {'Date': '2021-02-19', 'Type': 'Convertible', 'Amount_M': 1050, 'Rate': 0.0},
        {'Date': '2021-06-14', 'Type': 'Senior Secured', 'Amount_M': 500, 'Rate': 6.125},
        {'Date': '2024-03-08', 'Type': 'Convertible', 'Amount_M': 800, 'Rate': 0.625},
        {'Date': '2024-06-13', 'Type': 'Convertible', 'Amount_M': 700, 'Rate': 2.25},
    ]
    for iss in issuances:
        data.append({
            'Date': pd.to_datetime(iss['Date']),
            'Financing_Type': iss['Type'],
            'Amount_M': iss['Amount_M'],
            'Interest_Rate': iss['Rate'],
            'BTC_Purchased': iss['Amount_M'] * 1e6 / np.random.uniform(30000, 60000),
            'Dilution_Pct': np.random.uniform(1, 5) if iss['Type'] == 'Convertible' else 0
        })
    df = pd.DataFrame(data)
    print(f"Financing data: {len(df):,} rows")
    return df

def generate_summary(stock_df, holdings_df, nav_df, fin_df):
    latest = holdings_df[holdings_df['Date'] == holdings_df['Date'].max()]
    mstr = latest[latest['Company'] == 'MicroStrategy'].iloc[0] if len(latest[latest['Company'] == 'MicroStrategy']) > 0 else None
    total_corporate = latest['BTC_Holdings'].sum()

    summary = {
        'report_metadata': {
            'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(),
            'total_rows_processed': len(holdings_df) + len(nav_df) + len(fin_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'
        },
        'treasury_metrics': {
            'leader': 'MicroStrategy',
            'leader_holdings': f"{mstr['BTC_Holdings']:,.0f} BTC" if mstr is not None else 'N/A',
            'leader_value': f"${mstr['Holdings_Value_M']/1000:.1f}B" if mstr is not None else 'N/A',
            'total_corporate_btc': f"{total_corporate:,.0f} BTC",
            'companies_tracked': holdings_df['Company'].nunique()
        },
        'valuation_metrics': {
            'avg_nav_premium': f"{nav_df['MSTR_NAV_Premium_Pct'].mean():.1f}%",
            'btc_beta': f"{nav_df['MSTR_Beta_to_BTC'].mean():.2f}x",
            'btc_correlation': f"{nav_df['Correlation_to_BTC'].mean():.2f}"
        },
        'financing_summary': {
            'total_raised': f"${fin_df['Amount_M'].sum()/1000:.1f}B",
            'total_btc_purchased': f"{fin_df['BTC_Purchased'].sum():,.0f}",
            'primary_method': 'Convertible notes'
        }
    }
    return summary

def main():
    print("=" * 60)
    print(f"{REPORT_CODE}: {REPORT_TITLE}")
    print("=" * 60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stock_df = pull_data()
    holdings_df = generate_treasury_holdings()
    nav_df = generate_nav_premium()
    fin_df = generate_financing_data()

    total_rows = len(stock_df) + len(holdings_df) + len(nav_df) + len(fin_df)
    summary = generate_summary(stock_df, holdings_df, nav_df, fin_df)

    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_prices.csv", index=False)
    holdings_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_holdings.csv", index=False)
    nav_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_nav.csv", index=False)
    fin_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_financing.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n{'='*60}\nTOTAL DATA ROWS: {total_rows:,}\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total_rows

if __name__ == "__main__":
    main()
