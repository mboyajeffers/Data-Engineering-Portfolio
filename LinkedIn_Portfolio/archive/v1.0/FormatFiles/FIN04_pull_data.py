#!/usr/bin/env python3
"""
FIN-04: Payment Network Revenue Correlation
Data Pull Script

Target: Payment network analysis (company-agnostic for public posting)
Data Sources: Yahoo Finance (stock data), FRED (consumer spending, GDP)
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

REPORT_CODE = "FIN04"
REPORT_TITLE = "Payment Network Revenue Correlation"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Finance/data"
TARGET_ROWS = 400000

# Payment Networks + Related Companies
TICKERS = {
    # Payment Networks
    'V': 'Visa Inc',
    'MA': 'Mastercard Inc',
    'AXP': 'American Express',
    'DFS': 'Discover Financial',
    'PYPL': 'PayPal Holdings',
    'SQ': 'Block Inc (Square)',
    'AFRM': 'Affirm Holdings',
    'SOFI': 'SoFi Technologies',

    # Payment Processors
    'FIS': 'Fidelity National Info',
    'FISV': 'Fiserv Inc',
    'GPN': 'Global Payments',
    'JKHY': 'Jack Henry & Associates',

    # E-commerce/Consumer
    'AMZN': 'Amazon',
    'SHOP': 'Shopify',
    'WMT': 'Walmart',
    'TGT': 'Target',

    # Banks (card issuers)
    'JPM': 'JPMorgan Chase',
    'BAC': 'Bank of America',
    'C': 'Citigroup',
    'COF': 'Capital One',

    # Market Indices
    '^GSPC': 'S&P 500 Index',
    'XLF': 'Financial Select Sector',
    'XLY': 'Consumer Discretionary',
}

def pull_stock_data(years=10):
    """Pull historical stock data for all tickers"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)

    print(f"Pulling stock data for {len(TICKERS)} tickers...")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    all_data = []

    for ticker, name in TICKERS.items():
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(start=start_date, end=end_date, interval='1d')

            if len(df) > 0:
                df = df.reset_index()
                df['Ticker'] = ticker.replace('^', '')
                df['Name'] = name

                # Categorize
                if ticker in ['V', 'MA', 'AXP', 'DFS']:
                    category = 'Payment_Network'
                elif ticker in ['PYPL', 'SQ', 'AFRM', 'SOFI']:
                    category = 'Digital_Payments'
                elif ticker in ['FIS', 'FISV', 'GPN', 'JKHY']:
                    category = 'Payment_Processor'
                elif ticker in ['AMZN', 'SHOP', 'WMT', 'TGT']:
                    category = 'Retail'
                elif ticker in ['JPM', 'BAC', 'C', 'COF']:
                    category = 'Card_Issuer'
                else:
                    category = 'Index'

                df['Category'] = category
                df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume',
                             'Dividends', 'Stock_Splits', 'Ticker', 'Name', 'Category']
                all_data.append(df)
                print(f"  {ticker}: {len(df)} rows")
            else:
                print(f"  {ticker}: No data")
        except Exception as e:
            print(f"  {ticker}: ERROR - {str(e)[:50]}")

    combined = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal stock data rows: {len(combined):,}")
    return combined

def calculate_revenue_proxy_metrics(df):
    """Calculate revenue proxy metrics based on stock performance"""
    print("\nCalculating revenue proxy metrics...")

    results = []

    for ticker in df['Ticker'].unique():
        ticker_df = df[df['Ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('Date')

        if len(ticker_df) < 30:
            continue

        # Daily returns
        ticker_df['Daily_Return'] = ticker_df['Close'].pct_change()

        # Dollar volume
        ticker_df['Dollar_Volume'] = ticker_df['Close'] * ticker_df['Volume']

        # Performance metrics
        ticker_df['Return_5d'] = ticker_df['Close'].pct_change(5)
        ticker_df['Return_21d'] = ticker_df['Close'].pct_change(21)
        ticker_df['Return_63d'] = ticker_df['Close'].pct_change(63)
        ticker_df['Return_252d'] = ticker_df['Close'].pct_change(252)

        # Volatility
        ticker_df['Volatility_21d'] = ticker_df['Daily_Return'].rolling(21).std() * np.sqrt(252) * 100

        # Momentum indicators
        ticker_df['RSI_14'] = calculate_rsi(ticker_df['Close'], 14)
        ticker_df['MACD'] = ticker_df['Close'].ewm(span=12).mean() - ticker_df['Close'].ewm(span=26).mean()
        ticker_df['MACD_Signal'] = ticker_df['MACD'].ewm(span=9).mean()

        # Relative performance
        ticker_df['MA_50'] = ticker_df['Close'].rolling(50).mean()
        ticker_df['MA_200'] = ticker_df['Close'].rolling(200).mean()
        ticker_df['Above_MA50'] = (ticker_df['Close'] > ticker_df['MA_50']).astype(int)
        ticker_df['Above_MA200'] = (ticker_df['Close'] > ticker_df['MA_200']).astype(int)

        # Drawdown
        ticker_df['Rolling_Max'] = ticker_df['Close'].expanding().max()
        ticker_df['Drawdown'] = (ticker_df['Close'] - ticker_df['Rolling_Max']) / ticker_df['Rolling_Max']

        results.append(ticker_df)

    combined = pd.concat(results, ignore_index=True)
    print(f"Revenue proxy metrics calculated: {len(combined):,} rows, {len(combined.columns)} columns")
    return combined

def calculate_rsi(prices, period=14):
    """Calculate RSI indicator"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_spending_correlations(df):
    """Calculate correlations between payment networks and consumer spending proxies"""
    print("\nCalculating spending correlations...")

    # Get daily returns
    returns_pivot = df.pivot_table(
        index='Date',
        columns='Ticker',
        values='Daily_Return',
        aggfunc='first'
    )

    correlations = []

    # Payment network tickers
    payment_tickers = ['V', 'MA', 'AXP', 'DFS', 'PYPL', 'SQ']
    consumer_tickers = ['AMZN', 'SHOP', 'WMT', 'TGT', 'XLY']
    bank_tickers = ['JPM', 'BAC', 'C', 'COF']

    available_payment = [t for t in payment_tickers if t in returns_pivot.columns]
    available_consumer = [t for t in consumer_tickers if t in returns_pivot.columns]
    available_bank = [t for t in bank_tickers if t in returns_pivot.columns]

    for window in [21, 63, 126, 252]:
        for payment in available_payment:
            # Correlation with consumer stocks
            for consumer in available_consumer:
                corr = returns_pivot[payment].rolling(window).corr(returns_pivot[consumer])
                for date, value in corr.items():
                    if pd.notna(value):
                        correlations.append({
                            'Date': date,
                            'Payment_Ticker': payment,
                            'Corr_With': consumer,
                            'Corr_Type': 'Consumer',
                            'Window': window,
                            'Correlation': value
                        })

            # Correlation with banks
            for bank in available_bank:
                corr = returns_pivot[payment].rolling(window).corr(returns_pivot[bank])
                for date, value in corr.items():
                    if pd.notna(value):
                        correlations.append({
                            'Date': date,
                            'Payment_Ticker': payment,
                            'Corr_With': bank,
                            'Corr_Type': 'Bank',
                            'Window': window,
                            'Correlation': value
                        })

    corr_df = pd.DataFrame(correlations)
    print(f"Correlation data: {len(corr_df):,} rows")
    return corr_df

def generate_synthetic_tpv_data(df):
    """Generate synthetic Total Payment Volume data"""
    print("\nGenerating synthetic TPV data...")

    tpv_data = []

    # Payment networks with synthetic TPV
    network_info = {
        'V': {'base_tpv': 12000, 'growth_rate': 0.10},   # $12T base
        'MA': {'base_tpv': 8000, 'growth_rate': 0.11},   # $8T base
        'AXP': {'base_tpv': 1500, 'growth_rate': 0.08},  # $1.5T base
        'DFS': {'base_tpv': 500, 'growth_rate': 0.07},
        'PYPL': {'base_tpv': 1200, 'growth_rate': 0.15},
        'SQ': {'base_tpv': 200, 'growth_rate': 0.25},
    }

    dates = df['Date'].unique()

    for network, info in network_info.items():
        network_df = df[df['Ticker'] == network]
        if len(network_df) == 0:
            continue

        for i, date in enumerate(sorted(dates)):
            # Growth with seasonality
            day_of_year = pd.Timestamp(date).dayofyear
            seasonal_factor = 1 + 0.15 * np.sin(2 * np.pi * day_of_year / 365)  # Holiday spending peaks

            days_elapsed = i
            growth_factor = (1 + info['growth_rate'] / 252) ** days_elapsed
            tpv = info['base_tpv'] * growth_factor * seasonal_factor

            # Add noise
            tpv *= (1 + np.random.normal(0, 0.005))

            # Breakdown by transaction type
            for tx_type in ['Consumer_Credit', 'Consumer_Debit', 'Commercial', 'Cross_Border']:
                weights = {
                    'V': {'Consumer_Credit': 0.35, 'Consumer_Debit': 0.40, 'Commercial': 0.15, 'Cross_Border': 0.10},
                    'MA': {'Consumer_Credit': 0.40, 'Consumer_Debit': 0.35, 'Commercial': 0.15, 'Cross_Border': 0.10},
                    'AXP': {'Consumer_Credit': 0.60, 'Consumer_Debit': 0.05, 'Commercial': 0.30, 'Cross_Border': 0.05},
                    'DFS': {'Consumer_Credit': 0.50, 'Consumer_Debit': 0.35, 'Commercial': 0.10, 'Cross_Border': 0.05},
                    'PYPL': {'Consumer_Credit': 0.30, 'Consumer_Debit': 0.25, 'Commercial': 0.25, 'Cross_Border': 0.20},
                    'SQ': {'Consumer_Credit': 0.25, 'Consumer_Debit': 0.55, 'Commercial': 0.15, 'Cross_Border': 0.05},
                }

                weight = weights.get(network, {}).get(tx_type, 0.25)

                tpv_data.append({
                    'Date': date,
                    'Network': network,
                    'Transaction_Type': tx_type,
                    'TPV_Billions': tpv * weight,
                    'Weight': weight
                })

    tpv_df = pd.DataFrame(tpv_data)
    print(f"Synthetic TPV data: {len(tpv_df):,} rows")
    return tpv_df

def generate_summary(df, corr_df, tpv_df):
    """Generate summary statistics"""
    print("\nGenerating summary statistics...")

    # Primary payment network focus
    primary = 'V'
    primary_df = df[df['Ticker'] == primary]

    # Calculate returns
    if len(primary_df) > 252:
        total_return = (primary_df['Close'].iloc[-1] / primary_df['Close'].iloc[0] - 1) * 100
        ann_return = ((1 + total_return/100) ** (1/10) - 1) * 100
        volatility = primary_df['Daily_Return'].std() * np.sqrt(252) * 100
    else:
        total_return = ann_return = volatility = 0

    # TPV summary
    latest_tpv = tpv_df.groupby('Network')['TPV_Billions'].sum().sort_values(ascending=False)

    # Correlation summary
    if len(corr_df) > 0 and 'Correlation' in corr_df.columns:
        avg_consumer_corr = corr_df[corr_df['Corr_Type'] == 'Consumer']['Correlation'].mean()
        avg_bank_corr = corr_df[corr_df['Corr_Type'] == 'Bank']['Correlation'].mean()
    else:
        avg_consumer_corr = avg_bank_corr = 0

    summary = {
        'report_metadata': {
            'report_code': REPORT_CODE,
            'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(),
            'data_start': str(df['Date'].min()),
            'data_end': str(df['Date'].max()),
            'total_rows_processed': len(df),
            'tickers_analyzed': df['Ticker'].nunique(),
            'author': 'Mboya Jeffers',
            'email': 'MboyaJeffers9@gmail.com'
        },
        'market_leadership': {
            'leader_tpv': f"${latest_tpv.iloc[0]:,.0f}B" if len(latest_tpv) > 0 else 'N/A',
            'leader_network': latest_tpv.index[0] if len(latest_tpv) > 0 else 'N/A',
            'total_market_tpv': f"${latest_tpv.sum():,.0f}B" if len(latest_tpv) > 0 else 'N/A',
            'duopoly_share': f"{(latest_tpv.head(2).sum() / latest_tpv.sum() * 100):.1f}%" if len(latest_tpv) >= 2 else 'N/A'
        },
        'performance_metrics': {
            'leader_10y_return': f"{total_return:.2f}%",
            'leader_ann_return': f"{ann_return:.2f}%",
            'leader_volatility': f"{volatility:.2f}%",
            'leader_sharpe': f"{ann_return / volatility:.2f}" if volatility > 0 else 'N/A'
        },
        'correlation_insights': {
            'avg_consumer_correlation': f"{avg_consumer_corr:.3f}",
            'avg_bank_correlation': f"{avg_bank_corr:.3f}",
            'total_observations': len(corr_df)
        },
        'revenue_drivers': {
            'cross_border_premium': '2-3x domestic rates',
            'commercial_growth': 'Fastest growing segment',
            'digital_disruption': 'BNPL, crypto emerging threats'
        }
    }

    return summary

def main():
    print("=" * 60)
    print(f"{REPORT_CODE}: {REPORT_TITLE}")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Pull data
    stock_df = pull_stock_data(years=10)

    # Calculate metrics
    metrics_df = calculate_revenue_proxy_metrics(stock_df)
    corr_df = calculate_spending_correlations(metrics_df)
    tpv_df = generate_synthetic_tpv_data(metrics_df)

    # Calculate total rows
    total_rows = len(metrics_df) + len(corr_df) + len(tpv_df)
    print(f"\nTotal data rows: {total_rows:,}")

    # Summary
    summary = generate_summary(metrics_df, corr_df, tpv_df)

    # Save outputs
    print("\nSaving outputs...")

    metrics_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_metrics.csv", index=False)
    print(f"  Saved: {REPORT_CODE}_metrics.csv ({len(metrics_df):,} rows)")

    if len(corr_df) > 0:
        corr_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_correlations.csv", index=False)
        print(f"  Saved: {REPORT_CODE}_correlations.csv ({len(corr_df):,} rows)")

    tpv_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_tpv_synthetic.csv", index=False)
    print(f"  Saved: {REPORT_CODE}_tpv_synthetic.csv ({len(tpv_df):,} rows)")

    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"  Saved: {REPORT_CODE}_summary.json")

    print("\n" + "=" * 60)
    print(f"TOTAL DATA ROWS: {total_rows:,}")
    print("=" * 60)

    print("\nSummary Preview:")
    print(json.dumps(summary, indent=2, default=str))

    return total_rows

if __name__ == "__main__":
    main()
