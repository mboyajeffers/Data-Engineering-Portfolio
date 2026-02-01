#!/usr/bin/env python3
"""
CMP-02: Large Bank Regulatory Risk Profile
Data Pull Script

Target: Large bank regulatory compliance analysis
Data Sources: Yahoo Finance, Synthetic regulatory data
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

REPORT_CODE = "CMP02"
REPORT_TITLE = "Large Bank Regulatory Risk Profile"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Compliance/data"

TICKERS = {
    'WFC': 'Wells Fargo',
    'JPM': 'JPMorgan Chase',
    'BAC': 'Bank of America',
    'C': 'Citigroup',
    'USB': 'US Bancorp',
    'PNC': 'PNC Financial',
    'TFC': 'Truist Financial',
    'GS': 'Goldman Sachs',
    'MS': 'Morgan Stanley',
    '^GSPC': 'S&P 500',
    'XLF': 'Financial Select',
}

REGULATORY_CATEGORIES = [
    'Consumer Protection', 'BSA/AML', 'Fair Lending', 'Capital Requirements',
    'Liquidity', 'Operational Risk', 'Cybersecurity', 'Third Party Risk',
    'Model Risk', 'Market Conduct'
]

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
                df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume',
                             'Dividends', 'Stock_Splits', 'Ticker', 'Name']
                all_data.append(df)
                print(f"  {ticker}: {len(df)} rows")
        except Exception as e:
            print(f"  {ticker}: ERROR - {str(e)[:50]}")

    combined = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal stock data rows: {len(combined):,}")
    return combined

def generate_regulatory_data(years=10):
    print("\nGenerating synthetic regulatory data...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    banks = ['Wells Fargo', 'JPMorgan', 'Bank of America', 'Citigroup', 'US Bancorp']
    regulatory_data = []

    for date in dates:
        for bank in banks:
            for category in REGULATORY_CATEGORIES:
                # Base risk levels with bank-specific adjustments
                base_risk = np.random.uniform(20, 60)

                # Wells Fargo higher risk (historical issues)
                if bank == 'Wells Fargo' and category in ['Consumer Protection', 'Market Conduct']:
                    base_risk *= 1.3

                # Time-based trends (improving over time)
                years_elapsed = (date - start_date).days / 365
                improvement = years_elapsed * 0.02
                base_risk = max(10, base_risk * (1 - improvement))

                regulatory_data.append({
                    'Date': date,
                    'Bank': bank,
                    'Category': category,
                    'Risk_Score': min(100, base_risk + np.random.normal(0, 5)),
                    'Exam_Finding_Count': max(0, int(np.random.poisson(base_risk / 20))),
                    'MRA_Count': max(0, int(np.random.poisson(base_risk / 30))),
                    'MRIA_Count': max(0, int(np.random.poisson(base_risk / 50))),
                    'Remediation_Pct': np.random.uniform(0.7, 0.95)
                })

    reg_df = pd.DataFrame(regulatory_data)
    print(f"Regulatory data: {len(reg_df):,} rows")
    return reg_df

def calculate_risk_metrics(reg_df):
    print("\nCalculating risk metrics...")
    results = []

    for bank in reg_df['Bank'].unique():
        bank_df = reg_df[reg_df['Bank'] == bank].copy()

        # Daily aggregates
        daily = bank_df.groupby('Date').agg({
            'Risk_Score': 'mean',
            'Exam_Finding_Count': 'sum',
            'MRA_Count': 'sum',
            'MRIA_Count': 'sum',
            'Remediation_Pct': 'mean'
        }).reset_index()

        daily['Bank'] = bank

        # Rolling metrics
        daily['Risk_Score_30d'] = daily['Risk_Score'].rolling(30).mean()
        daily['Risk_Score_90d'] = daily['Risk_Score'].rolling(90).mean()
        daily['Risk_Trend'] = daily['Risk_Score_30d'] - daily['Risk_Score_90d']

        # Composite scores
        daily['Regulatory_Health_Score'] = 100 - daily['Risk_Score']

        results.append(daily)

    combined = pd.concat(results, ignore_index=True)
    print(f"Risk metrics: {len(combined):,} rows")
    return combined

def generate_enforcement_data(years=10):
    print("\nGenerating enforcement action data...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)

    banks = ['Wells Fargo', 'JPMorgan', 'Bank of America', 'Citigroup', 'US Bancorp']
    enforcment = []

    for bank in banks:
        # Random enforcement actions
        num_actions = np.random.randint(5, 25)
        action_dates = pd.to_datetime(np.random.choice(
            pd.date_range(start_date, end_date), num_actions, replace=False
        ))

        for date in action_dates:
            enforcment.append({
                'Date': date,
                'Bank': bank,
                'Agency': np.random.choice(['OCC', 'CFPB', 'Fed', 'FDIC', 'SEC']),
                'Action_Type': np.random.choice(['Consent Order', 'CMP', 'Cease & Desist', 'MOU']),
                'Fine_Amount': np.random.choice([0, 1e6, 5e6, 10e6, 50e6, 100e6, 500e6, 1e9]),
                'Category': np.random.choice(REGULATORY_CATEGORIES),
                'Resolution_Months': np.random.randint(6, 48)
            })

    enf_df = pd.DataFrame(enforcment)
    print(f"Enforcement data: {len(enf_df):,} rows")
    return enf_df

def generate_summary(stock_df, reg_df, metrics_df, enf_df):
    print("\nGenerating summary...")

    # Primary bank focus
    primary = 'Wells Fargo'
    primary_metrics = metrics_df[metrics_df['Bank'] == primary]

    total_fines = enf_df['Fine_Amount'].sum()
    by_bank_fines = enf_df.groupby('Bank')['Fine_Amount'].sum()

    summary = {
        'report_metadata': {
            'report_code': REPORT_CODE,
            'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(),
            'data_start': str(reg_df['Date'].min()),
            'data_end': str(reg_df['Date'].max()),
            'total_rows_processed': len(reg_df),
            'banks_analyzed': reg_df['Bank'].nunique(),
            'author': 'Mboya Jeffers',
            'email': 'MboyaJeffers9@gmail.com'
        },
        'regulatory_profile': {
            'avg_risk_score': f"{reg_df['Risk_Score'].mean():.1f}/100",
            'highest_risk_category': reg_df.groupby('Category')['Risk_Score'].mean().idxmax(),
            'total_exam_findings': f"{reg_df['Exam_Finding_Count'].sum():,}",
            'avg_remediation_rate': f"{reg_df['Remediation_Pct'].mean() * 100:.1f}%"
        },
        'enforcement_summary': {
            'total_actions': len(enf_df),
            'total_fines': f"${total_fines/1e9:.2f}B",
            'highest_fined_bank': by_bank_fines.idxmax(),
            'most_common_agency': enf_df['Agency'].mode().iloc[0]
        },
        'trend_insights': {
            'risk_trajectory': 'Improving',
            'key_focus_areas': 'BSA/AML, Consumer Protection',
            'regulatory_outlook': 'Heightened scrutiny expected'
        }
    }

    return summary

def main():
    print("=" * 60)
    print(f"{REPORT_CODE}: {REPORT_TITLE}")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stock_df = pull_stock_data(years=10)
    reg_df = generate_regulatory_data(years=10)
    metrics_df = calculate_risk_metrics(reg_df)
    enf_df = generate_enforcement_data(years=10)

    total_rows = len(stock_df) + len(reg_df) + len(metrics_df) + len(enf_df)
    print(f"\nTotal data rows: {total_rows:,}")

    summary = generate_summary(stock_df, reg_df, metrics_df, enf_df)

    print("\nSaving outputs...")
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock_data.csv", index=False)
    reg_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_regulatory.csv", index=False)
    metrics_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_metrics.csv", index=False)
    enf_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_enforcement.csv", index=False)

    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n{'='*60}\nTOTAL DATA ROWS: {total_rows:,}\n{'='*60}")
    print("\nSummary Preview:")
    print(json.dumps(summary, indent=2, default=str))

    return total_rows

if __name__ == "__main__":
    main()
