#!/usr/bin/env python3
"""
CMP-04: Systemically Important Bank Compliance Monitor
Data Pull Script

Target: SIFI bank compliance monitoring analysis
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

REPORT_CODE = "CMP04"
REPORT_TITLE = "Systemically Important Bank Compliance Monitor"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Compliance/data"

# G-SIBs (Global Systemically Important Banks)
TICKERS = {
    'BAC': 'Bank of America',
    'JPM': 'JPMorgan Chase',
    'C': 'Citigroup',
    'WFC': 'Wells Fargo',
    'GS': 'Goldman Sachs',
    'MS': 'Morgan Stanley',
    'BK': 'Bank of New York Mellon',
    'STT': 'State Street',
    '^GSPC': 'S&P 500',
    'XLF': 'Financial Select',
}

SIFI_METRICS = [
    'CET1 Ratio', 'Leverage Ratio', 'LCR', 'NSFR', 'TLAC',
    'Stress Test Buffer', 'G-SIB Surcharge', 'Resolution Planning'
]

RISK_CATEGORIES = [
    'Credit Risk', 'Market Risk', 'Operational Risk', 'Liquidity Risk',
    'Compliance Risk', 'Reputational Risk', 'Model Risk', 'Cyber Risk'
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

    combined = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    print(f"\nTotal stock data rows: {len(combined):,}")
    return combined

def generate_capital_metrics(years=10):
    print("\nGenerating capital adequacy metrics...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    dates = pd.date_range(start=start_date, end=end_date, freq='Q')  # Quarterly

    banks = ['Bank of America', 'JPMorgan', 'Citigroup', 'Wells Fargo', 'Goldman Sachs', 'Morgan Stanley']
    capital_data = []

    for date in dates:
        for bank in banks:
            # Base capital ratios with bank-specific profiles
            base_cet1 = np.random.uniform(11, 14)
            base_leverage = np.random.uniform(5, 7)
            base_lcr = np.random.uniform(110, 140)

            capital_data.append({
                'Date': date,
                'Bank': bank,
                'CET1_Ratio': base_cet1 + np.random.normal(0, 0.3),
                'Leverage_Ratio': base_leverage + np.random.normal(0, 0.2),
                'LCR': base_lcr + np.random.normal(0, 5),
                'NSFR': np.random.uniform(105, 125),
                'TLAC_Ratio': np.random.uniform(20, 26),
                'G_SIB_Surcharge': np.random.choice([1.5, 2.0, 2.5, 3.0, 3.5]),
                'SCB': np.random.uniform(2.5, 6.5),
                'Total_Assets_B': np.random.uniform(2000, 4000)
            })

    df = pd.DataFrame(capital_data)
    print(f"Capital metrics: {len(df):,} rows")
    return df

def generate_risk_assessment(years=10):
    print("\nGenerating risk assessment data...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)
    dates = pd.date_range(start=start_date, end=end_date, freq='M')

    banks = ['Bank of America', 'JPMorgan', 'Citigroup', 'Wells Fargo', 'Goldman Sachs', 'Morgan Stanley']
    risk_data = []

    for date in dates:
        for bank in banks:
            for category in RISK_CATEGORIES:
                risk_data.append({
                    'Date': date,
                    'Bank': bank,
                    'Risk_Category': category,
                    'Risk_Score': np.random.uniform(20, 70) + np.random.normal(0, 10),
                    'Control_Effectiveness': np.random.uniform(0.7, 0.95),
                    'Residual_Risk': np.random.uniform(10, 40),
                    'Trend': np.random.choice(['Improving', 'Stable', 'Deteriorating'], p=[0.4, 0.4, 0.2])
                })

    df = pd.DataFrame(risk_data)
    df['Risk_Score'] = df['Risk_Score'].clip(0, 100)
    print(f"Risk assessment: {len(df):,} rows")
    return df

def generate_stress_test_results(years=10):
    print("\nGenerating stress test results...")
    banks = ['Bank of America', 'JPMorgan', 'Citigroup', 'Wells Fargo', 'Goldman Sachs', 'Morgan Stanley']
    scenarios = ['Baseline', 'Adverse', 'Severely Adverse']
    years_list = list(range(2016, 2027))

    stress_data = []
    for year in years_list:
        for bank in banks:
            for scenario in scenarios:
                base_loss = 0 if scenario == 'Baseline' else (3 if scenario == 'Adverse' else 6)
                stress_data.append({
                    'Year': year,
                    'Bank': bank,
                    'Scenario': scenario,
                    'Projected_Loss_Rate': base_loss + np.random.uniform(0, 2),
                    'Min_CET1': np.random.uniform(6, 10) if scenario != 'Baseline' else np.random.uniform(10, 13),
                    'PPNR_B': np.random.uniform(20, 60),
                    'Loan_Losses_B': np.random.uniform(10, 80) if scenario != 'Baseline' else np.random.uniform(5, 20),
                    'Qualitative_Result': np.random.choice(['Pass', 'Conditional', 'Objection'], p=[0.8, 0.15, 0.05])
                })

    df = pd.DataFrame(stress_data)
    print(f"Stress test data: {len(df):,} rows")
    return df

def generate_resolution_metrics(years=10):
    print("\nGenerating resolution planning metrics...")
    banks = ['Bank of America', 'JPMorgan', 'Citigroup', 'Wells Fargo', 'Goldman Sachs', 'Morgan Stanley']
    years_list = list(range(2016, 2027))

    resolution_data = []
    for year in years_list:
        for bank in banks:
            resolution_data.append({
                'Year': year,
                'Bank': bank,
                'Resolution_Plan_Rating': np.random.choice(['Satisfactory', 'Needs Improvement', 'Deficient'], p=[0.6, 0.3, 0.1]),
                'Critical_Operations_Mapped': np.random.uniform(0.85, 1.0),
                'Legal_Entity_Rationalization': np.random.uniform(0.7, 0.95),
                'Separability_Score': np.random.uniform(60, 95),
                'TLAC_Compliance': np.random.choice([True, True, True, False]),
                'Remediation_Items': np.random.randint(0, 15)
            })

    df = pd.DataFrame(resolution_data)
    print(f"Resolution metrics: {len(df):,} rows")
    return df

def generate_summary(stock_df, capital_df, risk_df, stress_df, resolution_df):
    print("\nGenerating summary...")

    # Primary focus
    primary = 'Bank of America'

    avg_cet1 = capital_df['CET1_Ratio'].mean()
    avg_lcr = capital_df['LCR'].mean()
    avg_risk = risk_df['Risk_Score'].mean()

    summary = {
        'report_metadata': {
            'report_code': REPORT_CODE,
            'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(),
            'data_start': str(capital_df['Date'].min()),
            'data_end': str(capital_df['Date'].max()),
            'total_rows_processed': len(capital_df) + len(risk_df) + len(stress_df),
            'banks_analyzed': capital_df['Bank'].nunique(),
            'author': 'Mboya Jeffers',
            'email': 'MboyaJeffers9@gmail.com'
        },
        'capital_adequacy': {
            'avg_cet1_ratio': f"{avg_cet1:.1f}%",
            'avg_leverage_ratio': f"{capital_df['Leverage_Ratio'].mean():.1f}%",
            'avg_lcr': f"{avg_lcr:.0f}%",
            'avg_gsib_surcharge': f"{capital_df['G_SIB_Surcharge'].mean():.1f}%"
        },
        'risk_profile': {
            'avg_risk_score': f"{avg_risk:.1f}/100",
            'highest_risk_category': risk_df.groupby('Risk_Category')['Risk_Score'].mean().idxmax(),
            'avg_control_effectiveness': f"{risk_df['Control_Effectiveness'].mean() * 100:.1f}%"
        },
        'stress_testing': {
            'pass_rate': f"{(stress_df['Qualitative_Result'] == 'Pass').mean() * 100:.1f}%",
            'avg_severely_adverse_loss': f"{stress_df[stress_df['Scenario'] == 'Severely Adverse']['Projected_Loss_Rate'].mean():.1f}%",
            'min_cet1_stress': f"{stress_df[stress_df['Scenario'] == 'Severely Adverse']['Min_CET1'].mean():.1f}%"
        },
        'resolution_readiness': {
            'satisfactory_plans': f"{(resolution_df['Resolution_Plan_Rating'] == 'Satisfactory').mean() * 100:.0f}%",
            'avg_separability': f"{resolution_df['Separability_Score'].mean():.0f}/100"
        }
    }

    return summary

def main():
    print("=" * 60)
    print(f"{REPORT_CODE}: {REPORT_TITLE}")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stock_df = pull_stock_data(years=10)
    capital_df = generate_capital_metrics(years=10)
    risk_df = generate_risk_assessment(years=10)
    stress_df = generate_stress_test_results(years=10)
    resolution_df = generate_resolution_metrics(years=10)

    total_rows = len(stock_df) + len(capital_df) + len(risk_df) + len(stress_df) + len(resolution_df)
    print(f"\nTotal data rows: {total_rows:,}")

    summary = generate_summary(stock_df, capital_df, risk_df, stress_df, resolution_df)

    print("\nSaving outputs...")
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock_data.csv", index=False)
    capital_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_capital.csv", index=False)
    risk_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_risk.csv", index=False)
    stress_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stress.csv", index=False)
    resolution_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_resolution.csv", index=False)

    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n{'='*60}\nTOTAL DATA ROWS: {total_rows:,}\n{'='*60}")
    print("\nSummary Preview:")
    print(json.dumps(summary, indent=2, default=str))

    return total_rows

if __name__ == "__main__":
    main()
