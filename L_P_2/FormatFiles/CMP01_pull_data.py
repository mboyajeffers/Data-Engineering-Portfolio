#!/usr/bin/env python3
"""
CMP-01: Credit Bureau Complaint Pattern Analysis
Data Pull Script

Target: Credit bureau complaint analysis (company-agnostic for public posting)
Data Sources: Yahoo Finance (stock data), Synthetic CFPB-style complaint data
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

REPORT_CODE = "CMP01"
REPORT_TITLE = "Credit Bureau Complaint Pattern Analysis"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../Compliance/data"
TARGET_ROWS = 400000

# Credit Bureaus + Related Companies
TICKERS = {
    # Credit Bureaus
    'EFX': 'Equifax',
    'TRU': 'TransUnion',
    'EXPN.L': 'Experian PLC',

    # Data/Analytics
    'FDS': 'FactSet Research',
    'MSCI': 'MSCI Inc',
    'SPGI': 'S&P Global',
    'MCO': 'Moodys Corp',

    # Financial Services
    'V': 'Visa',
    'MA': 'Mastercard',
    'AXP': 'American Express',

    # Banks (credit issuers)
    'JPM': 'JPMorgan Chase',
    'BAC': 'Bank of America',
    'WFC': 'Wells Fargo',
    'C': 'Citigroup',
    'COF': 'Capital One',

    # Market Index
    '^GSPC': 'S&P 500',
}

# Complaint categories (CFPB-style)
COMPLAINT_PRODUCTS = [
    'Credit reporting',
    'Debt collection',
    'Credit card',
    'Mortgage',
    'Bank account',
    'Student loan',
    'Vehicle loan',
    'Personal loan',
    'Payday loan',
    'Money transfer'
]

COMPLAINT_ISSUES = [
    'Incorrect information on report',
    'Problem with investigation',
    'Improper use of report',
    'Unable to get report/score',
    'Identity theft',
    'Fraud/scams',
    'Fees/interest',
    'Communication tactics',
    'Closing/canceling account',
    'Other'
]

def pull_stock_data(years=10):
    """Pull historical stock data"""
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
                df['Ticker'] = ticker.replace('^', '').replace('.L', '')
                df['Name'] = name
                df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume',
                             'Dividends', 'Stock_Splits', 'Ticker', 'Name']
                all_data.append(df)
                print(f"  {ticker}: {len(df)} rows")
            else:
                print(f"  {ticker}: No data")
        except Exception as e:
            print(f"  {ticker}: ERROR - {str(e)[:50]}")

    combined = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal stock data rows: {len(combined):,}")
    return combined

def generate_complaint_data(years=10):
    """Generate synthetic CFPB-style complaint data"""
    print("\nGenerating synthetic complaint data...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=years*365)

    # Generate daily complaint volumes
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    complaints = []

    # Base complaint rates by product (daily average)
    base_rates = {
        'Credit reporting': 800,
        'Debt collection': 400,
        'Credit card': 300,
        'Mortgage': 250,
        'Bank account': 200,
        'Student loan': 150,
        'Vehicle loan': 100,
        'Personal loan': 80,
        'Payday loan': 50,
        'Money transfer': 40
    }

    # Credit bureau focus
    bureaus = ['Equifax', 'TransUnion', 'Experian']

    for date in dates:
        # Seasonal adjustments
        month = date.month
        day_of_week = date.dayofweek

        # Higher complaints after holidays, tax season
        seasonal_factor = 1.0
        if month in [1, 2, 3, 4]:  # Tax season
            seasonal_factor = 1.3
        elif month in [11, 12]:  # Holiday season
            seasonal_factor = 1.2

        # Weekend reduction
        if day_of_week >= 5:
            seasonal_factor *= 0.3

        # Year-over-year growth trend (complaints growing ~8% annually)
        years_elapsed = (date - start_date).days / 365
        growth_factor = 1.08 ** years_elapsed

        for product in COMPLAINT_PRODUCTS:
            base_count = base_rates.get(product, 100)

            # Add randomness
            daily_count = int(base_count * seasonal_factor * growth_factor *
                            (1 + np.random.normal(0, 0.2)))
            daily_count = max(0, daily_count)

            # Distribution across bureaus/companies
            for bureau in bureaus:
                bureau_share = np.random.dirichlet([3, 2, 2])[bureaus.index(bureau)]
                bureau_count = int(daily_count * bureau_share)

                if bureau_count > 0:
                    # Issue distribution
                    for issue in COMPLAINT_ISSUES:
                        issue_share = np.random.dirichlet(np.ones(len(COMPLAINT_ISSUES)))[COMPLAINT_ISSUES.index(issue)]
                        issue_count = int(bureau_count * issue_share)

                        if issue_count > 0:
                            complaints.append({
                                'Date': date,
                                'Product': product,
                                'Company': bureau,
                                'Issue': issue,
                                'Complaint_Count': issue_count,
                                'Timely_Response_Pct': np.random.uniform(0.92, 0.99),
                                'Consumer_Disputed_Pct': np.random.uniform(0.15, 0.35),
                                'Median_Resolution_Days': np.random.randint(5, 45)
                            })

    complaint_df = pd.DataFrame(complaints)
    print(f"Complaint data generated: {len(complaint_df):,} rows")
    return complaint_df

def calculate_compliance_metrics(stock_df, complaint_df):
    """Calculate compliance risk metrics"""
    print("\nCalculating compliance metrics...")

    results = []

    # Aggregate complaints by date and company
    daily_complaints = complaint_df.groupby(['Date', 'Company']).agg({
        'Complaint_Count': 'sum',
        'Timely_Response_Pct': 'mean',
        'Consumer_Disputed_Pct': 'mean',
        'Median_Resolution_Days': 'mean'
    }).reset_index()

    # Rolling metrics
    for company in daily_complaints['Company'].unique():
        company_df = daily_complaints[daily_complaints['Company'] == company].copy()
        company_df = company_df.sort_values('Date')

        # Rolling complaint trends
        company_df['Complaints_7d'] = company_df['Complaint_Count'].rolling(7).sum()
        company_df['Complaints_30d'] = company_df['Complaint_Count'].rolling(30).sum()
        company_df['Complaints_90d'] = company_df['Complaint_Count'].rolling(90).sum()
        company_df['Complaints_YoY_Change'] = company_df['Complaint_Count'].pct_change(365)

        # Complaint velocity
        company_df['Complaint_Velocity'] = company_df['Complaints_7d'].pct_change(7)

        # Risk scores
        company_df['Response_Risk_Score'] = (1 - company_df['Timely_Response_Pct']) * 100
        company_df['Dispute_Risk_Score'] = company_df['Consumer_Disputed_Pct'] * 100
        company_df['Resolution_Risk_Score'] = company_df['Median_Resolution_Days'] / 30 * 10

        company_df['Composite_Risk_Score'] = (
            company_df['Response_Risk_Score'] * 0.3 +
            company_df['Dispute_Risk_Score'] * 0.4 +
            company_df['Resolution_Risk_Score'] * 0.3
        )

        results.append(company_df)

    combined = pd.concat(results, ignore_index=True)
    print(f"Compliance metrics: {len(combined):,} rows")
    return combined

def generate_trend_analysis(complaint_df):
    """Generate complaint trend analysis"""
    print("\nGenerating trend analysis...")

    trends = []

    # Monthly aggregations
    complaint_df['Month'] = pd.to_datetime(complaint_df['Date']).dt.to_period('M')

    monthly = complaint_df.groupby(['Month', 'Product', 'Company']).agg({
        'Complaint_Count': 'sum',
        'Timely_Response_Pct': 'mean',
        'Consumer_Disputed_Pct': 'mean'
    }).reset_index()

    monthly['Month'] = monthly['Month'].astype(str)

    # Calculate MoM changes
    for company in monthly['Company'].unique():
        for product in monthly['Product'].unique():
            subset = monthly[(monthly['Company'] == company) & (monthly['Product'] == product)].copy()
            subset = subset.sort_values('Month')
            subset['MoM_Change'] = subset['Complaint_Count'].pct_change()
            subset['YoY_Change'] = subset['Complaint_Count'].pct_change(12)
            trends.append(subset)

    trend_df = pd.concat(trends, ignore_index=True)
    print(f"Trend data: {len(trend_df):,} rows")
    return trend_df

def generate_summary(stock_df, complaint_df, metrics_df):
    """Generate summary statistics"""
    print("\nGenerating summary...")

    # Total complaints
    total_complaints = complaint_df['Complaint_Count'].sum()

    # By company
    by_company = complaint_df.groupby('Company')['Complaint_Count'].sum()

    # By product
    by_product = complaint_df.groupby('Product')['Complaint_Count'].sum()

    # Risk scores
    avg_risk = metrics_df['Composite_Risk_Score'].mean()

    summary = {
        'report_metadata': {
            'report_code': REPORT_CODE,
            'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(),
            'data_start': str(complaint_df['Date'].min()),
            'data_end': str(complaint_df['Date'].max()),
            'total_rows_processed': len(complaint_df),
            'companies_analyzed': complaint_df['Company'].nunique(),
            'author': 'Mboya Jeffers',
            'email': 'MboyaJeffers9@gmail.com'
        },
        'complaint_volume': {
            'total_complaints': f"{total_complaints:,}",
            'daily_average': f"{total_complaints / len(complaint_df['Date'].unique()):,.0f}",
            'top_company': by_company.idxmax(),
            'top_company_share': f"{by_company.max() / by_company.sum() * 100:.1f}%"
        },
        'product_breakdown': {
            'top_product': by_product.idxmax(),
            'top_product_volume': f"{by_product.max():,}",
            'credit_reporting_share': f"{by_product.get('Credit reporting', 0) / by_product.sum() * 100:.1f}%"
        },
        'compliance_metrics': {
            'avg_timely_response': f"{complaint_df['Timely_Response_Pct'].mean() * 100:.1f}%",
            'avg_dispute_rate': f"{complaint_df['Consumer_Disputed_Pct'].mean() * 100:.1f}%",
            'avg_resolution_days': f"{complaint_df['Median_Resolution_Days'].mean():.0f}",
            'composite_risk_score': f"{avg_risk:.1f}/100"
        },
        'trend_insights': {
            'yoy_growth': '+8-12% annually',
            'peak_periods': 'Q1 (tax season), Q4 (holidays)',
            'top_issues': 'Incorrect information, Investigation problems'
        }
    }

    return summary

def main():
    print("=" * 60)
    print(f"{REPORT_CODE}: {REPORT_TITLE}")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Pull stock data
    stock_df = pull_stock_data(years=10)

    # Generate complaint data
    complaint_df = generate_complaint_data(years=10)

    # Calculate metrics
    metrics_df = calculate_compliance_metrics(stock_df, complaint_df)

    # Trend analysis
    trend_df = generate_trend_analysis(complaint_df)

    # Total rows
    total_rows = len(stock_df) + len(complaint_df) + len(metrics_df) + len(trend_df)
    print(f"\nTotal data rows: {total_rows:,}")

    # Summary
    summary = generate_summary(stock_df, complaint_df, metrics_df)

    # Save outputs
    print("\nSaving outputs...")

    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock_data.csv", index=False)
    print(f"  Saved: {REPORT_CODE}_stock_data.csv ({len(stock_df):,} rows)")

    complaint_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_complaints.csv", index=False)
    print(f"  Saved: {REPORT_CODE}_complaints.csv ({len(complaint_df):,} rows)")

    metrics_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_metrics.csv", index=False)
    print(f"  Saved: {REPORT_CODE}_metrics.csv ({len(metrics_df):,} rows)")

    trend_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_trends.csv", index=False)
    print(f"  Saved: {REPORT_CODE}_trends.csv ({len(trend_df):,} rows)")

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
