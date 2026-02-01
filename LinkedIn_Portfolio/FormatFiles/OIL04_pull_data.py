#!/usr/bin/env python3
"""
OIL-04: Energy Transition Strategy (Shell)
Author: Mboya Jeffers
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, os, warnings
warnings.filterwarnings('ignore')

REPORT_CODE = "OIL04"
REPORT_TITLE = "Energy Transition Strategy"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../OilGas/data"

TICKERS = {'SHEL': 'Shell', 'BP': 'BP', 'TTE': 'TotalEnergies', 'EQNR': 'Equinor', '^GSPC': 'S&P 500'}

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

def generate_transition_data():
    print("\nGenerating transition metrics...")
    dates = pd.date_range(end=datetime.now(), periods=40, freq='Q')
    data = []
    for i, d in enumerate(dates):
        renewable_growth = 1.1 ** i
        data.append({'Date': d, 'Renewables_Capacity_GW': np.random.uniform(5, 15) * renewable_growth,
                    'EV_Charging_Points_K': np.random.uniform(20, 100) * renewable_growth,
                    'LNG_MTPA': np.random.uniform(25, 35),
                    'Carbon_Intensity_Reduction_Pct': min(30, i * 0.8),
                    'Low_Carbon_CAPEX_Pct': np.random.uniform(10, 25)})
    return pd.DataFrame(data)

def generate_segment_data():
    print("\nGenerating segment performance data...")
    dates = pd.date_range(end=datetime.now(), periods=60, freq='M')
    segments = ['Upstream', 'Integrated Gas', 'Downstream', 'Renewables', 'Marketing']
    data = []
    for d in dates:
        for s in segments:
            mult = {'Upstream': 1.0, 'Integrated Gas': 1.2, 'Downstream': 0.7, 'Renewables': 0.3, 'Marketing': 0.5}[s]
            data.append({'Date': d, 'Segment': s, 'Earnings_B': np.random.uniform(1, 8) * mult,
                        'CAPEX_B': np.random.uniform(0.5, 3) * mult,
                        'ROACE_Pct': np.random.uniform(5, 25)})
    return pd.DataFrame(data)

def generate_lng_data():
    print("\nGenerating LNG data...")
    dates = pd.date_range(end=datetime.now(), periods=1095, freq='D')
    data = []
    for d in dates:
        winter = d.month in [11, 12, 1, 2]
        mult = 1.4 if winter else 1.0
        data.append({'Date': d, 'LNG_Cargoes': np.random.randint(3, 12),
                    'Spot_Price_MMBTU': np.random.uniform(5, 20) * mult,
                    'Contract_Price_MMBTU': np.random.uniform(8, 15),
                    'Trading_Margin_M': np.random.uniform(20, 100) * mult,
                    'Utilization_Pct': np.random.uniform(85, 100)})
    return pd.DataFrame(data)

def generate_summary(stock_df, trans_df, seg_df, lng_df):
    latest = trans_df[trans_df['Date'] == trans_df['Date'].max()].iloc[0]
    return {
        'report_metadata': {'report_code': REPORT_CODE, 'report_title': REPORT_TITLE,
            'generated_date': datetime.now().isoformat(), 'total_rows_processed': len(trans_df)+len(seg_df)+len(lng_df),
            'author': 'Mboya Jeffers', 'email': 'MboyaJeffers9@gmail.com'},
        'transition': {'renewables_capacity': f"{latest['Renewables_Capacity_GW']:.0f} GW",
            'ev_chargers': f"{latest['EV_Charging_Points_K']:.0f}K",
            'carbon_reduction': f"{latest['Carbon_Intensity_Reduction_Pct']:.0f}%",
            'low_carbon_capex': f"{latest['Low_Carbon_CAPEX_Pct']:.0f}%"},
        'segments': {'top_segment': seg_df.groupby('Segment')['Earnings_B'].sum().idxmax(),
            'lng_position': f"{latest['LNG_MTPA']:.0f} MTPA"},
        'lng_trading': {'daily_cargoes': f"{lng_df['LNG_Cargoes'].mean():.0f}",
            'avg_margin': f"${lng_df['Trading_Margin_M'].mean():.0f}M/day"}
    }

def main():
    print("="*60 + f"\n{REPORT_CODE}: {REPORT_TITLE}\n" + "="*60)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stock_df, trans_df, seg_df, lng_df = pull_data(), generate_transition_data(), generate_segment_data(), generate_lng_data()
    total = len(stock_df) + len(trans_df) + len(seg_df) + len(lng_df)
    summary = generate_summary(stock_df, trans_df, seg_df, lng_df)
    stock_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_stock.csv", index=False)
    trans_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_transition.csv", index=False)
    seg_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_segments.csv", index=False)
    lng_df.to_csv(f"{OUTPUT_DIR}/{REPORT_CODE}_lng.csv", index=False)
    with open(f"{OUTPUT_DIR}/{REPORT_CODE}_summary.json", 'w') as f: json.dump(summary, f, indent=2, default=str)
    print(f"\n{'='*60}\nTOTAL: {total:,} rows\n{'='*60}")
    print(json.dumps(summary, indent=2))
    return total

if __name__ == "__main__": main()
