#!/usr/bin/env python3
"""
P09-ECOM: E-Commerce Sector Intelligence Pipeline
===================================================
Public ecommerce-sector equity performance and consumer-demand context
using Yahoo Finance and FRED (Federal Reserve Economic Data).

Data Sources:
- Yahoo Finance chart API: https://query1.finance.yahoo.com/
- FRED: https://fred.stlouisfed.org/ (UMCSENT consumer sentiment, RSXFS retail sales)

Author: Mboya Jeffers
Version: 1.0.0
Created: 2026-08-13

QUALITY STANDARD COMPLIANCE:
- All data from REAL public APIs, no key required
- Verifiable at query1.finance.yahoo.com and fred.stlouisfed.org
- No simulated data
"""

import csv
import io
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; portfolio-pipeline/1.0)"}

TICKERS = [
    ("AMZN", "Amazon"), ("SHOP", "Shopify"), ("EBAY", "eBay"),
    ("ETSY", "Etsy"), ("W", "Wayfair"), ("CHWY", "Chewy"),
]
FRED_SERIES = {"UMCSENT": "Consumer Sentiment", "RSXFS": "Retail Sales (ex. food services)"}


class MarketClient:
    """Client for Yahoo Finance + FRED public endpoints."""

    def __init__(self):
        self.api_calls = 0
        self.api_errors = 0

    def _get_json(self, url: str) -> dict:
        self.api_calls += 1
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode())

    def _get_text(self, url: str) -> str:
        self.api_calls += 1
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.read().decode()

    def fetch_ticker_history(self, symbol: str, days: int = 90) -> List[Dict[str, Any]]:
        try:
            url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                   f"?range={days}d&interval=1d")
            data = self._get_json(url)
            result = data["chart"]["result"][0]
            timestamps = result["timestamp"]
            closes = result["indicators"]["quote"][0]["close"]
            rows = []
            for ts, close in zip(timestamps, closes):
                if close is None:
                    continue
                rows.append({
                    "symbol": symbol,
                    "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                    "close": round(close, 4),
                })
            return rows
        except Exception as e:
            self.api_errors += 1
            print(f"    ERROR fetching {symbol}: {e}")
            return []

    def fetch_fred_series(self, series_id: str) -> List[Dict[str, Any]]:
        try:
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
            text = self._get_text(url)
            reader = csv.reader(io.StringIO(text))
            rows = []
            for r in reader:
                if len(r) == 2 and r[0] not in ("DATE", "observation_date") and r[1].strip() not in (".", ""):
                    try:
                        rows.append({"series": series_id, "date": r[0], "value": float(r[1])})
                    except ValueError:
                        pass
            return rows
        except Exception as e:
            self.api_errors += 1
            print(f"    ERROR fetching FRED {series_id}: {e}")
            return []


class KPICalculator:
    """Ecommerce sector KPIs from real market + macro data."""

    def __init__(self, price_rows: List[Dict], fred_rows: List[Dict]):
        self.prices = price_rows
        self.fred = fred_rows

    def calculate_kpis(self) -> Dict[str, Any]:
        by_symbol: Dict[str, List[Dict]] = {}
        for row in self.prices:
            by_symbol.setdefault(row["symbol"], []).append(row)

        performance = []
        for symbol, rows in by_symbol.items():
            rows_sorted = sorted(rows, key=lambda r: r["date"])
            if len(rows_sorted) < 2:
                continue
            first, last = rows_sorted[0]["close"], rows_sorted[-1]["close"]
            performance.append({
                "symbol": symbol,
                "start_date": rows_sorted[0]["date"],
                "end_date": rows_sorted[-1]["date"],
                "start_close": first,
                "end_close": last,
                "period_return_pct": round((last / first - 1) * 100, 2),
                "trading_days": len(rows_sorted),
            })
        performance.sort(key=lambda p: p["period_return_pct"], reverse=True)

        sentiment_rows = [r for r in self.fred if r["series"] == "UMCSENT"]
        sentiment_rows.sort(key=lambda r: r["date"])
        retail_rows = [r for r in self.fred if r["series"] == "RSXFS"]
        retail_rows.sort(key=lambda r: r["date"])

        return {
            "metadata": {
                "pipeline": "P09-ECOM",
                "generated": datetime.now(timezone.utc).isoformat(),
                "source": "Yahoo Finance chart API + FRED (UMCSENT, RSXFS)",
                "data_disclaimer": "REAL public market/macro data - no simulation",
            },
            "summary": {
                "total_price_records": len(self.prices),
                "tickers_covered": len(by_symbol),
                "fred_records": len(self.fred),
            },
            "sector_performance": performance,
            "consumer_sentiment_latest": sentiment_rows[-1] if sentiment_rows else None,
            "retail_sales_latest": retail_rows[-1] if retail_rows else None,
            "leader": performance[0] if performance else None,
            "laggard": performance[-1] if performance else None,
        }


def run_pipeline():
    print("=" * 60)
    print("P09-ECOM: E-Commerce Sector Intelligence Pipeline")
    print("=" * 60)

    start_time = datetime.now(timezone.utc)
    client = MarketClient()

    print("\n[1/4] Fetching ecommerce-sector equity history (Yahoo Finance)...")
    price_rows: List[Dict[str, Any]] = []
    for symbol, name in TICKERS:
        rows = client.fetch_ticker_history(symbol)
        print(f"  {symbol} ({name}): {len(rows)} daily closes")
        price_rows.extend(rows)

    print("\n[2/4] Fetching macro context (FRED)...")
    fred_rows: List[Dict[str, Any]] = []
    for series_id, label in FRED_SERIES.items():
        rows = client.fetch_fred_series(series_id)
        print(f"  {series_id} ({label}): {len(rows)} observations")
        fred_rows.extend(rows)

    with open(DATA_DIR / "raw_prices.json", "w") as f:
        json.dump(price_rows, f, indent=2)
    with open(DATA_DIR / "raw_fred.json", "w") as f:
        json.dump(fred_rows, f, indent=2)

    print("\n[3/4] Calculating KPIs...")
    kpis = KPICalculator(price_rows, fred_rows).calculate_kpis()
    print(f"  Tickers covered: {kpis['summary']['tickers_covered']}")
    if kpis["leader"]:
        print(f"  Sector leader: {kpis['leader']['symbol']} ({kpis['leader']['period_return_pct']:+.2f}%)")
    if kpis["consumer_sentiment_latest"]:
        cs = kpis["consumer_sentiment_latest"]
        print(f"  Consumer sentiment ({cs['date']}): {cs['value']}")

    with open(DATA_DIR / "kpis.json", "w") as f:
        json.dump(kpis, f, indent=2, default=str)

    print("\n[4/4] Saving pipeline metrics...")
    end_time = datetime.now(timezone.utc)
    metrics = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds(),
        "api_calls": client.api_calls,
        "api_errors": client.api_errors,
        "records_fetched": len(price_rows) + len(fred_rows),
        "data_sources": {
            "yahoo_finance": {"url": "https://query1.finance.yahoo.com/", "verifiable": True},
            "fred": {"url": "https://fred.stlouisfed.org/", "verifiable": True},
        },
    }
    with open(DATA_DIR / "pipeline_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    print("\n" + "=" * 60)
    print("Pipeline Complete!")
    print(f"  Duration: {metrics['duration_seconds']:.1f} seconds")
    print(f"  Records: {metrics['records_fetched']}")
    print(f"  API errors: {client.api_errors}")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
