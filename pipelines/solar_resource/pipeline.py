#!/usr/bin/env python3
"""
P10-SOL: Solar Resource Assessment Pipeline
=============================================
Trailing-365-day solar irradiance and estimated PV production/economics
across multiple US markets using Open-Meteo's free archive API.

Data Sources:
- Open-Meteo Archive API: https://archive-api.open-meteo.com/
  (ERA5-based reanalysis, no key required)

Author: Mboya Jeffers
Version: 1.0.0
Created: 2026-08-13

QUALITY STANDARD COMPLIANCE:
- Irradiance data REAL, pulled live at run time - no simulation
- Production/economics figures apply disclosed formulas to real data;
  assumptions (system size, derate factor, electricity rate, install
  cost) are explicitly separated from the real irradiance data below
- Verifiable at open-meteo.com
"""

import json
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; portfolio-pipeline/1.0)"}

MARKETS = [
    ("Phoenix, AZ", 33.45, -112.07),
    ("Austin, TX", 30.27, -97.74),
    ("Charlotte, NC", 35.23, -80.84),
    ("Sacramento, CA", 38.58, -121.49),
    ("Miami, FL", 25.76, -80.19),
]

# Disclosed assumptions -- kept separate from the real irradiance data.
SYSTEM_SIZE_KW_DC = 7.6
DERATE_FACTOR = 0.80
ELECTRICITY_RATE_USD_PER_KWH = 0.16
INSTALL_COST_USD_PER_WATT = 2.80


class OpenMeteoClient:
    """Client for Open-Meteo's free archive API."""

    def __init__(self):
        self.api_calls = 0
        self.api_errors = 0

    def fetch_trailing_year_irradiance(self, lat: float, lon: float) -> Dict[str, Any]:
        self.api_calls += 1
        end = date.today() - timedelta(days=2)
        start = end - timedelta(days=365)
        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date={start.isoformat()}&end_date={end.isoformat()}"
            "&daily=shortwave_radiation_sum&timezone=auto"
        )
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=25) as resp:
                data = json.loads(resp.read().decode())
            values = [v for v in data["daily"]["shortwave_radiation_sum"] if v is not None]
            if len(values) < 300:
                raise ValueError(f"insufficient data ({len(values)} days)")
            return {
                "days": len(values),
                "start": data["daily"]["time"][0],
                "end": data["daily"]["time"][-1],
                "avg_mj_per_m2_day": sum(values) / len(values),
                "min_mj_per_m2_day": min(values),
                "max_mj_per_m2_day": max(values),
            }
        except Exception as e:
            self.api_errors += 1
            print(f"    ERROR fetching irradiance: {e}")
            return {}


class KPICalculator:
    """Solar resource + estimated PV economics KPIs."""

    def __init__(self, market_data: List[Dict[str, Any]]):
        self.market_data = market_data

    @staticmethod
    def _economics(avg_mj_per_m2_day: float) -> Dict[str, float]:
        peak_sun_hours = avg_mj_per_m2_day / 3.6
        annual_production_kwh = SYSTEM_SIZE_KW_DC * peak_sun_hours * 365 * DERATE_FACTOR
        annual_value_usd = annual_production_kwh * ELECTRICITY_RATE_USD_PER_KWH
        system_cost_usd = SYSTEM_SIZE_KW_DC * 1000 * INSTALL_COST_USD_PER_WATT
        return {
            "peak_sun_hours": round(peak_sun_hours, 2),
            "annual_production_kwh": round(annual_production_kwh, 0),
            "annual_value_usd": round(annual_value_usd, 0),
            "system_cost_usd": round(system_cost_usd, 0),
            "simple_payback_years": round(system_cost_usd / annual_value_usd, 1),
        }

    def calculate_kpis(self) -> Dict[str, Any]:
        markets = []
        for m in self.market_data:
            if not m.get("irradiance"):
                continue
            econ = self._economics(m["irradiance"]["avg_mj_per_m2_day"])
            markets.append({
                "market": m["name"],
                "avg_mj_per_m2_day": round(m["irradiance"]["avg_mj_per_m2_day"], 2),
                "data_window": f"{m['irradiance']['start']} to {m['irradiance']['end']}",
                **econ,
            })
        markets.sort(key=lambda x: x["avg_mj_per_m2_day"], reverse=True)

        return {
            "metadata": {
                "pipeline": "P10-SOL",
                "generated": datetime.now(timezone.utc).isoformat(),
                "source": "Open-Meteo Archive API (ERA5 reanalysis)",
                "data_disclaimer": "REAL irradiance data, no simulation. Economics figures apply disclosed assumptions (see assumptions block) to that real data.",
            },
            "assumptions": {
                "system_size_kw_dc": SYSTEM_SIZE_KW_DC,
                "derate_factor": DERATE_FACTOR,
                "electricity_rate_usd_per_kwh": ELECTRICITY_RATE_USD_PER_KWH,
                "install_cost_usd_per_watt": INSTALL_COST_USD_PER_WATT,
                "note": "Federal/state/utility incentives vary by jurisdiction and change over time; not included in payback figures.",
            },
            "summary": {
                "markets_analyzed": len(markets),
            },
            "markets": markets,
            "best_market": markets[0] if markets else None,
            "fastest_payback": min(markets, key=lambda m: m["simple_payback_years"]) if markets else None,
        }


def run_pipeline():
    print("=" * 60)
    print("P10-SOL: Solar Resource Assessment Pipeline")
    print("=" * 60)

    start_time = datetime.now(timezone.utc)
    client = OpenMeteoClient()

    print(f"\n[1/3] Fetching trailing-365-day irradiance for {len(MARKETS)} markets (Open-Meteo)...")
    market_data = []
    for name, lat, lon in MARKETS:
        print(f"  {name}...")
        irr = client.fetch_trailing_year_irradiance(lat, lon)
        if irr:
            print(f"    avg {irr['avg_mj_per_m2_day']:.2f} MJ/m2/day over {irr['days']} days")
        market_data.append({"name": name, "lat": lat, "lon": lon, "irradiance": irr})

    with open(DATA_DIR / "raw_irradiance.json", "w") as f:
        json.dump(market_data, f, indent=2, default=str)

    print("\n[2/3] Calculating solar resource + economics KPIs...")
    kpis = KPICalculator(market_data).calculate_kpis()
    print(f"  Markets analyzed: {kpis['summary']['markets_analyzed']}")
    if kpis["best_market"]:
        print(f"  Best resource: {kpis['best_market']['market']} ({kpis['best_market']['avg_mj_per_m2_day']} MJ/m2/day)")
    if kpis["fastest_payback"]:
        print(f"  Fastest payback: {kpis['fastest_payback']['market']} ({kpis['fastest_payback']['simple_payback_years']} yrs)")

    with open(DATA_DIR / "kpis.json", "w") as f:
        json.dump(kpis, f, indent=2, default=str)

    print("\n[3/3] Saving pipeline metrics...")
    end_time = datetime.now(timezone.utc)
    metrics = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds(),
        "api_calls": client.api_calls,
        "api_errors": client.api_errors,
        "markets_fetched": len(market_data),
        "data_sources": {
            "open_meteo_archive": {"url": "https://open-meteo.com/en/docs/historical-weather-api", "verifiable": True},
        },
    }
    with open(DATA_DIR / "pipeline_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    print("\n" + "=" * 60)
    print("Pipeline Complete!")
    print(f"  Duration: {metrics['duration_seconds']:.1f} seconds")
    print(f"  Markets: {len(market_data)}")
    print(f"  API errors: {client.api_errors}")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
