# SEC Financial Intelligence Pipeline

**Enterprise-Scale Financial Data Processing: 1M+ Financial Facts**

## Overview

This pipeline extracts, transforms, and analyzes financial data from SEC EDGAR for all publicly traded US companies. It processes over 1 million financial facts from 10-K and 10-Q filings, calculates 50+ financial KPIs, and structures data in an analytics-ready star schema.

## Scale

| Metric | Value |
|--------|-------|
| **Financial Facts Processed** | 1,000,000+ |
| **Companies Covered** | 500+ |
| **KPIs Calculated** | 50+ |
| **Years of Data** | 5+ |
| **Filing Types** | 10-K, 10-Q |

## Data Source

- **Source:** SEC EDGAR Company Facts API
- **URL:** https://data.sec.gov/api/xbrl/companyfacts/
- **Format:** JSON (XBRL-derived)
- **Refresh:** Quarterly

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline (1M+ facts)
python src/main.py --mode full

# Run for specific companies
python src/main.py --ciks CIK0000320193,CIK0000789019 --years 5

# Generate reports
python src/main.py --mode report
```

## Pipeline Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│   EXTRACT   │───▶│  TRANSFORM   │───▶│    LOAD     │───▶│   ANALYZE    │
│  SEC EDGAR  │    │  Normalize   │    │ Star Schema │    │    KPIs      │
│  1M+ Facts  │    │  Validate    │    │  PostgreSQL │    │   Reports    │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
```

## Star Schema

```
                    ┌─────────────────┐
                    │    dim_date     │
                    │  (fiscal_year,  │
                    │   fiscal_qtr)   │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│   dim_company   │──────────┼──────────│   dim_metric    │
│  (cik, ticker,  │          │          │  (metric_name,  │
│   name, sector) │          │          │   category)     │
└────────┬────────┘          │          └────────┬────────┘
         │                   │                   │
         │          ┌────────┴────────┐          │
         └──────────│  fact_financials │──────────┘
                    │  (value, unit,   │
                    │   accession_num) │
                    └─────────────────┘
```

## Financial KPIs Calculated

### Profitability
- Gross Margin, Operating Margin, Net Margin
- ROE, ROA, ROIC
- EPS (Basic, Diluted)

### Liquidity
- Current Ratio, Quick Ratio
- Cash Ratio, Working Capital

### Leverage
- Debt-to-Equity, Debt-to-Assets
- Interest Coverage Ratio
- Long-term Debt Ratio

### Efficiency
- Asset Turnover, Inventory Turnover
- Receivables Turnover, Payables Turnover
- Days Sales Outstanding

### Valuation
- P/E Ratio, P/B Ratio, P/S Ratio
- EV/EBITDA, EV/Revenue
- Dividend Yield

## Output Files

```
output/
├── fact_financials.parquet      # 1M+ rows - all financial facts
├── dim_company.parquet          # 500+ companies
├── dim_metric.parquet           # 200+ metrics
├── dim_date.parquet             # Time dimension
├── kpi_calculations.parquet     # Calculated KPIs
├── sector_analysis.csv          # Sector aggregations
├── top_performers.csv           # Ranked by metrics
└── data_quality_report.csv      # Validation results
```

## Data Quality

| Check | Threshold | Action |
|-------|-----------|--------|
| Null Values | <5% per column | Flag |
| Duplicate Facts | 0% | Dedupe |
| Value Range | Industry norms | Alert |
| Referential Integrity | 100% | Fail |

## Verification

Every data point can be verified:
1. Go to https://www.sec.gov/cgi-bin/browse-edgar
2. Search by CIK
3. Open 10-K or 10-Q filing
4. Compare values in XBRL viewer

## Project Structure

```
P01_SEC_Financial/
├── src/
│   ├── extract.py          # SEC EDGAR API client
│   ├── transform.py        # Data normalization
│   ├── load.py             # Schema loading
│   ├── kpis.py             # Financial calculations
│   ├── quality.py          # Data validation
│   └── main.py             # Pipeline orchestration
├── sql/
│   └── schema.sql          # Star schema DDL
├── data/                   # Raw data cache
├── output/                 # Processed outputs
├── tests/
│   └── test_kpis.py        # Unit tests
├── requirements.txt
└── README.md
```

## Performance

| Operation | Time | Records |
|-----------|------|---------|
| Full Extract | ~30 min | 1M+ facts |
| Transform | ~10 min | 1M+ rows |
| KPI Calculation | ~5 min | 50K+ KPIs |
| Total Pipeline | ~45 min | Complete |

## Author

Mboya Jeffers | MboyaJeffers9@gmail.com | github.com/mboyajeffers

All data sourced from SEC EDGAR. Fully verifiable.
