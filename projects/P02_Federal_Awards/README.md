# Federal Awards Intelligence Pipeline

**Author:** Mboya Jeffers
**Scale:** 1M+ Federal Awards
**Source:** USASpending.gov Bulk Data API

---

## Overview

Enterprise-scale data pipeline extracting and analyzing federal contract and grant awards from USASpending.gov. Processes 1M+ award transactions with dimensional modeling for procurement analytics.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    FEDERAL AWARDS PIPELINE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │  EXTRACT │───▶│  TRANSFORM   │───▶│       ANALYZE        │  │
│  │          │    │              │    │                      │  │
│  │ USASpend │    │ Star Schema  │    │ Agency Spend Trends  │  │
│  │ Bulk API │    │ Dimensions   │    │ Contractor Rankings  │  │
│  │ 1M+ rows │    │ Fact Tables  │    │ Geographic Patterns  │  │
│  └──────────┘    └──────────────┘    └──────────────────────┘  │
│                                                                  │
│  Data Flow: API → Parquet → Star Schema → Analytics Reports     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Star Schema

```
                    ┌─────────────────┐
                    │   dim_agency    │
                    ├─────────────────┤
                    │ agency_id   PK  │
                    │ agency_code     │
                    │ agency_name     │
                    │ sub_agency      │
                    │ department      │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  dim_recipient  │          │          │   dim_location  │
├─────────────────┤          │          ├─────────────────┤
│ recipient_id PK │          │          │ location_id  PK │
│ recipient_name  │          │          │ city            │
│ duns_number     │          │          │ state_code      │
│ business_type   │          │          │ state_name      │
│ recipient_type  │◀─────────┼─────────▶│ county          │
└─────────────────┘          │          │ zip_code        │
                             │          │ country         │
                             ▼          └─────────────────┘
                    ┌─────────────────┐
                    │  fact_awards    │
                    ├─────────────────┤
                    │ award_id     PK │
                    │ agency_id    FK │
                    │ recipient_id FK │
                    │ location_id  FK │
                    │ date_id      FK │
                    │                 │
                    │ award_amount    │
                    │ obligation_amt  │
                    │ outlay_amount   │
                    │ award_type      │
                    │ cfda_number     │
                    │ naics_code      │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │    dim_date     │
                    ├─────────────────┤
                    │ date_id      PK │
                    │ fiscal_year     │
                    │ fiscal_quarter  │
                    │ calendar_year   │
                    │ calendar_month  │
                    └─────────────────┘
```

## Key Metrics Calculated

### Spending Analysis
- Total obligations by agency
- Year-over-year spending growth
- Average award size by type
- Spending concentration (top 10 recipients)

### Contractor Analysis
- Top contractors by award volume
- Small business participation rates
- Geographic distribution of awards
- Industry sector breakdown (NAICS)

### Program Analysis
- CFDA program effectiveness
- Award type distribution
- Competition rates
- Modification patterns

## Data Source

**USASpending.gov Bulk Download API**
- Endpoint: `https://api.usaspending.gov/api/v2/bulk_download/`
- Format: CSV/ZIP bulk files
- Update Frequency: Daily
- Historical Data: FY2008-Present
- Documentation: https://api.usaspending.gov/

## Pipeline Modes

| Mode | Target Rows | Fiscal Years | Use Case |
|------|-------------|--------------|----------|
| full | 1,000,000+ | FY2020-2024 | Production analytics |
| test | 100,000 | FY2023-2024 | Development testing |
| quick | 50,000 | FY2024 | Quick validation |

## Output Files

```
output/
├── dim_agency.parquet          # ~500 agencies
├── dim_recipient.parquet       # ~100K+ recipients
├── dim_location.parquet        # ~50K locations
├── dim_date.parquet            # ~60 periods
├── fact_awards.parquet         # 1M+ awards
├── agency_spending_summary.csv # Analytics report
├── top_contractors.csv         # Contractor rankings
└── pipeline_summary.txt        # Execution metrics
```

## Usage

```bash
# Full extraction (1M+ awards)
python src/main.py --mode full

# Test mode (100K awards)
python src/main.py --mode test

# Generate reports from existing data
python src/main.py --mode report
```

## Technical Highlights

- **Bulk API Integration**: Efficient bulk download vs pagination
- **Incremental Loading**: Delta detection for daily updates
- **Data Quality**: Validation gates for completeness/accuracy
- **Scalable Storage**: Parquet with predicate pushdown
- **Analytics-Ready**: Pre-aggregated summary tables

## Requirements

```
pandas>=2.0.0
pyarrow>=14.0.0
requests>=2.31.0
tqdm>=4.66.0
```

---

*Pipeline processes federal spending data from USASpending.gov*
