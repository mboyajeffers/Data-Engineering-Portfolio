# Medicare Prescriber Intelligence Pipeline

**Author:** Mboya Jeffers
**Scale:** 5M+ Prescription Records
**Source:** CMS Medicare Part D Public Use Files

---

## Overview

Enterprise-scale data pipeline analyzing Medicare Part D prescriber data. Processes 5M+ prescription claims to identify prescribing patterns, drug utilization trends, and cost analytics across 1M+ healthcare providers.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                 MEDICARE PRESCRIBER PIPELINE                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │  EXTRACT │───▶│  TRANSFORM   │───▶│       ANALYZE        │  │
│  │          │    │              │    │                      │  │
│  │ CMS Part │    │ Star Schema  │    │ Prescriber Patterns  │  │
│  │ D Files  │    │ Dimensions   │    │ Drug Utilization     │  │
│  │ 5M+ rows │    │ Fact Tables  │    │ Cost Analytics       │  │
│  └──────────┘    └──────────────┘    └──────────────────────┘  │
│                                                                  │
│  Data Flow: CMS Files → Parquet → Star Schema → Analytics       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Star Schema

```
                    ┌─────────────────┐
                    │  dim_prescriber │
                    ├─────────────────┤
                    │ prescriber_id PK│
                    │ npi             │
                    │ provider_name   │
                    │ specialty       │
                    │ credential      │
                    │ entity_type     │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│    dim_drug     │          │          │   dim_location  │
├─────────────────┤          │          ├─────────────────┤
│ drug_id      PK │          │          │ location_id  PK │
│ drug_name       │          │          │ city            │
│ generic_name    │          │          │ state           │
│ brand_name      │◀─────────┼─────────▶│ zip_code        │
│ drug_class      │          │          │ ruca_code       │
│ is_opioid       │          │          │ urban_rural     │
│ is_antibiotic   │          │          └─────────────────┘
└─────────────────┘          │
                             ▼
                    ┌─────────────────┐
                    │fact_prescriptions│
                    ├─────────────────┤
                    │ fact_id      PK │
                    │ prescriber_id FK│
                    │ drug_id      FK │
                    │ location_id  FK │
                    │ year_id      FK │
                    │                 │
                    │ total_claims    │
                    │ total_day_supply│
                    │ total_cost      │
                    │ bene_count      │
                    │ avg_cost_claim  │
                    │ ge65_claims     │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │    dim_year     │
                    ├─────────────────┤
                    │ year_id      PK │
                    │ calendar_year   │
                    └─────────────────┘
```

## Key Metrics Calculated

### Prescriber Analytics
- Total claims per prescriber
- Cost per beneficiary
- Specialty prescribing patterns
- Opioid prescribing rates
- Brand vs generic ratio

### Drug Utilization
- Top drugs by claim volume
- Cost trends by drug class
- Generic substitution rates
- Opioid utilization metrics
- Antibiotic prescribing patterns

### Geographic Analysis
- State-level prescribing patterns
- Urban vs rural differences
- Regional cost variations
- Specialty distribution by area

## Data Source

**CMS Medicare Part D Prescriber Public Use Files**
- Source: data.cms.gov
- Format: CSV (compressed)
- Coverage: All Medicare Part D prescribers
- Years: 2013-2022
- Documentation: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers

## Pipeline Modes

| Mode | Target Rows | Years | Use Case |
|------|-------------|-------|----------|
| full | 5,000,000+ | 2019-2022 | Production analytics |
| test | 500,000 | 2021-2022 | Development testing |
| quick | 100,000 | 2022 | Quick validation |

## Output Files

```
output/
├── dim_prescriber.parquet    # ~1M prescribers
├── dim_drug.parquet          # ~5K drugs
├── dim_location.parquet      # ~50K locations
├── dim_year.parquet          # ~10 years
├── fact_prescriptions.parquet # 5M+ prescription records
├── prescriber_summary.csv    # Top prescribers
├── drug_utilization.csv      # Drug analytics
├── opioid_report.csv         # Opioid prescribing analysis
└── pipeline_summary.txt      # Execution metrics
```

## Usage

```bash
# Full extraction (5M+ records)
python src/main.py --mode full

# Test mode (500K records)
python src/main.py --mode test

# Generate reports from existing data
python src/main.py --mode report
```

## Technical Highlights

- **Bulk File Processing**: Efficient chunked reading of large CSVs
- **NPI Validation**: Provider deduplication and validation
- **Drug Classification**: Automated opioid/antibiotic flagging
- **RUCA Codes**: Urban/rural classification
- **Scalable Storage**: Parquet with compression

## Requirements

```
pandas>=2.0.0
pyarrow>=14.0.0
requests>=2.31.0
tqdm>=4.66.0
```

---

*Pipeline processes Medicare Part D data from CMS public use files*
