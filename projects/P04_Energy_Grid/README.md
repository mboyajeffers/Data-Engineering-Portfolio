# Energy Grid Analytics Pipeline

**Author:** Mboya Jeffers
**Scale:** 500K+ Grid Readings
**Source:** EIA-930 Hourly Electric Grid Monitor

---

## Overview

Enterprise-scale data pipeline analyzing U.S. electrical grid operations. Processes 500K+ hourly readings from the EIA-930 dataset to monitor demand, generation, and interchange across regional balancing authorities.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    ENERGY GRID PIPELINE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │  EXTRACT │───▶│  TRANSFORM   │───▶│       ANALYZE        │  │
│  │          │    │              │    │                      │  │
│  │ EIA-930  │    │ Star Schema  │    │ Demand Forecasting   │  │
│  │ Hourly   │    │ Dimensions   │    │ Generation Mix       │  │
│  │ 500K+ RW │    │ Fact Tables  │    │ Regional Patterns    │  │
│  └──────────┘    └──────────────┘    └──────────────────────┘  │
│                                                                  │
│  Data Flow: EIA API → Parquet → Star Schema → Analytics         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Star Schema

```
                    ┌─────────────────┐
                    │ dim_balancing   │
                    │ _authority      │
                    ├─────────────────┤
                    │ ba_id        PK │
                    │ ba_code         │
                    │ ba_name         │
                    │ region          │
                    │ timezone        │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  dim_fuel_type  │          │          │   dim_datetime  │
├─────────────────┤          │          ├─────────────────┤
│ fuel_id      PK │          │          │ datetime_id  PK │
│ fuel_code       │          │          │ timestamp       │
│ fuel_name       │◀─────────┼─────────▶│ date            │
│ fuel_category   │          │          │ hour            │
│ is_renewable    │          │          │ day_of_week     │
│ is_clean        │          │          │ month           │
└─────────────────┘          │          │ quarter         │
                             ▼          │ year            │
                    ┌─────────────────┐ │ is_peak_hour    │
                    │  fact_grid_ops  │ └─────────────────┘
                    ├─────────────────┤
                    │ fact_id      PK │
                    │ ba_id        FK │
                    │ datetime_id  FK │
                    │ fuel_id      FK │
                    │                 │
                    │ demand_mw       │
                    │ generation_mw   │
                    │ net_generation  │
                    │ interchange     │
                    │ demand_forecast │
                    └─────────────────┘
```

## Key Metrics Calculated

### Demand Analytics
- Peak demand by region
- Hourly load profiles
- Demand forecast accuracy
- Seasonal patterns
- Day-of-week variations

### Generation Mix
- Renewable generation share
- Clean energy percentage
- Fuel type distribution
- Generation efficiency
- Capacity utilization

### Grid Operations
- Net interchange flows
- Regional import/export balance
- Peak shaving effectiveness
- Load balancing metrics
- Grid stability indicators

## Data Source

**EIA-930 Hourly Electric Grid Monitor**
- Source: eia.gov
- Format: JSON/CSV API
- Frequency: Hourly updates
- Coverage: All U.S. balancing authorities
- History: 2015-Present
- Documentation: https://www.eia.gov/opendata/

## Pipeline Modes

| Mode | Target Rows | Time Range | Use Case |
|------|-------------|------------|----------|
| full | 500,000+ | 2 years | Production analytics |
| test | 100,000 | 6 months | Development testing |
| quick | 50,000 | 1 month | Quick validation |

## Output Files

```
output/
├── dim_balancing_authority.parquet  # ~60 balancing authorities
├── dim_fuel_type.parquet            # ~15 fuel types
├── dim_datetime.parquet             # ~17K+ hourly periods
├── fact_grid_ops.parquet            # 500K+ readings
├── demand_analysis.csv              # Regional demand patterns
├── generation_mix.csv               # Fuel type breakdown
├── regional_summary.csv             # BA-level metrics
└── pipeline_summary.txt             # Execution metrics
```

## Usage

```bash
# Full extraction (500K+ readings)
python src/main.py --mode full

# Test mode (100K readings)
python src/main.py --mode test

# Generate reports from existing data
python src/main.py --mode report
```

## Technical Highlights

- **EIA API Integration**: Authenticated API access with rate limiting
- **Hourly Granularity**: Sub-daily grid operations monitoring
- **Time Zone Handling**: UTC normalization across regions
- **Efficient Storage**: Parquet with time-based partitioning
- **Real-Time Ready**: Architecture supports streaming updates

## Balancing Authorities Tracked

| Region | BAs | Coverage |
|--------|-----|----------|
| Eastern | 20+ | PJM, MISO, NYISO, ISONE |
| Western | 15+ | CAISO, BPA, WAPA |
| Texas | 1 | ERCOT |
| Other | Various | SPP, AECI, etc. |

## Requirements

```
pandas>=2.0.0
pyarrow>=14.0.0
requests>=2.31.0
tqdm>=4.66.0
```

---

*Pipeline processes energy grid data from EIA-930 Hourly Electric Grid Monitor*
