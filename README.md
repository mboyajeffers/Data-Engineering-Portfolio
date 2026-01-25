# Data KPI Pipeline Demo

**A reference implementation demonstrating enterprise-grade data engineering and analytics engineering practices for transactional KPI pipelines.**

> **Note**: All data, reports, and outputs in this repository are synthetic examples created for portfolio demonstration purposes. No proprietary code or client information is included.

---

## Overview

This repository showcases a complete analytics data pipeline built with industry-standard patterns. It demonstrates the full lifecycle from raw data ingestion through validation, transformation, KPI computation, and executive-ready deliverables.

The pipeline processes transactional data across multiple industries and produces:
- **Validated, cleaned datasets** (CSV/Parquet)
- **KPI metrics** (JSON)
- **Executive summary reports** (PDF/Markdown)
- **Data quality scorecards**

### Industry Case Studies
- **E-Commerce** - Luxury jewelry retailer (800K transactions)
- **iGaming** - Online casino platform (400K events) with responsible gaming compliance
- **Brokerage** - FCA-regulated European broker (650K trades) with MiFID II support
- **Oil & Gas** - Upstream production operations (500K records) with multi-basin analysis
- **Sports Betting** - Multi-jurisdiction operator (5K bets) with RG compliance
- **Compliance** - Crypto AML/KYC analytics (5K transactions) with pattern detection

---

## What This Demonstrates

### Data Engineering
- Schema contract enforcement and validation gates
- Multi-stage data quality checks (nulls, duplicates, ranges, referential integrity)
- Quarantine handling for invalid records
- Reconciliation checks suitable for regulated environments
- Idempotent, re-runnable pipeline design
- Structured logging and observability patterns
- Configuration-driven architecture (YAML contracts)

### Analytics Engineering
- Standardized KPI definitions with documented formulas
- Metric catalog with grain, unit, and caveats
- Revenue, customer, product, and operational KPIs
- Segment and category breakdowns
- Repeat purchase rate and customer lifetime value
- Executive-ready report generation

---

## Pipeline Architecture

```
+--------------+    +--------------+    +--------------+    +--------------+    +--------------+
|   INGEST     | -> |  VALIDATE    | -> |  TRANSFORM   | -> |   METRICS    | -> |   EXPORT     |
|              |    |              |    |              |    |              |    |              |
| Load files   |    | Schema       |    | Normalize    |    | Compute      |    | Reports      |
| Type cast    |    | Nulls        |    | Aliases      |    | KPIs         |    | Datasets     |
| Normalize    |    | Duplicates   |    | Dates        |    | Aggregates   |    | Scorecards   |
| columns      |    | Ranges       |    | Derived      |    | Breakdowns   |    | JSON         |
+--------------+    +--------------+    +--------------+    +--------------+    +--------------+
```

See [docs/architecture.md](docs/architecture.md) for detailed diagrams.

---

## Repository Structure

```
data-kpi-demo/
├── README.md
├── LICENSE
├── requirements.txt
│
├── docs/
│   ├── methodology.md        # Data processing methodology
│   ├── quality_scorecard.md  # Quality check definitions
│   ├── kpi_catalog.md        # KPI definitions (20 KPIs)
│   ├── architecture.md       # Pipeline architecture diagrams
│   ├── outputs/
│   │   ├── pdf/              # Sample executive reports
│   │   └── md/               # Markdown report outputs
│   └── case-study/           # Demo case study
│
├── data/
│   └── sample_*.csv          # Sample transaction data (200 rows)
│
├── src/demo_pipeline/
│   ├── ingest.py             # Data ingestion module
│   ├── validate.py           # Validation module
│   ├── transform.py          # Transformation module
│   ├── metrics.py            # KPI computation module
│   ├── report_stub.py        # Report generation
│   └── run_demo.py           # Pipeline orchestrator
│
├── configs/
│   ├── schema_contract.yaml  # Expected data schema
│   ├── validation_rules.yaml # Quality check rules
│   └── kpi_definitions.yaml  # KPI formulas
│
└── tests/
    ├── test_validation.py    # Validation tests
    └── test_metrics.py       # KPI computation tests
```

---

## Quick Start

### Prerequisites
- Python 3.10+
- pip

### Installation

```bash
git clone https://github.com/CleanMetricsStudio/data-kpi-demo.git
cd data-kpi-demo
pip install -r requirements.txt
```

### Run the Pipeline

```bash
# Run with sample data
python -m src.demo_pipeline.run_demo

# Run with custom input
python -m src.demo_pipeline.run_demo --input data/sample_luxury_jewelry_transactions.csv

# Run with verbose logging
python -m src.demo_pipeline.run_demo -v
```

### Run Tests

```bash
pytest tests/ -v
```

---

## Sample Outputs

### Industry Case Studies

#### E-Commerce (Luxury Jewelry)
- [Executive Summary Report](docs/case-studies/ecommerce-jewelry/Demo_Executive_Summary_Report.pdf)
- [Case Study: Luxe Gemstone](docs/case-studies/ecommerce-jewelry/Demo_Case_Study_Luxe_Gemstone.pdf)
- [Customer Intelligence Report](docs/case-studies/ecommerce-jewelry/Demo_Customer_Intelligence_Report.pdf)
- [Product Category Report](docs/case-studies/ecommerce-jewelry/Demo_Product_Category_Report.pdf)
- [Sales Performance Report](docs/case-studies/ecommerce-jewelry/Demo_Sales_Performance_Report.pdf)

#### iGaming (Online Casino)
- [Executive Summary Report](docs/case-studies/igaming-casino/Demo_Executive_Summary_Report.pdf)
- [Gaming Revenue Report](docs/case-studies/igaming-casino/Demo_Gaming_Revenue_Report.pdf)
- [Player Intelligence Report](docs/case-studies/igaming-casino/Demo_Player_Intelligence_Report.pdf)
- [Game Market Report](docs/case-studies/igaming-casino/Demo_Game_Market_Report.pdf)

#### Brokerage (UK/European)
- [Executive Summary Report](docs/case-studies/brokerage-uk/Demo_Executive_Summary_Report.pdf)
- [Case Study: Sterling Capital](docs/case-studies/brokerage-uk/Demo_Case_Study_Sterling_Capital.pdf)
- [Client Intelligence Report](docs/case-studies/brokerage-uk/Demo_Client_Intelligence_Report.pdf)
- [Trading Revenue Report](docs/case-studies/brokerage-uk/Demo_Trading_Revenue_Report.pdf)
- [Asset Market Report](docs/case-studies/brokerage-uk/Demo_Asset_Market_Report.pdf)

#### Oil & Gas (Production Operations)
- [Oil & Gas Analytics Report](docs/case-studies/oilgas-energy/Demo_Oil_Gas_Analytics_Report.pdf)

#### Sports Betting (Multi-Jurisdiction)
- [Executive Summary Report](docs/case-studies/betting-sports/Demo_Executive_Summary_Report.md)
- [Case Study: FastLane](docs/case-studies/betting-sports/Demo_Case_Study_FastLane.md)
- [Betting Revenue Report](docs/case-studies/betting-sports/Demo_Betting_Revenue_Report.md)
- [Player Intelligence Report](docs/case-studies/betting-sports/Demo_Player_Intelligence_Report.md)
- [Sports Market Report](docs/case-studies/betting-sports/Demo_Sports_Market_Report.md)

#### Compliance (Crypto AML/KYC)
- [Compliance Analytics Report](docs/case-studies/compliance-aml/Demo_Compliance_Analytics_Report.pdf)

### Documentation
- [Methodology](docs/methodology.md) - Data processing approach
- [Quality Scorecard](docs/quality_scorecard.md) - Validation checks
- [KPI Catalog](docs/kpi_catalog.md) - Metric definitions
- [Architecture](docs/architecture.md) - System design
- [Case Studies Overview](docs/case-studies/README.md) - All industry demos

---

## KPI Highlights

| KPI | Description |
|-----|-------------|
| Total Revenue | Sum of completed order totals |
| Average Order Value | Mean revenue per order |
| Unique Customers | Distinct customer count |
| Repeat Purchase Rate | % of customers with >1 order |
| Revenue by Segment | Breakdown by customer tier |
| Category Mix | Revenue % by product category |
| Discount Utilization | % of orders using discounts |

See [docs/kpi_catalog.md](docs/kpi_catalog.md) for complete definitions.

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.10+ |
| Data Processing | pandas, NumPy |
| Configuration | YAML |
| Testing | pytest |
| Output Formats | CSV, JSON, Parquet, PDF, Markdown |

---

## License

MIT License - see [LICENSE](LICENSE) for details.
