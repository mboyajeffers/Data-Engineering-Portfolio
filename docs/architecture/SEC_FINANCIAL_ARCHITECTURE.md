# SEC Financial Pipeline Architecture
## P2-SEC: Enterprise Data Engineering on SEC EDGAR XBRL

**Author:** Mboya Jeffers
**Version:** 1.0.0
**Pipeline:** `/pipelines/sec_financial/pipeline.py` (889 lines)

---

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     SEC FINANCIAL PIPELINE (P2-SEC)                         │
│                    SEC EDGAR API → XBRL Facts → Financial KPIs              │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
     │  SEC EDGAR   │      │  40+ Public  │      │   ~500K+     │
     │    XBRL API  │─────▶│  Companies   │─────▶│  XBRL Facts  │
     │   (Public)   │      │  All Sectors │      │   Fetched    │
     └──────────────┘      └──────────────┘      └──────────────┘
            │                                           │
            ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXTRACT                                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ SECEdgarClient                                                         ││
│  │  • /submissions/CIK{cik}.json    → Company metadata + filings          ││
│  │  • /api/xbrl/companyfacts/CIK{cik}.json → All XBRL facts               ││
│  │  • Rate limiting: 0.15s (SEC policy: 10 req/sec max)                   ││
│  │  • User-Agent required (SEC policy)                                    ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TRANSFORM                                                                  │
│  ┌──────────────────────┐   ┌──────────────────────────────────────────────┐│
│  │  DataCleaner         │   │  DataModeler                                 ││
│  │  • Numeric conversion│   │  • Star schema creation                      ││
│  │  • Date parsing      │──▶│  • Company dimension                         ││
│  │  • Period type logic │   │  • Filings dimension                         ││
│  │  • Canonical mapping │   │  • Concept mapping                           ││
│  │  • Fact ID hashing   │   │  • XBRL facts table                          ││
│  └──────────────────────┘   └──────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  VALIDATE                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ QualityGateRunner (5 Gates)                                            ││
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐││
│  │  │   Unit    │ │  Period   │ │ Coverage  │ │Restatement│ │ Outliers  │││
│  │  │Consistency│ │   Logic   │ │           │ │           │ │           │││
│  │  │   (20%)   │ │   (20%)   │ │   (25%)   │ │   (15%)   │ │   (20%)   │││
│  │  └───────────┘ └───────────┘ └───────────┘ └───────────┘ └───────────┘││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LOAD                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ KPICalculator                                                          ││
│  │  • Company metrics (revenue, income, margins, ROA, ROE)                ││
│  │  • Cohort benchmarking (percentiles across companies)                  ││
│  │  • Coverage statistics (concept availability)                          ││
│  │  • Summary statistics                                                  ││
│  └────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ Outputs:                                                                ││
│  │  • raw_xbrl_facts.csv           → Raw XBRL data                        ││
│  │  • raw_filings.csv              → Filing metadata                      ││
│  │  • raw_companies.csv            → Company metadata                     ││
│  │  • cleaned_xbrl_facts.csv       → Standardized facts                   ││
│  │  • company_dim.csv              → Company dimension                    ││
│  │  • filings_dim.csv              → Filings dimension                    ││
│  │  • concept_map.csv              → XBRL → canonical mapping             ││
│  │  • xbrl_facts.csv               → Fact table                           ││
│  │  • kpis.json                    → Financial metrics                    ││
│  │  • pipeline_metrics.json        → Execution telemetry                  ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Star Schema Data Model

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         DIMENSIONAL MODEL                                  │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────────┐                         ┌───────────────────────┐
│     company_dim       │                         │     filings_dim       │
├───────────────────────┤                         ├───────────────────────┤
│ company_id (PK)       │                         │ filing_id (PK)        │
│ cik                   │                         │ cik                   │
│ name                  │                         │ accession             │
│ ticker                │                         │ form (10-K, 10-Q, 8-K)│
│ sector                │                         │ filing_date           │
│ sic                   │                         │ report_date           │
│ sic_description       │                         │ primary_document      │
│ fiscal_year_end       │                         └───────────────────────┘
│ state                 │
└───────────────────────┘
          │
          │ 1:N
          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           xbrl_facts                                     │
├─────────────────────────────────────────────────────────────────────────┤
│ fact_id (PK)           │ Generated MD5 hash                             │
│ company_id (FK)        │ → company_dim                                  │
│ cik                    │ SEC Central Index Key                          │
│ ticker                 │ Stock symbol                                   │
│ taxonomy               │ us-gaap                                        │
│ concept                │ Raw XBRL concept name                          │
│ canonical_metric       │ Mapped standard metric (revenue, net_income)   │
│ unit                   │ USD, shares, pure                              │
│ value                  │ Numeric value                                  │
│ period_start           │ Start of reporting period                      │
│ period_end             │ End of reporting period                        │
│ period_type            │ instant | quarterly | annual | multi-year      │
│ accession              │ SEC accession number                           │
│ fiscal_year            │ Company fiscal year                            │
│ fiscal_period          │ FY, Q1, Q2, Q3, Q4                             │
│ form                   │ 10-K, 10-Q, 8-K                                │
│ filed                  │ Filing date                                    │
└─────────────────────────────────────────────────────────────────────────┘

┌───────────────────────┐
│     concept_map       │
├───────────────────────┤
│ raw_concept           │   ← Raw XBRL tag (e.g., RevenueFromContract...)
│ canonical_metric      │   ← Standardized name (revenue)
│ category              │   ← Income Statement | Balance Sheet | Cash Flow
└───────────────────────┘
```

---

## XBRL Concept Mapping

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         XBRL CONCEPT MAPPING                                │
│             Raw XBRL Tags → Canonical Financial Metrics                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ INCOME STATEMENT                                                           │
├────────────────────────────────────────────────────────────────────────────┤
│ revenue          ←  Revenues                                               │
│                     RevenueFromContractWithCustomerExcludingAssessedTax    │
│                     SalesRevenueNet                                        │
│                     RevenuesNetOfInterestExpense                           │
├────────────────────────────────────────────────────────────────────────────┤
│ cost_of_revenue  ←  CostOfGoodsAndServicesSold                             │
│                     CostOfRevenue                                          │
│                     CostOfGoodsSold                                        │
├────────────────────────────────────────────────────────────────────────────┤
│ gross_profit     ←  GrossProfit                                            │
├────────────────────────────────────────────────────────────────────────────┤
│ operating_income ←  OperatingIncomeLoss                                    │
│                     OperatingIncome                                        │
├────────────────────────────────────────────────────────────────────────────┤
│ net_income       ←  NetIncomeLoss                                          │
│                     NetIncome                                              │
│                     ProfitLoss                                             │
├────────────────────────────────────────────────────────────────────────────┤
│ eps              ←  EarningsPerShareDiluted                                │
│                     EarningsPerShareBasic                                  │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ BALANCE SHEET                                                              │
├────────────────────────────────────────────────────────────────────────────┤
│ assets           ←  Assets                                                 │
│ liabilities      ←  Liabilities                                            │
│ equity           ←  StockholdersEquity                                     │
│                     StockholdersEquityIncluding...                         │
│ cash             ←  CashAndCashEquivalentsAtCarryingValue                  │
│                     Cash                                                   │
│ current_assets   ←  AssetsCurrent                                          │
│ current_liabilities ← LiabilitiesCurrent                                   │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ CASH FLOW                                                                  │
├────────────────────────────────────────────────────────────────────────────┤
│ operating_cash_flow  ←  NetCashProvidedByUsedInOperatingActivities         │
│ investing_cash_flow  ←  NetCashProvidedByUsedInInvestingActivities         │
│ financing_cash_flow  ←  NetCashProvidedByUsedInFinancingActivities         │
│ capex                ←  PaymentsToAcquirePropertyPlantAndEquipment         │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Quality Gate Framework

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SEC FINANCIAL QUALITY GATES                              │
│                   (Financial Data-Specific Validation)                      │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┬────────────────────────────────────────────────┬──────────┐
│ Gate            │ Validation Logic                               │ Weight   │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Unit            │ • Each concept uses consistent units           │   20%    │
│ Consistency     │ • Flag concepts with multiple unit types       │          │
│                 │ • Pass threshold: 90%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Period Logic    │ • Classify: instant vs quarterly vs annual     │   20%    │
│                 │ • Based on period_start to period_end duration │          │
│                 │ • Pass threshold: 95%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Coverage        │ • Required metrics per company:                │   25%    │
│                 │   revenue, net_income, assets, equity          │          │
│                 │ • Pass threshold: 80% company coverage         │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Restatement     │ • Detect same concept+period with different    │   15%    │
│                 │   values across filings                        │          │
│                 │ • Informational (not a failure)                │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Outliers        │ • Z-score analysis within company/metric       │   20%    │
│                 │ • Flag extreme outliers (z > 5)                │          │
│                 │ • Pass threshold: 85%                          │          │
└─────────────────┴────────────────────────────────────────────────┴──────────┘
```

---

## Period Type Classification

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PERIOD TYPE CLASSIFICATION LOGIC                         │
└─────────────────────────────────────────────────────────────────────────────┘

              ┌──────────────────┐
              │   period_start   │
              │   period_end     │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │  period_start    │───── NULL ────▶ INSTANT (point-in-time)
              │    is NULL?      │                  (Balance sheet items)
              └────────┬─────────┘
                       │ NOT NULL
                       ▼
              ┌──────────────────┐
              │  days = end -    │
              │         start    │
              └────────┬─────────┘
                       │
         ┌─────────────┼─────────────┐─────────────┐
         ▼             ▼             ▼             ▼
    days < 100    100 ≤ days    days ≥ 400    Invalid
         │         < 400             │             │
         ▼             ▼             ▼             ▼
    QUARTERLY      ANNUAL      MULTI-YEAR     UNKNOWN
    (Q1/Q2/Q3/Q4) (10-K data)  (Rare cases)

Example:
  • period_start: NULL → INSTANT (Assets, Liabilities)
  • period_start: 2024-01-01, period_end: 2024-03-31 (90 days) → QUARTERLY
  • period_start: 2024-01-01, period_end: 2024-12-31 (365 days) → ANNUAL
```

---

## API Integration Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      SEC EDGAR API INTEGRATION                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Endpoint 1: Company Submissions                                            │
│  ─────────────────────────────────────────────────────────────────────────  │
│  GET https://data.sec.gov/submissions/CIK{cik}.json                         │
│                                                                             │
│  Returns:                                                                   │
│  • Company name, SIC code, state of incorporation                           │
│  • Fiscal year end                                                          │
│  • Recent filings (form type, filing date, accession number)                │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Endpoint 2: Company Facts (XBRL)                                           │
│  ─────────────────────────────────────────────────────────────────────────  │
│  GET https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json               │
│                                                                             │
│  Returns:                                                                   │
│  • All XBRL facts filed by the company                                      │
│  • Organized by taxonomy (us-gaap, dei, etc.)                               │
│  • Each fact includes: value, period, unit, accession, fiscal info          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Rate Limiting & Requirements                                               │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  • User-Agent header REQUIRED:                                              │
│    "PortfolioProject/1.0 (email@domain.com)"                                │
│                                                                             │
│  • Rate limit: 10 requests/second max                                       │
│    Pipeline uses: 0.15s delay (6.7 req/sec)                                 │
│                                                                             │
│  • No authentication required (public data)                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Company Cohort

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      40+ COMPANY COHORT BY SECTOR                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────┬──────────────────────────────────────────────────────────┐
│ TECHNOLOGY (8) │ AAPL, MSFT, GOOGL, AMZN, META, NVDA, CRM, V              │
├────────────────┼──────────────────────────────────────────────────────────┤
│ HEALTHCARE (5) │ JNJ, PFE, ABBV, MRK, ABT                                 │
├────────────────┼──────────────────────────────────────────────────────────┤
│ FINANCIAL (6)  │ JPM, BAC, BRK-B, C, WFC, GS                              │
├────────────────┼──────────────────────────────────────────────────────────┤
│ CONSUMER (7)   │ KO, PEP, PG, NKE, WMT, DIS, COST                         │
├────────────────┼──────────────────────────────────────────────────────────┤
│ INDUSTRIAL (6) │ BA, GE, XOM, CVX, IBM, CSCO                              │
├────────────────┼──────────────────────────────────────────────────────────┤
│ COMMS (3)      │ T, VZ, NFLX                                              │
├────────────────┼──────────────────────────────────────────────────────────┤
│ REAL ESTATE (2)│ AMT, PLD                                                 │
├────────────────┼──────────────────────────────────────────────────────────┤
│ MATERIALS (1)  │ LIN                                                      │
├────────────────┼──────────────────────────────────────────────────────────┤
│ UTILITIES (2)  │ NEE, DUK                                                 │
└────────────────┴──────────────────────────────────────────────────────────┘
```

---

## KPI Calculations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       FINANCIAL KPI FRAMEWORK                               │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 1. COMPANY METRICS (per ticker)                                            │
├────────────────────────────────────────────────────────────────────────────┤
│ Core Metrics:                                                              │
│   • revenue, net_income, gross_profit, operating_income                    │
│   • assets, liabilities, equity                                            │
│   • operating_cash_flow                                                    │
│                                                                            │
│ Calculated Ratios:                                                         │
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │ net_margin    = net_income / revenue                              │    │
│   │ gross_margin  = gross_profit / revenue                            │    │
│   │ roa           = net_income / assets    (Return on Assets)         │    │
│   │ roe           = net_income / equity    (Return on Equity)         │    │
│   └───────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 2. COHORT BENCHMARKS                                                       │
├────────────────────────────────────────────────────────────────────────────┤
│ For revenue, net_income, assets:                                           │
│   • min, max, mean, std                                                    │
│   • p25, median (p50), p75                                                 │
│                                                                            │
│ Use Case: "Company X's revenue is in the 75th percentile of the cohort"    │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 3. COVERAGE STATS                                                          │
├────────────────────────────────────────────────────────────────────────────┤
│ • total_concepts: Unique XBRL tags found                                   │
│ • mapped_concepts: Tags mapped to canonical metrics                        │
│ • mapping_rate: mapped / total                                             │
│ • {metric}_coverage: % of companies with that metric                       │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ 4. SUMMARY STATS                                                           │
├────────────────────────────────────────────────────────────────────────────┤
│ • total_facts, total_companies, total_concepts                             │
│ • facts_by_type: {instant: N, quarterly: N, annual: N}                     │
│ • facts_by_form: {10-K: N, 10-Q: N, 8-K: N}                                │
│ • companies_by_sector: {Technology: 8, Financial: 6, ...}                  │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **Multi-concept mapping** | Same metric (revenue) can come from multiple XBRL tags across companies |
| **Period type derivation** | XBRL doesn't explicitly label periods; calculated from date ranges |
| **Restatement detection** | Same concept+period with different values indicates amended filings |
| **40+ company cohort** | Large enough for meaningful benchmarking; spans all major sectors |
| **MD5 fact hashing** | Fast deduplication for millions of facts |
| **Annual facts for KPIs** | Quarterly data has seasonality; annuals give cleaner comparisons |

---

## Files Produced

| File | Description | Typical Size |
|------|-------------|--------------|
| `raw_xbrl_facts.csv` | Raw XBRL facts from API | 100-500 MB |
| `raw_filings.csv` | Filing metadata | 1-5 MB |
| `raw_companies.csv` | Company metadata | < 1 MB |
| `cleaned_xbrl_facts.csv` | Standardized facts | 80-400 MB |
| `company_dim.csv` | Company dimension | < 1 MB |
| `filings_dim.csv` | Filings dimension | 1-5 MB |
| `concept_map.csv` | XBRL concept mapping | < 1 MB |
| `xbrl_facts.csv` | Fact table | 80-400 MB |
| `kpis.json` | Financial metrics + benchmarks | < 1 MB |
| `pipeline_metrics.json` | Execution telemetry | < 1 MB |

---

## Execution

```bash
# Run pipeline with default 40+ company cohort
python pipeline.py

# The cohort can be customized in the source (COMPANY_COHORT list)
```

---

*Architecture Document v1.0.0 | Mboya Jeffers | Feb 2026*
