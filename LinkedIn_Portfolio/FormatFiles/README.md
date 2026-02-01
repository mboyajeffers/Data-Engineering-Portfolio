# LinkedIn Portfolio - Enterprise Analytics Reports

## Project Overview
Generate enterprise-grade analytics reports for LinkedIn/GitHub portfolio showcasing data engineering capabilities.

**Structure:** 4 companies per vertical, 11 verticals, 3 reports per company
**Total:** 44 companies = 132 reports + 44 LinkedIn posts

---

## Quick Start

### Run FIN-01 (First Report)
```bash
cd /Users/mboyajeffers/Claude_Projects/Career/LinkedIn_Package/Proof_Package/LinkedIn_Portfolio
chmod +x run_fin01.sh
./run_fin01.sh
```

Or run steps individually:
```bash
cd Finance
python3 FIN01_pull_data.py      # Pull data (~2-5 min)
python3 FIN01_generate_reports.py  # Generate PDFs
```

### Dependencies
```bash
pip3 install yfinance pandas numpy weasyprint
```

---

## Folder Structure

```
LinkedIn_Portfolio/
├── README.md                    # This file
├── VERSION.md                   # Progress tracking
├── run_fin01.sh                 # Runner script
│
├── Finance/                     # Finance vertical
│   ├── FIN01_pull_data.py       # Data pull script
│   ├── FIN01_generate_reports.py # Report generator
│   ├── data/                    # Raw data outputs
│   │   ├── FIN01_stock_data.csv
│   │   ├── FIN01_correlations.csv
│   │   ├── FIN01_risk_metrics.csv
│   │   └── FIN01_summary.json
│   ├── reports/                 # PDF outputs
│   │   ├── FIN01_Executive_Summary_v1.0.pdf
│   │   └── FIN01_Technical_Analysis_v1.0.pdf
│   └── posts/                   # LinkedIn posts
│       └── FIN01_LinkedIn_Post_v1.0.md
│
├── Compliance/                  # Compliance vertical
├── Media/                       # Media vertical
├── Crypto/                      # Crypto vertical
├── Brokerage/                   # Brokerage vertical
├── Betting/                     # Betting vertical
├── Gaming/                      # Gaming vertical
├── Ecommerce/                   # Ecommerce vertical
├── Oilgas/                      # Oil & Gas vertical
├── Solar/                       # Solar vertical
└── Weather/                     # Weather vertical
```

---

## Report Codes

### Finance (FIN)
| Code | Report Title | Company (Private) |
|------|--------------|-------------------|
| FIN-01 | Major Bank Stock: 10-Year Performance Analysis | JPMorgan |
| FIN-02 | Investment Bank Volatility Study | Goldman Sachs |
| FIN-03 | Asset Manager Market Position Analysis | BlackRock |
| FIN-04 | Payment Network Revenue Correlation | Visa |

### Compliance (CMP)
| Code | Report Title | Company (Private) |
|------|--------------|-------------------|
| CMP-01 | Credit Bureau Complaint Pattern Analysis | Equifax |
| CMP-02 | Large Bank Regulatory Risk Profile | Wells Fargo |
| CMP-03 | Consumer Credit Compliance Trends | Capital One |
| CMP-04 | Systemically Important Bank Compliance Monitor | BofA |

### Media (MED)
| Code | Report Title | Company (Private) |
|------|--------------|-------------------|
| MED-01 | Streaming Leader Content Analysis | Netflix |
| MED-02 | Entertainment Conglomerate Streaming Study | Disney+ |
| MED-03 | Audio Streaming Market Analysis | Spotify |
| MED-04 | Media Consolidation Impact Study | WBD |

### Crypto (CRY)
| Code | Report Title | Company (Private) |
|------|--------------|-------------------|
| CRY-01 | US Crypto Exchange Market Structure | Coinbase |
| CRY-02 | Global Exchange Liquidity Analysis | Binance |
| CRY-03 | Corporate Bitcoin Treasury Analysis | MicroStrategy |
| CRY-04 | Bitcoin Mining Economics Study | Marathon |

*(Additional verticals in VERSION.md)*

---

## Deliverables Per Company

| Report Type | Filename Pattern | Pages |
|-------------|-----------------|-------|
| Executive Summary | `{CODE}_Executive_Summary_v1.0.pdf` | 2-3 |
| Technical Analysis | `{CODE}_Technical_Analysis_v1.0.pdf` | 5-8 |
| LinkedIn Post | `{CODE}_LinkedIn_Post_v1.0.md` | 1 |

---

## Data Sources

| Vertical | Source | API |
|----------|--------|-----|
| Finance | Yahoo Finance | yfinance |
| Compliance | CFPB Database | Download |
| Media | Public data | Web/SEC |
| Crypto | CoinGecko | API |
| Brokerage | Yahoo Finance | yfinance |
| Betting | TheSportsDB | API |
| Gaming | Steam | API |
| Ecommerce | Public data | Web |
| Oilgas | EIA | API |
| Solar | NREL | API |
| Weather | Open-Meteo | API |

---

## Privacy Note

**Company names are PRIVATE and should NEVER appear in:**
- PDF reports
- LinkedIn posts
- GitHub commits
- Public documentation

Reports use generic industry titles only.

---

*Project maintained by Claude Code for Mboya Jeffers*
