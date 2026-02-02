# Industry Demos Rebuild Plan
## 100% Verifiable Data - Zero Synthetic Metrics
**Author:** Mboya Jeffers | **Created:** 2026-02-01 | **Status:** IN PROGRESS

---

## Objective

Replace all 88 synthetic industry demo reports with reports built entirely from verifiable public data sources. Every metric must be traceable to a public API that anyone can query.

---

## Quality Standard (NON-NEGOTIABLE)

- [ ] Every numeric claim traceable to public API
- [ ] No synthetic/simulated operational data
- [ ] Data source URL provided for verification
- [ ] Methodology documented
- [ ] Passes public scrutiny test

---

## Phase 1: Data Source Architecture

### Primary Data Sources

| Source | Data Available | API Endpoint | Rate Limit |
|--------|---------------|--------------|------------|
| **SEC EDGAR XBRL** | Revenue, assets, segments, operational metrics from 10-K/10-Q | data.sec.gov/api/xbrl/companyfacts/ | 10 req/sec |
| **Yahoo Finance** | Stock prices, market cap, volume | yfinance Python library | Reasonable |
| **EIA API** | Energy production, consumption, prices | api.eia.gov | DEMO_KEY available |
| **FRED** | Economic indicators, interest rates | api.stlouisfed.org | Free with key |
| **NOAA/NWS** | Weather data, climate normals | api.weather.gov | Free |
| **CoinGecko** | Crypto prices, market cap, volume | api.coingecko.com | 10-30 req/min |
| **Blockchain.com** | Bitcoin network stats | api.blockchain.info | Free |
| **CFPB** | Consumer complaints | consumerfinance.gov/data-research/ | Free |

### Company CIK Mapping (SEC EDGAR)

#### Finance (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| JPMorgan Chase | JPM | 0000019617 | Major Bank Analysis |
| Goldman Sachs | GS | 0000886982 | Investment Bank Study |
| BlackRock | BLK | 0001364742 | Asset Manager Analysis |
| Visa | V | 0001403161 | Payment Network Analysis |

#### Brokerage (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| Charles Schwab | SCHW | 0000316709 | Retail Brokerage |
| Interactive Brokers | IBKR | 0001381197 | Institutional Brokerage |
| Robinhood | HOOD | 0001783879 | Digital Brokerage |
| Morgan Stanley | MS | 0000895421 | Full-Service Brokerage |

#### Media (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| Netflix | NFLX | 0001065280 | Streaming Analysis |
| Disney | DIS | 0001744489 | Media Conglomerate |
| Spotify | SPOT | 0001639920 | Audio Streaming |
| Warner Bros Discovery | WBD | 0001437107 | Legacy Media |

#### E-commerce (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| Amazon | AMZN | 0001018724 | Marketplace Dominance |
| Shopify | SHOP | 0001594805 | E-commerce Platform |
| Etsy | ETSY | 0001370637 | Artisan Marketplace |
| Wayfair | W | 0001616707 | Home Goods E-commerce |

#### Gaming (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| Activision/Microsoft | MSFT | 0000789019 | AAA Gaming Publisher |
| Electronic Arts | EA | 0000712515 | Sports Gaming |
| Take-Two | TTWO | 0000946581 | Open World Gaming |
| Roblox | RBLX | 0001315098 | UGC Platform |

#### Crypto (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| Coinbase | COIN | 0001679788 | US Exchange |
| N/A | BTC | N/A (CoinGecko) | Bitcoin Network Stats |
| MicroStrategy | MSTR | 0001050446 | Corporate Treasury |
| Marathon Digital | MARA | 0001507605 | Mining Economics |

#### Solar (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| First Solar | FSLR | 0001274494 | Utility-Scale Manufacturing |
| Enphase | ENPH | 0001463101 | Microinverter Market |
| NextEra Energy | NEE | 0000753308 | Renewable Utility |
| SunRun | RUN | 0001469367 | Residential Solar |

#### Oil & Gas (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| ExxonMobil | XOM | 0000034088 | Integrated Major |
| Valero | VLO | 0001035002 | Refining |
| ConocoPhillips | COP | 0001163165 | E&P Economics |
| Shell | SHEL | 0001306965 | European Major |

#### Betting (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| DraftKings | DKNG | 0001883685 | Daily Fantasy/Sports Betting |
| Flutter/FanDuel | FLUT | 0001966703 | Sports Betting Market |
| Penn Entertainment | PENN | 0000921738 | iGaming |
| Caesars | CZR | 0001590895 | Casino Omnichannel |

#### Weather (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| N/A | N/A | NOAA API | Enterprise Weather Data |
| N/A | N/A | NWS API | Weather Forecasting |
| Deere & Co | DE | 0000315189 | Agricultural Weather (Precision Ag) |
| N/A | N/A | NASA POWER | Solar/Climate Data |

#### Compliance (4 reports)
| Company | Ticker | CIK | Report Focus |
|---------|--------|-----|--------------|
| Equifax | EFX | 0000033185 | Consumer Credit Bureau |
| TransUnion | TRU | 0001552033 | Identity Verification |
| Experian | EXPGY | N/A (UK listed) | Commercial Credit |
| N/A | N/A | CFPB API | Consumer Complaints |

---

## Phase 2: Pipeline Architecture

### Master Pipeline Structure

```
Industry_Demos_Pipeline/
├── config/
│   ├── companies.json          # CIK mappings, tickers
│   └── metrics_mapping.json    # XBRL concept to metric mapping
├── extractors/
│   ├── sec_edgar.py           # XBRL fact extraction
│   ├── yahoo_finance.py       # Stock data
│   ├── eia_energy.py          # Energy data
│   ├── coingecko.py           # Crypto market data
│   ├── blockchain.py          # Bitcoin network stats
│   ├── noaa_weather.py        # Weather data
│   └── cfpb_complaints.py     # Consumer complaints
├── analyzers/
│   ├── financial_metrics.py   # Sharpe, VaR, returns
│   ├── growth_metrics.py      # YoY, CAGR calculations
│   └── industry_specific.py   # Per-vertical analysis
├── generators/
│   ├── executive_summary.py   # 1-page summary generator
│   └── technical_report.py    # Detailed technical report
├── quality/
│   ├── data_validator.py      # Completeness, accuracy checks
│   └── source_verifier.py     # API source documentation
└── main.py                    # Orchestration
```

### Key XBRL Concepts by Industry

```json
{
  "finance": {
    "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"],
    "assets": ["Assets", "AssetsCurrent"],
    "net_income": ["NetIncomeLoss"],
    "deposits": ["Deposits"],
    "loans": ["LoansAndLeasesReceivableNetReportedAmount"]
  },
  "media": {
    "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"],
    "subscribers": ["NumberOfSubscribers", "SubscriberCount"],
    "content_assets": ["ContentAssets", "FilmAndTelevisionCosts"]
  },
  "ecommerce": {
    "revenue": ["Revenues", "NetSales"],
    "gmv": ["GrossMerchandiseVolume"],
    "active_buyers": ["ActiveBuyers", "NumberOfActiveCustomers"]
  }
}
```

---

## Phase 3: Execution Plan

### Step 1: Build Core Extractors (Foundation)
- [ ] SEC EDGAR XBRL extractor (extend existing P2-SEC pipeline)
- [ ] Yahoo Finance stock data extractor
- [ ] Quality gate framework

### Step 2: Industry-Specific Extractors
- [ ] EIA API extractor (Solar, Oil & Gas)
- [ ] CoinGecko + Blockchain.com extractor (Crypto)
- [ ] NOAA/NWS extractor (Weather)
- [ ] CFPB extractor (Compliance)

### Step 3: Data Collection (By Industry)
- [ ] Finance: JPM, GS, BLK, V
- [ ] Brokerage: SCHW, IBKR, HOOD, MS
- [ ] Media: NFLX, DIS, SPOT, WBD
- [ ] E-commerce: AMZN, SHOP, ETSY, W
- [ ] Gaming: MSFT, EA, TTWO, RBLX
- [ ] Crypto: COIN, BTC network, MSTR, MARA
- [ ] Solar: FSLR, ENPH, NEE, RUN
- [ ] Oil & Gas: XOM, VLO, COP, SHEL
- [ ] Betting: DKNG, FLUT, PENN, CZR
- [ ] Weather: NOAA data, DE precision ag
- [ ] Compliance: EFX, TRU, CFPB data

### Step 4: Report Generation
- [ ] Generate Executive Summary for each company
- [ ] Generate Technical Analysis for each company
- [ ] Quality audit all 88 reports
- [ ] Verify every metric against source

### Step 5: Replacement
- [ ] Archive old synthetic reports
- [ ] Deploy new verified reports
- [ ] Update README with data sources

---

## Phase 4: Quality Audit Checklist

For EACH of the 88 reports:

```markdown
## Report: [INDUSTRY]_[COMPANY]_[TYPE].pdf

### Data Verification
- [ ] All financial metrics match SEC EDGAR filing
- [ ] Stock data matches Yahoo Finance
- [ ] Calculated metrics (Sharpe, VaR) methodology documented
- [ ] Date ranges clearly stated

### Source Attribution
- [ ] SEC filing CIK and accession number cited
- [ ] API endpoint documented
- [ ] "Verify at:" URL provided

### Accuracy Check
- [ ] Revenue within 0.1% of source
- [ ] No synthetic/simulated labels anywhere
- [ ] All claims verifiable by third party
```

---

## Timeline

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Data Source Architecture | COMPLETE |
| 2 | Pipeline Development | IN PROGRESS |
| 3 | Data Collection | PENDING |
| 4 | Report Generation | PENDING |
| 5 | Quality Audit | PENDING |
| 6 | Deployment | PENDING |

---

## Success Criteria

1. **Zero synthetic data** - Every metric from public API
2. **100% verifiable** - Anyone can reproduce the numbers
3. **Quality Standard compliant** - Passes pre-release audit
4. **Professional presentation** - Executive-ready PDFs
5. **Documented methodology** - Technical reports explain calculations

---

## Notes

- SEC EDGAR XBRL is the primary source for most operational metrics
- Some companies don't report certain metrics (e.g., subscriber counts) - use only what's available
- Weather and some Compliance reports will rely on government APIs, not company data
- Crypto uses CoinGecko for market data, blockchain APIs for network stats

---

*Plan created by Mboya Jeffers | Quality Standard: QUALITY_STANDARD.md*
