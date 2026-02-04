# P08: Crypto & Blockchain Analytics
## 5M+ Records | CMS Engine: crypto

---

## Overview

| Field | Value |
|-------|-------|
| **Target Rows** | 5,000,000+ |
| **CMS Engine** | crypto (30 KPIs - largest!) |
| **Industry** | Cryptocurrency / DeFi / Blockchain |
| **Status** | PENDING |

---

## Data Sources

| Source | URL | Data Type |
|--------|-----|-----------|
| CoinGecko API | api.coingecko.com | Market data, history |
| CryptoCompare | min-api.cryptocompare.com | OHLCV data |
| Messari API | data.messari.io | Fundamentals |
| Blockchain.com | api.blockchain.com | On-chain data |

---

## Star Schema

```
┌─────────────────────────────────────────────────────────────────┐
│                     CRYPTO STAR SCHEMA                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  dim_asset ─────────┐                                          │
│  dim_exchange ──────┼──────▶ fact_prices                       │
│  dim_chain ─────────┤        (OHLCV, market_cap)               │
│  dim_datetime ──────┘                                          │
│                              fact_on_chain                     │
│                              (addresses, txns, fees)           │
│                                                                 │
│                              fact_defi                         │
│                              (tvl, volume, users)              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## CMS Crypto KPIs

| KPI | Description |
|-----|-------------|
| Volatility | 30d, 90d price volatility |
| Volume | 24h trading volume |
| Market Cap | Total market capitalization |
| TVL | Total Value Locked (DeFi) |
| Active Addresses | Daily active addresses |
| BTC Dominance | Bitcoin market share |

---

## Execution

```bash
# Full extraction (5M+ rows)
python src/main.py --mode full

# Test mode (sample)
python src/main.py --mode test

# Generate CMS report
cms-pdf --engine crypto --data evidence/P08_data.json output/P08_Crypto_Report.pdf
```

---

## Evidence Artifacts

| File | Purpose |
|------|---------|
| `evidence/P08_evidence.json` | Row counts, checksums |
| `evidence/P08_quality_report.json` | Validation results |
| `evidence/P08_extraction_log.json` | API call logs |

---

*Author: Mboya Jeffers*
