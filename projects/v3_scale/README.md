# v3 Scale Projects
## 31M Rows | 4 CMS Engine Verticals | Enterprise Scale

---

## Overview

| Metric | Value |
|--------|-------|
| **Total Rows** | 31M+ |
| **Projects** | 4 |
| **CMS Engines** | gaming, betting, media, crypto |
| **Status** | IN PROGRESS |

---

## Projects

### P05: Gaming Analytics (8M rows)
- **CMS Engine:** gaming (16 KPIs)
- **Sources:** Steam API, IGDB, SteamSpy, Kaggle
- **Data:** Games, reviews, player stats
- **Analytics:** DAU/MAU, ARPU, retention, session time
- **Status:** PENDING

### P06: Betting & Sports Analytics (8M rows)
- **CMS Engine:** betting (16 KPIs)
- **Sources:** Sports Reference, ESPN, historical odds
- **Data:** Games, scores, odds, player stats
- **Analytics:** Spread accuracy, win rates, handle
- **Status:** PENDING

### P07: Media & Streaming Analytics (10M rows)
- **CMS Engine:** media (14 KPIs)
- **Sources:** IMDB datasets, TMDB, streaming charts
- **Data:** Titles, ratings, cast/crew
- **Analytics:** Engagement, watch time, genre trends
- **Status:** PENDING

### P08: Crypto & Blockchain Analytics (5M rows)
- **CMS Engine:** crypto (30 KPIs - largest!)
- **Sources:** CoinGecko, CryptoCompare, Messari
- **Data:** Prices, volume, on-chain metrics
- **Analytics:** Volatility, TVL, market cap
- **Status:** PENDING

---

## CMS Engine Integration

Each project uses a CMS engine for report generation:

```bash
# Generate reports using CMS engines
cms-pdf --engine gaming --data P05_data.json P05_Gaming_Report.pdf
cms-pdf --engine betting --data P06_data.json P06_Betting_Report.pdf
cms-pdf --engine media --data P07_data.json P07_Media_Report.pdf
cms-pdf --engine crypto --data P08_data.json P08_Crypto_Report.pdf
```

---

## Folder Structure

```
v3_scale/
├── README.md                    # This file
├── P05_Gaming_Analytics/
│   ├── README.md
│   ├── src/
│   ├── sql/
│   ├── evidence/
│   └── data/
├── P06_Betting_Sports/
├── P07_Media_Streaming/
└── P08_Crypto_Blockchain/
```

---

*v3 Scale - Demonstrates CMS engine integration at 30M+ scale*
