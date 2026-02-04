# P06: Betting & Sports Analytics
## 8M+ Records | CMS Engine: betting

---

## Overview

| Field | Value |
|-------|-------|
| **Target Rows** | 8,000,000+ |
| **CMS Engine** | betting (16 KPIs) |
| **Industry** | Sports Betting / iGaming |
| **Status** | PENDING |

---

## Data Sources

| Source | URL | Data Type |
|--------|-----|-----------|
| Sports Reference | sports-reference.com | Historical games |
| Kaggle Sports | kaggle.com/datasets | NFL, NBA, MLB stats |
| The Odds API | the-odds-api.com | Historical odds |
| ESPN API | site.api.espn.com | Live/historical |

---

## Star Schema

```
┌─────────────────────────────────────────────────────────────────┐
│                    BETTING STAR SCHEMA                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  dim_team ──────────┐                                          │
│  dim_player ────────┼──────▶ fact_games                        │
│  dim_league ────────┤        (scores, attendance)              │
│  dim_venue ─────────┤                                          │
│  dim_date ──────────┘        fact_odds                         │
│                              (spread, moneyline, o/u)          │
│                                                                 │
│                              fact_player_stats                 │
│                              (points, assists, etc)            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## CMS Betting KPIs

| KPI | Description |
|-----|-------------|
| Handle | Total amount wagered |
| Win Rate | Percentage of winning bets |
| Spread Accuracy | Line movement vs actual |
| Vig | House edge percentage |
| Market Share | By sport/league |

---

## Execution

```bash
# Full extraction (8M+ rows)
python src/main.py --mode full

# Test mode (sample)
python src/main.py --mode test

# Generate CMS report
cms-pdf --engine betting --data evidence/P06_data.json output/P06_Betting_Report.pdf
```

---

## Evidence Artifacts

| File | Purpose |
|------|---------|
| `evidence/P06_evidence.json` | Row counts, checksums |
| `evidence/P06_quality_report.json` | Validation results |
| `evidence/P06_extraction_log.json` | API call logs |

---

*Author: Mboya Jeffers*
