# P05: Gaming Analytics Pipeline
## Enterprise-Scale Steam/PC Gaming Data Platform

**Author:** Mboya Jeffers
**Target Scale:** 8M+ records
**Status:** Pipeline Ready

---

## Overview

Production-ready data pipeline for gaming industry analytics using Steam and PC gaming data sources.

| Component | Description |
|-----------|-------------|
| **Extract** | Steam Web API, SteamSpy API, Store API |
| **Transform** | Kimball star schema dimensional model |
| **Analyze** | Player engagement, revenue, genre KPIs |
| **Evidence** | Quality gates, checksums, audit logs |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    P05 GAMING ANALYTICS PIPELINE                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐        │
│  │   EXTRACT    │   │  TRANSFORM   │   │   ANALYZE    │        │
│  │              │   │              │   │              │        │
│  │ • Steam API  │──▶│ • dim_game   │──▶│ • DAU/MAU    │        │
│  │ • SteamSpy   │   │ • dim_genre  │   │ • ARPU       │        │
│  │ • Store API  │   │ • fact_*     │   │ • Retention  │        │
│  │              │   │              │   │ • Revenue    │        │
│  └──────────────┘   └──────────────┘   └──────────────┘        │
│         │                  │                  │                 │
│         ▼                  ▼                  ▼                 │
│  ┌─────────────────────────────────────────────────────┐       │
│  │                    EVIDENCE LAYER                    │       │
│  │  • Extraction logs  • Row counts  • Checksums       │       │
│  └─────────────────────────────────────────────────────┘       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Sources

| Source | URL | Data |
|--------|-----|------|
| Steam Web API | api.steampowered.com | App list (100K+ games) |
| SteamSpy | steamspy.com/api.php | Ownership, playtime estimates |
| Store API | store.steampowered.com/api | Game details, pricing |
| Kaggle (Optional) | kaggle.com/datasets | 7M+ reviews |

---

## Star Schema

### Dimensions
- `dim_game` - Game master data (SCD Type 2)
- `dim_developer` - Developer/publisher entities
- `dim_genre` - Game genres
- `dim_platform` - Windows/Mac/Linux/Steam Deck
- `dim_date` - Date dimension

### Facts
- `fact_game_metrics` - Player counts, playtime, revenue, reviews

### Bridges
- `game_genre_bridge` - Many-to-many game-genre
- `game_developer_bridge` - Many-to-many game-developer

---

## KPIs Calculated

| Category | Metrics |
|----------|---------|
| **Player Engagement** | Active players, playtime, concurrent users |
| **Reviews** | Sentiment score, positive rate, distribution |
| **Financial** | Revenue estimate, ARPU, F2P percentage |
| **Quality** | Metacritic scores, review correlation |
| **Genre** | Top genres by owners, revenue, engagement |

---

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run test mode (sample data)
python src/main.py --mode test

# Run full extraction (8M+ rows, several hours)
python src/main.py --mode full
```

---

## Project Structure

```
P05_Gaming_Analytics/
├── README.md
├── requirements.txt
├── src/
│   ├── main.py          # Pipeline orchestrator
│   ├── extract.py       # Steam API extraction
│   ├── transform.py     # Star schema transformation
│   └── analytics.py     # KPI calculations
├── sql/
│   └── schema.sql       # PostgreSQL DDL
├── data/                # Output data (gitignored)
└── evidence/
    └── P05_evidence.json
```

---

## Engineering Patterns

- **Rate Limiting**: Exponential backoff for API throttling
- **Checkpointing**: Resume capability for long extractions
- **Data Quality**: Validation gates at each pipeline stage
- **Surrogate Keys**: MD5-based keys for dimension tables
- **Evidence Trail**: Checksums and logs for auditability

---

## Data Verification

All data sources are publicly accessible:
- Steam API: No authentication required
- SteamSpy: Public API
- Kaggle: Free account for bulk data

---

**Author:** Mboya Jeffers | MboyaJeffers9@gmail.com
