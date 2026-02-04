# P07: Media & Streaming Analytics
## 10M+ Records | CMS Engine: media

---

## Overview

| Field | Value |
|-------|-------|
| **Target Rows** | 10,000,000+ |
| **CMS Engine** | media (14 KPIs) |
| **Industry** | Entertainment / Streaming |
| **Status** | PENDING |

---

## Data Sources

| Source | URL | Data Type |
|--------|-----|-----------|
| IMDB Datasets | datasets.imdbws.com | Titles, ratings, crew |
| TMDB API | api.themoviedb.org | Movie/TV metadata |
| Kaggle Netflix | kaggle.com/datasets | Watch history |
| Spotify Charts | spotifycharts.com | Streaming charts |

---

## Star Schema

```
┌─────────────────────────────────────────────────────────────────┐
│                     MEDIA STAR SCHEMA                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  dim_title ─────────┐                                          │
│  dim_person ────────┼──────▶ fact_ratings                      │
│  dim_genre ─────────┤        (avg_rating, num_votes)           │
│  dim_platform ──────┤                                          │
│  dim_region ────────┤        fact_cast_crew                    │
│  dim_date ──────────┘        (role, billing_order)             │
│                                                                 │
│                              fact_streaming                    │
│                              (rank, streams_estimate)          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## CMS Media KPIs

| KPI | Description |
|-----|-------------|
| Views | Total views/streams |
| Engagement | Watch time, completion rate |
| Churn | Subscriber cancellation rate |
| Genre Trends | Popularity by genre |
| Platform Share | Netflix vs Hulu vs etc |

---

## Execution

```bash
# Full extraction (10M+ rows)
python src/main.py --mode full

# Test mode (sample)
python src/main.py --mode test

# Generate CMS report
cms-pdf --engine media --data evidence/P07_data.json output/P07_Media_Report.pdf
```

---

## Evidence Artifacts

| File | Purpose |
|------|---------|
| `evidence/P07_evidence.json` | Row counts, checksums |
| `evidence/P07_quality_report.json` | Validation results |
| `evidence/P07_extraction_log.json` | API call logs |

---

*Author: Mboya Jeffers*
