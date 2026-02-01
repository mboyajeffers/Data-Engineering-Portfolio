# P2-AIR Data Dictionary
## Airline On-Time Reliability + Weather Attribution Pipeline
**Version:** 1.0.0 | **Last Updated:** 2026-02-01

---

## Overview

This document describes the data model for the P2-AIR pipeline, which combines airline on-time performance data with NOAA weather observations to quantify weather impact on flight operations.

---

## Source Systems

| Source | Type | Data Status | Endpoint |
|--------|------|-------------|----------|
| BTS TranStats Format | Airline Performance | **SYNTHETIC** (demo) | transtats.bts.gov/ontime |
| NOAA CDO API v2 | Weather Data | **REAL** (live API) | ncdc.noaa.gov/cdo-web/api/v2 |

> **IMPORTANT NOTICE:** Flight data in this pipeline is **SYNTHETIC** (programmatically generated in BTS TranStats format) for portfolio demonstration purposes. Weather data is **REAL** and fetched live from NOAA CDO API. This project demonstrates data engineering skills, not actual airline analysis.

---

## Data Model

### Fact Table: `flight_fact`

Core flight performance records.

| Column | Data Type | Description |
|--------|-----------|-------------|
| `flight_id` | INTEGER | Primary key |
| `flight_date` | DATE | Date of flight |
| `carrier` | VARCHAR(2) | Carrier code (AA, DL, UA, etc.) |
| `carrier_name` | VARCHAR(50) | Full carrier name |
| `flight_num` | INTEGER | Flight number |
| `origin` | VARCHAR(3) | Origin airport code |
| `origin_city` | VARCHAR(50) | Origin city |
| `origin_state` | VARCHAR(2) | Origin state |
| `dest` | VARCHAR(3) | Destination airport code |
| `dest_city` | VARCHAR(50) | Destination city |
| `dest_state` | VARCHAR(2) | Destination state |
| `crs_dep_time` | VARCHAR(4) | Scheduled departure (HHMM) |
| `dep_delay` | INTEGER | Departure delay (minutes) |
| `arr_delay` | INTEGER | Arrival delay (minutes) |
| `cancelled` | INTEGER | 1=cancelled, 0=operated |
| `diverted` | INTEGER | 1=diverted, 0=normal |
| `carrier_delay` | INTEGER | Delay due to carrier (min) |
| `weather_delay` | INTEGER | Delay due to weather (min) |
| `nas_delay` | INTEGER | Delay due to NAS (min) |
| `security_delay` | INTEGER | Delay due to security (min) |
| `late_aircraft_delay` | INTEGER | Delay due to late aircraft (min) |
| `distance` | INTEGER | Flight distance (miles) |

**Row Count:** ~100,000

---

### Fact Table: `weather_fact`

Daily weather observations by airport.

| Column | Data Type | Description |
|--------|-----------|-------------|
| `airport` | VARCHAR(3) | Airport code |
| `date` | DATE | Observation date |
| `precipitation` | DECIMAL | Daily precipitation (mm) |
| `temp_max` | DECIMAL | Maximum temperature (°C) |
| `temp_min` | DECIMAL | Minimum temperature (°C) |
| `avg_wind` | DECIMAL | Average wind speed (mph) |
| `max_wind_2min` | DECIMAL | Max 2-min wind (mph) |
| `fog` | INTEGER | Fog indicator (1=yes) |
| `thunder` | INTEGER | Thunder indicator (1=yes) |
| `severe_weather` | INTEGER | Severe weather flag |

**Row Count:** ~280 (airports × days)

---

### Fact Table: `flight_weather_fact`

Joined flight and weather data.

| Column | Data Type | Description |
|--------|-----------|-------------|
| All flight_fact columns | - | Flight data |
| `precipitation_origin` | DECIMAL | Origin airport precipitation |
| `temp_max_origin` | DECIMAL | Origin max temp |
| `avg_wind_origin` | DECIMAL | Origin avg wind |
| `severe_weather_origin` | INTEGER | Origin severe weather flag |
| `precipitation_dest` | DECIMAL | Destination precipitation |
| `temp_max_dest` | DECIMAL | Destination max temp |
| `avg_wind_dest` | DECIMAL | Destination avg wind |
| `severe_weather_dest` | INTEGER | Destination severe weather flag |

**Row Count:** ~100,000

---

### Dimension Table: `airport_dim`

Airport reference data.

| Column | Data Type | Description |
|--------|-----------|-------------|
| `airport_code` | VARCHAR(3) | IATA airport code |
| `airport_name` | VARCHAR(100) | Full airport name |
| `city` | VARCHAR(50) | City |
| `state` | VARCHAR(2) | State |
| `latitude` | DECIMAL | Latitude |
| `longitude` | DECIMAL | Longitude |
| `weather_station` | VARCHAR(20) | NOAA station ID |

**Row Count:** 10

---

### Dimension Table: `carrier_dim`

Carrier reference data.

| Column | Data Type | Description |
|--------|-----------|-------------|
| `carrier_code` | VARCHAR(2) | Carrier code |
| `carrier_name` | VARCHAR(50) | Full carrier name |

**Row Count:** 8

---

## Weather Attribution Logic

### Severe Weather Definition
```
severe_weather =
    (precipitation > 25 mm) OR
    (avg_wind > 25 mph) OR
    (fog = 1) OR
    (thunder = 1)
```

### Weather Impact Calculation
```
weather_delay_delta = avg_delay(severe_weather=1) - avg_delay(severe_weather=0)
```

---

## Quality Gates

| Gate | Threshold | Description |
|------|-----------|-------------|
| Timezone | ≥95% | Valid date/time parsing |
| Join Coverage | ≥80% | Flights matched with weather |
| Outliers | ≥95% | No impossible delay values |
| Delay Codes | ≥75% | Delayed flights have reason codes |
| Completeness | ≥95% | Required fields populated |

---

## KPI Definitions

| Metric | Formula |
|--------|---------|
| On-Time Rate | % of flights with arr_delay ≤ 15 min |
| Avg Delay | MEAN(dep_delay) for non-cancelled |
| P90 Delay | 90th percentile of dep_delay |
| Cancel Rate | % of flights with cancelled=1 |

---

## Data Lineage

```
NOAA CDO API                    BTS TranStats
      │                               │
      ▼                               ▼
┌─────────────┐               ┌─────────────┐
│ Raw Weather │               │ Raw Flights │
│ (1,536 rows)│               │ (100K rows) │
└─────────────┘               └─────────────┘
      │                               │
      ▼                               │
┌─────────────┐                       │
│ Pivot to    │                       │
│ airport/day │                       │
└─────────────┘                       │
      │                               │
      └──────────────┬────────────────┘
                     │
                     ▼
             ┌─────────────┐
             │ Join on     │
             │ airport+date│
             └─────────────┘
                     │
                     ▼
             ┌─────────────┐
             │ flight_     │
             │ weather_fact│
             └─────────────┘
                     │
                     ▼
             ┌─────────────┐
             │ Quality     │
             │ Gates       │
             └─────────────┘
                     │
                     ▼
             ┌─────────────┐
             │ KPIs &      │
             │ Attribution │
             └─────────────┘
                     │
                     ▼
             ┌─────────────┐
             │ PDF Reports │
             └─────────────┘
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-02-01 | Initial data dictionary |

---

*Document maintained as part of P2-AIR Pipeline*
