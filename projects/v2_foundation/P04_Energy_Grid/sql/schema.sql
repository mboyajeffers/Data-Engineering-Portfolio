-- Energy Grid Intelligence - Star Schema
-- Author: Mboya Jeffers
-- Scale: 500K+ hourly grid readings

-- ============================================
-- DIMENSION: Balancing Authorities
-- ============================================
CREATE TABLE IF NOT EXISTS dim_balancing_authority (
    ba_id           SERIAL PRIMARY KEY,
    ba_code         VARCHAR(10) NOT NULL UNIQUE,
    ba_name         VARCHAR(200) NOT NULL,
    region          VARCHAR(50),          -- Eastern, Western, Texas
    timezone        VARCHAR(50),
    interconnection VARCHAR(50),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_ba_code ON dim_balancing_authority(ba_code);
CREATE INDEX idx_dim_ba_region ON dim_balancing_authority(region);

-- ============================================
-- DIMENSION: Fuel Types
-- ============================================
CREATE TABLE IF NOT EXISTS dim_fuel_type (
    fuel_id         SERIAL PRIMARY KEY,
    fuel_code       VARCHAR(10) NOT NULL UNIQUE,
    fuel_name       VARCHAR(100) NOT NULL,
    fuel_category   VARCHAR(50),          -- Fossil, Renewable, Nuclear, Storage
    is_renewable    BOOLEAN DEFAULT FALSE,
    is_clean        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_fuel_code ON dim_fuel_type(fuel_code);
CREATE INDEX idx_dim_fuel_renewable ON dim_fuel_type(is_renewable);

-- ============================================
-- DIMENSION: DateTime (Hourly)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_id     SERIAL PRIMARY KEY,
    timestamp       TIMESTAMP NOT NULL UNIQUE,
    date            DATE NOT NULL,
    hour            INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,     -- 0=Monday
    day_name        VARCHAR(10),
    month           INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    year            INTEGER NOT NULL,
    is_weekend      BOOLEAN DEFAULT FALSE,
    is_peak_hour    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_datetime_ts ON dim_datetime(timestamp);
CREATE INDEX idx_dim_datetime_date ON dim_datetime(date);
CREATE INDEX idx_dim_datetime_hour ON dim_datetime(hour);
CREATE INDEX idx_dim_datetime_peak ON dim_datetime(is_peak_hour);

-- ============================================
-- FACT: Grid Operations (500K+ rows)
-- ============================================
CREATE TABLE IF NOT EXISTS fact_grid_ops (
    fact_id             BIGSERIAL PRIMARY KEY,
    ba_id               INTEGER REFERENCES dim_balancing_authority(ba_id),
    datetime_id         INTEGER REFERENCES dim_datetime(datetime_id),
    fuel_id             INTEGER REFERENCES dim_fuel_type(fuel_id),

    -- Measures
    demand_mw           NUMERIC(12,2),
    generation_mw       NUMERIC(12,2),
    net_generation      NUMERIC(12,2),
    interchange_mw      NUMERIC(12,2),
    demand_forecast_mw  NUMERIC(12,2),

    -- Raw value for flexibility
    value               NUMERIC(12,2),
    data_type           VARCHAR(20),      -- demand, generation
    type                VARCHAR(10),      -- D, NG, etc.

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Critical indexes for query performance at scale
CREATE INDEX idx_fact_ba ON fact_grid_ops(ba_id);
CREATE INDEX idx_fact_datetime ON fact_grid_ops(datetime_id);
CREATE INDEX idx_fact_fuel ON fact_grid_ops(fuel_id);
CREATE INDEX idx_fact_demand ON fact_grid_ops(demand_mw);
CREATE INDEX idx_fact_generation ON fact_grid_ops(generation_mw);
CREATE INDEX idx_fact_type ON fact_grid_ops(data_type);

-- Composite indexes for common queries
CREATE INDEX idx_fact_ba_datetime ON fact_grid_ops(ba_id, datetime_id);
CREATE INDEX idx_fact_fuel_datetime ON fact_grid_ops(fuel_id, datetime_id);

-- ============================================
-- AGGREGATE: Hourly Demand Summary
-- ============================================
CREATE TABLE IF NOT EXISTS agg_hourly_demand (
    ba_id               INTEGER REFERENCES dim_balancing_authority(ba_id),
    hour                INTEGER NOT NULL,
    avg_demand_mw       NUMERIC(12,2),
    max_demand_mw       NUMERIC(12,2),
    min_demand_mw       NUMERIC(12,2),
    reading_count       INTEGER,
    PRIMARY KEY (ba_id, hour)
);

-- ============================================
-- AGGREGATE: Daily Generation Mix
-- ============================================
CREATE TABLE IF NOT EXISTS agg_daily_generation (
    ba_id               INTEGER REFERENCES dim_balancing_authority(ba_id),
    date                DATE NOT NULL,
    total_generation_mw NUMERIC(15,2),
    renewable_mw        NUMERIC(15,2),
    fossil_mw           NUMERIC(15,2),
    nuclear_mw          NUMERIC(15,2),
    renewable_pct       NUMERIC(5,2),
    PRIMARY KEY (ba_id, date)
);

-- ============================================
-- VIEWS: Analytics Ready
-- ============================================

-- View: Complete grid operations with all dimensions
CREATE OR REPLACE VIEW v_grid_operations AS
SELECT
    ba.ba_code,
    ba.ba_name,
    ba.region,
    ft.fuel_name,
    ft.fuel_category,
    ft.is_renewable,
    dt.timestamp,
    dt.date,
    dt.hour,
    dt.day_name,
    dt.is_peak_hour,
    f.demand_mw,
    f.generation_mw,
    f.value,
    f.data_type
FROM fact_grid_ops f
JOIN dim_balancing_authority ba ON f.ba_id = ba.ba_id
LEFT JOIN dim_fuel_type ft ON f.fuel_id = ft.fuel_id
JOIN dim_datetime dt ON f.datetime_id = dt.datetime_id;

-- View: Demand by region
CREATE OR REPLACE VIEW v_regional_demand AS
SELECT
    ba.region,
    dt.date,
    dt.hour,
    SUM(f.demand_mw) as total_demand,
    AVG(f.demand_mw) as avg_demand,
    MAX(f.demand_mw) as peak_demand
FROM fact_grid_ops f
JOIN dim_balancing_authority ba ON f.ba_id = ba.ba_id
JOIN dim_datetime dt ON f.datetime_id = dt.datetime_id
WHERE f.data_type = 'demand'
GROUP BY ba.region, dt.date, dt.hour;

-- View: Renewable generation share
CREATE OR REPLACE VIEW v_renewable_share AS
SELECT
    ba.ba_code,
    ba.ba_name,
    ba.region,
    dt.date,
    SUM(CASE WHEN ft.is_renewable THEN f.generation_mw ELSE 0 END) as renewable_mw,
    SUM(f.generation_mw) as total_mw,
    ROUND(SUM(CASE WHEN ft.is_renewable THEN f.generation_mw ELSE 0 END) /
          NULLIF(SUM(f.generation_mw), 0) * 100, 2) as renewable_pct
FROM fact_grid_ops f
JOIN dim_balancing_authority ba ON f.ba_id = ba.ba_id
JOIN dim_fuel_type ft ON f.fuel_id = ft.fuel_id
JOIN dim_datetime dt ON f.datetime_id = dt.datetime_id
WHERE f.data_type = 'generation'
GROUP BY ba.ba_id, ba.ba_code, ba.ba_name, ba.region, dt.date;

-- ============================================
-- SAMPLE QUERIES
-- ============================================

-- Peak demand by region
-- SELECT region, MAX(peak_demand) as peak
-- FROM v_regional_demand
-- GROUP BY region
-- ORDER BY peak DESC;

-- Hourly demand profile
-- SELECT hour, AVG(total_demand) as avg_demand
-- FROM v_regional_demand
-- GROUP BY hour
-- ORDER BY hour;

-- Renewable leaders
-- SELECT ba_name, AVG(renewable_pct) as avg_renewable
-- FROM v_renewable_share
-- GROUP BY ba_code, ba_name
-- ORDER BY avg_renewable DESC
-- LIMIT 10;
