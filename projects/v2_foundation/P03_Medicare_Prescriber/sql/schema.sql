-- Medicare Prescriber Intelligence - Star Schema
-- Author: Mboya Jeffers
-- Scale: 5M+ prescription records

-- ============================================
-- DIMENSION: Prescribers
-- ============================================
CREATE TABLE IF NOT EXISTS dim_prescriber (
    prescriber_id   SERIAL PRIMARY KEY,
    npi             VARCHAR(10) NOT NULL UNIQUE,
    provider_name   VARCHAR(500) NOT NULL,
    specialty       VARCHAR(200),
    credential      VARCHAR(50),
    entity_type     VARCHAR(50),      -- Individual, Organization
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_prescriber_npi ON dim_prescriber(npi);
CREATE INDEX idx_dim_prescriber_specialty ON dim_prescriber(specialty);

-- ============================================
-- DIMENSION: Drugs
-- ============================================
CREATE TABLE IF NOT EXISTS dim_drug (
    drug_id         SERIAL PRIMARY KEY,
    drug_name       VARCHAR(500) NOT NULL,
    generic_name    VARCHAR(500),
    brand_name      VARCHAR(500),
    drug_class      VARCHAR(100),
    is_opioid       BOOLEAN DEFAULT FALSE,
    is_antibiotic   BOOLEAN DEFAULT FALSE,
    is_controlled   BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_drug_name ON dim_drug(drug_name);
CREATE INDEX idx_dim_drug_generic ON dim_drug(generic_name);
CREATE INDEX idx_dim_drug_opioid ON dim_drug(is_opioid);

-- ============================================
-- DIMENSION: Locations
-- ============================================
CREATE TABLE IF NOT EXISTS dim_location (
    location_id     SERIAL PRIMARY KEY,
    city            VARCHAR(200),
    state           VARCHAR(2),
    state_name      VARCHAR(100),
    zip_code        VARCHAR(10),
    ruca_code       VARCHAR(10),       -- Rural-Urban Commuting Area
    urban_rural     VARCHAR(20),       -- Urban, Suburban, Rural
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_location_state ON dim_location(state);
CREATE INDEX idx_dim_location_zip ON dim_location(zip_code);

-- ============================================
-- DIMENSION: Year
-- ============================================
CREATE TABLE IF NOT EXISTS dim_year (
    year_id         SERIAL PRIMARY KEY,
    calendar_year   INTEGER NOT NULL UNIQUE
);

-- ============================================
-- FACT: Prescriptions (5M+ rows)
-- ============================================
CREATE TABLE IF NOT EXISTS fact_prescriptions (
    fact_id             BIGSERIAL PRIMARY KEY,
    prescriber_id       INTEGER REFERENCES dim_prescriber(prescriber_id),
    drug_id             INTEGER REFERENCES dim_drug(drug_id),
    location_id         INTEGER REFERENCES dim_location(location_id),
    year_id             INTEGER REFERENCES dim_year(year_id),

    -- Measures
    total_claims        INTEGER,
    total_30day_fills   INTEGER,
    total_day_supply    INTEGER,
    total_drug_cost     NUMERIC(15,2),
    total_beneficiaries INTEGER,

    -- Derived measures
    avg_cost_per_claim  NUMERIC(10,2),
    avg_day_supply      NUMERIC(10,2),

    -- 65+ specific metrics
    ge65_claims         INTEGER,
    ge65_drug_cost      NUMERIC(15,2),
    ge65_beneficiaries  INTEGER,

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Critical indexes for query performance at scale
CREATE INDEX idx_fact_prescriber ON fact_prescriptions(prescriber_id);
CREATE INDEX idx_fact_drug ON fact_prescriptions(drug_id);
CREATE INDEX idx_fact_location ON fact_prescriptions(location_id);
CREATE INDEX idx_fact_year ON fact_prescriptions(year_id);
CREATE INDEX idx_fact_claims ON fact_prescriptions(total_claims);
CREATE INDEX idx_fact_cost ON fact_prescriptions(total_drug_cost);

-- ============================================
-- AGGREGATE: Prescriber Summary
-- ============================================
CREATE TABLE IF NOT EXISTS agg_prescriber_summary (
    prescriber_id       INTEGER REFERENCES dim_prescriber(prescriber_id),
    year_id             INTEGER REFERENCES dim_year(year_id),
    total_claims        BIGINT,
    total_cost          NUMERIC(20,2),
    total_beneficiaries INTEGER,
    unique_drugs        INTEGER,
    opioid_claims       INTEGER,
    opioid_rate         NUMERIC(5,2),
    PRIMARY KEY (prescriber_id, year_id)
);

-- ============================================
-- AGGREGATE: Drug Utilization
-- ============================================
CREATE TABLE IF NOT EXISTS agg_drug_utilization (
    drug_id             INTEGER REFERENCES dim_drug(drug_id),
    year_id             INTEGER REFERENCES dim_year(year_id),
    total_claims        BIGINT,
    total_cost          NUMERIC(20,2),
    total_beneficiaries INTEGER,
    prescriber_count    INTEGER,
    avg_cost_per_claim  NUMERIC(10,2),
    PRIMARY KEY (drug_id, year_id)
);

-- ============================================
-- VIEWS: Analytics Ready
-- ============================================

-- View: Complete prescription facts with all dimensions
CREATE OR REPLACE VIEW v_prescription_facts AS
SELECT
    p.npi,
    p.provider_name,
    p.specialty,
    d.drug_name,
    d.generic_name,
    d.is_opioid,
    d.is_antibiotic,
    l.city,
    l.state,
    l.state_name,
    y.calendar_year,
    f.total_claims,
    f.total_drug_cost,
    f.total_beneficiaries,
    f.total_day_supply,
    f.avg_cost_per_claim
FROM fact_prescriptions f
JOIN dim_prescriber p ON f.prescriber_id = p.prescriber_id
JOIN dim_drug d ON f.drug_id = d.drug_id
LEFT JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_year y ON f.year_id = y.year_id;

-- View: Opioid prescribing summary
CREATE OR REPLACE VIEW v_opioid_prescribers AS
SELECT
    p.npi,
    p.provider_name,
    p.specialty,
    l.state,
    SUM(f.total_claims) as opioid_claims,
    SUM(f.total_drug_cost) as opioid_cost,
    SUM(f.total_beneficiaries) as opioid_beneficiaries
FROM fact_prescriptions f
JOIN dim_prescriber p ON f.prescriber_id = p.prescriber_id
JOIN dim_drug d ON f.drug_id = d.drug_id
LEFT JOIN dim_location l ON f.location_id = l.location_id
WHERE d.is_opioid = TRUE
GROUP BY p.prescriber_id, p.npi, p.provider_name, p.specialty, l.state;

-- View: Specialty prescribing patterns
CREATE OR REPLACE VIEW v_specialty_summary AS
SELECT
    p.specialty,
    y.calendar_year,
    COUNT(DISTINCT p.prescriber_id) as prescriber_count,
    SUM(f.total_claims) as total_claims,
    SUM(f.total_drug_cost) as total_cost,
    AVG(f.avg_cost_per_claim) as avg_cost_per_claim
FROM fact_prescriptions f
JOIN dim_prescriber p ON f.prescriber_id = p.prescriber_id
JOIN dim_year y ON f.year_id = y.year_id
GROUP BY p.specialty, y.calendar_year;

-- ============================================
-- SAMPLE QUERIES
-- ============================================

-- Top prescribers by claims
-- SELECT provider_name, specialty, SUM(total_claims) as claims
-- FROM v_prescription_facts
-- GROUP BY npi, provider_name, specialty
-- ORDER BY claims DESC
-- LIMIT 20;

-- Opioid prescribing by state
-- SELECT state, SUM(opioid_claims) as claims
-- FROM v_opioid_prescribers
-- GROUP BY state
-- ORDER BY claims DESC;

-- Drug utilization
-- SELECT drug_name, SUM(total_claims) as claims, SUM(total_drug_cost) as cost
-- FROM v_prescription_facts
-- GROUP BY drug_name
-- ORDER BY claims DESC
-- LIMIT 20;
