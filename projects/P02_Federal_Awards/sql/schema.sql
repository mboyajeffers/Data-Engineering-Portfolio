-- Federal Awards Intelligence - Star Schema
-- Author: Mboya Jeffers
-- Scale: 1M+ federal awards

-- ============================================
-- DIMENSION: Agencies
-- ============================================
CREATE TABLE IF NOT EXISTS dim_agency (
    agency_id       SERIAL PRIMARY KEY,
    agency_code     VARCHAR(10),
    agency_name     VARCHAR(500) NOT NULL,
    sub_agency      VARCHAR(500),
    department      VARCHAR(200),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_agency_name ON dim_agency(agency_name);
CREATE INDEX idx_dim_agency_code ON dim_agency(agency_code);

-- ============================================
-- DIMENSION: Recipients
-- ============================================
CREATE TABLE IF NOT EXISTS dim_recipient (
    recipient_id    SERIAL PRIMARY KEY,
    recipient_name  VARCHAR(500) NOT NULL,
    duns_number     VARCHAR(13),
    business_type   VARCHAR(100),
    recipient_type  VARCHAR(50),     -- Corporation, Government, Nonprofit, etc.
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_recipient_name ON dim_recipient(recipient_name);
CREATE INDEX idx_dim_recipient_type ON dim_recipient(recipient_type);

-- ============================================
-- DIMENSION: Locations
-- ============================================
CREATE TABLE IF NOT EXISTS dim_location (
    location_id     SERIAL PRIMARY KEY,
    city            VARCHAR(200),
    state_code      VARCHAR(2),
    state_name      VARCHAR(100),
    county          VARCHAR(100),
    zip_code        VARCHAR(10),
    country         VARCHAR(3) DEFAULT 'USA',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_location_state ON dim_location(state_code);
CREATE INDEX idx_dim_location_zip ON dim_location(zip_code);

-- ============================================
-- DIMENSION: Date
-- ============================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    fiscal_year     INTEGER NOT NULL,
    fiscal_quarter  VARCHAR(4),        -- FY, Q1, Q2, Q3, Q4
    calendar_year   INTEGER,
    calendar_month  INTEGER,
    quarter_label   VARCHAR(10),       -- FY2024Q1
    UNIQUE(fiscal_year, fiscal_quarter)
);

CREATE INDEX idx_dim_date_fy ON dim_date(fiscal_year);

-- ============================================
-- FACT: Awards (1M+ rows)
-- ============================================
CREATE TABLE IF NOT EXISTS fact_awards (
    fact_id             BIGSERIAL PRIMARY KEY,
    award_id            VARCHAR(50),
    agency_id           INTEGER REFERENCES dim_agency(agency_id),
    recipient_id        INTEGER REFERENCES dim_recipient(recipient_id),
    location_id         INTEGER REFERENCES dim_location(location_id),
    date_id             INTEGER REFERENCES dim_date(date_id),

    -- Measures
    award_amount        NUMERIC(20,2),
    total_outlays       NUMERIC(20,2),

    -- Award attributes
    award_type          VARCHAR(10),       -- A, B, C, D, 02, 03, etc.
    naics_code          VARCHAR(10),
    cfda_number         VARCHAR(10),
    description         TEXT,

    -- Dates
    start_date          DATE,
    end_date            DATE,

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Critical indexes for query performance at scale
CREATE INDEX idx_fact_agency ON fact_awards(agency_id);
CREATE INDEX idx_fact_recipient ON fact_awards(recipient_id);
CREATE INDEX idx_fact_location ON fact_awards(location_id);
CREATE INDEX idx_fact_date ON fact_awards(date_id);
CREATE INDEX idx_fact_amount ON fact_awards(award_amount);
CREATE INDEX idx_fact_type ON fact_awards(award_type);
CREATE INDEX idx_fact_naics ON fact_awards(naics_code);

-- ============================================
-- AGGREGATE: Agency Spending Summary
-- ============================================
CREATE TABLE IF NOT EXISTS agg_agency_spending (
    agency_id           INTEGER REFERENCES dim_agency(agency_id),
    date_id             INTEGER REFERENCES dim_date(date_id),
    total_obligated     NUMERIC(20,2),
    total_outlays       NUMERIC(20,2),
    award_count         INTEGER,
    avg_award_size      NUMERIC(20,2),
    yoy_growth          NUMERIC(10,4),
    PRIMARY KEY (agency_id, date_id)
);

-- ============================================
-- VIEWS: Analytics Ready
-- ============================================

-- View: Complete award facts with all dimensions
CREATE OR REPLACE VIEW v_award_facts AS
SELECT
    a.agency_name,
    a.sub_agency,
    r.recipient_name,
    r.recipient_type,
    l.city,
    l.state_code,
    l.state_name,
    d.fiscal_year,
    d.fiscal_quarter,
    f.award_amount,
    f.total_outlays,
    f.award_type,
    f.naics_code,
    f.cfda_number,
    f.description
FROM fact_awards f
JOIN dim_agency a ON f.agency_id = a.agency_id
JOIN dim_recipient r ON f.recipient_id = r.recipient_id
LEFT JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id;

-- View: Spending by state
CREATE OR REPLACE VIEW v_state_spending AS
SELECT
    l.state_code,
    l.state_name,
    d.fiscal_year,
    SUM(f.award_amount) as total_spending,
    COUNT(*) as award_count,
    AVG(f.award_amount) as avg_award
FROM fact_awards f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY l.state_code, l.state_name, d.fiscal_year;

-- View: Top recipients
CREATE OR REPLACE VIEW v_top_recipients AS
SELECT
    r.recipient_name,
    r.recipient_type,
    SUM(f.award_amount) as total_awards,
    COUNT(*) as award_count,
    AVG(f.award_amount) as avg_award
FROM fact_awards f
JOIN dim_recipient r ON f.recipient_id = r.recipient_id
GROUP BY r.recipient_id, r.recipient_name, r.recipient_type
ORDER BY total_awards DESC;

-- ============================================
-- SAMPLE QUERIES
-- ============================================

-- Total spending by agency
-- SELECT agency_name, SUM(award_amount) as total
-- FROM v_award_facts
-- GROUP BY agency_name
-- ORDER BY total DESC
-- LIMIT 20;

-- Spending by state
-- SELECT * FROM v_state_spending
-- WHERE fiscal_year = 2024
-- ORDER BY total_spending DESC;

-- Top contractors
-- SELECT * FROM v_top_recipients
-- LIMIT 50;
