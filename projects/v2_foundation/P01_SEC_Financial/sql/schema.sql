-- SEC Financial Intelligence - Star Schema
-- Author: Mboya Jeffers
-- Scale: 1M+ financial facts

-- ============================================
-- DIMENSION: Companies
-- ============================================
CREATE TABLE IF NOT EXISTS dim_company (
    company_id      SERIAL PRIMARY KEY,
    cik             VARCHAR(10) NOT NULL UNIQUE,
    entity_name     VARCHAR(500) NOT NULL,
    ticker          VARCHAR(10),
    sic_code        VARCHAR(4),
    sector          VARCHAR(100),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_company_cik ON dim_company(cik);
CREATE INDEX idx_dim_company_ticker ON dim_company(ticker);

-- ============================================
-- DIMENSION: Metrics (XBRL Tags)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_metric (
    metric_id       SERIAL PRIMARY KEY,
    metric          VARCHAR(200) NOT NULL,
    taxonomy        VARCHAR(20) NOT NULL,  -- us-gaap, dei, etc.
    category        VARCHAR(50),           -- Income Statement, Balance Sheet, etc.
    description     TEXT,
    UNIQUE(metric, taxonomy)
);

CREATE INDEX idx_dim_metric_category ON dim_metric(category);

-- ============================================
-- DIMENSION: Time (Fiscal Periods)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    fiscal_year     INTEGER NOT NULL,
    fiscal_period   VARCHAR(4),            -- FY, Q1, Q2, Q3, Q4
    quarter_label   VARCHAR(10),           -- 2024Q3, 2024FY
    UNIQUE(fiscal_year, fiscal_period)
);

CREATE INDEX idx_dim_date_year ON dim_date(fiscal_year);

-- ============================================
-- FACT: Financial Facts (1M+ rows)
-- ============================================
CREATE TABLE IF NOT EXISTS fact_financials (
    fact_id             BIGSERIAL PRIMARY KEY,
    company_id          INTEGER NOT NULL REFERENCES dim_company(company_id),
    metric_id           INTEGER NOT NULL REFERENCES dim_metric(metric_id),
    date_id             INTEGER NOT NULL REFERENCES dim_date(date_id),

    -- Measures
    value               NUMERIC(20,4),
    unit                VARCHAR(20),        -- USD, shares, pure

    -- Filing metadata
    form                VARCHAR(10),        -- 10-K, 10-Q
    filed               DATE,
    start_date          DATE,
    end_date            DATE,
    accession_number    VARCHAR(25),

    -- Technical
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Critical indexes for query performance at scale
CREATE INDEX idx_fact_company ON fact_financials(company_id);
CREATE INDEX idx_fact_metric ON fact_financials(metric_id);
CREATE INDEX idx_fact_date ON fact_financials(date_id);
CREATE INDEX idx_fact_company_date ON fact_financials(company_id, date_id);

-- ============================================
-- FACT: Calculated KPIs
-- ============================================
CREATE TABLE IF NOT EXISTS fact_kpis (
    kpi_id              BIGSERIAL PRIMARY KEY,
    company_id          INTEGER NOT NULL REFERENCES dim_company(company_id),
    date_id             INTEGER NOT NULL REFERENCES dim_date(date_id),

    -- Profitability
    gross_margin        NUMERIC(10,4),
    operating_margin    NUMERIC(10,4),
    net_margin          NUMERIC(10,4),
    roe                 NUMERIC(10,4),
    roa                 NUMERIC(10,4),

    -- Liquidity
    current_ratio       NUMERIC(10,4),
    quick_ratio         NUMERIC(10,4),
    cash_ratio          NUMERIC(10,4),

    -- Leverage
    debt_to_equity      NUMERIC(10,4),
    debt_to_assets      NUMERIC(10,4),
    equity_ratio        NUMERIC(10,4),

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company_id, date_id)
);

CREATE INDEX idx_kpi_company ON fact_kpis(company_id);
CREATE INDEX idx_kpi_date ON fact_kpis(date_id);

-- ============================================
-- VIEWS: Analytics Ready
-- ============================================

-- View: Complete financial facts with all dimensions
CREATE OR REPLACE VIEW v_financial_facts AS
SELECT
    c.cik,
    c.entity_name,
    c.ticker,
    m.metric,
    m.category,
    d.fiscal_year,
    d.fiscal_period,
    d.quarter_label,
    f.value,
    f.unit,
    f.form,
    f.filed,
    f.accession_number
FROM fact_financials f
JOIN dim_company c ON f.company_id = c.company_id
JOIN dim_metric m ON f.metric_id = m.metric_id
JOIN dim_date d ON f.date_id = d.date_id;

-- View: KPIs with company info
CREATE OR REPLACE VIEW v_company_kpis AS
SELECT
    c.cik,
    c.entity_name,
    c.ticker,
    d.fiscal_year,
    d.fiscal_period,
    k.gross_margin,
    k.operating_margin,
    k.net_margin,
    k.roe,
    k.roa,
    k.current_ratio,
    k.quick_ratio,
    k.debt_to_equity
FROM fact_kpis k
JOIN dim_company c ON k.company_id = c.company_id
JOIN dim_date d ON k.date_id = d.date_id;

-- ============================================
-- SAMPLE QUERIES
-- ============================================

-- Count total facts
-- SELECT COUNT(*) FROM fact_financials;

-- Top companies by ROE
-- SELECT * FROM v_company_kpis
-- WHERE roe IS NOT NULL
-- ORDER BY roe DESC
-- LIMIT 20;

-- Revenue by company over time
-- SELECT entity_name, fiscal_year, value
-- FROM v_financial_facts
-- WHERE metric IN ('Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax')
-- ORDER BY entity_name, fiscal_year;
