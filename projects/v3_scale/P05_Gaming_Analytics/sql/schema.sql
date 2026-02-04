-- P05 Gaming Analytics - Star Schema DDL
-- Author: Mboya Jeffers
-- Database: PostgreSQL
-- Pattern: Kimball Dimensional Model

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Game Dimension
CREATE TABLE IF NOT EXISTS dim_game (
    game_key        VARCHAR(16) PRIMARY KEY,
    game_id         INTEGER NOT NULL UNIQUE,
    game_name       VARCHAR(255),
    game_type       VARCHAR(50),
    is_free         BOOLEAN DEFAULT FALSE,
    description     TEXT,
    coming_soon     BOOLEAN DEFAULT FALSE,
    total_achievements INTEGER DEFAULT 0,
    effective_date  DATE NOT NULL,
    is_current      BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_game_id ON dim_game(game_id);
CREATE INDEX idx_dim_game_name ON dim_game(game_name);

-- Developer/Publisher Dimension
CREATE TABLE IF NOT EXISTS dim_developer (
    developer_key   VARCHAR(16) PRIMARY KEY,
    developer_id    SERIAL,
    developer_name  VARCHAR(255) NOT NULL,
    is_publisher    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_developer_name ON dim_developer(developer_name);

-- Genre Dimension
CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key       VARCHAR(16) PRIMARY KEY,
    genre_id        SERIAL,
    genre_name      VARCHAR(100) NOT NULL UNIQUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_genre_name ON dim_genre(genre_name);

-- Platform Dimension
CREATE TABLE IF NOT EXISTS dim_platform (
    platform_key    VARCHAR(16) PRIMARY KEY,
    platform_id     INTEGER NOT NULL,
    platform_name   VARCHAR(50) NOT NULL,
    platform_code   VARCHAR(10) NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        VARCHAR(16) PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    year_month      VARCHAR(7) NOT NULL,
    is_current      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_year ON dim_date(year);
CREATE INDEX idx_dim_date_year_month ON dim_date(year_month);

-- ============================================
-- FACT TABLES
-- ============================================

-- Game Metrics Fact Table
CREATE TABLE IF NOT EXISTS fact_game_metrics (
    fact_id                 VARCHAR(32) PRIMARY KEY,
    game_key                VARCHAR(16) NOT NULL REFERENCES dim_game(game_key),
    game_id                 INTEGER NOT NULL,
    snapshot_date           DATE NOT NULL,

    -- Player Metrics
    owners_low              BIGINT DEFAULT 0,
    owners_high             BIGINT DEFAULT 0,
    owners_estimate         BIGINT DEFAULT 0,
    players_forever         BIGINT DEFAULT 0,
    players_2weeks          BIGINT DEFAULT 0,
    concurrent_users        INTEGER DEFAULT 0,

    -- Playtime Metrics (in minutes)
    avg_playtime_forever    INTEGER DEFAULT 0,
    avg_playtime_2weeks     INTEGER DEFAULT 0,
    median_playtime_forever INTEGER DEFAULT 0,
    median_playtime_2weeks  INTEGER DEFAULT 0,

    -- Review Metrics
    positive_reviews        INTEGER DEFAULT 0,
    negative_reviews        INTEGER DEFAULT 0,
    total_reviews           INTEGER DEFAULT 0,
    review_score_pct        DECIMAL(5,2),
    recommendations         INTEGER DEFAULT 0,

    -- Financial Metrics
    price_usd               DECIMAL(10,2) DEFAULT 0,
    revenue_estimate_usd    DECIMAL(15,2) DEFAULT 0,

    -- Quality Metrics
    metacritic_score        INTEGER,

    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_game_key ON fact_game_metrics(game_key);
CREATE INDEX idx_fact_snapshot_date ON fact_game_metrics(snapshot_date);
CREATE INDEX idx_fact_revenue ON fact_game_metrics(revenue_estimate_usd DESC);
CREATE INDEX idx_fact_owners ON fact_game_metrics(owners_estimate DESC);

-- ============================================
-- BRIDGE TABLES
-- ============================================

-- Game-Genre Bridge (Many-to-Many)
CREATE TABLE IF NOT EXISTS game_genre_bridge (
    game_key        VARCHAR(16) NOT NULL REFERENCES dim_game(game_key),
    genre_key       VARCHAR(16) NOT NULL REFERENCES dim_genre(genre_key),
    PRIMARY KEY (game_key, genre_key)
);

CREATE INDEX idx_bridge_game ON game_genre_bridge(game_key);
CREATE INDEX idx_bridge_genre ON game_genre_bridge(genre_key);

-- Game-Developer Bridge (Many-to-Many)
CREATE TABLE IF NOT EXISTS game_developer_bridge (
    game_key        VARCHAR(16) NOT NULL REFERENCES dim_game(game_key),
    developer_key   VARCHAR(16) NOT NULL REFERENCES dim_developer(developer_key),
    role            VARCHAR(20) DEFAULT 'developer', -- 'developer' or 'publisher'
    PRIMARY KEY (game_key, developer_key, role)
);

-- Game-Platform Bridge
CREATE TABLE IF NOT EXISTS game_platform_bridge (
    game_key        VARCHAR(16) NOT NULL REFERENCES dim_game(game_key),
    platform_key    VARCHAR(16) NOT NULL REFERENCES dim_platform(platform_key),
    is_supported    BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (game_key, platform_key)
);

-- ============================================
-- ANALYTICS VIEWS
-- ============================================

-- Top Games by Revenue
CREATE OR REPLACE VIEW v_top_games_revenue AS
SELECT
    g.game_name,
    f.owners_estimate,
    f.price_usd,
    f.revenue_estimate_usd,
    f.review_score_pct,
    f.concurrent_users
FROM fact_game_metrics f
JOIN dim_game g ON f.game_key = g.game_key
ORDER BY f.revenue_estimate_usd DESC
LIMIT 100;

-- Genre Performance Summary
CREATE OR REPLACE VIEW v_genre_performance AS
SELECT
    gen.genre_name,
    COUNT(DISTINCT f.game_key) AS game_count,
    SUM(f.owners_estimate) AS total_owners,
    AVG(f.review_score_pct) AS avg_review_score,
    SUM(f.revenue_estimate_usd) AS total_revenue
FROM fact_game_metrics f
JOIN game_genre_bridge gg ON f.game_key = gg.game_key
JOIN dim_genre gen ON gg.genre_key = gen.genre_key
GROUP BY gen.genre_name
ORDER BY total_revenue DESC;

-- Player Engagement Summary
CREATE OR REPLACE VIEW v_player_engagement AS
SELECT
    g.game_name,
    f.owners_estimate,
    f.players_2weeks,
    CASE
        WHEN f.owners_estimate > 0
        THEN ROUND((f.players_2weeks::DECIMAL / f.owners_estimate) * 100, 2)
        ELSE 0
    END AS activity_rate_pct,
    f.avg_playtime_forever / 60 AS avg_hours_played,
    f.concurrent_users
FROM fact_game_metrics f
JOIN dim_game g ON f.game_key = g.game_key
WHERE f.owners_estimate > 0
ORDER BY activity_rate_pct DESC;

-- ============================================
-- COMMENTS
-- ============================================

COMMENT ON TABLE dim_game IS 'Game master data dimension - SCD Type 2 enabled';
COMMENT ON TABLE fact_game_metrics IS 'Game metrics snapshot fact - daily grain';
COMMENT ON COLUMN fact_game_metrics.owners_estimate IS 'Midpoint of SteamSpy ownership range';
COMMENT ON COLUMN fact_game_metrics.revenue_estimate_usd IS 'Estimated revenue = owners * price * 0.7 (Steam cut)';
