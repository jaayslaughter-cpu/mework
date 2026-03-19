-- ============================================================
-- PropIQ Analytics: Projection & Market Layer
-- File: 02_projection_market_layer.sql
-- ============================================================

-- Update live_projections.market_id column width on existing tables
ALTER TABLE IF EXISTS live_projections ALTER COLUMN market_id TYPE VARCHAR(100);

-- ── Live Projections (Hot Table — updated every 60 seconds) ─
CREATE TABLE IF NOT EXISTS live_projections (
    market_id            VARCHAR(100) PRIMARY KEY,
    player_name          VARCHAR(100) NOT NULL,
    prop_type            VARCHAR(30) NOT NULL,
    line                 FLOAT NOT NULL,
    model_prob           FLOAT,
    implied_prob         FLOAT,
    edge_pct             FLOAT,
    park_id              VARCHAR(10) REFERENCES park_factors(park_id),
    barrel_pct_14d       FLOAT,
    lineup_position      INT,
    pitcher_xwoba_con    FLOAT,
    csw_pct_14d          FLOAT,
    last_updated         TIMESTAMPTZ DEFAULT NOW()
-- ── Live Projections (Hot Table — updated every 15 seconds) ─
CREATE TABLE IF NOT EXISTS live_projections (
    market_id            VARCHAR(50) PRIMARY KEY,
    player_name          VARCHAR(100) NOT NULL,
    prop_type            VARCHAR(30) NOT NULL,  -- 'strikeouts', 'hits', 'total_bases', 'hr'
    line                 FLOAT NOT NULL,
    model_prob           FLOAT,    -- XGBoost output probability (0.0 - 1.0)
    implied_prob         FLOAT,    -- De-vigged true market probability
    edge_pct             FLOAT,    -- (model_prob - implied_prob) * 100
    park_id              VARCHAR(10) REFERENCES park_factors(park_id),
    barrel_pct_14d       FLOAT,
    lineup_position      INT,      -- 1-9 batting order slot
    pitcher_xwoba_con    FLOAT,    -- Opponent pitcher xwOBA-contact, 14-day rolling
    csw_pct_14d          FLOAT,    -- Called Strike + Whiff %, 14-day rolling
    last_updated         TIMESTAMP DEFAULT NOW()
);

-- ── Pitcher Metrics (Updated by pybaseball slow-data job) ───
CREATE TABLE IF NOT EXISTS pitcher_metrics (
    pitcher_id      INT PRIMARY KEY REFERENCES players(player_id),
    xwoba_con_14d   FLOAT,
    csw_pct_14d     FLOAT,
    velo_95th       FLOAT,
    spin_rate_fb    FLOAT,
    ttop_penalty    FLOAT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
    xwoba_con_14d   FLOAT,    -- xwOBA on contact, 14-day rolling avg (min 20 PA)
    csw_pct_14d     FLOAT,    -- CSW%, 14-day rolling avg (min 20 PA)
    velo_95th       FLOAT,    -- 95th percentile fastball velocity
    spin_rate_fb    FLOAT,    -- Fastball spin rate
    ttop_penalty    FLOAT,    -- Times Through Order Penalty coefficient
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ── Immutable Projection Snapshots (Hindsight-Bias Prevention) ──
CREATE TABLE IF NOT EXISTS projections (
    projection_id          SERIAL PRIMARY KEY,
    game_id                VARCHAR(20) REFERENCES games(game_id),
    pitcher_id             INT REFERENCES players(player_id),
    model_version          VARCHAR(20) REFERENCES model_versions(version_id),
    as_of_timestamp        TIMESTAMPTZ NOT NULL,
    projected_batters_faced FLOAT,
    adjusted_k_rate         FLOAT,
    projected_mean_k        FLOAT,
    as_of_timestamp        TIMESTAMP NOT NULL,  -- CRITICAL: captured at moment of generation
    -- Model Outputs
    projected_batters_faced FLOAT,
    adjusted_k_rate         FLOAT,
    projected_mean_k        FLOAT,   -- λ (lambda) for Poisson distribution
    -- Context captured at time of projection (locks in the math)
    lineup_k_rate_used      FLOAT,
    park_factor_used        FLOAT,
    wind_mph_at_projection  FLOAT,
    roof_closed_at_proj     BOOLEAN DEFAULT FALSE
);

-- ── Sportsbook Betting Markets (Live Lines) ─────────────────
CREATE TABLE IF NOT EXISTS betting_markets (
    market_id       VARCHAR(255) PRIMARY KEY,
    game_id         VARCHAR(50),
    pitcher_id      INT,
    sportsbook      VARCHAR(50),
    prop_category   VARCHAR(50),
    line            FLOAT,
    over_odds       INT,
    under_odds      INT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
-- ── Betting Markets (Sportsbook state captured at a point in time) ──
CREATE TABLE IF NOT EXISTS betting_markets (
    market_id       SERIAL PRIMARY KEY,
    game_id         VARCHAR(20) REFERENCES games(game_id),
    pitcher_id      INT REFERENCES players(player_id),
    sportsbook      VARCHAR(50) NOT NULL,
    recorded_at     TIMESTAMP NOT NULL,
    prop_line       FLOAT NOT NULL,
    over_odds       INT NOT NULL,      -- American odds format
    under_odds      INT NOT NULL,
    implied_prob_over FLOAT NOT NULL   -- Raw (vigged) implied probability
);
