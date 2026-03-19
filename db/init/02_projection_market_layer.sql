-- ============================================================
-- PropIQ Analytics: Projection & Market Layer
-- File: 02_projection_market_layer.sql
-- ============================================================

-- ── Live Projections (Hot Table — updated every 15 seconds) ─
CREATE TABLE IF NOT EXISTS live_projections (
    market_id            VARCHAR(50) PRIMARY KEY,
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
);
