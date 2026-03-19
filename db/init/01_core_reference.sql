-- ============================================================
-- PropIQ Analytics: Core Reference Tables
-- File: 01_core_reference.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS teams (
    team_id     VARCHAR(10) PRIMARY KEY,
    team_name   VARCHAR(50) NOT NULL,
    league      VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS players (
    player_id   INT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    handedness  CHAR(1),     -- 'R', 'L', or 'S'
    position    VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS park_factors (
    park_id         VARCHAR(10) PRIMARY KEY,
    stadium_name    VARCHAR(100) NOT NULL,
    xwoba_factor    FLOAT NOT NULL,  -- e.g., 110.2 = 10.2% booster
    hr_factor       FLOAT NOT NULL,  -- e.g., 122.1 = 22.1% HR booster
    has_roof        BOOLEAN DEFAULT FALSE
);

-- ── Seed Park Factors (2026 Savant Validated) ──────────────
INSERT INTO park_factors (park_id, stadium_name, xwoba_factor, hr_factor, has_roof) VALUES
    ('COL', 'Coors Field',          110.2, 122.1, FALSE),
    ('SF',  'Oracle Park',           91.8,  84.7, FALSE),
    ('BOS', 'Fenway Park',          107.1, 110.4, FALSE),
    ('PHI', 'Citizens Bank Park',   108.4, 115.3, FALSE),
    ('NYY', 'Yankee Stadium',        98.7, 102.3, FALSE),
    ('HOU', 'Minute Maid Park',     100.1,  98.2, TRUE),
    ('MIA', 'loanDepot park',        97.3,  94.1, TRUE),
    ('SEA', 'T-Mobile Park',         96.8,  91.5, TRUE),
    ('ARI', 'Chase Field',           99.4,  97.8, TRUE),
    ('TB',  'Tropicana Field',       98.1,  96.0, TRUE)
ON CONFLICT (park_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS games (
    game_id          VARCHAR(20) PRIMARY KEY,
    game_date        DATE NOT NULL,
    home_team_id     VARCHAR(10) REFERENCES teams(team_id),
    away_team_id     VARCHAR(10) REFERENCES teams(team_id),
    park_id          VARCHAR(10) REFERENCES park_factors(park_id),
    weather_wind_mph FLOAT,
    weather_wind_dir VARCHAR(20),
    umpire_id        INT,
    roof_status      VARCHAR(15), -- 'open', 'closed', 'retractable'
    status           VARCHAR(15)  -- 'scheduled', 'in_progress', 'final'
);

CREATE TABLE IF NOT EXISTS model_versions (
    version_id   VARCHAR(20) PRIMARY KEY,
    description  TEXT,
    deployed_at  TIMESTAMP DEFAULT NOW()
);

-- ── Seed the locked model version ─────────────────────────
INSERT INTO model_versions (version_id, description, deployed_at) VALUES
    ('v1.0-xgb-676', 'XGBoost hybrid model. 67.6% accuracy on 124,800-prop 2025 backtest. Weights locked.', NOW())
ON CONFLICT (version_id) DO NOTHING;
