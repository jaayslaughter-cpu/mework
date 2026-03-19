-- ============================================================
-- PropIQ Analytics: Core Reference Tables
-- File: 01_core_reference.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS teams (
    team_id     VARCHAR(10) PRIMARY KEY,
    team_name   VARCHAR(50) NOT NULL,
    league      VARCHAR(2)
);

-- ── Seed Teams (Required before games table FK) ────────────
INSERT INTO teams (team_id, team_name, league) VALUES
    ('ARI', 'Arizona Diamondbacks', 'NL'),
    ('ATL', 'Atlanta Braves', 'NL'),
    ('BAL', 'Baltimore Orioles', 'AL'),
    ('BOS', 'Boston Red Sox', 'AL'),
    ('CHC', 'Chicago Cubs', 'NL'),
    ('CHW', 'Chicago White Sox', 'AL'),
    ('CIN', 'Cincinnati Reds', 'NL'),
    ('CLE', 'Cleveland Guardians', 'AL'),
    ('COL', 'Colorado Rockies', 'NL'),
    ('DET', 'Detroit Tigers', 'AL'),
    ('HOU', 'Houston Astros', 'AL'),
    ('KC', 'Kansas City Royals', 'AL'),
    ('LAA', 'Los Angeles Angels', 'AL'),
    ('LAD', 'Los Angeles Dodgers', 'NL'),
    ('MIA', 'Miami Marlins', 'NL'),
    ('MIL', 'Milwaukee Brewers', 'NL'),
    ('MIN', 'Minnesota Twins', 'AL'),
    ('NYM', 'New York Mets', 'NL'),
    ('NYY', 'New York Yankees', 'AL'),
    ('OAK', 'Oakland Athletics', 'AL'),
    ('PHI', 'Philadelphia Phillies', 'NL'),
    ('PIT', 'Pittsburgh Pirates', 'NL'),
    ('SD', 'San Diego Padres', 'NL'),
    ('SF', 'San Francisco Giants', 'NL'),
    ('SEA', 'Seattle Mariners', 'AL'),
    ('STL', 'St. Louis Cardinals', 'NL'),
    ('TB', 'Tampa Bay Rays', 'AL'),
    ('TEX', 'Texas Rangers', 'AL'),
    ('TOR', 'Toronto Blue Jays', 'AL'),
    ('WSH', 'Washington Nationals', 'NL')
ON CONFLICT (team_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS players (
    player_id   INT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    handedness  CHAR(1) CHECK (handedness IN ('R', 'L', 'S')),
    position    VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS park_factors (
    park_id         VARCHAR(10) PRIMARY KEY,
    stadium_name    VARCHAR(100) NOT NULL,
    xwoba_factor    FLOAT NOT NULL,  -- e.g., 110.2 = 10.2% booster
    hr_factor       FLOAT NOT NULL,  -- e.g., 122.1 = 22.1% HR booster
    has_roof        BOOLEAN DEFAULT FALSE
);

-- ── Seed Park Factors (2026 Savant Validated + Neutral Baselines) ──
INSERT INTO park_factors (park_id, stadium_name, xwoba_factor, hr_factor, has_roof) VALUES
    -- Validated park factors
    ('COL', 'Coors Field',          110.2, 122.1, FALSE),
    ('SF',  'Oracle Park',           91.8,  84.7, FALSE),
    ('BOS', 'Fenway Park',          107.1, 110.4, FALSE),
    ('PHI', 'Citizens Bank Park',   108.4, 115.3, FALSE),
    ('NYY', 'Yankee Stadium',        98.7, 102.3, FALSE),
    ('HOU', 'Minute Maid Park',     100.1,  98.2, TRUE),
    ('MIA', 'loanDepot park',        97.3,  94.1, TRUE),
    ('SEA', 'T-Mobile Park',         96.8,  91.5, TRUE),
    ('ARI', 'Chase Field',           99.4,  97.8, TRUE),
    ('TB',  'Tropicana Field',       98.1,  96.0, TRUE),
    -- Remaining 20 teams with neutral baseline (100.0)
    ('LAA', 'Angel Stadium',        100.0, 100.0, FALSE),
    ('BAL', 'Oriole Park',          100.0, 100.0, FALSE),
    ('ATL', 'Truist Park',          100.0, 100.0, FALSE),
    ('CHC', 'Wrigley Field',        100.0, 100.0, FALSE),
    ('CHW', 'Guaranteed Rate Field', 100.0, 100.0, FALSE),
    ('CIN', 'Great American Ball Park', 100.0, 100.0, FALSE),
    ('CLE', 'Progressive Field',    100.0, 100.0, FALSE),
    ('DET', 'Comerica Park',        100.0, 100.0, FALSE),
    ('KC',  'Kauffman Stadium',     100.0, 100.0, FALSE),
    ('LAD', 'Dodger Stadium',       100.0, 100.0, FALSE),
    ('MIL', 'American Family Field', 100.0, 100.0, TRUE),
    ('MIN', 'Target Field',         100.0, 100.0, FALSE),
    ('NYM', 'Citi Field',           100.0, 100.0, FALSE),
    ('OAK', 'Oakland Coliseum',     100.0, 100.0, FALSE),
    ('PIT', 'PNC Park',             100.0, 100.0, FALSE),
    ('SD',  'Petco Park',           100.0, 100.0, FALSE),
    ('STL', 'Busch Stadium',        100.0, 100.0, FALSE),
    ('TEX', 'Globe Life Field',     100.0, 100.0, TRUE),
    ('TOR', 'Rogers Centre',        100.0, 100.0, TRUE),
    ('WSH', 'Nationals Park',       100.0, 100.0, FALSE)
ON CONFLICT (park_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS games (
    game_id          VARCHAR(20) PRIMARY KEY,
    game_date        DATE NOT NULL,
    home_team_id     VARCHAR(10) REFERENCES teams(team_id) NOT NULL,
    away_team_id     VARCHAR(10) REFERENCES teams(team_id) NOT NULL,
    park_id          VARCHAR(10) REFERENCES park_factors(park_id),
    weather_wind_mph FLOAT,
    weather_wind_dir VARCHAR(20),
    umpire_id        INT,
    roof_status      VARCHAR(15) CHECK (roof_status IN ('open', 'closed', 'retractable')),
    status           VARCHAR(15) CHECK (status IN ('scheduled', 'in_progress', 'final')),
    CONSTRAINT no_self_play CHECK (home_team_id != away_team_id)
);

CREATE TABLE IF NOT EXISTS model_versions (
    version_id   VARCHAR(20) PRIMARY KEY,
    description  TEXT,
    deployed_at  TIMESTAMPTZ DEFAULT NOW()
);

-- ── Seed the locked model version ─────────────────────────
INSERT INTO model_versions (version_id, description, deployed_at) VALUES
    ('v1.0-xgb-676', 'XGBoost hybrid model. 67.6% accuracy on 124,800-prop 2025 backtest. Weights locked.', NOW())
ON CONFLICT (version_id) DO NOTHING;

-- ── Idempotent Constraint Applications ───────────────────────
ALTER TABLE players DROP CONSTRAINT IF EXISTS players_handedness_check;
ALTER TABLE players ADD CONSTRAINT players_handedness_check CHECK (handedness IN ('R', 'L', 'S'));

ALTER TABLE games DROP CONSTRAINT IF EXISTS games_roof_status_check;
ALTER TABLE games ADD CONSTRAINT games_roof_status_check CHECK (roof_status IN ('open', 'closed', 'retractable'));

ALTER TABLE games DROP CONSTRAINT IF EXISTS games_status_check;
ALTER TABLE games ADD CONSTRAINT games_status_check CHECK (status IN ('scheduled', 'in_progress', 'final'));

ALTER TABLE games DROP CONSTRAINT IF EXISTS no_self_play;
ALTER TABLE games ADD CONSTRAINT no_self_play CHECK (home_team_id != away_team_id);

ALTER TABLE model_versions ALTER COLUMN deployed_at TYPE TIMESTAMPTZ USING deployed_at::timestamptz;
