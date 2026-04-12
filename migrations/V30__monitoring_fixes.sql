-- V30: Monitoring schema fixes
-- (1) Add settled_at to propiq_season_record — used by edge_health_monitor + calibration_monitor
-- (2) Create line_snapshots table — used by edge_health_monitor CLV tracking (line_stream populates it)
-- (3) Add agent_leaderboard table if missing (referenced by bug_checker required tables list)

ALTER TABLE propiq_season_record
    ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS line_snapshots (
    id               SERIAL PRIMARY KEY,
    player_name      TEXT NOT NULL,
    prop_type        TEXT NOT NULL,
    game_date        DATE NOT NULL,
    line             FLOAT NOT NULL,
    is_closing_line  BOOLEAN NOT NULL DEFAULT FALSE,
    bookmaker        TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_line_snapshots_player_date
    ON line_snapshots (player_name, prop_type, game_date);

CREATE TABLE IF NOT EXISTS agent_leaderboard (
    id           SERIAL PRIMARY KEY,
    agent_name   TEXT NOT NULL,
    wins         INTEGER NOT NULL DEFAULT 0,
    losses       INTEGER NOT NULL DEFAULT 0,
    pushes       INTEGER NOT NULL DEFAULT 0,
    roi_pct      FLOAT NOT NULL DEFAULT 0.0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (agent_name)
);
