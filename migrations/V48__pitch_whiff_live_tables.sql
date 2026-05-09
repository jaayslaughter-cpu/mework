-- V48: Create pitch_whiff_live table for nightly live whiff% from MLB Stats API
-- Replaces/supplements bundled batter_pitch_arsenal_2026.csv with live season data.
-- Written by pitch_whiff_refresh.py nightly at 3:30 AM PT.

BEGIN;

CREATE TABLE IF NOT EXISTS pitch_whiff_live (
    pitcher_id      INTEGER     NOT NULL,
    pitch_type      VARCHAR(4)  NOT NULL,
    season          SMALLINT    NOT NULL DEFAULT EXTRACT(YEAR FROM NOW()),
    games           SMALLINT    NOT NULL DEFAULT 0,
    pitches         INTEGER     NOT NULL DEFAULT 0,
    swings          INTEGER     NOT NULL DEFAULT 0,
    whiffs          INTEGER     NOT NULL DEFAULT 0,
    whiff_pct       NUMERIC(6,4) NOT NULL DEFAULT 0.0,  -- 0.0–1.0 (e.g. 0.312 = 31.2%)
    k_pct           NUMERIC(6,4) NOT NULL DEFAULT 0.0,
    put_away_pct    NUMERIC(6,4) NOT NULL DEFAULT 0.0,
    spin_rate       NUMERIC(8,2),                        -- avg rpm (NULL if unavailable)
    velocity        NUMERIC(5,2),                        -- avg mph
    ivb             NUMERIC(6,2),                        -- induced vertical break inches
    hb              NUMERIC(6,2),                        -- horizontal break inches
    refreshed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pitcher_id, pitch_type, season)
);

-- Batter vs pitch-type whiff (from tnestico parser, per-batter per-pitch-type)
CREATE TABLE IF NOT EXISTS batter_pitch_whiff_live (
    batter_id       INTEGER     NOT NULL,
    pitch_type      VARCHAR(4)  NOT NULL,
    season          SMALLINT    NOT NULL DEFAULT EXTRACT(YEAR FROM NOW()),
    pa              SMALLINT    NOT NULL DEFAULT 0,
    swings          INTEGER     NOT NULL DEFAULT 0,
    whiffs          INTEGER     NOT NULL DEFAULT 0,
    whiff_pct       NUMERIC(6,4) NOT NULL DEFAULT 0.0,
    k_pct           NUMERIC(6,4) NOT NULL DEFAULT 0.0,
    woba            NUMERIC(6,4) NOT NULL DEFAULT 0.0,
    refreshed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batter_id, pitch_type, season)
);

CREATE INDEX IF NOT EXISTS idx_pitch_whiff_pitcher ON pitch_whiff_live (pitcher_id, season);
CREATE INDEX IF NOT EXISTS idx_batter_pitch_whiff_batter ON batter_pitch_whiff_live (batter_id, season);

-- Record this migration
INSERT INTO migration_history (filename, version, description, applied_at)
VALUES ('V48__pitch_whiff_live_tables.sql', 'V48', 'pitch_whiff_live_tables', NOW())
ON CONFLICT (version) DO NOTHING;

COMMIT;
