-- V33: Fix dedup index (delete duplicates first) + ensure missing tables exist
-- Required because V32 migration aborted when UNIQUE INDEX creation hit duplicate rows

-- Step 1: Remove duplicate bet_ledger rows, keeping the lowest id for each unique combination
DELETE FROM bet_ledger
WHERE id NOT IN (
    SELECT MIN(id)
    FROM bet_ledger
    GROUP BY player_name, prop_type, line, side, agent_name, bet_date
);

-- Step 2: Now create the unique index (was blocked by duplicates in V32)
CREATE UNIQUE INDEX IF NOT EXISTS ux_bet_ledger_dedup
    ON bet_ledger (player_name, prop_type, line, side, agent_name, bet_date);

-- Step 3: Create brier_ledger (was never created because V32 aborted)
CREATE TABLE IF NOT EXISTS brier_ledger (
    id          SERIAL PRIMARY KEY,
    agent_name  TEXT NOT NULL,
    brier_score FLOAT NOT NULL,
    n_samples   INT NOT NULL,
    graded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Step 4: Create agent_unit_sizing (was never created because V32 aborted)
CREATE TABLE IF NOT EXISTS agent_unit_sizing (
    agent_name  TEXT PRIMARY KEY,
    tier        TEXT NOT NULL DEFAULT 'D',
    stake       FLOAT NOT NULL DEFAULT 5.0,
    roi_7d      FLOAT NOT NULL DEFAULT 0.0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Step 5: Seed all 17 canonical agents at $5 if not already present
INSERT INTO agent_unit_sizing (agent_name, tier, stake, roi_7d)
VALUES
    ('EVHunter',             'D', 5.0, 0.0),
    ('UnderMachine',         'D', 5.0, 0.0),
    ('UmpireAgent',          'D', 5.0, 0.0),
    ('F5Agent',              'D', 5.0, 0.0),
    ('FadeAgent',            'D', 5.0, 0.0),
    ('LineValueAgent',       'D', 5.0, 0.0),
    ('BullpenAgent',         'D', 5.0, 0.0),
    ('WeatherAgent',         'D', 5.0, 0.0),
    ('MLEdgeAgent',          'D', 5.0, 0.0),
    ('UnderDogAgent',        'D', 5.0, 0.0),
    ('StackSmithAgent',      'D', 5.0, 0.0),
    ('ChalkBusterAgent',     'D', 5.0, 0.0),
    ('SharpFadeAgent',       'D', 5.0, 0.0),
    ('CorrelatedParlayAgent','D', 5.0, 0.0),
    ('PropCycleAgent',       'D', 5.0, 0.0),
    ('LineupChaseAgent',     'D', 5.0, 0.0),
    ('LineDriftAgent',       'D', 5.0, 0.0)
ON CONFLICT (agent_name) DO NOTHING;

-- Step 6: Create draftedge_cache if missing (from V30, may have been missed)
CREATE TABLE IF NOT EXISTS draftedge_cache (
    cache_key   TEXT PRIMARY KEY,
    cache_date  DATE NOT NULL,
    data_json   TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Step 7: Create line_snapshots if missing (from V30)
CREATE TABLE IF NOT EXISTS line_snapshots (
    id           SERIAL PRIMARY KEY,
    player_name  TEXT NOT NULL,
    prop_type    TEXT NOT NULL,
    line         FLOAT NOT NULL,
    platform     TEXT NOT NULL,
    snapped_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
