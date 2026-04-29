-- V32: Emergency — create missing tables and indexes from V29/V31 migrations
-- V29 migration (agent_unit_sizing) and V31 migration (ux_bet_ledger_dedup)
-- never executed in Railway. This combines both into a single catch-up migration.

-- ── Agent unit sizing table (from V29) ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS agent_unit_sizing (
    id           SERIAL PRIMARY KEY,
    agent_name   VARCHAR(100) UNIQUE NOT NULL,
    tier         INTEGER      NOT NULL DEFAULT 1,
    stake        NUMERIC(8,2) NOT NULL DEFAULT 5.00,
    roi_7d       NUMERIC(8,4) NOT NULL DEFAULT 0.0,
    wins         INTEGER      NOT NULL DEFAULT 0,
    losses       INTEGER      NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Seed all 17 canonical agents at tier=1, stake=$5 (idempotent)
INSERT INTO agent_unit_sizing (agent_name, tier, stake, roi_7d) VALUES
    ('EVHunter',              1, 5.00, 0.0),
    ('UnderMachine',          1, 5.00, 0.0),
    ('UmpireAgent',           1, 5.00, 0.0),
    ('F5Agent',               1, 5.00, 0.0),
    ('FadeAgent',             1, 5.00, 0.0),
    ('LineValueAgent',        1, 5.00, 0.0),
    ('BullpenAgent',          1, 5.00, 0.0),
    ('WeatherAgent',          1, 5.00, 0.0),
    ('MLEdgeAgent',           1, 5.00, 0.0),
    ('UnderDogAgent',         1, 5.00, 0.0),
    ('StackSmithAgent',       1, 5.00, 0.0),
    ('ChalkBusterAgent',      1, 5.00, 0.0),
    ('SharpFadeAgent',        1, 5.00, 0.0),
    ('CorrelatedParlayAgent', 1, 5.00, 0.0),
    ('PropCycleAgent',        1, 5.00, 0.0),
    ('LineupChaseAgent',      1, 5.00, 0.0),
    ('LineDriftAgent',        1, 5.00, 0.0)
ON CONFLICT (agent_name) DO NOTHING;

-- ── Dedup index on bet_ledger (from V31) ─────────────────────────────────────
-- Prevents duplicate rows when ON CONFLICT DO NOTHING is used in INSERT
CREATE UNIQUE INDEX IF NOT EXISTS ux_bet_ledger_dedup
    ON bet_ledger (player_name, prop_type, line, side, agent_name, bet_date);

-- ── Brier ledger table (from PR #291) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS brier_ledger (
    id          SERIAL PRIMARY KEY,
    scored_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    score_date  DATE        NOT NULL,
    brier_score NUMERIC(8,6),
    n_bets      INTEGER,
    notes       TEXT
);
