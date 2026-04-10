-- V29: agent_unit_sizing + draftedge_cache tables
-- Phase 43: Per-agent dynamic unit sizing tier ladder ($5 → $8 → $12 → $16 → $20)
-- Phase 47: temperature column for temperature scaling calibration
-- Phase 100+: draftedge_cache for Railway-redeploy-safe DraftEdge projections

-- ── agent_unit_sizing ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agent_unit_sizing (
    id                  SERIAL PRIMARY KEY,
    agent_name          VARCHAR(100) NOT NULL UNIQUE,
    tier                INTEGER      NOT NULL DEFAULT 1,
    unit_dollars        REAL         NOT NULL DEFAULT 5.0,
    consecutive_wins    INTEGER      NOT NULL DEFAULT 0,
    consecutive_losses  INTEGER      NOT NULL DEFAULT 0,
    last_result         VARCHAR(1),   -- 'W', 'L', 'P', or NULL
    temperature         REAL         NOT NULL DEFAULT 1.5,  -- Phase 47 temperature scaling
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_unit_sizing_name
    ON agent_unit_sizing (agent_name);

-- Seed all 17 agents at Tier 1 ($5) if not already present
INSERT INTO agent_unit_sizing (agent_name, tier, unit_dollars)
VALUES
    ('EVHunter',             1, 5.0),
    ('UnderMachine',         1, 5.0),
    ('UmpireAgent',          1, 5.0),
    ('F5Agent',              1, 5.0),
    ('FadeAgent',            1, 5.0),
    ('LineValueAgent',       1, 5.0),
    ('BullpenAgent',         1, 5.0),
    ('WeatherAgent',         1, 5.0),
    ('MLEdgeAgent',          1, 5.0),
    ('UnderDogAgent',        1, 5.0),
    ('StackSmithAgent',      1, 5.0),
    ('ChalkBusterAgent',     1, 5.0),
    ('SharpFadeAgent',       1, 5.0),
    ('CorrelatedParlayAgent',1, 5.0),
    ('PropCycleAgent',       1, 5.0),
    ('LineupChaseAgent',     1, 5.0),
    ('LineDriftAgent',       1, 5.0)
ON CONFLICT (agent_name) DO NOTHING;

-- ── draftedge_cache ──────────────────────────────────────────────────────────
-- Survives Railway container restarts (/tmp is wiped; Postgres is not)
CREATE TABLE IF NOT EXISTS draftedge_cache (
    id         SERIAL PRIMARY KEY,
    kind       VARCHAR(20)  NOT NULL,   -- 'batters' or 'pitchers'
    cache_date DATE         NOT NULL,
    data       TEXT         NOT NULL,   -- JSON array of player rows
    cached_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE(kind, cache_date)
);

CREATE INDEX IF NOT EXISTS idx_draftedge_cache_kind_date
    ON draftedge_cache (kind, cache_date);
