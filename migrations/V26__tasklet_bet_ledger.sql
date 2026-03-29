-- ============================================================
-- Migration V26: Tasklet-compatible bet_ledger + season record
-- Created: 2026-03-29
-- 
-- Adds columns that tasklets.py INSERTs/SELECTs/UPDATEs.
-- Non-destructive: uses IF NOT EXISTS and ADD COLUMN IF NOT EXISTS.
-- Run on Railway Postgres before deploying updated tasklets.py.
-- ============================================================

-- ── Primary bet ledger (AgentTasklet writes here every 30s) ──────────────────
CREATE TABLE IF NOT EXISTS bet_ledger (
    id              SERIAL PRIMARY KEY,
    player_name     VARCHAR(150)  NOT NULL,
    prop_type       VARCHAR(50)   NOT NULL,
    line            DECIMAL(6,2)  NOT NULL,
    side            VARCHAR(10)   NOT NULL,          -- OVER / UNDER
    odds_american   INTEGER,
    kelly_units     DECIMAL(8,4),
    model_prob      DECIMAL(6,2),                    -- 0–100
    ev_pct          DECIMAL(8,2),
    agent_name      VARCHAR(60)   NOT NULL,
    status          VARCHAR(20)   NOT NULL DEFAULT 'OPEN',  -- OPEN WIN LOSS PUSH
    bet_date        DATE          NOT NULL DEFAULT CURRENT_DATE,
    platform        VARCHAR(30)   DEFAULT 'underdog',
    features_json   TEXT,                            -- JSON float array for XGBoost retraining
    actual_result   DECIMAL(6,2),                    -- actual stat value post-game
    profit_loss     DECIMAL(8,4),                    -- units profit/loss
    clv             DECIMAL(6,2),                    -- closing line value
    graded_at       TIMESTAMPTZ,
    actual_outcome  SMALLINT,                            -- 1=WIN 0=LOSS NULL=ungraded/PUSH
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- units_wagered column: leaderboard reads this, computed from kelly_units
-- Add as regular column (not generated) for broader Postgres compatibility
ALTER TABLE bet_ledger ADD COLUMN IF NOT EXISTS units_wagered DECIMAL(8,4);

-- Backfill units_wagered from kelly_units for any existing rows
UPDATE bet_ledger SET units_wagered = ABS(kelly_units)
WHERE units_wagered IS NULL AND kelly_units IS NOT NULL;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_bet_ledger_status_date  ON bet_ledger (status, bet_date);
CREATE INDEX IF NOT EXISTS idx_bet_ledger_agent        ON bet_ledger (agent_name);
CREATE INDEX IF NOT EXISTS idx_bet_ledger_graded_at    ON bet_ledger (graded_at) WHERE graded_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_bet_ledger_player       ON bet_ledger (player_name, prop_type);

-- ── Season record table (LiveDispatcher + nightly_recap use this) ─────────────
-- Schema matches season_record.py _ensure_table() exactly
CREATE TABLE IF NOT EXISTS propiq_season_record (
    id          SERIAL PRIMARY KEY,
    date        TEXT NOT NULL,
    agent_name  TEXT NOT NULL,
    parlay_legs INTEGER NOT NULL,
    platform    TEXT NOT NULL DEFAULT 'Mixed',
    stake       NUMERIC(8,2) NOT NULL DEFAULT 5.00,
    payout      NUMERIC(8,2) NOT NULL DEFAULT 0.00,
    confidence  NUMERIC(5,2) NOT NULL DEFAULT 0.00,
    status      TEXT NOT NULL DEFAULT 'PENDING',     -- PENDING WIN LOSS PUSH
    legs_json   TEXT,                                -- JSON of leg dicts
    created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_season_record_status  ON propiq_season_record (status);
CREATE INDEX IF NOT EXISTS idx_season_record_date    ON propiq_season_record (date);
CREATE INDEX IF NOT EXISTS idx_season_record_agent   ON propiq_season_record (agent_name);

-- ── Unified performance view (leaderboard reads this) ────────────────────────
-- Reads from bet_ledger (AgentTasklet bets, 14-day window)
CREATE OR REPLACE VIEW agent_performance AS
SELECT
    agent_name,
    COUNT(*)                                                               AS total_bets,
    SUM(CASE WHEN status = 'WIN'  THEN 1 ELSE 0 END)                      AS wins,
    SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END)                      AS losses,
    SUM(CASE WHEN status = 'PUSH' THEN 1 ELSE 0 END)                      AS pushes,
    ROUND(COALESCE(SUM(profit_loss), 0)::NUMERIC, 3)                      AS total_profit,
    ROUND(
        COALESCE(SUM(profit_loss), 0) /
        NULLIF(SUM(COALESCE(units_wagered, kelly_units, 1)), 0) * 100
    , 2)                                                                   AS roi_pct,
    ROUND(AVG(COALESCE(clv, 0))::NUMERIC, 3)                              AS avg_clv,
    MAX(bet_date)                                                          AS last_bet_date
FROM bet_ledger
WHERE status IN ('WIN', 'LOSS', 'PUSH')
  AND graded_at >= NOW() - INTERVAL '14 days'
GROUP BY agent_name
ORDER BY roi_pct DESC;

-- ── Streak tables (from V25 migration — idempotent) ───────────────────────────
CREATE TABLE IF NOT EXISTS streak_state (
    id              SERIAL PRIMARY KEY,
    entry_amount    INTEGER NOT NULL DEFAULT 1,
    current_pick    INTEGER NOT NULL DEFAULT 0,
    wins_in_row     INTEGER NOT NULL DEFAULT 0,
    status          TEXT    NOT NULL DEFAULT 'ACTIVE',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_pick_at    TIMESTAMPTZ,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS streak_picks (
    id              SERIAL PRIMARY KEY,
    streak_id       INTEGER NOT NULL REFERENCES streak_state(id) ON DELETE CASCADE,
    pick_number     INTEGER NOT NULL,
    player_name     TEXT NOT NULL,
    prop_type       TEXT NOT NULL,
    line            NUMERIC(6,2) NOT NULL,
    direction       TEXT NOT NULL,
    platform        TEXT NOT NULL DEFAULT 'underdog',
    result          TEXT,
    placed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at      TIMESTAMPTZ
);
