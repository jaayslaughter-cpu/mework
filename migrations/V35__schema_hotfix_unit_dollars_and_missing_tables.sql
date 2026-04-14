-- V35: Schema hotfix
-- Fix 1: agent_unit_sizing has column 'stake' but code queries 'unit_dollars'
-- Fix 2: Create 5 tables that are referenced in code but were never migrated

-- ── Fix unit_dollars column ──────────────────────────────────────────────────
-- V33 created agent_unit_sizing with 'stake' instead of 'unit_dollars'.
-- Add unit_dollars and populate from stake (or default 5.0).
ALTER TABLE agent_unit_sizing
    ADD COLUMN IF NOT EXISTS unit_dollars FLOAT NOT NULL DEFAULT 5.0;

-- Copy existing stake values into unit_dollars (if stake column exists)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'agent_unit_sizing' AND column_name = 'stake'
    ) THEN
        UPDATE agent_unit_sizing
        SET unit_dollars = COALESCE(stake, 5.0)
        WHERE unit_dollars = 5.0;
    END IF;
END;
$$;

-- ── agent_freeze_log ─────────────────────────────────────────────────────────
-- Required by PR #309 freeze/unfreeze system (get_frozen_agents() query)
CREATE TABLE IF NOT EXISTS agent_freeze_log (
    id              SERIAL PRIMARY KEY,
    agent_name      TEXT NOT NULL,
    freeze_date     DATE NOT NULL,
    unfreeze_date   DATE,
    reason          TEXT,
    consecutive_neg_roi INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_freeze_log_agent
    ON agent_freeze_log (agent_name);

CREATE INDEX IF NOT EXISTS idx_agent_freeze_log_unfreeze
    ON agent_freeze_log (unfreeze_date);

-- ── agent_diagnostics ────────────────────────────────────────────────────────
-- Required by run_agent_diagnostics() (PR #309 + wired PR #320)
-- Tracks 30-day rolling ROI, win rate, Brier score per agent
CREATE TABLE IF NOT EXISTS agent_diagnostics (
    id                      SERIAL PRIMARY KEY,
    agent_name              TEXT NOT NULL,
    snapshot_date           DATE NOT NULL,
    roi_30d                 FLOAT NOT NULL DEFAULT 0.0,
    win_rate_30d            FLOAT NOT NULL DEFAULT 0.0,
    brier_30d               FLOAT NOT NULL DEFAULT 0.0,
    picks_30d               INTEGER NOT NULL DEFAULT 0,
    consecutive_neg_roi     INTEGER NOT NULL DEFAULT 0,
    is_frozen               BOOLEAN NOT NULL DEFAULT FALSE,
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (agent_name, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_agent_diagnostics_agent_date
    ON agent_diagnostics (agent_name, snapshot_date DESC);

-- ── xgb_feature_importance ───────────────────────────────────────────────────
-- Required by PR #319: XGBoost feature importances saved after each Sunday retrain
CREATE TABLE IF NOT EXISTS xgb_feature_importance (
    id              SERIAL PRIMARY KEY,
    feature_name    TEXT NOT NULL,
    importance_gain FLOAT NOT NULL,
    importance_weight FLOAT,
    importance_cover  FLOAT,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_xgb_feature_importance_date
    ON xgb_feature_importance (recorded_at DESC);

-- ── isotonic_cal_buckets ─────────────────────────────────────────────────────
-- Required by rebuild_isotonic_calibration() (PR #310 + wired PR #320)
CREATE TABLE IF NOT EXISTS isotonic_cal_buckets (
    id              SERIAL PRIMARY KEY,
    bucket_key      TEXT NOT NULL,   -- '{prop_type}_{line_bucket}'
    prop_type       TEXT NOT NULL,
    line_bucket     TEXT NOT NULL,
    brier_score     FLOAT NOT NULL,
    n_samples       INTEGER NOT NULL,
    calibrated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (bucket_key, calibrated_at)
);

-- ── dispatch_date_log ────────────────────────────────────────────────────────
-- Required by _record_dispatch_ran_today() dedup guard
CREATE TABLE IF NOT EXISTS dispatch_date_log (
    id          SERIAL PRIMARY KEY,
    run_date    DATE NOT NULL UNIQUE,
    ran_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── startup_ping_log ─────────────────────────────────────────────────────────
-- Required by send_startup_ping() once-per-day guard (PR #264)
CREATE TABLE IF NOT EXISTS startup_ping_log (
    id          SERIAL PRIMARY KEY,
    ping_date   DATE NOT NULL UNIQUE,
    pinged_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
