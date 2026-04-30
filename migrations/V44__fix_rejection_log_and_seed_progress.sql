-- V44: Fix two schema issues found in Postgres error logs on 2026-04-29/30.
--
-- Issue 1: rejection_log missing player_name and other columns
-- The table was first created with only (id, agent_name, reject_reason, rejected_at).
-- The INSERT in tasklets.py expects player_name, prop_type, side, line, model_prob,
-- ev_pct, confidence, reject_reason — causing 500+ errors per day.
--
-- Issue 2: seed_progress missing id column
-- Queries like "SELECT ... FROM seed_progress ORDER BY id DESC LIMIT 1" fail because
-- the table uses (season, player_type) as composite key, not a SERIAL id.

-- ── 1. Fix rejection_log ──────────────────────────────────────────────────────
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS player_name  TEXT;
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS prop_type    TEXT;
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS side         TEXT;
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS line         NUMERIC;
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS model_prob   NUMERIC;
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS ev_pct       NUMERIC;
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS confidence   NUMERIC;
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS reject_reason TEXT;
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS reject_date  DATE DEFAULT CURRENT_DATE;
ALTER TABLE rejection_log ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ DEFAULT NOW();

-- ── 2. Fix seed_progress missing id column ────────────────────────────────────
-- Add a surrogate id so ORDER BY id queries work. SERIAL fills in for existing rows.
ALTER TABLE seed_progress ADD COLUMN IF NOT EXISTS id SERIAL;

-- Create an index to support ORDER BY id DESC queries
CREATE INDEX IF NOT EXISTS idx_seed_progress_id ON seed_progress (id);

DO $$
BEGIN
  RAISE NOTICE 'V44: rejection_log columns added, seed_progress id column added.';
END;
$$;
