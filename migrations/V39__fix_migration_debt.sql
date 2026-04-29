-- V39: Fix accumulated migration debt from V26/V32/V33/V37 failures.
-- Safe to run on any DB state — all statements are idempotent.
-- Created: 2026-04-29

-- ── 1. Fix V26: agent_performance view roi_pct ROUND type error ──────────────
-- ROUND(double precision, integer) does not exist in PostgreSQL.
-- Must cast the expression to NUMERIC before passing to ROUND().
CREATE OR REPLACE VIEW agent_performance AS
SELECT
    agent_name,
    COUNT(*)                                                               AS total_bets,
    SUM(CASE WHEN status = 'WIN'  THEN 1 ELSE 0 END)                      AS wins,
    SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END)                      AS losses,
    SUM(CASE WHEN status = 'PUSH' THEN 1 ELSE 0 END)                      AS pushes,
    ROUND(COALESCE(SUM(profit_loss), 0)::NUMERIC, 3)                      AS total_profit,
    ROUND(
        (
            COALESCE(SUM(profit_loss), 0) /
            NULLIF(SUM(COALESCE(units_wagered, kelly_units, 1)), 0) * 100
        )::NUMERIC
    , 2)                                                                   AS roi_pct,
    ROUND(AVG(COALESCE(clv, 0))::NUMERIC, 3)                              AS avg_clv,
    MAX(bet_date)                                                          AS last_bet_date
FROM bet_ledger
WHERE status IN ('WIN', 'LOSS', 'PUSH')
  AND graded_at >= NOW() - INTERVAL '14 days'
GROUP BY agent_name
ORDER BY roi_pct DESC;

-- ── 2. Fix V32/V33: agent_unit_sizing column name chaos ─────────────────────
-- V29 created the table with unit_dollars + consecutive_wins/losses/last_result/temperature.
-- V32 re-created it with stake + wins/losses (wrong names, missing columns).
-- V33 re-created again with stake + roi_7d (yet another schema).
-- V35 added unit_dollars back as FLOAT but stake may still be the primary column.
-- This migration normalises to the V29 schema (unit_dollars as the source of truth)
-- regardless of which version the DB ended up at.

-- Add unit_dollars if it doesn't exist (covers DBs that only got V32/V33)
ALTER TABLE agent_unit_sizing
    ADD COLUMN IF NOT EXISTS unit_dollars FLOAT NOT NULL DEFAULT 5.0;

-- Copy stake → unit_dollars where unit_dollars is still at default and stake has data
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'agent_unit_sizing' AND column_name = 'stake'
    ) THEN
        UPDATE agent_unit_sizing
        SET unit_dollars = COALESCE(stake, 5.0)
        WHERE unit_dollars = 5.0 AND stake IS NOT NULL AND stake != 5.0;
    END IF;
END;
$$;

-- Add missing columns from the original V29 schema (all idempotent)
ALTER TABLE agent_unit_sizing
    ADD COLUMN IF NOT EXISTS consecutive_wins   INTEGER     NOT NULL DEFAULT 0;
ALTER TABLE agent_unit_sizing
    ADD COLUMN IF NOT EXISTS consecutive_losses INTEGER     NOT NULL DEFAULT 0;
ALTER TABLE agent_unit_sizing
    ADD COLUMN IF NOT EXISTS last_result        VARCHAR(1);
ALTER TABLE agent_unit_sizing
    ADD COLUMN IF NOT EXISTS temperature        REAL        NOT NULL DEFAULT 1.5;
ALTER TABLE agent_unit_sizing
    ADD COLUMN IF NOT EXISTS updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Add wins/losses columns (used by V32 version of the table) as aliases
-- so both old and new Python code work without changes
ALTER TABLE agent_unit_sizing
    ADD COLUMN IF NOT EXISTS wins   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE agent_unit_sizing
    ADD COLUMN IF NOT EXISTS losses INTEGER NOT NULL DEFAULT 0;

-- Sync wins/losses ↔ consecutive_wins/consecutive_losses
UPDATE agent_unit_sizing
SET wins   = GREATEST(wins,   consecutive_wins),
    losses = GREATEST(losses, consecutive_losses)
WHERE wins = 0 AND losses = 0 AND (consecutive_wins > 0 OR consecutive_losses > 0);

UPDATE agent_unit_sizing
SET consecutive_wins   = GREATEST(consecutive_wins,   wins),
    consecutive_losses = GREATEST(consecutive_losses, losses)
WHERE consecutive_wins = 0 AND consecutive_losses = 0 AND (wins > 0 OR losses > 0);

-- ── 3. Fix V37: seed_progress_monitor view column rename ─────────────────────
-- V36 created the view with column "type" (alias of player_type).
-- V37 tried to CREATE OR REPLACE VIEW adding "player_type" as a new column name —
-- PostgreSQL rejects this because you cannot rename an existing view column via
-- CREATE OR REPLACE. Drop and recreate instead.
DROP VIEW IF EXISTS seed_progress_monitor;

CREATE VIEW seed_progress_monitor AS
SELECT
    season,
    player_type,
    player_type                                                      AS type,
    player_type                                                      AS game_type,
    COUNT(*) FILTER (WHERE done = TRUE)                             AS done_players,
    COUNT(*) FILTER (WHERE done = TRUE)                             AS players_done,
    COUNT(*)                                                         AS total_players,
    COUNT(*)                                                         AS players_total,
    COALESCE(SUM(inserted) FILTER (WHERE done = TRUE), 0)          AS rows_inserted,
    MAX(processed_at) FILTER (WHERE done = TRUE)                    AS completed_at,
    MAX(processed_at)                                                AS last_activity
FROM seed_progress
GROUP BY season, player_type
ORDER BY season, player_type;
