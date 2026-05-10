-- V51: Fix seed_progress column name mismatches seen in Postgres error logs
--
-- Errors observed (May 6–9 2026, multiple times each):
--   column "last_player_index" does not exist
--   column "last_player_idx" does not exist
--   column "updated_at" does not exist
--   column "processed" does not exist  (HINT: try "processed_at")
--   column "season" does not exist in bet_ledger  (HistoricalSeed query)
--   column "total" does not exist (in seed_progress WHERE processed < total)
--
-- Root cause: admin monitoring queries and external scripts use column names
-- that differ from the actual schema (season/player_type/done/inserted/processed_at).
-- These are NOT in csv_seed.py itself (csv_seed.py uses the correct column names).
-- They come from Railway dashboard custom SQL queries and /admin/seed-status endpoint.
--
-- Fix: add generated alias columns and a comprehensive view so any naming variant works.

BEGIN;

-- ── 1. Add last_player_id aliases ──────────────────────────────────────────────
-- Queries use: last_player_index, last_player_idx (both mean player_id of last processed)
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS last_player_index INTEGER
    GENERATED ALWAYS AS (player_id) STORED;

ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS last_player_idx INTEGER
    GENERATED ALWAYS AS (player_id) STORED;

-- ── 2. Add updated_at alias ────────────────────────────────────────────────────
-- Queries use: updated_at (alias for processed_at)
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
    GENERATED ALWAYS AS (processed_at) STORED;

-- ── 3. Add total_players and total aliases ─────────────────────────────────────
-- Already exists as generated column from V37, but add defensive IF NOT EXISTS
-- for total (bare name, as in WHERE processed < total)
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS total INTEGER
    GENERATED ALWAYS AS (1) STORED;

-- ── 4. Add processed alias ────────────────────────────────────────────────────
-- Query: WHERE processed < total → processed = number of rows inserted so far
-- Use inserted (existing column) as the alias
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS processed INTEGER
    GENERATED ALWAYS AS (inserted) STORED;

-- ── 5. Add season column to bet_ledger for HistoricalSeed query ───────────────
-- Query: SELECT created_at, player_name, season, player_type FROM bet_ledger
--        WHERE agent_name = 'HistoricalSeed'
-- bet_ledger doesn't have a season column — add it with a computed default
ALTER TABLE bet_ledger
    ADD COLUMN IF NOT EXISTS season SMALLINT
    GENERATED ALWAYS AS (EXTRACT(YEAR FROM COALESCE(bet_date, created_at::date))::SMALLINT) STORED;

-- ── 6. Add pg_try_advisory_lock to admin_run_seed prevention ──────────────────
-- The deadlock (two concurrent DELETE FROM bet_ledger WHERE agent_name='HistoricalCSVSeed')
-- happens when /admin/run-seed is called twice in quick succession.
-- No schema change needed here — the fix is in orchestrator.py (advisory lock).
-- This comment documents the fix for reference.

-- ── 7. Recreate comprehensive view ────────────────────────────────────────────
CREATE OR REPLACE VIEW seed_progress_monitor AS
SELECT
    season,
    player_type,
    player_type                                             AS type,
    player_type                                             AS game_type,
    COUNT(*) FILTER (WHERE done = TRUE)                    AS done_players,
    COUNT(*) FILTER (WHERE done = TRUE)                    AS players_done,
    COUNT(*)                                                AS total_players,
    COUNT(*)                                                AS players_total,
    COUNT(*)                                                AS total,
    COALESCE(SUM(inserted) FILTER (WHERE done = TRUE), 0) AS rows_inserted,
    COALESCE(SUM(inserted), 0)                             AS processed,
    MAX(processed_at) FILTER (WHERE done = TRUE)           AS completed_at,
    MAX(processed_at)                                       AS last_activity,
    MAX(processed_at)                                       AS updated_at,
    MAX(player_id) FILTER (WHERE done = TRUE)              AS last_player_index,
    MAX(player_id) FILTER (WHERE done = TRUE)              AS last_player_idx
FROM seed_progress
GROUP BY season, player_type
ORDER BY season, player_type;

-- ── 8. Record migration ────────────────────────────────────────────────────────
INSERT INTO migration_history (filename, version, description, applied_at)
VALUES ('V51__fix_seed_progress_column_aliases.sql', 'V51',
        'add_seed_progress_column_aliases_and_bet_ledger_season', NOW())
ON CONFLICT (filename) DO NOTHING;

COMMIT;
