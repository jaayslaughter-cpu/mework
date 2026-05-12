-- V52: Fix V51 view regression — DROP + CREATE instead of CREATE OR REPLACE
--
-- Root cause: V51 used CREATE OR REPLACE VIEW to add a new column (game_type)
-- before the existing done_players column. Postgres disallows changing the
-- ordinal position of existing output columns with CREATE OR REPLACE.
-- Error: "cannot change name of view column 'done_players' to 'game_type'"
--
-- Fix: DROP the view first (CASCADE handles any dependent views/rules),
-- then CREATE fresh with the exact column order V51 intended.
-- Migration retried every startup since May 11 — stops after this commit.

BEGIN;

-- ── 1. Add missing seed_progress alias columns (idempotent) ────────────────
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS last_player_index INTEGER
    GENERATED ALWAYS AS (player_id) STORED;

ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS last_player_idx INTEGER
    GENERATED ALWAYS AS (player_id) STORED;

ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
    GENERATED ALWAYS AS (processed_at) STORED;

ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS total INTEGER
    GENERATED ALWAYS AS (1) STORED;

ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS processed INTEGER
    GENERATED ALWAYS AS (inserted) STORED;

-- ── 2. Add season column to bet_ledger ─────────────────────────────────────
ALTER TABLE bet_ledger
    ADD COLUMN IF NOT EXISTS season SMALLINT
    GENERATED ALWAYS AS (EXTRACT(YEAR FROM COALESCE(bet_date, created_at::date))::SMALLINT) STORED;

-- ── 3. DROP old view (CASCADE) then CREATE with correct column order ────────
-- V51 failed here because CREATE OR REPLACE cannot reorder/rename existing columns.
DROP VIEW IF EXISTS seed_progress_monitor CASCADE;

CREATE VIEW seed_progress_monitor AS
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

-- ── 4. Record migration ─────────────────────────────────────────────────────
INSERT INTO migration_history (filename, version, description, applied_at)
VALUES ('V52__fix_seed_progress_view_regression.sql', 'V52',
        'drop_recreate_seed_progress_monitor_view', NOW())
ON CONFLICT (filename) DO NOTHING;

-- Mark V51 as applied so it stops retrying
INSERT INTO migration_history (filename, version, description, applied_at)
VALUES ('V51__fix_seed_progress_column_aliases.sql', 'V51',
        'add_seed_progress_column_aliases_and_bet_ledger_season', NOW())
ON CONFLICT (filename) DO NOTHING;

COMMIT;
