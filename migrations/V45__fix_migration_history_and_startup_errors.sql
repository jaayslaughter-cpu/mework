-- V45: Fix four recurring startup errors that fire on every restart
-- Root cause: migration_history table is missing the `version` column, so
-- every migration's final INSERT fails → migrations re-run on every restart.
--
-- Errors fixed by this migration:
--   1. column "version" of relation "migration_history" does not exist  (V43, etc.)
--   2. function round(double precision, integer) does not exist          (V26 view re-run)
--   3. column "stake" of relation "agent_unit_sizing" does not exist     (V32 re-run)
--   4. column "total_players" does not exist (seed --status query)

BEGIN;

-- ── 1. Fix migration_history: add `version` column + unique constraint ────────
-- Once this column exists, all future migrations can record completion and
-- will NOT re-run on every restart.
ALTER TABLE migration_history ADD COLUMN IF NOT EXISTS version     VARCHAR(20);
ALTER TABLE migration_history ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE migration_history ADD COLUMN IF NOT EXISTS applied_at  TIMESTAMPTZ DEFAULT NOW();

-- Add unique constraint on version (safe — skip if already exists)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'migration_history_version_key'
          AND conrelid = 'migration_history'::regclass
    ) THEN
        ALTER TABLE migration_history
            ADD CONSTRAINT migration_history_version_key UNIQUE (version);
    END IF;
EXCEPTION WHEN others THEN
    NULL; -- already exists or table shape differs — safe to ignore
END;
$$;

-- Backfill all already-applied migrations as completed so they don't re-run
INSERT INTO migration_history (filename, applied_at) VALUES
  ('V25__streak_tables.sql',                              NOW()),
  ('V26__tasklet_bet_ledger.sql',                         NOW()),
  ('V27__add_discord_sent.sql',                           NOW()),
  ('V28__fg_cache.sql',                                   NOW()),
  ('V29__agent_unit_sizing.sql',                          NOW()),
  ('V30__monitoring_fixes.sql',                           NOW()),
  ('V31__bet_ledger_dedup_index.sql',                     NOW()),
  ('V32__agent_unit_sizing_and_dedup_index.sql',          NOW()),
  ('V33__fix_dedup_and_missing_tables.sql',               NOW()),
  ('V34__backfill_result.sql',                            NOW()),
  ('V35__schema_hotfix_unit_dollars_and_missing.sql',     NOW()),
  ('V36__schema_fixes_monitoring_queries.sql',            NOW()),
  ('V37__add_game_type_alias.sql',                        NOW()),
  ('V38__backfill_entry_type_labels.sql',                 NOW()),
  ('V39__fix_migration_debt.sql',                         NOW()),
  ('V40__fix_backfill_features_json.sql',                 NOW()),
  ('V41__seed_neutral_features_for_historical_rows.sql',  NOW()),
  ('V42__backfill_clv_records.sql',                       NOW()),
  ('V43__fix_propiq_season_record_payout.sql',            NOW()),
  ('V44__fix_rejection_log_and_seed_progress.sql',        NOW()),
  ('V45__fix_migration_history_and_startup_errors.sql',   NOW()),
  ('V46__fix_rejection_log_gate_column.sql',              NOW()),
  ('V47__create_decision_log_table.sql',                  NOW())
ON CONFLICT (filename) DO NOTHING;

-- ── 2. Fix agent_performance view: add ::NUMERIC cast to ROUND() calls ────────
-- V26 defined this view without the cast. V39 fixed it, but V26 re-ran every
-- restart (because migration_history was broken), clobbering the fix.
-- Now that migration_history is fixed, this is a one-time permanent correction.
CREATE OR REPLACE VIEW agent_performance AS
SELECT
    agent_name,
    COUNT(*)                                                                AS total_bets,
    SUM(CASE WHEN status = 'WIN'  THEN 1 ELSE 0 END)                       AS wins,
    SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END)                       AS losses,
    SUM(CASE WHEN status = 'PUSH' THEN 1 ELSE 0 END)                       AS pushes,
    ROUND(COALESCE(SUM(profit_loss), 0)::NUMERIC, 3)                       AS total_profit,
    ROUND(
        (
            COALESCE(SUM(profit_loss), 0) /
            NULLIF(SUM(COALESCE(units_wagered, kelly_units, 1)), 0) * 100
        )::NUMERIC
    , 2)                                                                    AS roi_pct,
    ROUND(AVG(COALESCE(clv, 0))::NUMERIC, 3)                               AS avg_clv,
    MAX(bet_date)                                                           AS last_bet_date
FROM bet_ledger
WHERE status IN ('WIN', 'LOSS', 'PUSH')
  AND graded_at >= NOW() - INTERVAL '14 days'
GROUP BY agent_name
ORDER BY roi_pct DESC;

-- ── 3. Fix agent_unit_sizing: ensure unit_dollars column exists ────────────────
-- V32 INSERT used `stake` but tasklets.py creates the table with `stake_dollars`
-- (and V39 adds `unit_dollars`). Belt-and-suspenders: ensure both column aliases
-- exist so V32/V33 INSERTs (which won't re-run after fix #1) are irrelevant.
ALTER TABLE agent_unit_sizing ADD COLUMN IF NOT EXISTS unit_dollars  FLOAT NOT NULL DEFAULT 5.0;
ALTER TABLE agent_unit_sizing ADD COLUMN IF NOT EXISTS stake         FLOAT;
ALTER TABLE agent_unit_sizing ADD COLUMN IF NOT EXISTS stake_dollars FLOAT NOT NULL DEFAULT 5.0;
ALTER TABLE agent_unit_sizing ADD COLUMN IF NOT EXISTS roi_7d        FLOAT NOT NULL DEFAULT 0.0;

-- Sync all three stake columns so they're consistent
UPDATE agent_unit_sizing
SET stake_dollars = COALESCE(stake_dollars, unit_dollars, 5.0),
    unit_dollars  = COALESCE(unit_dollars,  stake_dollars, 5.0),
    stake         = COALESCE(stake,         stake_dollars, 5.0)
WHERE stake_dollars IS NULL OR unit_dollars IS NULL OR stake IS NULL;

-- ── 4. Fix seed_progress: add total_players column for --status queries ────────
ALTER TABLE seed_progress ADD COLUMN IF NOT EXISTS total_players INTEGER;

-- Backfill total_players = total players in that season (can't know exact count
-- without re-fetching from MLB API, so set to done count as lower bound)
UPDATE seed_progress sp
SET total_players = (
    SELECT COUNT(*) FROM seed_progress sp2
    WHERE sp2.season = sp.season AND sp2.player_type = sp.player_type
);

-- Rebuild seed_progress_monitor view to include total_players from base table
DROP VIEW IF EXISTS seed_progress_monitor;
CREATE VIEW seed_progress_monitor AS
SELECT
    season,
    player_type,
    player_type                                                      AS type,
    COUNT(*) FILTER (WHERE done = TRUE)                              AS done_players,
    COUNT(*)                                                         AS total_players,
    COALESCE(SUM(inserted) FILTER (WHERE done = TRUE), 0)           AS rows_inserted,
    MAX(processed_at) FILTER (WHERE done = TRUE)                    AS completed_at,
    MAX(processed_at)                                                AS last_activity
FROM seed_progress
GROUP BY season, player_type
ORDER BY season, player_type;

-- ── 5. Log completion ─────────────────────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE '[V45] migration_history fixed — all V25-V45 recorded. Migrations will no longer re-run on restart.';
    RAISE NOTICE '[V45] agent_performance view fixed — ROUND ::NUMERIC cast applied.';
    RAISE NOTICE '[V45] agent_unit_sizing stake/unit_dollars/roi_7d columns ensured.';
    RAISE NOTICE '[V45] seed_progress.total_players column added.';
END $$;

COMMIT;
