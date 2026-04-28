-- Migration V37: Add remaining missing columns to seed_progress.
-- Covers all column errors seen in PG logs Apr 24-28:
--   players_done, players_total, status, game_type, players_done (external monitor queries)
-- Also fixes: layer_cache old schema conflict (covers_layer was using wrong INSERT)

-- game_type: alias for player_type (used by external monitoring queries)
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS game_type TEXT
    GENERATED ALWAYS AS (player_type) STORED;

-- players_done: alias for done count (used by external monitoring)
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS players_done INTEGER
    GENERATED ALWAYS AS (CASE WHEN done THEN 1 ELSE 0 END) STORED;

-- players_total: always 1 per row (row = one player); monitoring tools sum this
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS players_total INTEGER
    GENERATED ALWAYS AS (1) STORED;

-- status: 'done' or 'pending' (used by external monitoring queries)
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS status TEXT
    GENERATED ALWAYS AS (CASE WHEN done THEN 'done' ELSE 'pending' END) STORED;

-- Recreate seed_progress_monitor to expose ALL column name variants
-- so both old external queries and new Python code work without changes
CREATE OR REPLACE VIEW seed_progress_monitor AS
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

-- layer_cache: covers_layer was using old schema (key, value, created_at).
-- layer_cache_helper uses correct schema (layer_name, cache_key, cache_date, data).
-- covers_layer.py has been fixed to use layer_cache_helper.pg_cache_set().
-- No schema change needed here — the correct table is created by layer_cache_helper
-- on first call. This comment documents the fix for reference.
