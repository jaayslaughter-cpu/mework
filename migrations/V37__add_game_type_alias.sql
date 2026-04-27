-- Migration V37: Add game_type column alias to seed_progress
-- The external health-check query (from Railway monitoring or an older client)
-- expects SELECT season, game_type, players_done, total_players FROM seed_progress.
-- The column was renamed to player_type in V26 and aliased as "type" in the V36 view.
-- We add game_type as a generated column alias so both old and new queries work.

ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS game_type TEXT
    GENERATED ALWAYS AS (player_type) STORED;

-- Also add players_done as alias for done count, consumed by same external query
-- Re-create the monitor view to expose both old and new column names
CREATE OR REPLACE VIEW seed_progress_monitor AS
SELECT
    season,
    player_type,
    player_type                                                      AS type,
    player_type                                                      AS game_type,
    COUNT(*) FILTER (WHERE done = TRUE)                             AS done_players,
    COUNT(*) FILTER (WHERE done = TRUE)                             AS players_done,
    COUNT(*)                                                         AS total_players,
    COALESCE(SUM(inserted) FILTER (WHERE done = TRUE), 0)          AS rows_inserted,
    MAX(processed_at) FILTER (WHERE done = TRUE)                    AS completed_at,
    MAX(processed_at)                                                AS last_activity
FROM seed_progress
GROUP BY season, player_type
ORDER BY season, player_type;
