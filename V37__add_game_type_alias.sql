-- Migration V37: Add remaining missing columns to seed_progress.
-- Covers all column errors seen in PG logs Apr 24-28:
--   players_done, players_total, status, game_type (external monitor queries)

-- game_type: alias for player_type
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS game_type TEXT
    GENERATED ALWAYS AS (player_type) STORED;

-- players_done: 1 if done, 0 if not
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS players_done INTEGER
    GENERATED ALWAYS AS (CASE WHEN done THEN 1 ELSE 0 END) STORED;

-- players_total: always 1 per row
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS players_total INTEGER
    GENERATED ALWAYS AS (1) STORED;

-- status: 'done' or 'pending'
ALTER TABLE seed_progress
    ADD COLUMN IF NOT EXISTS status TEXT
    GENERATED ALWAYS AS (CASE WHEN done THEN 'done' ELSE 'pending' END) STORED;

-- DROP existing view before recreating (PostgreSQL won't rename existing columns in place)
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
