-- Phase 100: FanGraphs season stats Postgres cache
-- Dual-write target alongside /tmp disk cache.
-- Survives Railway container restarts (/tmp is wiped; Postgres is not).
CREATE TABLE IF NOT EXISTS fg_cache (
    id          SERIAL PRIMARY KEY,
    season      INTEGER      NOT NULL,
    data_type   VARCHAR(20)  NOT NULL,
    data        JSONB        NOT NULL,
    cached_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE(season, data_type)
);

CREATE INDEX IF NOT EXISTS idx_fg_cache_season_type
    ON fg_cache (season, data_type);
