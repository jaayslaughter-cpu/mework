-- Phase 100: FanGraphs persistent cache table
-- Survives Railway container restarts (replaces /tmp-only cache)
-- Run this on Railway Postgres before deploying Phase 100.

CREATE TABLE IF NOT EXISTS fg_cache (
    id         SERIAL PRIMARY KEY,
    season     INTEGER      NOT NULL,
    data_type  TEXT         NOT NULL,   -- 'batters' or 'pitchers'
    data       JSONB        NOT NULL,
    cached_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (season, data_type)
);

CREATE INDEX IF NOT EXISTS idx_fg_cache_season ON fg_cache(season);

COMMENT ON TABLE fg_cache IS
    'FanGraphs season stats cache. Avoids re-fetching on Railway container restart. '
    'Populated by fangraphs_layer._pg_save_cache(). '
    'One row per (season, data_type) — upserted on each successful FanGraphs fetch.';
