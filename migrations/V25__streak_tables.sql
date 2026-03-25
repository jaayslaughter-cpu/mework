-- ============================================================
-- Migration V25: Underdog Fantasy Streaks state tracking
-- Created: 2026-03-22
-- Phase 25 — StreakAgent (19th agent)
-- ============================================================

-- Active / historical streak sessions
-- One row per active streak; new row on each new streak attempt.
CREATE TABLE IF NOT EXISTS streak_state (
    id              SERIAL PRIMARY KEY,
    entry_amount    INTEGER NOT NULL DEFAULT 1,   -- 1, 5, or 10 (dollars)
    current_pick    INTEGER NOT NULL DEFAULT 0,   -- last pick number placed (0 = not started)
    wins_in_row     INTEGER NOT NULL DEFAULT 0,   -- consecutive wins so far
    status          TEXT    NOT NULL DEFAULT 'ACTIVE',
    -- ACTIVE  : streak in progress
    -- WON     : 11/11 completed
    -- LOST    : a pick was wrong (picks reset)
    -- CASHED  : 10-day window expired
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_pick_at    TIMESTAMPTZ,
    notes           TEXT
);

COMMENT ON TABLE streak_state IS
    'Tracks active and historical Underdog Fantasy Streaks sessions for StreakAgent.';

-- Individual pick records within a streak
CREATE TABLE IF NOT EXISTS streak_picks (
    id              SERIAL PRIMARY KEY,
    streak_id       INTEGER NOT NULL REFERENCES streak_state(id) ON DELETE CASCADE,
    pick_number     INTEGER NOT NULL,        -- 1–11
    player_name     TEXT    NOT NULL,
    team            TEXT    NOT NULL DEFAULT '',
    prop_type       TEXT    NOT NULL,        -- normalised: hits, strikeouts, etc.
    line            REAL    NOT NULL,
    direction       TEXT    NOT NULL,        -- 'Over' or 'Under'
    platform        TEXT    NOT NULL DEFAULT 'Underdog',
    confidence      REAL    NOT NULL,        -- StreakAgent score 1.0–10.0
    probability     REAL    NOT NULL,        -- estimated win probability 0.0–1.0
    ev_pct          REAL    NOT NULL,        -- expected value %
    signal_count    INTEGER NOT NULL DEFAULT 0,  -- agents that confirmed the pick
    game_date       TEXT    NOT NULL,        -- YYYY-MM-DD
    status          TEXT    NOT NULL DEFAULT 'PENDING',
    -- PENDING  : not yet settled
    -- WIN      : pick was correct
    -- LOSS     : pick was wrong
    -- PUSH     : exactly at the line
    -- VOID     : player scratched / not enough time
    -- RESCUED  : void rescue used
    picked_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at      TIMESTAMPTZ,
    actual_result   REAL        -- actual stat value from ESPN
);

COMMENT ON TABLE streak_picks IS
    'Individual Streaks pick log for StreakAgent — one row per daily pick.';

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_streak_picks_streak_date
    ON streak_picks (streak_id, game_date);
CREATE INDEX IF NOT EXISTS idx_streak_picks_status
    ON streak_picks (status) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_streak_state_active
    ON streak_state (status) WHERE status = 'ACTIVE';
