-- Phase 99: Add discord_sent flag to bet_ledger
-- Enables crash-safe dedup: on Railway restart, agent preloads which
-- picks were already sent to Discord today so it never double-sends.
-- NOTE: Originally in repo root. Moved to migrations/ in Phase 101.
ALTER TABLE bet_ledger
    ADD COLUMN IF NOT EXISTS discord_sent BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_bet_ledger_discord_sent
    ON bet_ledger (bet_date, discord_sent)
    WHERE discord_sent = FALSE;
