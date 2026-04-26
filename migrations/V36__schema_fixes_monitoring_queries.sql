-- Migration V36: Fix schema mismatches. Already applied live to Railway DB.
-- Safe to re-run.

ALTER TABLE xgb_model_store ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
UPDATE xgb_model_store SET created_at = trained_at WHERE created_at IS NULL AND trained_at IS NOT NULL;

ALTER TABLE bet_ledger ADD COLUMN IF NOT EXISTS reject_reason TEXT;

ALTER TABLE seed_progress ADD COLUMN IF NOT EXISTS rows_inserted INTEGER;
UPDATE seed_progress SET rows_inserted = inserted WHERE rows_inserted IS NULL;
CREATE OR REPLACE FUNCTION sync_seed_progress_rows_inserted() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN NEW.rows_inserted := NEW.inserted; RETURN NEW; END; $$;
DROP TRIGGER IF EXISTS trg_seed_progress_rows_inserted ON seed_progress;
CREATE TRIGGER trg_seed_progress_rows_inserted BEFORE INSERT OR UPDATE ON seed_progress FOR EACH ROW EXECUTE FUNCTION sync_seed_progress_rows_inserted();

ALTER TABLE seed_progress ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
UPDATE seed_progress SET completed_at = processed_at WHERE completed_at IS NULL;
CREATE OR REPLACE FUNCTION sync_seed_progress_completed_at() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN NEW.completed_at := NEW.processed_at; RETURN NEW; END; $$;
DROP TRIGGER IF EXISTS trg_seed_progress_completed_at ON seed_progress;
CREATE TRIGGER trg_seed_progress_completed_at BEFORE INSERT OR UPDATE ON seed_progress FOR EACH ROW EXECUTE FUNCTION sync_seed_progress_completed_at();

ALTER TABLE seed_progress ADD COLUMN IF NOT EXISTS type VARCHAR(10);
UPDATE seed_progress SET type = player_type WHERE type IS NULL;
CREATE OR REPLACE FUNCTION sync_seed_progress_type() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN NEW.type := NEW.player_type; RETURN NEW; END; $$;
DROP TRIGGER IF EXISTS trg_seed_progress_type ON seed_progress;
CREATE TRIGGER trg_seed_progress_type BEFORE INSERT OR UPDATE ON seed_progress FOR EACH ROW EXECUTE FUNCTION sync_seed_progress_type();

CREATE OR REPLACE VIEW seed_progress_monitor AS
SELECT season, player_type AS type,
    COUNT(*) FILTER (WHERE done = TRUE) AS done_players,
    COUNT(*) AS total_players,
    COALESCE(SUM(inserted) FILTER (WHERE done = TRUE), 0) AS rows_inserted,
    MAX(processed_at) FILTER (WHERE done = TRUE) AS completed_at,
    MAX(processed_at) AS last_activity
FROM seed_progress GROUP BY season, player_type ORDER BY season, player_type;

ALTER TABLE bet_ledger ADD COLUMN IF NOT EXISTS discord_sent BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_bet_ledger_xgb_training ON bet_ledger (prop_type, discord_sent, actual_outcome) WHERE discord_sent = TRUE AND actual_outcome IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_seed_progress_season_type_done ON seed_progress (season, player_type, done);
