-- V50: Fix V48 migration tracking + guarantee strikeouts/pa columns exist.
--
-- Root cause chain:
--   1. Original V48 INSERT omitted `filename` → null constraint violation
--   2. V48 never recorded in migration_history (filename=NULL, version='V48')
--   3. Runner checks WHERE filename='V48__...' → NULL row doesn't match → re-runs V48 every startup
--   4. V48 re-run: ON CONFLICT (version) DO NOTHING → INSERT skipped, no error
--   5. Runner's own INSERT (filename only) adds correct row BUT V49 ordering still uncertain
--   6. Belt-and-suspenders: this migration guarantees the columns exist regardless

BEGIN;

-- ── 1. Fix the existing V48 null-filename row ─────────────────────────────────
UPDATE migration_history
SET filename = 'V48__pitch_whiff_live_tables.sql'
WHERE version = 'V48'
  AND (filename IS NULL OR filename = '');

INSERT INTO migration_history (filename, version, description, applied_at)
VALUES ('V48__pitch_whiff_live_tables.sql', 'V48', 'pitch_whiff_live_tables', NOW())
ON CONFLICT (filename) DO NOTHING;

-- ── 2. Guarantee pitch_whiff_live has the columns V49 was supposed to add ─────
ALTER TABLE pitch_whiff_live
    ADD COLUMN IF NOT EXISTS strikeouts INTEGER NOT NULL DEFAULT 0;

ALTER TABLE pitch_whiff_live
    ADD COLUMN IF NOT EXISTS pa INTEGER NOT NULL DEFAULT 0;

ALTER TABLE batter_pitch_whiff_live
    ADD COLUMN IF NOT EXISTS strikeouts INTEGER NOT NULL DEFAULT 0;

-- ── 3. Ensure V49 is recorded so it doesn't double-run ───────────────────────
INSERT INTO migration_history (filename, version, description, applied_at)
VALUES ('V49__fix_pitch_whiff_missing_columns.sql', 'V49',
        'add_missing_strikeouts_pa_columns_to_pitch_whiff_tables', NOW())
ON CONFLICT (filename) DO NOTHING;

-- ── 4. Record V50 completion ──────────────────────────────────────────────────
INSERT INTO migration_history (filename, version, description, applied_at)
VALUES ('V50__fix_v48_migration_tracking.sql', 'V50',
        'fix_v48_null_filename_and_guarantee_pitch_whiff_columns', NOW())
ON CONFLICT (filename) DO NOTHING;

COMMIT;
