-- V50: Fix V48 migration tracking + guarantee strikeouts/pa columns exist.
--
-- Root cause chain:
--   1. Original V48 INSERT omitted `filename` → null constraint violation
--   2. V48 never recorded in migration_history (filename=NULL, version='V48')
--   3. Runner checks WHERE filename='V48__...' → NULL row doesn't match → re-runs V48 every startup
--   4. Re-run: ON CONFLICT (version) DO NOTHING → INSERT skipped, no error
--   5. Runner's own INSERT (filename only) should add a correct row...
--   6. BUT if it somehow doesn't → V49 (strikeouts/pa columns) never reaches execution
--
-- Fix: belt-and-suspenders
--   a. Update the null-filename V48 row directly
--   b. Add strikeouts/pa/batter strikeouts IF NOT EXISTS regardless of V49 state
--   c. Record V50 so this never re-runs

BEGIN;

-- ── 1. Fix the existing V48 null-filename row ─────────────────────────────────
-- If V48 row has filename=NULL, set it to the correct filename so the runner
-- will find it on the next startup check and never re-run V48 again.
UPDATE migration_history
SET filename = 'V48__pitch_whiff_live_tables.sql'
WHERE version = 'V48'
  AND (filename IS NULL OR filename = '');

-- Also insert V48 record if it's missing entirely (belt-and-suspenders)
INSERT INTO migration_history (filename, version, description, applied_at)
VALUES ('V48__pitch_whiff_live_tables.sql', 'V48', 'pitch_whiff_live_tables', NOW())
ON CONFLICT (filename) DO NOTHING;

-- ── 2. Ensure pitch_whiff_live has the columns V49 was supposed to add ────────
-- V49 may not have run yet because V48 was blocking it. Add all three columns
-- with IF NOT EXISTS so this is safe regardless of whether V49 already ran.
ALTER TABLE pitch_whiff_live
    ADD COLUMN IF NOT EXISTS strikeouts INTEGER NOT NULL DEFAULT 0;

ALTER TABLE pitch_whiff_live
    ADD COLUMN IF NOT EXISTS pa INTEGER NOT NULL DEFAULT 0;

ALTER TABLE batter_pitch_whiff_live
    ADD COLUMN IF NOT EXISTS strikeouts INTEGER NOT NULL DEFAULT 0;

-- ── 3. Also ensure V49 is recorded (so it doesn't re-run after V50) ──────────
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
