-- V49: Add missing columns to pitch_whiff_live and batter_pitch_whiff_live.
--
-- Root cause: V48 DDL created both tables without strikeouts/pa columns,
-- but pitch_whiff_refresh.py SELECTs and INSERTs those columns every night
-- at 3:30 AM PT → crash: "column strikeouts does not exist".
--
-- Fix: add the three missing columns with safe DEFAULT 0 so existing rows
-- and future upserts both work without data loss.

BEGIN;

-- pitch_whiff_live: add strikeouts count + pa (plate appearances vs this pitch)
ALTER TABLE pitch_whiff_live
    ADD COLUMN IF NOT EXISTS strikeouts INTEGER NOT NULL DEFAULT 0;

ALTER TABLE pitch_whiff_live
    ADD COLUMN IF NOT EXISTS pa INTEGER NOT NULL DEFAULT 0;

-- batter_pitch_whiff_live: add strikeouts count
ALTER TABLE batter_pitch_whiff_live
    ADD COLUMN IF NOT EXISTS strikeouts INTEGER NOT NULL DEFAULT 0;

INSERT INTO migration_history (filename, version, description, applied_at)
VALUES ('V49__fix_pitch_whiff_missing_columns.sql', 'V49',
        'add_missing_strikeouts_pa_columns_to_pitch_whiff_tables', NOW())
ON CONFLICT (version) DO NOTHING;

COMMIT;
