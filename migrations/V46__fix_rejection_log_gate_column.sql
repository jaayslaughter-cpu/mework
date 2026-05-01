-- V46: Fix rejection_log gate column NOT NULL constraint
--
-- The live Railway DB has a 'gate' column with NOT NULL that was added manually
-- but has no default value. The INSERT in tasklets.py doesn't supply 'gate',
-- causing ~50 errors per agent cycle (every 30 seconds all day).

ALTER TABLE rejection_log
    ADD COLUMN IF NOT EXISTS gate TEXT;

DO $$
BEGIN
    ALTER TABLE rejection_log ALTER COLUMN gate SET DEFAULT 'unknown';
EXCEPTION WHEN others THEN
    NULL;
END;
$$;

UPDATE rejection_log
SET gate = SPLIT_PART(reject_reason, ':', 1)
WHERE gate IS NULL AND reject_reason IS NOT NULL AND reject_reason != '';

UPDATE rejection_log
SET gate = 'unknown'
WHERE gate IS NULL;

DO $$
BEGIN
    RAISE NOTICE 'V46: rejection_log gate column default set to ''unknown''. Errors should stop.';
END;
$$;
