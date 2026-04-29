-- V40: Fix placeholder features_json rows from direct_backfill.py.
--
-- direct_backfill.py stored {"backfilled": True, "source": "discord_history"}
-- as features_json. This is a dict, not a positional list, and causes
-- run_xgboost_tasklet() to crash with "unsupported operand type: dict + list"
-- meaning XGBoost has never successfully trained on any backfilled row.
--
-- This migration sets features_json = NULL for all such rows so the grading
-- tasklet will rebuild them with real player signals at next grade time.
-- Rows with NULL features_json are handled gracefully by run_xgboost_tasklet
-- (replaced with neutral [0.5]*27 defaults until grading rebuilds them).
--
-- Run-once, idempotent: rows already NULL are unaffected.

UPDATE bet_ledger
SET features_json = NULL
WHERE features_json LIKE '{"backfilled"%'
  AND features_json IS NOT NULL;

-- Log how many rows were fixed (visible in migration output)
DO $$
DECLARE
    _fixed INTEGER;
BEGIN
    GET DIAGNOSTICS _fixed = ROW_COUNT;
    RAISE NOTICE 'V40: Reset % placeholder features_json rows to NULL for grading tasklet rebuild.', _fixed;
END;
$$;
