-- V41: Add neutral features_json to historical seed rows that have NULL.
--
-- historical_seed.py previously stored no features_json (NULL) for its rows.
-- The XGBoost training query now handles NULLs with [0.5]*27 defaults in Python,
-- but this migration also ensures lookahead_safe=TRUE is stamped on all seed rows
-- so they pass the lookahead_safe filter without needing a code change.
--
-- Idempotent: only updates rows with NULL features_json from HistoricalSeed agent.

UPDATE bet_ledger
SET
    features_json  = '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.55,0.575,0.5,0.15,0.55,0.5,0.0,0.25,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]',
    lookahead_safe = TRUE
WHERE agent_name = 'HistoricalSeed'
  AND (features_json IS NULL OR features_json LIKE '{"backfilled"%');

DO $$
DECLARE _n INTEGER;
BEGIN
    GET DIAGNOSTICS _n = ROW_COUNT;
    RAISE NOTICE 'V41: Stamped neutral features_json on % HistoricalSeed rows.', _n;
END;
$$;
