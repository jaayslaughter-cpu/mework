-- V53: Add layer_audit JSONB column to bet_ledger
--
-- Stores a per-prop record of exactly which model layers fired and what
-- they contributed. Enables the daily layer coverage report in bug_checker.py
-- and makes silent failures detectable from real production data.
--
-- Schema: layer_audit JSONB, e.g.:
-- {
--   "bayesian": 0.023,        -- _bayesian_nudge value
--   "cv": 0.0,                -- _cv_nudge (0 = layer returned nothing)
--   "form": -0.012,           -- _form_adj
--   "chase": 0.008,           -- _chase_k_adj
--   "drama": -3.0,            -- _drama_penalty_pp
--   "arsenal": 0.015,         -- _arsenal_k_sig
--   "umpire": 0.011,          -- _ump_k_adj
--   "steamer": 0.004,         -- _steamer_adj
--   "ttop": -0.025,           -- _tto_k_adj
--   "bp2vec": 1.2,            -- _bp2vec_adj (pp, 0 if models not loaded)
--   "pa_model": 0.587,        -- _pa_model_hit_prob (null for non-hit props)
--   "dampener": true,         -- _dampener_applied flag
--   "xgb_k": true,            -- XGBoost K blend fired
--   "xgb_hit": false,         -- XGBoost hit blend fired
--   "market_flag": "CLEAN",   -- _market_flag from market_validator
--   "injury": 0.0,            -- _injury_confidence_penalty
--   "park": 0.95,             -- _park_k_factor
--   "lambda_bias": -0.067     -- adaptive cal lambda_bias applied
-- }
--
-- GIN index for efficient querying by layer presence.

BEGIN;

ALTER TABLE bet_ledger
    ADD COLUMN IF NOT EXISTS layer_audit JSONB;

-- GIN index enables fast queries like:
--   WHERE layer_audit->>'dampener' = 'true'
--   WHERE (layer_audit->>'xgb_k')::bool = true
CREATE INDEX IF NOT EXISTS idx_bet_ledger_layer_audit
    ON bet_ledger USING GIN (layer_audit)
    WHERE layer_audit IS NOT NULL;

-- Partial index for quick daily layer coverage queries
CREATE INDEX IF NOT EXISTS idx_bet_ledger_layer_audit_date
    ON bet_ledger (bet_date, layer_audit)
    WHERE layer_audit IS NOT NULL;

COMMIT;
