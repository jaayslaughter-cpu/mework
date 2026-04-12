-- V34: Backfill result column from status for all rows graded before this fix.
-- run_grading_tasklet() was setting status but not result — XGBoost had 0 training rows.
UPDATE bet_ledger
SET result = status
WHERE result IS NULL
  AND status IN ('WIN', 'LOSS', 'PUSH');
