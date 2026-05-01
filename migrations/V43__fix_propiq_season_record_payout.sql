-- V43: Fix inflated ROI in propiq_season_record
-- Root cause: GradingTasklet stored payout = stake + profit (total return)
-- instead of payout = profit (net change). For LOSS rows this meant
-- payout = stake + (-stake) = 0 instead of -stake, so losses contributed
-- ZERO to the ROI denominator, making ROI look 5x too high.
--
-- Fix:
--   WIN rows:  payout was (stake + profit) → subtract stake to get profit
--   LOSS rows: payout was 0               → set to -stake (actual loss)
--   PUSH rows: payout was 0               → correct, leave as is
--
-- After this migration, get_overall_season_stats() ROI will reflect
-- actual net profit / total staked.

BEGIN;

-- Fix WIN rows: payout was stake+profit, we want just profit
UPDATE propiq_season_record
SET    payout = payout - stake
WHERE  status = 'WIN'
  AND  payout > 0
  AND  payout > stake;  -- guard: only rows where payout > stake (i.e., stake was included)

-- Fix LOSS rows: payout was 0, should be -stake
UPDATE propiq_season_record
SET    payout = -stake
WHERE  status = 'LOSS'
  AND  payout = 0;

-- Log counts for Railway startup output
DO $$
DECLARE
  v_wins  INT;
  v_losses INT;
BEGIN
  SELECT COUNT(*) INTO v_wins  FROM propiq_season_record WHERE status = 'WIN';
  SELECT COUNT(*) INTO v_losses FROM propiq_season_record WHERE status = 'LOSS';
  RAISE NOTICE '[V43] Recalculated payout for % WIN rows and % LOSS rows', v_wins, v_losses;
END $$;

INSERT INTO migration_history (filename, applied_at) VALUES ('V43__fix_propiq_season_record_payout.sql', NOW()) ON CONFLICT (filename) DO NOTHING;

COMMIT;
