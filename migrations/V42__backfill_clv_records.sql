-- V42: Backfill clv_records from already-graded bet_ledger rows
--
-- Root cause: clv_records has 0 rows because the INSERT in run_grading_tasklet()
-- only fires for rows being graded in the current 2 AM run (status='OPEN').
-- The 163 rows dispatched and graded before this wiring existed were missed.
-- This migration backfills them directly from bet_ledger.
--
-- CLV meaning: model_prob minus the implied probability of the closing odds.
-- When closing odds = pick-time odds (hub:market expired), CLV ≈ ev_pct.
-- Still valuable: reveals which agents/prop_types have consistent positive edge.

INSERT INTO clv_records (
    game_date,
    agent_name,
    player_name,
    prop_type,
    side,
    pick_line,
    closing_line,
    clv_pts,
    beat_close,
    recorded_at
)
SELECT
    bet_date                                           AS game_date,
    agent_name,
    player_name,
    prop_type,
    side,
    line                                               AS pick_line,
    line                                               AS closing_line,   -- best proxy when closing odds unavailable
    COALESCE(clv, 0.0)                                 AS clv_pts,
    CASE WHEN COALESCE(clv, 0) > 0 THEN 1 ELSE 0 END  AS beat_close,
    COALESCE(graded_at, NOW())                         AS recorded_at
FROM bet_ledger
WHERE discord_sent  = TRUE
  AND actual_outcome IS NOT NULL        -- already graded
  AND status        IN ('WIN','LOSS','PUSH')
  -- Skip rows that are already in clv_records (idempotent)
  AND NOT EXISTS (
      SELECT 1
      FROM clv_records cr
      WHERE cr.game_date   = bet_ledger.bet_date
        AND cr.agent_name  = bet_ledger.agent_name
        AND cr.player_name = bet_ledger.player_name
        AND cr.prop_type   = bet_ledger.prop_type
        AND cr.side        = bet_ledger.side
  )
;
