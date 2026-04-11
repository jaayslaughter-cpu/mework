-- V31: Add UNIQUE INDEX on bet_ledger to make ON CONFLICT DO NOTHING actually fire.
-- Without this index, duplicate rows could be inserted in overlapping agent cycles,
-- causing inflated ROI numbers and double-grading in GradingTasklet.
CREATE UNIQUE INDEX IF NOT EXISTS ux_bet_ledger_dedup
ON bet_ledger (player_name, prop_type, line, side, agent_name, bet_date);
