-- V31: Add unique dedup index on bet_ledger so ON CONFLICT DO NOTHING actually fires
-- PR #287 directive: prevents duplicate rows from multi-replica race conditions

CREATE UNIQUE INDEX IF NOT EXISTS ux_bet_ledger_dedup
    ON bet_ledger (player_name, prop_type, line, side, agent_name, bet_date);
