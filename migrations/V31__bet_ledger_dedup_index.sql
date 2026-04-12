-- V31: Unique dedup index on bet_ledger
-- ON CONFLICT DO NOTHING in tasklets.py now actually fires
CREATE UNIQUE INDEX IF NOT EXISTS ux_bet_ledger_dedup
    ON bet_ledger (player_name, prop_type, line, side, agent_name, bet_date);
