-- V47: Create decision_log table for decision_logger.py
-- decision_logger.py writes every prop evaluation (hit or rejected) to this table.
-- replay_tool.py reads from it to reconstruct any day's decisions.
-- Without this table, every flush_buffer() call fails silently.

CREATE TABLE IF NOT EXISTS decision_log (
    id              SERIAL PRIMARY KEY,
    agent_name      TEXT            NOT NULL,
    player_name     TEXT            NOT NULL,
    prop_type       TEXT            NOT NULL,
    direction       TEXT,                           -- OVER / UNDER
    line            NUMERIC(6,2),
    platform        TEXT,
    prob_base       NUMERIC(6,4),                   -- base_rate_model probability
    prob_draftedge  NUMERIC(6,4),                   -- DraftEdge k_pct / hit_pct
    prob_statcast   NUMERIC(6,4),                   -- Statcast whiff / hard-hit
    prob_sbd        NUMERIC(6,4),                   -- sharp book reference
    prob_form       NUMERIC(6,4),                   -- form/rolling adjustment
    prob_fangraphs  NUMERIC(6,4),                   -- FanGraphs xFIP / wRC+
    prob_final      NUMERIC(6,4),                   -- final blended probability
    edge_pct        NUMERIC(8,4),                   -- EV edge (ratio)
    decision        TEXT,                           -- HIT / REJECTED
    reject_reason   TEXT,                           -- ev_low / eval_none / no_sharp / vig
    evaluated_at    TIMESTAMPTZ     DEFAULT NOW(),
    bet_date        DATE            DEFAULT CURRENT_DATE
);

CREATE INDEX IF NOT EXISTS idx_decision_log_date    ON decision_log (bet_date);
CREATE INDEX IF NOT EXISTS idx_decision_log_agent   ON decision_log (agent_name, bet_date);
CREATE INDEX IF NOT EXISTS idx_decision_log_player  ON decision_log (player_name, bet_date);

DO $$
BEGIN
    RAISE NOTICE 'V47: decision_log table created. replay_tool.py and decision_logger.py will now work.';
END;
$$;
