-- ============================================================
-- PropIQ Analytics: Bets Log, Views & Indexes
-- File: 03_bets_log_views_indexes.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS bets_log (
    bet_id                SERIAL PRIMARY KEY,
    game_id               VARCHAR(20) REFERENCES games(game_id),
    pitcher_id            INT REFERENCES players(player_id),
    projection_id         INT REFERENCES projections(projection_id),
    market_id             INT REFERENCES betting_markets(market_id),
    agent_name            VARCHAR(30) NOT NULL, -- '+ev_hunter', 'under_machine', '3leg_parlay', 'steam_chaser'

    -- ── Execution State ─────────────────────────────────────
    bet_time              TIMESTAMP NOT NULL,
    bet_type              VARCHAR(10) NOT NULL,   -- 'Over' or 'Under'
    line_at_bet           FLOAT NOT NULL,
    odds_at_bet           INT NOT NULL,           -- American odds
    implied_prob_at_bet   FLOAT NOT NULL,         -- De-vigged probability at bet time

    -- ── Model State at Execution ────────────────────────────
    model_prob            FLOAT NOT NULL,
    edge_at_bet           FLOAT NOT NULL,         -- (model_prob - implied_prob_at_bet)
    stake_units           FLOAT NOT NULL,         -- Quarter-Kelly calculated stake

    -- ── Closing Line Value (populated pre-first-pitch) ──────
    closing_line          FLOAT,
    closing_odds          INT,
    closing_implied_prob  FLOAT,
    clv                   FLOAT,    -- closing_implied_prob - implied_prob_at_bet (positive = beat the market)

    -- ── Settlement (populated post-game) ────────────────────
    actual_k              INT,      -- Actual outcome value (strikeouts, hits, etc.)
    result                VARCHAR(10), -- 'Win', 'Loss', 'Push'
    profit_units          FLOAT
);

-- ============================================================
-- EVALUATION VIEWS (Feed directly into Streamlit dashboard)
-- ============================================================

-- ── View A: Probability Calibration ─────────────────────────
-- Bins model predictions to verify a projected 60% bet hits ~60% of the time
CREATE OR REPLACE VIEW vw_calibration_curve AS
SELECT
    ROUND(CAST(model_prob * 20 AS NUMERIC), 0) / 20.0  AS probability_bucket,
    COUNT(*)                                             AS total_bets,
    SUM(CASE WHEN result = 'Win' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0)                            AS actual_win_rate,
    AVG(model_prob)                                      AS avg_predicted_prob
FROM bets_log
WHERE result IN ('Win', 'Loss')
GROUP BY 1
ORDER BY 1;

-- ── View B: CLV & Weekly ROI Health ─────────────────────────
-- Tracks whether early bets consistently beat the closing market
CREATE OR REPLACE VIEW vw_executive_health AS
SELECT
    DATE_TRUNC('week', bet_time)   AS bet_week,
    agent_name,
    COUNT(*)                       AS volume,
    AVG(edge_at_bet)               AS avg_model_edge,
    AVG(clv)                       AS avg_clv,          -- Positive avg CLV = real edge
    SUM(profit_units)              AS total_profit,
    SUM(profit_units) / NULLIF(SUM(stake_units), 0) AS roi
FROM bets_log
WHERE result IS NOT NULL
GROUP BY 1, 2
ORDER BY 1;

-- ── View C: Projection Residuals ────────────────────────────
-- Evaluates raw projection accuracy independent of market pricing
CREATE OR REPLACE VIEW vw_projection_residuals AS
SELECT
    b.game_id,
    p.name                      AS pitcher_name,
    pr.projected_mean_k,
    b.actual_k,
    (b.actual_k - pr.projected_mean_k) AS residual_error,
    b.agent_name,
    b.bet_time
FROM bets_log b
JOIN projections  pr ON b.projection_id = pr.projection_id
JOIN players       p ON b.pitcher_id    = p.player_id
WHERE b.actual_k IS NOT NULL;

-- ── View D: Agent Leaderboard ────────────────────────────────
-- Powers the lowdb/SQLite leaderboard in the dashboard
CREATE OR REPLACE VIEW vw_agent_leaderboard AS
SELECT
    agent_name,
    COUNT(*)                                               AS total_bets,
    SUM(CASE WHEN result = 'Win' THEN 1 ELSE 0 END)       AS wins,
    SUM(CASE WHEN result = 'Loss' THEN 1 ELSE 0 END)      AS losses,
    ROUND(
        SUM(CASE WHEN result = 'Win' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 2
    )                                                       AS win_pct,
    ROUND(SUM(profit_units)::NUMERIC, 3)                   AS total_profit_units,
    ROUND((SUM(profit_units) / NULLIF(SUM(stake_units), 0) * 100)::NUMERIC, 2) AS roi_pct,
    ROUND(AVG(clv)::NUMERIC, 4)                            AS avg_clv
FROM bets_log
WHERE result IN ('Win', 'Loss')
GROUP BY agent_name
ORDER BY roi_pct DESC;

-- ============================================================
-- PERFORMANCE INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_bets_game_pitcher  ON bets_log(game_id, pitcher_id);
CREATE INDEX IF NOT EXISTS idx_bets_time          ON bets_log(bet_time);
CREATE INDEX IF NOT EXISTS idx_bets_agent         ON bets_log(agent_name);
CREATE INDEX IF NOT EXISTS idx_projections_time   ON projections(as_of_timestamp);
CREATE INDEX IF NOT EXISTS idx_live_proj_edge      ON live_projections(edge_pct DESC);
CREATE INDEX IF NOT EXISTS idx_live_proj_updated   ON live_projections(last_updated);
