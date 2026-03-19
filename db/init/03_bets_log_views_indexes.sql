-- ============================================================
-- PropIQ Analytics: Bets Log, Evaluation Views & Performance Indexes
-- File: 03_bets_log_views_indexes.sql
-- ============================================================

-- ── Bets Log (Immutable Audit Trail) ────────────────────────
CREATE TABLE IF NOT EXISTS bets_log (
    bet_id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    game_id                 VARCHAR(50),
    bet_time                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    bet_type                VARCHAR(50) NOT NULL,
    line_at_bet             FLOAT NOT NULL,
    odds_at_bet             INT,
    implied_prob_at_bet     FLOAT,
    model_prob              FLOAT NOT NULL,
    edge_at_bet             FLOAT,
    stake_units             FLOAT NOT NULL,
    closing_line            FLOAT,
    closing_odds            INT,
    closing_implied_prob    FLOAT,
    clv                     FLOAT,
    actual_k                FLOAT,
    result                  VARCHAR(10) CHECK (result IN ('WIN', 'LOSS', 'PUSH', 'PENDING')),
    profit_units            FLOAT,
    agent_id                VARCHAR(30),
    market_ids              TEXT[],
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

-- ── Calibration Curve View ──────────────────────────────────
-- Groups bets by model probability buckets to assess calibration
CREATE OR REPLACE VIEW calibration_curve AS
SELECT
    WIDTH_BUCKET(model_prob, 0, 1, 10) AS prob_bucket,
    ROUND((WIDTH_BUCKET(model_prob, 0, 1, 10) - 0.5) / 10.0, 2) AS bucket_midpoint,
    COUNT(*) AS total_bets,
    SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) AS wins,
    ROUND(AVG(CASE WHEN result = 'WIN' THEN 1.0 ELSE 0.0 END), 4) AS actual_hit_rate,
    ROUND(AVG(model_prob), 4) AS avg_model_prob,
    ROUND(AVG(model_prob) - AVG(CASE WHEN result = 'WIN' THEN 1.0 ELSE 0.0 END), 4) AS calibration_error
FROM bets_log
WHERE result IN ('WIN', 'LOSS')
GROUP BY prob_bucket
ORDER BY prob_bucket;

-- ── Executive Health Dashboard View ─────────────────────────
-- Daily summary for quick health checks
CREATE OR REPLACE VIEW executive_health AS
SELECT
    DATE(bet_time) AS trade_date,
    COUNT(*) AS bets_placed,
    SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) AS losses,
    ROUND(AVG(CASE WHEN result = 'WIN' THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_rate_pct,
    ROUND(SUM(stake_units), 2) AS total_staked,
    ROUND(SUM(profit_units), 2) AS net_profit,
    ROUND(SUM(profit_units) / NULLIF(SUM(stake_units), 0) * 100, 2) AS roi_pct,
    ROUND(AVG(edge_at_bet) * 100, 2) AS avg_edge_pct,
    ROUND(AVG(clv) * 100, 3) AS avg_clv_pct
FROM bets_log
WHERE result IN ('WIN', 'LOSS', 'PUSH')
GROUP BY DATE(bet_time)
ORDER BY trade_date DESC;

-- ── Projection Residuals View ───────────────────────────────
-- Compares model projections to actual outcomes for error analysis
CREATE OR REPLACE VIEW projection_residuals AS
SELECT
    bl.bet_id,
    bl.bet_type,
    bl.model_prob,
    bl.implied_prob_at_bet,
    bl.edge_at_bet,
    bl.actual_k,
    bl.line_at_bet,
    (bl.actual_k - bl.line_at_bet) AS residual,
    ABS(bl.actual_k - bl.line_at_bet) AS abs_residual,
    CASE 
        WHEN bl.actual_k > bl.line_at_bet THEN 'OVER'
        WHEN bl.actual_k < bl.line_at_bet THEN 'UNDER'
        ELSE 'PUSH'
    END AS actual_direction,
    bl.result,
    bl.agent_id
FROM bets_log bl
WHERE bl.actual_k IS NOT NULL;

-- ── Agent Leaderboard View ──────────────────────────────────
-- Per-agent performance comparison
CREATE OR REPLACE VIEW agent_leaderboard AS
SELECT
    agent_id,
    COUNT(*) AS total_bets,
    SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) AS wins,
    ROUND(AVG(CASE WHEN result = 'WIN' THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_rate_pct,
    ROUND(SUM(stake_units), 2) AS total_staked,
    ROUND(SUM(profit_units), 2) AS total_profit,
    ROUND(SUM(profit_units) / NULLIF(SUM(stake_units), 0) * 100, 2) AS roi_pct,
    ROUND(AVG(edge_at_bet) * 100, 2) AS avg_edge_pct,
    ROUND(AVG(clv) * 100, 3) AS avg_clv_pct,
    MIN(bet_time) AS first_bet,
    MAX(bet_time) AS last_bet
FROM bets_log
WHERE result IN ('WIN', 'LOSS', 'PUSH')
  AND agent_id IS NOT NULL
GROUP BY agent_id
ORDER BY roi_pct DESC;

-- ── Performance Indexes ─────────────────────────────────────

-- Index for time-based queries (daily P&L, recent bets)
CREATE INDEX IF NOT EXISTS idx_bets_log_bet_time 
    ON bets_log (bet_time DESC);

-- Index for agent-based leaderboard queries
CREATE INDEX IF NOT EXISTS idx_bets_log_agent_date 
    ON bets_log (agent_id, bet_time);

-- Index for game-based lookups
CREATE INDEX IF NOT EXISTS idx_bets_log_game_id 
    ON bets_log (game_id);

-- Index for result filtering (settled vs pending)
CREATE INDEX IF NOT EXISTS idx_bets_log_result 
    ON bets_log (result);

-- Index for calibration curve bucketing
CREATE INDEX IF NOT EXISTS idx_bets_log_model_prob 
    ON bets_log (model_prob);

-- Index for CLV analysis
CREATE INDEX IF NOT EXISTS idx_bets_log_clv 
    ON bets_log (clv) WHERE clv IS NOT NULL;

-- ── Live Projections Performance Indexes ────────────────────

-- Fast lookup by market_id (primary operations)
CREATE INDEX IF NOT EXISTS idx_live_projections_market 
    ON live_projections (market_id);

-- Edge-based sorting for agent scanning
CREATE INDEX IF NOT EXISTS idx_live_projections_edge 
    ON live_projections (edge_pct DESC) WHERE edge_pct > 0.03;

-- Freshness check for 15-second polling
CREATE INDEX IF NOT EXISTS idx_live_projections_updated 
    ON live_projections (last_updated DESC);

-- ── Betting Markets Performance Indexes ─────────────────────

-- Fast lookup by game for slate views
CREATE INDEX IF NOT EXISTS idx_betting_markets_game 
    ON betting_markets (game_id);

-- Sportsbook filtering
CREATE INDEX IF NOT EXISTS idx_betting_markets_sportsbook 
    ON betting_markets (sportsbook);

-- Freshness for sync worker
CREATE INDEX IF NOT EXISTS idx_betting_markets_updated 
    ON betting_markets (updated_at DESC);
