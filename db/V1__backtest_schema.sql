-- PropIQ DB Migration V1: Backtest + Feature Audit Tables
-- Run once against your Railway Postgres instance before first BacktestTasklet execution.
-- Compatible with: PostgreSQL 14+

-- ── model_dropped_features ────────────────────────────────────────────────────
-- Persists features flagged for removal by the SHAP audit.
-- XGBoostTasklet reads this table on retrain to exclude low-accuracy features.
CREATE TABLE IF NOT EXISTS model_dropped_features (
    feature_name  TEXT PRIMARY KEY,
    audit_date    DATE        NOT NULL,
    dropped_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  model_dropped_features IS 'Features dropped by BacktestTasklet SHAP audit.';
COMMENT ON COLUMN model_dropped_features.feature_name IS 'Exact column name as used in the XGBoost feature matrix.';
COMMENT ON COLUMN model_dropped_features.audit_date   IS 'Calendar date of the audit run that dropped this feature.';

-- ── backtest_runs ─────────────────────────────────────────────────────────────
-- One row per daily BacktestTasklet execution. Used for trend analysis and
-- model health monitoring in LeaderboardTasklet.
CREATE TABLE IF NOT EXISTS backtest_runs (
    run_date              DATE PRIMARY KEY,
    overall_accuracy      NUMERIC(6, 4)  NOT NULL,
    sample_size           INT            NOT NULL,
    dropped_features      TEXT[]         NOT NULL DEFAULT '{}',
    feature_accuracy_json JSONB          NOT NULL DEFAULT '{}',
    created_at            TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  backtest_runs IS 'Daily SHAP feature audit summary from BacktestTasklet.';
COMMENT ON COLUMN backtest_runs.overall_accuracy      IS 'Fraction of settled bets correctly predicted (xgboost_prob >= 0.55 vs prop_hit_actual).';
COMMENT ON COLUMN backtest_runs.dropped_features      IS 'Features dropped in this run (SHAP-estimated accuracy < min_feature_accuracy).';
COMMENT ON COLUMN backtest_runs.feature_accuracy_json IS 'Per-feature pseudo-accuracy map {feature_name: accuracy_fraction}.';

-- ── stat_correction_flags ─────────────────────────────────────────────────────
-- Tracks bets flagged for potential stat correction anomalies by the
-- /api/ml/anomaly-detect endpoint during GradingTasklet.
CREATE TABLE IF NOT EXISTS stat_correction_flags (
    id         BIGSERIAL   PRIMARY KEY,
    bet_id     TEXT        NOT NULL REFERENCES bet_ledger(bet_id) ON DELETE CASCADE,
    flagged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT stat_correction_flags_bet_unique UNIQUE (bet_id)
);

COMMENT ON TABLE stat_correction_flags IS 'Bets flagged by anomaly detection for potential stat correction review.';

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_backtest_runs_created_at
    ON backtest_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_dropped_features_audit_date
    ON model_dropped_features (audit_date);

-- ── agent_stats table (if not already present) ───────────────────────────────
-- Referenced by PostgresService.updateAgentStats()
CREATE TABLE IF NOT EXISTS agent_stats (
    agent_name   TEXT PRIMARY KEY,
    total_bets   INT            NOT NULL DEFAULT 0,
    total_profit NUMERIC(10, 4) NOT NULL DEFAULT 0,
    roi_pct      NUMERIC(8, 4)  NOT NULL DEFAULT 0,
    win_pct      NUMERIC(6, 4)  NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- ── model_audit_log (if not already present) ─────────────────────────────────
-- Referenced by PostgresService.logAuditSuccess() / logAuditFailure()
CREATE TABLE IF NOT EXISTS model_audit_log (
    id               BIGSERIAL   PRIMARY KEY,
    run_date         DATE        NOT NULL DEFAULT CURRENT_DATE,
    holdout_accuracy NUMERIC(6, 4),
    valid_features   TEXT,
    dropped_features TEXT,
    passed           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
