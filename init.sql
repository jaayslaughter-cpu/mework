-- ============================================================
-- PropIQ Analytics v3 — Production Database Schema
-- Mount: ./init.sql:/docker-entrypoint-initdb.d/init.sql
-- ============================================================

-- Games & Lineage
CREATE TABLE IF NOT EXISTS games (
    game_id          VARCHAR(50) PRIMARY KEY,
    game_date        DATE NOT NULL,
    away_team        VARCHAR(10) NOT NULL,
    home_team        VARCHAR(10) NOT NULL,
    status           VARCHAR(20) DEFAULT 'SCHEDULED',
    first_pitch      TIMESTAMP,
    venue            VARCHAR(100),
    roof_closed      BOOLEAN DEFAULT FALSE,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Prop Markets (e.g., Aaron Judge O/U 1.5 Hits)
CREATE TABLE IF NOT EXISTS prop_markets (
    market_id        SERIAL PRIMARY KEY,
    game_id          VARCHAR(50) REFERENCES games(game_id),
    player_id        VARCHAR(20) NOT NULL,
    player_name      VARCHAR(100) NOT NULL,
    prop_type        VARCHAR(30) NOT NULL,   -- Hits, Strikeouts, TotalBases, HomeRuns
    target_line      DECIMAL(4,1) NOT NULL,
    actual_result    DECIMAL(4,1),
    dfs_platform     VARCHAR(30),            -- PrizePicks, Underdog
    UNIQUE(game_id, player_id, prop_type, target_line)
);

-- CLV Tracking: Opening → Bet Placed → Closing Line
CREATE TABLE IF NOT EXISTS line_history (
    id                   SERIAL PRIMARY KEY,
    market_id            INT REFERENCES prop_markets(market_id),
    event_type           VARCHAR(20) NOT NULL,  -- OPENING_LINE, BET_PLACED, CLOSING_LINE
    sportsbook           VARCHAR(20) NOT NULL,  -- draftkings, fanduel, betmgm, bet365
    over_odds            INTEGER,
    under_odds           INTEGER,
    no_vig_prob_over     DECIMAL(5,4),
    no_vig_prob_under    DECIMAL(5,4),
    recorded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Agent Bet Ledger (The Money Table)
CREATE TABLE IF NOT EXISTS bet_ledger (
    bet_id               VARCHAR(36) PRIMARY KEY,
    market_id            INT REFERENCES prop_markets(market_id),
    agent_name           VARCHAR(30) NOT NULL,  -- EV_Hunter, Under_Machine, Umpire_Agent…
    direction            VARCHAR(10) NOT NULL,  -- OVER, UNDER
    units_risked         DECIMAL(8,2) NOT NULL,
    kelly_fraction       DECIMAL(5,4),
    placed_odds          DECIMAL(8,2),
    placed_no_vig_prob   DECIMAL(5,4),
    xgboost_prob         DECIMAL(5,4),
    ev_pct               DECIMAL(5,2),
    status               VARCHAR(20) DEFAULT 'PENDING', -- PENDING, WIN, LOSS, PUSH, ABORTED
    profit_loss          DECIMAL(8,2),
    closing_no_vig_prob  DECIMAL(5,4),
    clv_pct              DECIMAL(5,2),           -- Closing Line Value
    placed_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    settled_at           TIMESTAMP
);

-- Agent 14-Day Performance (Leaderboard source)
CREATE TABLE IF NOT EXISTS agent_stats (
    agent_name     VARCHAR(30) PRIMARY KEY,
    total_bets     INT DEFAULT 0,
    total_profit   DECIMAL(10,2) DEFAULT 0,
    roi_pct        DECIMAL(6,2) DEFAULT 0,
    win_pct        DECIMAL(5,2) DEFAULT 0,
    avg_clv        DECIMAL(5,2) DEFAULT 0,
    capital_weight DECIMAL(3,1) DEFAULT 1.0,  -- 0.5x, 1.0x, or 2.0x
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- XGBoost Audit Log
CREATE TABLE IF NOT EXISTS model_audit_log (
    id               SERIAL PRIMARY KEY,
    run_date         DATE NOT NULL,
    holdout_accuracy DECIMAL(5,4),
    valid_features   TEXT,    -- JSON array
    dropped_features TEXT,    -- JSON array
    passed           BOOLEAN,
    notes            TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stat Correction Flags (anomaly detection)
CREATE TABLE IF NOT EXISTS stat_correction_flags (
    id           SERIAL PRIMARY KEY,
    bet_id       VARCHAR(36) REFERENCES bet_ledger(bet_id),
    player_name  VARCHAR(100),
    original_val DECIMAL(5,2),
    flagged_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved     BOOLEAN DEFAULT FALSE
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_bet_ledger_agent_status ON bet_ledger(agent_name, status);
CREATE INDEX IF NOT EXISTS idx_bet_ledger_placed_at    ON bet_ledger(placed_at);
CREATE INDEX IF NOT EXISTS idx_line_history_market     ON line_history(market_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_prop_markets_player     ON prop_markets(player_name);
CREATE INDEX IF NOT EXISTS idx_prop_markets_game       ON prop_markets(game_id);

-- Seed agent roster (10-agent army)
INSERT INTO agent_stats (agent_name, capital_weight) VALUES
    ('EV_Hunter',           1.0),
    ('Under_Machine',       1.0),
    ('Three_Leg_Correlated',1.0),
    ('Standard_Parlay',     1.0),
    ('Live_Agent',          1.0),
    ('Arb_Agent',           1.0),
    ('Fade_Agent',          1.0),
    ('Umpire_Agent',        1.0),
    ('F5_Agent',            1.0),
    ('Live_Micro_Agent',    1.0)
ON CONFLICT (agent_name) DO NOTHING;

-- Spring Batch metadata tables (managed by Spring but declaring explicitly)
CREATE TABLE IF NOT EXISTS BATCH_JOB_INSTANCE (
    JOB_INSTANCE_ID BIGINT NOT NULL PRIMARY KEY,
    VERSION         BIGINT,
    JOB_NAME        VARCHAR(100) NOT NULL,
    JOB_KEY         VARCHAR(32) NOT NULL,
    CONSTRAINT JOB_INST_UN UNIQUE (JOB_NAME, JOB_KEY)
);
CREATE TABLE IF NOT EXISTS BATCH_JOB_EXECUTION (
    JOB_EXECUTION_ID  BIGINT NOT NULL PRIMARY KEY,
    VERSION           BIGINT,
    JOB_INSTANCE_ID   BIGINT NOT NULL,
    CREATE_TIME       TIMESTAMP NOT NULL,
    START_TIME        TIMESTAMP,
    END_TIME          TIMESTAMP,
    STATUS            VARCHAR(10),
    EXIT_CODE         VARCHAR(2500),
    EXIT_MESSAGE      VARCHAR(2500),
    LAST_UPDATED      TIMESTAMP,
    CONSTRAINT JOB_INST_EXEC_FK FOREIGN KEY (JOB_INSTANCE_ID) REFERENCES BATCH_JOB_INSTANCE(JOB_INSTANCE_ID)
);
CREATE TABLE IF NOT EXISTS BATCH_JOB_EXECUTION_PARAMS (
    JOB_EXECUTION_ID BIGINT NOT NULL,
    PARAMETER_NAME   VARCHAR(100) NOT NULL,
    PARAMETER_TYPE   VARCHAR(100) NOT NULL,
    PARAMETER_VALUE  VARCHAR(2500),
    IDENTIFYING      CHAR(1) NOT NULL,
    CONSTRAINT JOB_EXEC_PARAMS_FK FOREIGN KEY (JOB_EXECUTION_ID) REFERENCES BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
);
CREATE TABLE IF NOT EXISTS BATCH_STEP_EXECUTION (
    STEP_EXECUTION_ID  BIGINT NOT NULL PRIMARY KEY,
    VERSION            BIGINT NOT NULL,
    STEP_NAME          VARCHAR(100) NOT NULL,
    JOB_EXECUTION_ID   BIGINT NOT NULL,
    CREATE_TIME        TIMESTAMP NOT NULL,
    START_TIME         TIMESTAMP,
    END_TIME           TIMESTAMP,
    STATUS             VARCHAR(10),
    COMMIT_COUNT       BIGINT,
    READ_COUNT         BIGINT,
    FILTER_COUNT       BIGINT,
    WRITE_COUNT        BIGINT,
    READ_SKIP_COUNT    BIGINT,
    WRITE_SKIP_COUNT   BIGINT,
    PROCESS_SKIP_COUNT BIGINT,
    ROLLBACK_COUNT     BIGINT,
    EXIT_CODE          VARCHAR(2500),
    EXIT_MESSAGE       VARCHAR(2500),
    LAST_UPDATED       TIMESTAMP,
    CONSTRAINT JOB_EXEC_STEP_FK FOREIGN KEY (JOB_EXECUTION_ID) REFERENCES BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
);
CREATE TABLE IF NOT EXISTS BATCH_STEP_EXECUTION_CONTEXT (
    STEP_EXECUTION_ID  BIGINT NOT NULL PRIMARY KEY,
    SHORT_CONTEXT      VARCHAR(2500) NOT NULL,
    SERIALIZED_CONTEXT TEXT,
    CONSTRAINT STEP_EXEC_CTX_FK FOREIGN KEY (STEP_EXECUTION_ID) REFERENCES BATCH_STEP_EXECUTION(STEP_EXECUTION_ID)
);
CREATE TABLE IF NOT EXISTS BATCH_JOB_EXECUTION_CONTEXT (
    JOB_EXECUTION_ID   BIGINT NOT NULL PRIMARY KEY,
    SHORT_CONTEXT      VARCHAR(2500) NOT NULL,
    SERIALIZED_CONTEXT TEXT,
    CONSTRAINT JOB_EXEC_CTX_FK FOREIGN KEY (JOB_EXECUTION_ID) REFERENCES BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
);
CREATE SEQUENCE IF NOT EXISTS BATCH_STEP_EXECUTION_SEQ MAXVALUE 9223372036854775807 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS BATCH_JOB_EXECUTION_SEQ  MAXVALUE 9223372036854775807 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS BATCH_JOB_SEQ             MAXVALUE 9223372036854775807 NO CYCLE;
