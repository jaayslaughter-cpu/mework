# PropIQ Analytics — Technical Design Document
**Version:** 2.0 | **Date:** March 2026 | **Status:** Production-Ready

---

## 1. System Overview

PropIQ is a headless MLB DFS analytics platform targeting PrizePicks, Underdog Fantasy, and Sleeper. It is California DFS compliant (no traditional sportsbook wagering).

The system consists of two co-deployed services:

| Service | Stack | Role |
|---|---|---|
| **Spring Boot Backend** | Java 21 + Spring Batch + Kafka + Redis + Postgres | Data ingestion, batch job orchestration, Spring Batch tasklets |
| **Python ML Microservice** | Python 3.12 + FastAPI + RabbitMQ + Redis | ML inference, agent execution, Discord alerts |

---

## 2. Architecture Diagram

```
External Data Sources
├── Tank01 (box scores, player stats)       → Spring Boot TaskletChain
├── The Odds API (live prop odds, dual-key) → market_scanners.py
├── SportsBooksReview (sharp CLV lines)     → odds_fetcher.py
├── Baseball Savant (Statcast CSV exports)  → statcast_hub.py
└── Apify (RotoWire, Action Network, SBR)   → apify_scrapers.py

Spring Boot (Java 21)
└── 7 Tasklets (Spring Batch):
    ├── DataHubTasklet     — raw data ingestion (Tank01 + SBR + Apify)
    ├── AgentTasklet       — triggers Python execution squad
    ├── BetAnalyzerTasklet — slip grading
    ├── LeaderboardTasklet — performance metrics
    ├── GradingTasklet     — daily recap trigger
    ├── BacktestTasklet    — historical simulation
    └── XGBoostTasklet     — calls /api/ml/* FastAPI endpoints

Python ML Microservice (FastAPI + RabbitMQ)
└── 5 Tiers:
    ├── Tier 1: ML Pipeline (ml_pipeline.py)
    │   └── XGBoost + isotonic calibration → mlb.projections.*
    ├── Tier 2: Market Scanners (market_scanners.py)
    │   └── LineValue + Steam + Fade → alerts.market_edges
    ├── Tier 3: Context Modifiers (context_modifiers.py)
    │   └── Bullpen fatigue + weather + umpire → feature enrichment
    ├── Tier 4: Execution Squad (execution_agents.py)
    │   └── 15 agents → UnderdogMathEngine → alerts.discord.slips
    └── Tier 5: Discord Dispatcher (discord_dispatcher.py)
        └── RabbitMQ consumer → Discord webhook
```

---

## 3. New Components (Phase 14+ Enhancements)

### 3.1 Multi-Provider Odds Integration (`api/services/odds_fetcher.py`)

**Purpose:** Ingest MLB player prop odds from multiple providers, strip vig, and surface top Closing Line Value (CLV) opportunities.

**Providers:**
- `OddsApiOddsFetcher` — The Odds API v4 (dual-key rotation on 429)
- `SportsBooksReviewOddsFetcher` — SBR sharp-book lines (Pinnacle, Circa) for CLV anchor

**Key Classes:**
```python
OddsLine      # Normalised single-provider line
MergedOdds    # Consensus across providers with CLV estimate
OddsFetcher   # Orchestrator: fetch_all() → merge_odds() → top_clv_opportunities()
```

**CLV Formula:**
```
CLV edge = (soft_book_no_vig_prob / sharp_book_no_vig_prob) - 1
Gate: CLV ≥ 2%, ≥2 providers required
```

### 3.2 Market Fusion Engine (`api/services/market_fusion.py`)

**Purpose:** Bridge MergedOdds → PropEdge dicts for the 15-agent execution squad.

**Features:**
- Converts all CLV-qualified lines to PropEdge format
- `arbitrage_scan()` — finds true arbitrage (implied over + under < 1.0)
- Quality gate: CLV ≥ 2%, min 2 providers

### 3.3 Strikeout Prop Model (`api/services/strikeout_model.py`)

**Purpose:** Dedicated XGBoost + RandomForest ensemble for pitcher strikeout props.

**Feature Vector (34 dimensions):**
- Rolling K rates (L7/L14/L30), K%, WHIP, ERA, BB rate
- Arsenal: fastball%, breaking%, offspeed%, velocity, spin rate
- Per-pitch whiff rates (FB, SL, CU, CH), chase rate, zone contact
- Arsenal cluster (0=FB-dominant, 1=breaking-ball, 2=offspeed-mix)
- Opposing lineup: K%, wRC+, contact%, chase%, handedness split
- Context: park K factor, wind, temp, home/away, umpire K rate, days rest

**Ensemble Architecture:**
```
XGBoost (binary:logistic + isotonic, weight 0.65)
    +
RandomForest (300 trees + isotonic, weight 0.35)
    ↓
Ensemble method: average | stack (LogReg meta-learner) | bagging (median)
    ↓
StrikeoutPrediction (prob_over, prob_under, confidence)
```

### 3.4 Modular Backtest Engine (`api/tasklets/backtest_tasklet.py`)

**Purpose:** Pluggable backtesting framework following Odds-Gym `gym.Env` pattern.

**Components:**
```
BaseSimulator (abstract)
├── PropSimulator     — all 15 agents × all prop types × 10 seasons
└── StrikeoutSimulator — standalone strikeout model rapid iteration

BacktestDataset       — Tank01 + disk cache (zero API re-hits)
BacktestRunner        — N-cycle orchestration
BacktestReport        — CLV, ROI, Sharpe, max drawdown, by-agent, by-season
```

**Metrics Produced:**
| Metric | Formula |
|---|---|
| Win rate | wins / (wins + losses) |
| ROI % | profit / wagered × 100 |
| Sharpe (annualised) | mean_daily_return / std_daily × √252 |
| Max drawdown | peak − trough over equity curve |
| CLV | closing line no-vig prob vs sharp-book anchor |
| Kelly fraction | (b×p − q) / b / 2 capped at 10% |

### 3.5 Kelly Criterion + Portfolio Optimizer (`api/services/risk_management.py`)

**Purpose:** Optimal bankroll allocation across the daily prop slate.

**Kelly Formula:**
```
f* = (b × p − q) / b
f_half = f* / 2
f_capped = min(f_half, 10%)
```

**PortfolioOptimizer logic:**
1. Sort by EV% descending
2. Size each bet with half-Kelly capped at 10%
3. Correlation penalty: −25% if same game_id has >5% exposure
4. Same-player penalty: −50% on second prop for same player
5. Portfolio cap: total exposure ≤ 30% bankroll
6. Redistribute freed units to highest remaining EV
7. Compute diversification score (1 − HHI normalised)

### 3.6 Statcast DataHub (`api/services/statcast_hub.py`)

**Purpose:** Ingest and cache Statcast pitch-level data from Baseball Savant.

**Data Models:**
- `PitchRecord` — single pitch (type, velocity, spin, movement, outcome)
- `PitcherArsenal` — aggregated pitch-type stats (usage%, whiff%, spin, cluster)
- `BatterPlateDiscipline` — swing/contact metrics (chase%, zone%, xBA, xwOBA)

**Data Sources (no API key required):**
- `baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats` CSV
- `baseballsavant.mlb.com/leaderboard/expected_statistics` CSV

**Feature Enrichment:**
```python
hub.enrich_pitcher_features(pitcher_id) → 12 ML-ready features
hub.enrich_batter_features(batter_id)   → 10 ML-ready features
hub.batch_enrich(pitchers, batters)     → merged dict by player_id
```

---

## 4. Data Flow (Complete)

```
Tank01 Box Scores
    ↓
BacktestDataset / DataHubTasklet
    ↓ (disk cache + Redis)
PropSimulator.step(date)
    ↓
Per-player rolling buffers (L7/L14/L30)
    ↓
simulate_line() → L14 median rounded to 0.5
model_prob()    → L30 hit rate + Laplace smoothing
    ↓
15 Agent filters (prop_type affinity, min_prob, min_ev gates)
    ↓
odds_math.strip_vig() → true probabilities
odds_math.calculate_ev() → EV% (gate: ≥3%)
    ↓
KellyCriterion.size() → half-Kelly capped at 10%
PortfolioOptimizer.optimize() → correlation-adjusted allocations
    ↓
BetRecord (date, player, prop, line, prob, ev, kelly, outcome, profit)
    ↓
BacktestReport.generate() → JSON + CSV + equity curve
```

---

## 5. API Reference

### FastAPI Endpoints (`api_server.py`)

| Method | Path | Description |
|---|---|---|
| POST | `/api/ml/predict` | Batch prop predictions (≤50 players) |
| POST | `/api/ml/predict-live` | Live in-game prediction |
| POST | `/api/ml/correlation` | Correlation matrix for slip validation |
| POST | `/api/ml/game-prob` | Game-level probabilities (F5, team totals) |
| GET  | `/api/ml/anomaly-detect` | Fatigue/weather anomaly warnings |
| POST | `/api/ml/backtest-audit` | SHAP feature audit |
| GET  | `/api/ml/health` | Health check |

---

## 6. Mathematical Guardrails

| Rule | Value | Location |
|---|---|---|
| EV minimum gate | ≥ 3% | `odds_math.py` + all 15 agents |
| Kelly cap | ½ Kelly, max 10% | `risk_management.py` + `underdog_math_engine.py` |
| Portfolio cap | 30% total | `risk_management.py` |
| CLV gate | ≥ 2% | `odds_fetcher.py` + `market_fusion.py` |
| Correlation block | ≥4 legs from same game_id | `execution_agents.py` |
| In-game polling | NONE | `apify_scrapers.py` — pre-match only |

---

## 7. Deployment

**Platform:** Railway.app (Python ML microservice)

**Required Environment Variables:**
```
REDIS_URL          redis://...
DATABASE_URL       postgresql://...
RABBITMQ_URL       amqp://...
APIFY_API_KEY      apify_api_...
DISCORD_WEBHOOK_URL https://discordapp.com/api/webhooks/...
TANK01_KEY         58a304828b...
ODDS_API_KEY       e4e30098...
ODDS_API_KEY_BACKUP 673bf195...
```

**Process Types (Procfile):**
```
web:    uvicorn api_server:app --host 0.0.0.0 --port $PORT
worker: python ml_pipeline.py
```

---

## 8. Testing Strategy

| Layer | Coverage Target | Files |
|---|---|---|
| Unit — odds math | 95% | `tests/test_odds_fetcher.py` |
| Unit — strikeout model | 90% | `tests/test_strikeout_model.py` |
| Unit — risk management | 92% | `tests/test_risk_management.py` |
| Integration — backtest | 88% | `tests/test_backtest_engine.py` |
| E2E smoke test | 3-day cycle | `TestEndToEndSmoke` in backtest tests |

**Run all tests:**
```bash
python -m pytest tests/ -v --tb=short --cov=api --cov-report=term-missing
```

---

## 9. Production Readiness Checklist

- [x] All 15 agents with real EV math (3% gate)
- [x] ½ Kelly sizing with 10% hard cap
- [x] SportsData.io → Tank01 auto-fallback on 403
- [x] Dual Odds API key rotation on quota exhaustion
- [x] RabbitMQ topic exchange wired (all 5 tiers)
- [x] Discord webhook with entry-type stamps + all legs visible
- [x] Daily recap at 11:30 PM PT after GradingTasklet
- [x] Startup ping on service launch
- [x] Game_id correlation blocking (≥4 same-game legs)
- [x] Redis sorted-set state restoration on restart
- [x] FastAPI HTTP layer (7 endpoints, Java ↔ Python)
- [x] Railway deployment config (Procfile + railway.toml)
- [x] DB migration (V1__backtest_schema.sql)
- [x] Multi-provider odds (The Odds API + SportsBooksReview)
- [x] CLV estimation engine
- [x] Strikeout XGBoost + ensemble
- [x] Modular backtest engine (PropSimulator + StrikeoutSimulator)
- [x] Portfolio optimizer with correlation penalties
- [x] Statcast DataHub (Baseball Savant CSV integration)
- [x] Unit test suite (4 test files, 90%+ target coverage)
- [ ] Staging environment mirror (pending Railway env setup)
- [ ] Performance benchmarks + auto-scaling (Railway metrics)
- [ ] 10-season backtest results (pending first full run)
