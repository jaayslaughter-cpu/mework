# PropIQ Backtesting Workbench

> Guided reference for running, interpreting, and extending PropIQ backtests.  
> Covers `BacktestTasklet`, `StrikeoutBacktester`, `PropSimulator`, and the historical Tank01 engine.

---

## 1. Backtest Architecture

```
backtest_historical.py          — 10-season Tank01 box score engine (PR #96)
api/tasklets/backtest_tasklet.py — Modular simulator framework (PR #98 + #100)
    ├── PropSimulator            — General prop backtest
    ├── StrikeoutSimulator       — Synthetic K-prop environment
    └── StrikeoutBacktester      — 3-model comparison (RF vs XGB vs Ensemble)
run_backtest.sh                  — Orchestration script
```

---

## 2. Running a Backtest

### Full Historical Backtest (Tank01, 10 seasons)
```bash
# From project root
bash run_backtest.sh full

# Dry run (no disk write, logs to stdout)
bash run_backtest.sh dry

# Single season
bash run_backtest.sh full --season 2024
```

Output files:
- `reports/backtest_{date}.json` — Full per-game, per-prop results
- `reports/backtest_summary_{date}.csv` — Aggregated metrics table

### Strikeout Model Comparison Backtest
```python
from api.tasklets.backtest_tasklet import StrikeoutBacktester

backtester = StrikeoutBacktester(n_games=500, random_seed=42)
report = backtester.run_comparison()

# report keys:
# {
#   "rf":       {"accuracy": 0.618, "auc": 0.621, "roi": 6.1, "sharpe": 0.98, ...},
#   "xgb":      {"accuracy": 0.634, "auc": 0.651, "roi": 9.8, "sharpe": 1.31, ...},
#   "ensemble": {"accuracy": 0.641, "auc": 0.659, "roi": 11.2, "sharpe": 1.42, ...},
#   "summary":  {"winner": "ensemble", "xgb_vs_rf_lift_roi": 3.7, ...}
# }
```

### PropSimulator (General Props)
```python
from api.tasklets.backtest_tasklet import PropSimulator

sim = PropSimulator(
    n_games=1000,
    prop_types=["strikeouts", "hits", "total_bases"],
    ev_gate=0.03,
    kelly_cap=0.10
)
results = sim.run()
```

---

## 3. Key Evaluation Metrics

### Classification Metrics (per model)

| Metric | Formula | Target |
|--------|---------|--------|
| **AUC-ROC** | Area under ROC curve | ≥ 0.63 |
| **Brier Score** | Mean squared prob error | ≤ 0.23 |
| **Accuracy @ threshold** | `(TP+TN)/(total)` at optimal threshold | ≥ 61% |
| **Precision** | `TP/(TP+FP)` | ≥ 58% |
| **Recall** | `TP/(TP+FN)` | ≥ 55% |
| **F1 Score** | Harmonic mean of P/R | ≥ 0.565 |

### Profitability Metrics

| Metric | Formula | Target |
|--------|---------|--------|
| **ROI** | `total_profit / total_staked` | ≥ +8% |
| **CLV** | Avg closing line value | ≥ +1.5% |
| **Bet Frequency** | Bets fired / eligible props | 15–25% |
| **Units Won** | Sum of Kelly-sized outcomes | Track vs baseline |
| **Sharpe Ratio** | `mean(daily_pnl) / std(daily_pnl) × √252` | ≥ 1.0 |
| **Max Drawdown** | Peak-to-trough portfolio decline | ≤ -20% |
| **Calmar Ratio** | `Annualized ROI / Max Drawdown` | ≥ 0.5 |

### EV Quality Metrics

| Metric | Definition | Target |
|--------|-----------|--------|
| **Mean EV%** | Average EV of fired slips | 4–10% |
| **EV Accuracy** | % of +EV bets that actually won | > 52% |
| **Edge Decay** | CLV at close vs CLV at open | Positive = alpha is real |

---

## 4. Threshold Analysis

The `StrikeoutBacktester` sweeps probability thresholds from 0.50 to 0.65 to find optimal fire threshold:

```
Threshold  Bets    Win%    ROI     Sharpe
0.50       1,847   53.1%   +3.2%   0.72
0.52       1,241   55.8%   +6.7%   0.98
0.54       891     58.3%   +9.4%   1.19
0.55       712     60.1%   +11.2%  1.31   ← CURRENT DEFAULT
0.57       512     62.4%   +12.8%  1.42
0.60       311     64.1%   +14.1%  1.51
0.62       188     65.2%   +11.3%  1.38   ← diminishing volume
0.65       89      66.8%   +7.2%   0.91   ← too few bets
```

**Guidance:** 0.55 is the production default. Consider 0.57 during high-confidence market conditions (sharp books agree). Never go below 0.52 (guardrail).

---

## 5. Avoiding Backtest Leakage

Critical rules to prevent overfitting backtest results:

### 5.1 Temporal Validation Only
```python
# ❌ WRONG: Random split leaks future data into training
X_train, X_test = train_test_split(df, test_size=0.2, random_state=42)

# ✅ CORRECT: Temporal split — train on past, test on future
cutoff = df["game_date"].quantile(0.8)
X_train = df[df["game_date"] < cutoff]
X_test  = df[df["game_date"] >= cutoff]
```

### 5.2 No Lookahead in Rolling Features
```python
# ❌ WRONG: Rolling includes same-day stats
df["k_rate_l7"] = df.groupby("player_id")["k_per_9"].transform(
    lambda s: s.rolling(7).mean()
)

# ✅ CORRECT: Shift to exclude same-day observation
df["k_rate_l7"] = df.groupby("player_id")["k_per_9"].transform(
    lambda s: s.shift(1).rolling(7).mean()   # shift(1) excludes today
)
```

### 5.3 Calibration Set Must Post-Date Training Set
```python
# ❌ WRONG: Calibrate on same data as training
model = CalibratedClassifierCV(xgb_model, method='isotonic', cv=5)
model.fit(X_train, y_train)

# ✅ CORRECT: Train on first 70%, calibrate on next 15%, test on last 15%
train_end   = int(len(df) * 0.70)
cal_end     = int(len(df) * 0.85)
X_train, y_train = df.iloc[:train_end], y.iloc[:train_end]
X_cal,   y_cal   = df.iloc[train_end:cal_end], y.iloc[train_end:cal_end]
X_test,  y_test  = df.iloc[cal_end:], y.iloc[cal_end:]
```

### 5.4 Closing Line as Target (Not Result)
- For CLV-based backtests, the "win" signal should be: **did the line move in our direction by close?** Not just: did the prop result hit?
- This avoids survivorship bias from only testing settled props

---

## 6. Stress Tests

### Market Stress Test
Simulates high-volatility market conditions:

```python
# Stress: inject artificial line movement noise
sim = PropSimulator(n_games=500, line_noise_std=0.5)
results_stress = sim.run()

# Compare Sharpe: if stress Sharpe < 0.7 × baseline Sharpe → flag model fragility
```

### Bet Frequency Sensitivity
```python
# Test profitability across bet frequency ranges
for freq_gate in [0.10, 0.15, 0.20, 0.25, 0.30]:
    sim = PropSimulator(ev_gate=0.03, max_bet_freq=freq_gate)
    r = sim.run()
    print(f"freq_gate={freq_gate}: ROI={r['roi']:.1%}, Sharpe={r['sharpe']:.2f}")
```

### Agent-Level Isolation
Run each of the 16 agents in isolation to measure individual contribution:

```python
for agent_class in [EVHunter, UnderMachine, F5Agent, ...ArbitrageAgent]:
    sim = PropSimulator(agents=[agent_class()])
    print(f"{agent_class.__name__}: {sim.run()['roi']:.1%} ROI")
```

---

## 7. Report Interpretation Guide

### `backtest_summary_{date}.csv` Fields

| Column | Meaning |
|--------|---------|
| `model` | RF / XGB / Ensemble |
| `prop_type` | strikeouts / hits / total_bases / etc. |
| `n_bets` | Total bets fired in period |
| `win_rate` | % of bets that hit |
| `roi` | Total return on investment |
| `clv_avg` | Average closing line value |
| `sharpe` | Daily P&L Sharpe ratio |
| `max_dd` | Worst peak-to-trough drawdown |
| `calmar` | ROI / Max Drawdown |
| `ev_accuracy` | % of +EV bets that won |
| `kelly_avg` | Average Kelly fraction used |
| `best_agent` | Agent with highest ROI in this prop type |

### Red Flags in Results
- **Win rate < 52% at threshold ≥ 0.55** → Calibration failure. Retrain.
- **CLV avg < 0%** → No real edge. Either data quality issue or market has corrected.
- **Max drawdown > 25%** → Kelly sizing too aggressive. Reduce cap.
- **Bet frequency > 30%** → EV gate too low. Raise to 0.04.
- **Ensemble worse than XGB standalone** → Stacking with too little data. Switch to `average` mode.

---

## 8. Extending the Backtest Framework

### Adding a New Prop Type Simulator

```python
# api/tasklets/backtest_tasklet.py
class HitsPropSimulator(BaseSimulator):
    """Simulator for hits props."""

    PROP_TYPE = "hits"
    MEAN_HITS_PER_GAME = 8.7
    STD_HITS = 2.1

    def generate_game(self, game_id: int) -> pd.DataFrame:
        n_players = 9
        hits = np.random.normal(
            self.MEAN_HITS_PER_GAME / n_players,
            self.STD_HITS / n_players,
            size=n_players
        )
        # ... build rows and return DataFrame
```

### Adding a New Evaluation Metric

```python
# Add to StrikeoutBacktester._compute_metrics()
def _calmar_ratio(self, daily_pnl: pd.Series) -> float:
    annual_return = daily_pnl.sum() / len(daily_pnl) * 252
    max_dd = self._max_drawdown(daily_pnl)
    return annual_return / abs(max_dd) if max_dd != 0 else 0.0
```
