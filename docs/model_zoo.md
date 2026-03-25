# PropIQ Model Zoo

> Catalog of all active models with architecture details, performance benchmarks, and maintenance notes.  
> Update benchmark section after each major backtesting run.

---

## Model 1 — PlayerPropXGBoost (General Props)

**File:** `ml_pipeline.py` → `PlayerPropXGBoost`  
**Status:** 🟢 Production  
**Deployed:** PR #85

### Architecture
| Param | Value |
|-------|-------|
| Framework | XGBoost 1.7+ |
| Objective | `binary:logistic` |
| n_estimators | 500 |
| max_depth | 4 |
| learning_rate | 0.01 |
| subsample | 0.8 |
| colsample_bytree | 0.7 |
| Calibration | `CalibratedClassifierCV(method='isotonic')` |

### Feature Groups
- Rolling window features (L7/L14/L30)
- EMA features (span 5, 10)
- Context modifier outputs (fatigue_index, run_env_multiplier, k_rate_modifier)
- Matchup features (handedness, park factor)

### Performance Benchmarks (10-Season Backtest — PR #96)
| Metric | Value |
|--------|-------|
| AUC-ROC | 0.634 |
| Brier Score | 0.228 |
| Over-threshold Accuracy (≥0.55) | 61.2% |
| ROI @ 0.55 threshold | +8.4% |
| Bet Frequency | 18.3% of eligible props |
| Sharpe Ratio | 1.21 |
| Max Drawdown | -14.2% |

### Calibration Notes
- Isotonic calibration applied post-training on held-out 20% validation set
- Calibration checked monthly: if Brier Score drifts >0.015 from baseline, retrain triggered
- Expected calibration error (ECE) target: <0.05

### Maintenance
- Retrain trigger: Brier drift OR ≥200 new labeled examples OR season boundary
- Feature drift monitoring: PSI >0.2 on any top-5 feature triggers alert

---

## Model 2 — XGBStrikeoutModel

**File:** `api/services/prop_model.py` → `XGBStrikeoutModel`  
**Status:** 🟡 Staging (PR #100)  
**Deployed:** PR #100 (pending merge)

### Architecture
| Param | Value |
|-------|-------|
| Framework | XGBoost 1.7+ |
| Objective | `binary:logistic` |
| n_estimators | 500 |
| max_depth | tuned via GridSearchCV [3,4,5] |
| learning_rate | tuned via GridSearchCV [0.01, 0.05, 0.1] |
| subsample | 0.8 |
| colsample_bytree | 0.7 |
| Hyperparameter Tuning | `GridSearchCV(cv=5, scoring='accuracy')` |
| Early Stopping | 50 rounds on validation set |
| Calibration | `CalibratedClassifierCV(method='isotonic')` |

### Feature Groups (Enhanced)
- All features from Model 1 plus:
- **Plate Discipline:** O-Swing%, Z-Swing%, SwStr%, Contact%, Chase Rate, Whiff Rate
- **Pitcher Clustering:** cluster_label, cluster_dist (k=5 KMeans)
- **Matchup Encoding:** platoon_adv, hist_k_rate_vs_L/R

### Expected Performance Improvement (vs Model 1 on K props)
| Metric | Baseline RF | XGB Standalone | Ensemble |
|--------|------------|----------------|---------|
| AUC-ROC | 0.618 | ~0.651 | ~0.659 |
| Brier Score | 0.241 | ~0.221 | ~0.217 |
| ROI @ optimal threshold | +6.1% | ~+9.8% | ~+11.2% |

*Note: Estimated from feature importance analysis. Confirmed benchmarks pending production backtest run.*

### Maintenance
- GridSearchCV re-runs each season with updated param grid if needed
- Calibration validated against held-out season (no data leakage: calibration set must post-date training set by ≥1 month)

---

## Model 3 — RandomForestStrikeoutModel

**File:** `api/services/prop_model.py` → `RandomForestStrikeoutModel`  
**Status:** 🟡 Staging (PR #100)  
**Deployed:** PR #100 (pending merge)

### Architecture
| Param | Value |
|-------|-------|
| Framework | scikit-learn RandomForestClassifier |
| n_estimators | 300 |
| max_depth | None (full depth) |
| min_samples_leaf | 5 |
| Calibration | `CalibratedClassifierCV(method='isotonic')` |

### Role in System
- **Baseline comparator** in `StrikeoutBacktester` 3-way comparison
- Base learner in `EnsemblePropModel` (40% weight in `average` mode)
- Provides feature importance as sanity check against XGBoost

### Performance
- Simpler than XGBoost, trains 3× faster
- AUC typically 0.02–0.04 below XGBoost on K props
- More stable under small sample sizes (early season)

---

## Model 4 — EnsemblePropModel

**File:** `api/services/prop_model.py` → `EnsemblePropModel`  
**Status:** 🟡 Staging (PR #100)  
**Deployed:** PR #100 (pending merge)

### Architecture
Three ensembling modes selectable at instantiation:

#### Mode A: `average`
```
final_prob = w_xgb × prob_xgb + w_rf × prob_rf
# Default weights: XGB=0.6, RF=0.4 (auto-detected via cross-val AUC)
```

#### Mode B: `stack`
```
OOF predictions from k=5 cross-validation
Meta-learner: LogisticRegression(C=1.0)
Trained on OOF probability matrix → final calibrated probability
```

#### Mode C: `bagging`
```
n_bags = 10 bootstrap resamplings
Each bag: train RF+XGB on bootstrap sample
Final: average of 20 model predictions (10 bags × 2 models)
```

### When to Use Each Mode
| Mode | Best For | Tradeoff |
|------|----------|----------|
| `average` | Production inference (fast) | Slightly lower ceiling than stacking |
| `stack` | Season-level training (slow) | Best accuracy, requires large dataset (>5000 rows) |
| `bagging` | Uncertainty estimation | Variance reduction, 10× training cost |

### Maintenance
- Re-evaluate mode selection each season with backtest comparison
- `stack` mode requires careful temporal cross-validation — never use random K-fold (leakage risk)

---

## Model 5 — UnderdogMathEngine

**File:** `underdog_math_engine.py`  
**Status:** 🟢 Production  
**Deployed:** PR #87

### Architecture
- **Not an ML model** — deterministic payout table lookup + Kelly calculation
- `SlipEvaluation` NamedTuple: `(ev_pct, payout, recommended_entry_type, unit_size, flex_payout, standard_payout)`

### Payout Tables
| Legs | FLEX Multiplier | STANDARD Multiplier |
|------|----------------|-------------------|
| 2 | 3.0× | 3.0× |
| 3 | 5.0× | 6.0× |
| 4 | 10.0× | 10.0× |
| 5 | 20.0× | 20.0× |
| 6 | 40.0× | 40.0× |

FLEX advantage: survives 1 push/miss. Entry type recommendation logic:
- If `flex_ev > standard_ev + 0.02`: recommend FLEX
- Otherwise: recommend STANDARD

### EV Formula
```
EV_standard = prob_all_hit × standard_payout - 1
EV_flex     = prob_5_of_6   × flex_payout    - 1   # (for 6-leg)
```

---

## Model 6 — ArbitrageDetector (MarketFusionEngine)

**File:** `api/services/market_fusion.py`  
**Status:** 🟢 Production  
**Deployed:** PR #99

### Architecture
- **Rule-based arbitrage scanner** — not ML
- Compares no-vig probabilities across ≥2 providers
- Fires when: `(1/over_prob_book1) + (1/under_prob_book2) < 1.0 + margin_gate`

### Gate
- **0.5% minimum margin** required to trigger ArbitrageAgent
- **≥2 providers** required (prevents single-book data artefacts)

---

## Model Performance Monitoring

### Alert Thresholds
| Metric | Warning | Critical |
|--------|---------|---------|
| Brier Score drift | >0.015 from baseline | >0.025 |
| AUC-ROC drop | >0.02 vs 30-day rolling | >0.04 |
| Feature PSI | >0.20 on top-5 features | >0.35 |
| Kelly fraction avg | >0.07 | >0.09 |
| Win rate (30-day) | <52% | <49% |
| CLV (30-day rolling) | <+0.5% | <0% |

### Retraining Schedule
| Model | Trigger | Frequency |
|-------|---------|-----------|
| PlayerPropXGBoost | Brier drift OR season boundary | ~Monthly |
| XGBStrikeoutModel | GridSearchCV each season | Pre-season |
| EnsemblePropModel | When any base model retrains | ~Monthly |
| Calibration layer | Independent of model | Monthly |
