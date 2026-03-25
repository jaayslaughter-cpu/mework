# PropIQ Feature Engineering Cookbook

> Codified techniques for building, validating, and extending the feature set.  
> Each recipe includes rationale, implementation notes, and gotchas.

---

## Recipe 1 — Rolling Window Features

**Goal:** Capture recent-form trends without over-weighting noisy single-game outcomes.

### Implementation
```python
# ml_pipeline.py → FeatureEngineer.add_rolling_features()
windows = [7, 14, 30]  # calendar days
for w in windows:
    df[f"k_per_9_l{w}"] = (
        df.groupby("player_id")["k_per_9"]
          .transform(lambda s: s.rolling(w, min_periods=3).mean())
    )
```

### Rationale
- L7 captures current hot/cold streak
- L14 is the primary signal window (≈2 starts)
- L30 anchors regression to the mean
- `min_periods=3` prevents NaN explosion early in season

### Gotchas
- **Date gaps:** Rolling by calendar day, not start count — use `.rolling('7D')` on DatetimeIndex for correctness
- **Pitcher changes:** Re-compute when player_id transfers teams mid-season
- **Spring training:** Exclude pre-season rows (`game_date < season_start`)

---

## Recipe 2 — Exponential Moving Averages

**Goal:** Down-weight old performance while keeping full history in signal.

### Implementation
```python
# ml_pipeline.py → FeatureEngineer.add_ema_features()
spans = {"ema_5": 5, "ema_10": 10}
for name, span in spans.items():
    df[f"k_rate_{name}"] = (
        df.groupby("player_id")["k_per_9"]
          .transform(lambda s: s.ewm(span=span, adjust=False).mean())
    )
```

### Rationale
- EMA naturally handles the recency bias problem
- `span=5` reacts fast (good for hot streaks); `span=10` is smoother
- Together they form a "fast/slow" signal pair (MACD-inspired)

### Gotchas
- `adjust=False` matches pandas online EMA (important for streaming inference)
- EMA is undefined for a single data point — ensure `≥2` rows per player before feature extraction

---

## Recipe 3 — No-Vig True Probability

**Goal:** Strip bookmaker margin to get market's true implied probability.

### Implementation
```python
# odds_math.py → strip_vig()
def _american_to_decimal(american: int) -> float:
    if american > 0:
        return american / 100 + 1
    return 100 / abs(american) + 1

def strip_vig(over_price: int, under_price: int) -> tuple[float, float]:
    over_dec  = _american_to_decimal(over_price)
    under_dec = _american_to_decimal(under_price)
    raw_over  = 1 / over_dec
    raw_under = 1 / under_dec
    total_vig = raw_over + raw_under           # > 1.0 = the juice
    return raw_over / total_vig, raw_under / total_vig
```

### Rationale
- Books shade lines toward 50/50 to extract juice — raw implied probs sum >1
- Divide by total to normalize to true probability space
- Required for any EV calculation: `EV = (true_prob × payout) - 1`

### Gotchas
- Never use raw implied prob for EV — always strip vig first
- Correlated markets (same game) need correlated probability adjustment — handled in `UnderdogValidator`
- **3% EV gate** is hard minimum: `ev_pct ≥ 0.03` before any alert fires

---

## Recipe 4 — Pitcher Clustering

**Goal:** Group pitchers into archetypes to improve matchup feature generalization.

### Implementation
```python
# api/services/mlb_data.py → PitcherClusterer
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

CLUSTER_FEATURES = ["k_per_9", "bb_per_9", "whip", "fip", "gb_pct"]

scaler   = StandardScaler()
X_scaled = scaler.fit_transform(df[CLUSTER_FEATURES])
kmeans   = KMeans(n_clusters=5, n_init=20, random_state=42)
labels   = kmeans.fit_predict(X_scaled)

# Distance to centroid as a continuous feature
dists = np.linalg.norm(X_scaled - kmeans.cluster_centers_[labels], axis=1)
df["cluster_label"] = labels
df["cluster_dist"]  = dists
```

### Rationale
- Cluster label gives XGBoost a high-level "pitcher type" without hand-crafting logic
- `cluster_dist` captures how "pure" the pitcher is for that type (outliers = higher dist)
- `n_init=20` prevents local minima in KMeans

### Gotchas
- **Refit on new data each season** — archetypes shift (e.g., "opener" as new cluster)
- Fix `random_state` for reproducibility across training runs
- `StandardScaler` params must be serialized with the model — never scale inference data with fresh scaler

---

## Recipe 5 — Plate Discipline Encoding

**Goal:** Capture batter/pitcher swing tendencies for strikeout prediction.

### Implementation
```python
# api/services/mlb_data.py → PlateDisciplineEncoder
DISC_COLS = ["o_swing_pct", "z_swing_pct", "swstr_pct",
             "contact_pct", "chase_rate", "whiff_rate"]

# Temporal weights: last 15 games = 2×
n_games = len(player_df)
weights = np.ones(n_games)
weights[-15:] = 2.0
weights /= weights.sum()

for col in DISC_COLS:
    df.loc[mask, f"{col}_wtd"] = np.average(
        player_df[col].fillna(player_df[col].median()), weights=weights
    )
```

### Rationale
- SwStr% is the single strongest K predictor (r² ≈ 0.72 with K rate)
- O-Swing% captures "free swing" tendencies critical for K+ props
- Temporal weighting down-weights early-season data (different sequencing, weather)

### Gotchas
- **Missing Statcast data** (< 2021): Fall back to season-level aggregates, log warning
- Chase rate and O-Swing% are correlated (~0.85) — include both but expect some feature importance dilution
- Z-Swing% alone is not predictive; its interaction with SwStr% is the signal

---

## Recipe 6 — Handedness Matchup Encoding

**Goal:** Encode platoon advantages without creating sparse one-hot blowup.

### Implementation
```python
# api/services/mlb_data.py → MatchupEncoder
def _platoon_advantage(batter_hand: str, pitcher_hand: str) -> float:
    """Same-hand = pitcher advantage (-1), cross-hand = batter advantage (+1)."""
    if batter_hand == "S":     # switch hitter: neutral
        return 0.0
    return 1.0 if batter_hand != pitcher_hand else -1.0
```

### Rationale
- Platoon splits are systematic and stable year-over-year
- Switch hitters get 0 (they self-neutralize) — don't assign arbitrary value
- Continuous encoding avoids one-hot matrix explosion for XGBoost

### Gotchas
- **Switch hitters vs LHP:** Often have weaker L-side splits — check player-specific overrides
- Historical K rate by matchup type needs ≥50 PA sample to be reliable — apply shrinkage toward league mean for small samples: `hist_k = (pa * player_k + 50 * league_k) / (pa + 50)`

---

## Recipe 7 — Fatigue Index Computation

**Goal:** Quantify bullpen stress to predict run environment and K-rate suppression.

### Implementation
```python
# context_modifiers.py → BullpenFatigueScorer
def compute_fatigue_index(appearances: list[dict]) -> float:
    score = 0.0
    for app in appearances:
        days_ago  = app["days_ago"]           # 0 = today, 1 = yesterday
        pitches   = app["pitches"]
        leverage  = app["leverage_index"]     # 0.5–3.0
        decay     = 0.7 ** days_ago           # exponential decay per day
        score    += (pitches / 30.0) * leverage * decay
    return min(score / 5.0, 1.0)             # normalize to [0,1]
```

### Rationale
- 30-pitch threshold = typical "used" reliever
- `0.7 ** days_ago` means yesterday = 70% weight, 2 days ago = 49%, 3 days = 34%
- Leverage multiplier ensures high-leverage extras weight 2–3× more than mop-up

### Gotchas
- **Day games after night games** have 0 days_ago for both games — use hour gap, not calendar day
- Closers rest patterns: `closer_available = bool(high_leverage_appearances_l3 == 0)`
- Gate: BullpenAgent only fires when `fatigue_index ≥ 0.70`

---

## Recipe 8 — Kelly Criterion Bet Sizing

**Goal:** Maximize long-run bankroll growth without over-betting on correlated positions.

### Implementation
```python
# api/services/risk_management.py → kelly_fraction()
def kelly_fraction(prob: float, decimal_odds: float) -> float:
    """Full Kelly: f* = (p*b - q) / b"""
    b = decimal_odds - 1          # net profit per unit
    q = 1 - prob                  # probability of loss
    f = (prob * b - q) / b
    f = max(f, 0.0)               # no negative bets
    return min(f * 0.5, 0.10)    # ½ Kelly, hard cap at 10%

# Correlation penalty for same-game legs
def portfolio_kelly(slips: list[SlipEvaluation]) -> dict[str, float]:
    sizes = {}
    for slip in slips:
        base = kelly_fraction(slip.ev_pct, slip.payout)
        penalty = sum(
            0.20 for other in slips
            if other is not slip and other.game_id == slip.game_id
        )
        sizes[slip.slip_id] = max(base - penalty, 0.0)
    return sizes
```

### Rationale
- Full Kelly is theoretically optimal but too volatile — ½ Kelly balances growth vs drawdown
- 10% hard cap prevents ruin from calibration errors at tail probabilities
- Correlation penalty (−20% per same-game co-leg) prevents over-concentration

### Gotchas
- **Never use uncalibrated ML probabilities** in Kelly — must be isotonic-calibrated first
- Kelly assumes independent bets; parlays require modified Kelly with combined probability
- Re-compute Kelly after each graded slip — bankroll denominator changes

---

## Recipe 9 — CLV (Closing Line Value) Calculation

**Goal:** Measure whether we beat the closing line (the ultimate edge validation metric).

### Implementation
```python
# api/services/market_fusion.py → MarketFusionEngine.compute_clv()
def compute_clv(open_price: int, close_price: int, side: str) -> float:
    """
    Positive CLV = we got better price than closing.
    side: 'over' or 'under'
    """
    open_prob  = _american_to_implied(open_price)
    close_prob = _american_to_implied(close_price)
    # Over: lower implied = better price (line moved in our favor)
    if side == "over":
        return close_prob - open_prob    # positive = line moved away (we got it cheap)
    else:
        return open_prob - close_prob
```

### Rationale
- CLV is the gold standard for long-run edge validation (beating close = true alpha)
- Positive CLV means: line moved in our predicted direction after bet placement
- Tracked in `backtest_tasklet.py` as primary evaluation metric alongside ROI

### Gotchas
- CLV requires storing open price at alert time — write to Redis with TTL=24h
- Vig at open vs close differs — strip vig from both before computing CLV
- Sample size: CLV is noisy under 500 bets — use rolling 60-day window for stability

---

## Recipe 10 — Steam Detection

**Goal:** Detect sharp money (steam) moving lines for follow-the-sharp strategy.

### Implementation
```python
# market_scanners.py → SteamScanner
VELOCITY_THRESH   = 2.0   # pts/min
BOOK_COUNT_THRESH = 3     # books moving same direction

def _compute_velocity(ticks: deque[OddsTick]) -> float:
    if len(ticks) < 2:
        return 0.0
    span_min = (ticks[-1].ts - ticks[0].ts) / 60
    if span_min < 0.1:
        return 0.0
    line_delta = abs(ticks[-1].line - ticks[0].line)
    return line_delta / span_min

def is_steam(ticks: deque[OddsTick], all_books: dict[str, OddsTick]) -> bool:
    velocity    = _compute_velocity(ticks)
    same_dir    = sum(1 for b in all_books.values() if moved_same_direction(b, ticks[-1]))
    return velocity >= VELOCITY_THRESH or same_dir >= BOOK_COUNT_THRESH
```

### Rationale
- Sharp money typically moves lines quickly at multiple books simultaneously
- Velocity alone can be noise — requiring 3+ books moving eliminates single-book outliers
- `OR` logic: either velocity OR book count can trigger (either is sufficient steam signal)

### Gotchas
- Redis sorted-set stores tick history — must call `_seed_from_redis()` on restart to restore history
- SteamAgent is capped at 3 legs MAX (higher velocity = lower correlation tolerance)
- Velocity threshold should be tuned by prop type: K props move faster than hits props
