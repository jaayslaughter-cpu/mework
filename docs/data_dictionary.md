# PropIQ Data Dictionary

> Last updated: 2026-03-21  
> Covers all feature groups used across ML Pipeline Tier 1, Context Modifiers Tier 3, and the MLBFeaturePipeline.

---

## 1. Raw Ingest Fields

### 1.1 Player Prop Line (from OddsLine / OddsFetcher)

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `player_id` | str | SportsData.io / Tank01 | Unique player identifier |
| `player_name` | str | SportsData.io | Full display name |
| `team` | str | SportsData.io | 3-letter team abbreviation |
| `opponent` | str | SportsData.io | Opposing team abbreviation |
| `game_id` | str | Tank01 | Unique game identifier (used for correlation blocking) |
| `game_date` | date | SportsData.io | Scheduled game date (ET) |
| `prop_type` | str | The Odds API | Prop market: `strikeouts`, `hits`, `total_bases`, `earned_runs`, `walks`, `home_runs` |
| `line` | float | The Odds API / SBR | Numerical prop line (e.g., 5.5 Ks) |
| `over_price` | int | The Odds API | American odds for Over side |
| `under_price` | int | The Odds API | American odds for Under side |
| `book` | str | The Odds API / SBR | Sportsbook name (e.g., `draftkings`, `pinnacle`, `fanduel`) |
| `timestamp` | datetime | System | UTC timestamp of line fetch |
| `is_f5` | bool | System | True if this is a First-5-Innings prop |

### 1.2 OddsTick (line movement tracking)

| Field | Type | Description |
|-------|------|-------------|
| `player_id` | str | Player identifier |
| `prop_type` | str | Prop market type |
| `line` | float | Line value at tick time |
| `over_price` | int | American odds at tick |
| `under_price` | int | American odds at tick |
| `book` | str | Source book |
| `ts` | float | Unix timestamp of tick |
| `velocity` | float | Computed: line pts moved / minute over last window |

---

## 2. Pitcher Feature Groups

### 2.1 Season-Level Pitcher Stats

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `era` | float | 0–15 | Earned Run Average |
| `whip` | float | 0.5–3.0 | Walks + Hits per IP |
| `k_per_9` | float | 0–20 | Strikeouts per 9 innings |
| `bb_per_9` | float | 0–10 | Walks per 9 innings |
| `fip` | float | 1–8 | Fielding Independent Pitching |
| `gb_pct` | float | 0–1 | Ground ball % |
| `hr_per_9` | float | 0–5 | Home runs allowed per 9 IP |
| `left_on_base_pct` | float | 0–1 | Strand rate (regression candidate) |
| `babip` | float | 0.2–0.4 | Batting average on balls in play |

### 2.2 Rolling Window Features (L7 / L14 / L30)

Suffix pattern: `{stat}_l{window}` (e.g., `k_per_9_l7`)

| Feature | Window | Description |
|---------|--------|-------------|
| `k_per_9_l7` | 7 days | K/9 over last 7 calendar days |
| `k_per_9_l14` | 14 days | K/9 over last 14 calendar days |
| `k_per_9_l30` | 30 days | K/9 over last 30 calendar days |
| `era_l14` | 14 days | ERA over rolling 14-day window |
| `whip_l14` | 14 days | WHIP over rolling 14-day window |
| `pitch_count_l7` | 7 days | Total pitches thrown over L7 |
| `innings_pitched_l7` | 7 days | IP over L7 (fatigue signal) |

### 2.3 Exponential Moving Averages

| Feature | Span | Description |
|---------|------|-------------|
| `k_rate_ema_5` | 5 starts | EMA of K rate (decayed 5-start span) |
| `k_rate_ema_10` | 10 starts | EMA of K rate (decayed 10-start span) |
| `era_ema_5` | 5 starts | EMA of ERA |

### 2.4 Pitcher Clustering (PitcherClusterer)

| Feature | Type | Description |
|---------|------|-------------|
| `cluster_label` | int | Assigned cluster (0–4, KMeans k=5) |
| `cluster_dist` | float | Euclidean distance to cluster centroid |

**Cluster definitions (typical):**
- **0 — Power Arms:** High K/9 (>10), moderate BB/9
- **1 — Contact Managers:** Low K/9 (<6), low BB/9, high GB%
- **2 — Groundball Sinkers:** High GB% (>55%), below-avg K/9
- **3 — Fly Ball Risk:** High HR/9, high GB% inverted
- **4 — Wild Cards:** High BB/9, unpredictable K

---

## 3. Plate Discipline Features (PlateDisciplineEncoder)

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `o_swing_pct` | float | 0–1 | O-Swing%: % pitches outside zone swung at |
| `z_swing_pct` | float | 0–1 | Z-Swing%: % pitches in zone swung at |
| `swstr_pct` | float | 0–0.5 | SwStr%: Swinging strikes / total pitches |
| `contact_pct` | float | 0–1 | Contact% on swings |
| `chase_rate` | float | 0–1 | O-Swing% (alias emphasizing out-of-zone chasing) |
| `whiff_rate` | float | 0–1 | Swinging strikes / swings |
| `zone_pct` | float | 0–1 | % pitches thrown in strike zone |
| `first_strike_pct` | float | 0–1 | % plate appearances starting 0-1 |

**Temporal weighting:** Recent 15 games weighted 2× vs rest of season in encoder.

---

## 4. Matchup Features (MatchupEncoder)

| Feature | Type | Description |
|---------|------|-------------|
| `batter_hand` | str | `L` or `R` (switch = `S`) |
| `pitcher_hand` | str | `L` or `R` |
| `platoon_adv` | float | +1.0 (batter advantage), 0.0 (neutral), -1.0 (pitcher advantage) |
| `hist_k_rate_vs_L` | float | Batter's K rate vs LHP (season) |
| `hist_k_rate_vs_R` | float | Batter's K rate vs RHP (season) |
| `platoon_obp_diff` | float | OBP split L vs R for batter |
| `platoon_slg_diff` | float | SLG split L vs R for batter |
| `hist_k_matchup` | float | Pitcher's K rate specifically vs this batter hand |

---

## 5. Context Modifier Features (Tier 3)

### 5.1 BullpenFatigueScorer

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `fatigue_index` | float | 0–1 | Composite bullpen fatigue: (days_since_rest × pitch_load) / norm_factor |
| `high_leverage_appearances_l3` | int | 0–10+ | High-leverage appearances in last 3 days |
| `closer_available` | bool | — | Whether primary closer is rested (≥2 days) |
| `bullpen_era_l7` | float | 0–20 | Bullpen ERA over last 7 days |

**Gate:** `fatigue_index ≥ 0.70` required for BullpenAgent slip inclusion.

### 5.2 WeatherParkAdjuster

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `wind_speed_mph` | float | 0–50 | Wind speed at game time |
| `wind_direction` | str | — | `in`, `out`, `cross`, `calm` |
| `temp_f` | float | 20–110 | Temperature at first pitch (°F) |
| `park_factor_hr` | float | 0.7–1.4 | Park HR factor (1.0 = neutral) |
| `run_environment_multiplier` | float | 0.8–1.3 | Composite run environment (temp + wind + park) |

**Gate:** `wind_speed_mph ≥ 15` required for WeatherAgent HR/TB/hits inclusion.

### 5.3 UmpireRunEnvironment

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `ump_k_rate` | float | 0–1 | Umpire historical K/PA rate |
| `ump_bb_rate` | float | 0–1 | Umpire historical BB/PA rate |
| `k_rate_modifier` | float | 0.8–1.2 | Multiplier applied to K probability (ump-adjusted) |
| `run_env_score` | float | 0–1 | Normalized run environment score for this ump |
| `ump_era_factor` | float | 0.85–1.15 | Historical ERA factor under this ump |

---

## 6. Market / Odds Features

| Feature | Type | Description |
|---------|------|-------------|
| `no_vig_prob` | float | True probability after vig removal (no-vig formula) |
| `implied_prob_over` | float | Raw implied probability of Over side |
| `implied_prob_under` | float | Raw implied probability of Under side |
| `sharp_consensus_prob` | float | Pinnacle-weighted consensus probability |
| `edge_pct` | float | `no_vig_prob - sharp_consensus_prob` (EV signal) |
| `clv_score` | float | Closing Line Value: price at bet vs closing price |
| `dislocation_score` | float | Inter-book price gap (from MarketFusionEngine) |
| `steam_velocity` | float | Line movement pts/min over last 30 min |
| `steam_book_count` | int | Number of books moving in same direction |
| `ticket_pct` | float | Public betting ticket % on Over |
| `money_pct` | float | Public betting money % on Over |
| `fade_signal` | float | `ticket_pct - money_pct` (sharp fade signal when >40%) |

---

## 7. Model Output Fields

| Field | Type | Range | Description |
|-------|------|-------|-------------|
| `ml_prob` | float | 0–1 | Calibrated XGBoost/RF/Ensemble probability |
| `ml_edge` | float | -1–1 | `ml_prob - no_vig_prob` (model vs market gap) |
| `ev_pct` | float | 0–0.5 | Expected value %: `(ml_prob × payout) - 1` |
| `kelly_fraction` | float | 0–0.1 | Kelly fraction (½ Kelly, capped at 10%) |
| `unit_size` | float | 0–5 | Recommended bet size in units |
| `recommended_entry_type` | str | — | `FLEX` or `STANDARD` (Underdog) |
| `slip_ev_pct` | float | 0–1 | Combined slip EV across all legs |
| `correlation_risk` | float | 0–1 | Game-id correlation penalty applied |

---

## 8. Validation Rules

| Feature | Min | Max | Action on Violation |
|---------|-----|-----|-------------------|
| `k_per_9` | 0 | 20 | Warning → clip |
| `era` | 0 | 15 | Warning → clip |
| `bb_per_9` | 0 | 10 | Warning → clip |
| `whip` | 0.5 | 3.0 | Warning → clip |
| `no_vig_prob` | 0.01 | 0.99 | Error → drop row |
| `ev_pct` | 0.03 | — | Gate: below threshold → no alert |
| `kelly_fraction` | 0 | 0.10 | Hard cap at 0.10 (½ Kelly) |
| `fatigue_index` | 0 | 1 | Clip |
| `wind_speed_mph` | 0 | 60 | Clip |
| Missingness | — | 30% | Error: fail validation pass |
