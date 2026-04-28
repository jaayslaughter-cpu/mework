# PropIQ — Full Pipeline Process

Every step from Railway startup to Discord pick, in order.

---

## 1. Startup (Railway deploys `python orchestrator.py`)

```
orchestrator.py → lifespan()
```

1. **Migration runner** — scans `migrations/V*.sql`, applies any not yet in `migration_history`. V36 and V37 run here on first deploy after they were added.
2. **Scheduler starts** — all jobs registered:
   - `job_data_hub` every 15s
   - `job_agents` every 30s
   - `job_predict_plus_prefetch` at 8:15 AM PT
   - `job_streak` at 8:45 AM PT
   - `job_log_watcher` at 9:15 AM PT
   - `job_line_stream` every 30min 10AM–10PM PT
   - `job_xgboost` at 2:30 AM PT
   - `job_grading` at 2:00 AM PT
   - `job_settle` at 11:00 PM PT
3. **Discord startup ping** — fires once per PT calendar day.
4. **Initial DataHub pull** — `job_data_hub()` called immediately.

---

## 2. DataHub (every 15 seconds)

```
orchestrator.py → job_data_hub() → tasklets.run_data_hub_tasklet() → Redis hub
```

Staggered across 4 TTL-gated groups. Each group only re-fetches when its Redis key expires.

### Group 1 — Physics / Arsenal (TTL 15 min)
- **`steamer_layer.prefetch()`** — Steamer 2026 projections (FanGraphs → mlb_stats_layer fallback → pybaseball fallback). Cached to Postgres `fg_cache`.
- **`mlb_stats_layer.warm_cache(hub)`** — Loads today's probable starters + pitcher stats from `statsapi.mlb.com`. Always works on Railway.
- **`fangraphs_layer._load()`** — Supplements with CSW%, xFIP, SIERA if not 403-blocked.
- **`game_prediction_layer.get_game_predictions()`** — Win probabilities per game.
- **`fangraphs_layer.get_pitcher()`** per probable starter — `pitch_arsenal` dict (K%, BB%, CSW%, SwStr%, xFIP). Written to `hub["physics"]["pitch_arsenal"]`.
- **`bvi_layer.get_team_bvi()`** — Bullpen Volatility Index per team (impact × inherited × fatigue). Written to `hub["physics"]["bullpen_bvi"]`.

### Group 2 — Context / Environment (TTL 5 min)
- **`_fetch_weather_today()`** — Open-Meteo free API. Wind speed/direction/temp per stadium.
- **`_fetch_mlb_lineups_today()`** — Today's confirmed batting orders.
- **`_fetch_mlb_probable_starters()`** — SP for each game.
- **`_fetch_mlb_standings()`** — Team win% / run differential.
- **`lock_time_gate.fetch_game_times_today()`** — First pitch times per team from MLB Stats API. Each entry includes:
  - `game_time_utc` — ISO 8601
  - `game_time_pt` — `"HH:MM"` string in PT (e.g. `"15:10"`) ← used by dispatch window
  - `abstract_state` — Scheduled / InProgress / Final
- All written to `hub["context"]`.

### Group 3 — Market / Sharp Steam (TTL 5 min)
- **`action_network_layer.fetch_mlb_game_sentiment()`** — Public bet % / money % per game.
- **`action_network_layer.build_sharp_report()`** — Player-prop ticket% / money% (requires AN PRO token).
- **`_odds_api_get()`** — Game-level h2h/spreads/totals from OddsAPI. **Daily cache (12h TTL)** — only 2 real API calls per day.
  - Fallback: PropOdds API (`PROP_ODDS_API_KEY`) → Covers → DraftEdge → VegasInsider
- **`sportsbook_reference_layer.build_sportsbook_reference()`** — Player prop vig-stripped implied probs from DK/FD/BetMGM. Daily file cache + Postgres.
- Written to `hub["market"]`.

### Group 4 — DFS Props (TTL 2 min)
- **`_fetch_underdog_props_direct()`** — Live UD props (beta/v5/over_under_lines). 1539 pre-game props typical.
- **`_fetch_prizepicks_direct()`** — Live PP props (partner-api). 255 props typical.
- Sleeper fallback if both return 0.
- Written to `hub["dfs"]`.

### Hub merge
All 4 groups merged into a single Redis key (`hub:merged`) read by agents via `read_hub()`.
Also updates `game_states` dict (ESPN game status per game_id) and `xgb_sample_counts` (training row counts per prop type).

---

## 3. Predict+ Prefetch (8:15 AM PT daily)

```
orchestrator.py → job_predict_plus_prefetch() → predict_plus_layer.PredictPlusScorer.prefetch()
```

- Reads today's pitcher MLBAM IDs from `hub["player_props"]`.
- For each pitcher: checks Redis L1 cache (`pp_pitches_{mlbam_id}_{year}w{week}`) → Postgres L2 → live Savant CSV fetch.
- Fits LogisticRegression model on pitch-level data, computes Predict+ score (mean=100, SD=10).
- Score stored in weekly Postgres cache. `_get_predict_plus_adj()` in enrichment reads it.

---

## 4. Dispatch Window Check (every 30 seconds)

```
orchestrator.py → job_agents() → tasklets.run_agent_tasklet()
```

Two independent gates, both must pass:

**Gate 1 — Orchestrator (fast, before spinning up thread):**
```python
open_pt  = now.replace(hour=8, minute=30)          # 8:30 AM PT
game_times = hub["context"]["game_times"]
earliest_pt = min(gt["game_time_pt"] for gt in game_times.values()
                  if gt["abstract_state"] == "Scheduled")
cutoff_pt = earliest_pt - 30min                     # e.g. "15:10" → "14:40"
# fallback cutoff: "12:30" if no game_time_pt data
skip if now < open_pt or now >= cutoff_pt
```

**Gate 2 — AgentTasklet (inside thread, same logic):**
- Identical window check using `hub["context"]["game_times"]["game_time_pt"]`.
- Additional game-state check: if all games Final/Postponed → force fresh ESPN fetch before skipping.

**Result for today (Apr 28, first pitch ~3:10 PM PT):**
- Window open: 8:30 AM PT
- Window close: 2:40 PM PT
- Runway: ~6 hours

---

## 5. Props Fetch + LockGate Filter

```
tasklets.run_agent_tasklet() → _get_props(hub) → lock_time_gate._stamp_prop()
```

1. Raw props from `hub["dfs"]["underdog"]` + `hub["dfs"]["prizepicks"]`.
2. **LockGate** stamps each prop with `game_time_utc`, `lookahead_safe`, skips props where `abstract_state` is Live/Final.
3. **Injury filter** — removes props where `_skip_injury=True` (IL players).
4. Props with no matching game time → kept (safety default: treat as pre-game).

---

## 6. Prop Enrichment

```
tasklets.run_agent_tasklet() → _enrich_props(props, hub) → prop_enrichment_layer.enrich_props()
```

Each prop gets stamped with ~40 fields used by agents and XGBoost:

| Field | Source | Used by |
|---|---|---|
| `k_rate`, `bb_rate`, `era`, `whip` | `fangraphs_layer.get_pitcher()` | `_F5Agent`, XGBoost |
| `csw_pct`, `swstr_pct`, `xfip` | `fangraphs_layer` / `mlb_stats_layer` | `_F5Agent`, Predict+ |
| `_lineup_chase_adj`, `_opp_avg_k_pct` | `lineup_chase_layer.get_lineup_chase_score()` | `_F5Agent`, pa_model |
| `_player_specific_prob` | `pa_model.prop_matchup_prob()` (K props), base_rate_model | `_model_prob` fallback |
| `_predict_plus_adj` | `predict_plus_layer.get_score()` | `_model_prob` base-rate path |
| `_marcel_adj` | `marcel_layer.get_marcel()` | `_model_prob` base-rate path |
| `_park_factor` | `park_factors.get_park_factor()` | `_WeatherAgent`, XGBoost |
| `_wind_speed`, `_wind_direction`, `_temp_f` | `hub["context"]["weather"]` | `_WeatherAgent` |
| `_batter_hand` | `mlb_stats_layer` | `_WeatherAgent` LHH boost |
| `_batters_faced`, `_pitches_thrown`, `_arsenal_size` | `mlb_stats_layer` live feed | `_F5Agent` TTOP |
| `sb_implied_prob` | `sportsbook_reference_layer` | `_LineDriftAgent`, `_get_sharp_consensus()` |
| `sb_implied_prob_over/under` | same | XGBoost feature vector |
| `_bvi_score` | `bvi_layer.get_team_bvi()` | `_BullpenAgent` |
| `game_over_prob` | `game_prediction_layer` | `_model_prob` game env nudge |
| `_bernoulli_meltdown` | Bernoulli pitcher state | `_model_prob` meltdown cap |
| `over_american`, `under_american` | UD/PP fetch + `sportsbook_reference_layer` | All agents EV calc |

---

## 7. pa_model Matchup Probability (K props)

```
prop_enrichment_layer._player_specific_rate() → pa_model.odds_ratio_blend()
```

For `strikeouts` props specifically:

```
batter_k_rate = prop["_opp_avg_k_pct"]  ← from lineup_chase_layer
pitcher_k_rate = prop["k_rate"]          ← from fangraphs_layer
league_k = LEAGUE_RATES["K"] = 0.223

blended_k_pa = (batter_k_rate × pitcher_k_rate) / league_k
lambda = blended_k_pa × 22  ← 22 BF = 2025 empirical avg start
P(K ≥ line) = 1 - Poisson.CDF(line-1, lambda)
```

Stored as `prop["_player_specific_prob"]`. `_model_prob()` reads this in the base-rate path (when XGBoost is unavailable or sample count < threshold).

---

## 8. Model Probability (_model_prob)

```
_BaseAgent._model_prob(player, prop_type, prop)
```

Priority chain:

1. **SimEngine** (`simulation_engine.py`) — Monte Carlo PA simulation. Returns `prob_over` with variance penalty. *(If `_SIM_ENGINE_AVAILABLE`)*
2. **XGBoost** — 27-feature vector (pitcher stats, batter stats, weather, park, implied odds, season weight). Requires `xgb_sample_counts[prop_type] > 0`.
3. **`generate_pick` 5-stage pipeline** — `_player_specific_prob` override if set, else base_rate_model.
4. **`base_rate_model` + adjustments** — Combines:
   - `_player_specific_prob` (pa_model for K, historical rate for others)
   - `_predict_plus_adj` (Predict+ score delta)
   - `_marcel_adj` (Marcel regression-to-mean)
   - `_park_factor_adj` (park_factors.py canonical table)
   - `game_env_nudge` (game total × 4.5pp calibrated multiplier)
   - `_last10_adj` (rolling 10-game form)
   - Brier calibration governor (shrinks overconfident probs toward 50%)
5. **Absolute fallback** — 50% + Brier governor.

---

## 9. Agent Evaluation

```
run_agent_tasklet() → [agent.evaluate(prop) for prop in props]
```

10 independent agents, each with its own signal logic:

| Agent | Primary Signal | Side Logic |
|---|---|---|
| `_EVHunterAgent` | XGBoost EV vs sportsbook implied | Both — best EV wins |
| `_UnderMachineAgent` | Under-side specialist (contact props) | Under only (by design) |
| `_UmpireAgent` | HP umpire K-rate delta vs league avg | Both |
| `_F5Agent` | Pitcher K/ERA + TTOP decay (Tango/Brill 2023) | Both |
| `_FadeAgent` | Public bet% extremity (scaled boost, not flat) | Both — fades heavy Over OR Under |
| `_LineValueAgent` | Steam: ticket% ≥70% + RLM confirmation | Both |
| `_BullpenAgent` | BVI score (continuous linear, not step function) | Both |
| `_WeatherAgent` | Temp + wind along spray axis + hr_factor from park_factors.py | Both |
| `_ChalkBusterAgent` | Contrarian: fades extreme chalk both sides | Both |
| `_SharpFadeAgent` | RLM divergence (line moved against public) | Both — shrinks toward 50% by signal strength |

Each agent calls `_model_prob()` → applies its own adjustment → checks EV threshold → returns a `bet` dict or `None`.

**EV calculation** (all agents):
```python
ev_pct = (model_prob / 100 - implied_prob) / implied_prob
# implied_prob from _american_to_implied(over/under_american)
# None/0 guard: returns 52.4 (≡ -110) for invalid odds
```

**Sharp consensus gate** (applied after each agent hit):
```python
sharp_prob = _get_sharp_consensus(hub, player, prop_type)
# reads from sportsbook_reference_layer (OddsAPI → PropOdds → Covers → DraftEdge)
edge = sharp_prob - _american_to_implied(ud_odds)
if edge < MIN_EV_THRESH * 100: skip  # sportsbook says no edge
```

---

## 10. Parlay Assembly + Dedup

```
run_agent_tasklet() → _build_agent_parlays(agent_hits, agent_name)
```

1. Each agent builds 2–3 leg slips from its hits. Legs must be from different teams (correlation protection).
2. **Cross-agent dedup** — identical leg sets removed regardless of which agent built them (sorted by EV, highest kept).
3. **Player appearance cap** — max 2 slips per player per cycle.
4. **Platform purity** — all legs must be same platform (UD or PP, not mixed).
5. **Entry type** — `FLEX` / `POWER` / `GOBLIN` / `DEMON` based on leg count and platform.

---

## 11. Bet Queue + Dedup Guard

```
run_agent_tasklet() → Redis "bet_queue" → DB-backed dedup
```

1. Each parlay pushed to `Redis["bet_queue"]` (max 500).
2. **DB-backed dedup preload** — on startup, loads today's already-sent fingerprints from `bet_ledger` (`discord_sent=TRUE`). Prevents resend after Railway crash + restart.
3. **In-memory dedup** — `_AGENT_SENT_TODAY[agent]` per PT calendar day.
4. Fingerprint = `frozenset((player, prop_type, side) for leg in legs)`.

---

## 12. Discord Send

```
run_agent_tasklet() → discord_alert.send_parlay_slip(parlay)
```

1. Formats slip with agent name, legs, odds, EV%, model prob, recommended platform.
2. **`bet_ledger` INSERT** — written at send time with `discord_sent=TRUE` baked in.
3. Discord webhook POST. On failure: logged at ERROR with full traceback.
4. **Kelly sizing** — `kelly_units = ev_pct / (1 / implied - 1)` × 0.25 (quarter-Kelly). Dollar amount from `agent_unit_sizing` table.

---

## 13. Streak Pick (8:45 AM PT daily)

```
orchestrator.py → job_streak() → streak_agent.run_streak_pick()
```

Independent of the main agent dispatch. Uses Underdog Streaks format ($10 → $10,000 via 11 consecutive wins).

1. Fetches UD props directly (not from hub).
2. For each qualifying prop: computes `streak_confidence = prob_score + ev_bonus + signal_bonus` (gate: ≥ 6.0).
3. **`STREAK_CONF_MIN = 6.0`** — lowered from 7.0 (7.0 was mathematically unreachable for `hits Over 0.5` at prob=0.62).
4. `select_start_picks` requires 2 picks from **different teams**.
5. Posts to Discord with streak ID and current win count.

---

## 14. Nightly Maintenance (2 AM PT)

```
job_grading() → GradingTasklet → boxscore settlement + CLV calc
job_xgboost() → XGBoostTasklet → retrain on bet_ledger (discord_sent=TRUE rows)
```

**XGBoost retraining** reads `xgb_sample_counts` per prop type:

| Prop type | Sample count | XGBoost quality |
|---|---|---|
| `hits`, `total_bases`, `hitter_strikeouts`, `hits_runs_rbis` | ~44,000–67,000 | Strong |
| `strikeouts`, `earned_runs`, `pitching_outs`, `walks_allowed` | ~11,000 | Good |
| `runs` | 1 ← **needs historical_seed re-run** | Falls back to pa_model |
| `hits_allowed` | 1 ← **needs historical_seed re-run** | Falls back to pa_model |

**To fix `runs=1` and `hits_allowed=1`**: run `python historical_seed.py` once after deploy. Both prop types are now in `PITCHER_LINES` and `BATTER_LINES`.

---

## 15. Settlement (11 PM PT)

```
job_settle() → subprocess: nightly_recap.py → settlement_engine.settle_parlay()
```

1. Fetches final box scores from ESPN.
2. Grades each leg (WIN/LOSS/PUSH) against actual stat.
3. Updates `bet_ledger.result` and `bet_ledger.actual_outcome`.
4. Posts settlement recap to Discord.
5. CLV engine calculates closing line value vs final market price.

---

## Key Data Flow Summary

```
Railway startup
    └─ Migrations (V36, V37)
    └─ Scheduler

Every 15s: DataHub
    ├─ MLB Stats API → pitcher stats → hub["physics"]
    ├─ lock_time_gate → game_time_pt → hub["context"]["game_times"]
    ├─ OddsAPI (daily cache) → hub["market"]["odds"]
    ├─ sportsbook_reference_layer → hub["market"]["sb_reference"]
    └─ UD/PP direct fetch → hub["dfs"]

8:15 AM: Predict+ prefetch
    └─ Savant CSV per pitcher → LogReg model → weekly Postgres cache

8:30 AM: Dispatch window OPENS
    └─ game_time_pt used to compute cutoff = first_pitch - 30min

Every 30s inside window: AgentTasklet
    ├─ Props from hub["dfs"] → LockGate filter → injury filter
    ├─ prop_enrichment_layer
    │   ├─ fangraphs stats stamped on each prop
    │   ├─ pa_model odds-ratio → _player_specific_prob (K props)
    │   └─ sb_implied_prob from sportsbook_reference_layer
    ├─ _model_prob: SimEngine → XGBoost → generate_pick → base_rate+pa_model
    ├─ 10 agents evaluate independently
    ├─ sharp consensus gate (_get_sharp_consensus)
    ├─ parlay assembly + dedup
    └─ Discord send + bet_ledger INSERT

8:45 AM: Streak pick (independent)
first_pitch - 30min: Dispatch window CLOSES
2:00 AM: Grading
2:30 AM: XGBoost retrain
11:00 PM: Settlement
```
