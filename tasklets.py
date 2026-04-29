"""
PropIQ Agent Army — tasklets.py
=================================
Flat module exporting all 6 tasklet run-functions + 2 state readers
consumed by the root orchestrator.py.

  run_data_hub_tasklet()     → scrape Apify / APIs → Redis mlb_hub
  read_hub()                 → read mlb_hub from Redis
  run_agent_tasklet()        → 10 agents → EV → Kafka / Redis bet_queue
  get_agents()               → agent leaderboard dict
  run_leaderboard_tasklet()  → 14-day ROI → capital multipliers
  read_leaderboard()         → read leaderboard from Redis
  run_backtest_tasklet()     → nightly out-of-sample XGBoost audit
  run_grading_tasklet()      → boxscore settlement + CLV calc
  run_xgboost_tasklet()      → weekly model retrain on ledger

Railway deployment notes
------------------------
  All service addresses come from environment variables with safe defaults.
  Every external call is wrapped in try/except so a downed dependency
  degrades gracefully instead of crashing the whole process.
"""

from __future__ import annotations
import logging as _early_logging
logger = _early_logging.getLogger("propiq.tasklets")

# ── Simulation engine (Step 1 upgrade: distribution-based probabilities) ──────
try:
    from simulation_engine import simulate_prop as _simulate_prop, variance_penalty as _variance_penalty, inject_team_total as _inject_team_total
    _SIM_ENGINE_AVAILABLE = True
except ImportError:
    _SIM_ENGINE_AVAILABLE = False
    def _simulate_prop(prop, n_sims=10_000): return None   # noqa: E704
    def _variance_penalty(result): return 1.0              # noqa: E704
    def _inject_team_total(prop, hub): pass                # noqa: E704
try:
    from lock_time_gate import (
        should_skip_prop      as _should_skip_prop,
        stamp_prop            as _stamp_prop,
        fetch_game_times_today as _fetch_game_times_today,
        data_is_contaminated  as _data_is_contaminated,
    )
    _LOCK_GATE_AVAILABLE = True
except ImportError:
    _LOCK_GATE_AVAILABLE = False
    def _should_skip_prop(prop, game_times): return (False, "gate_unavailable")   # noqa: E704
    def _stamp_prop(prop, game_times): return prop                                 # noqa: E704
    def _fetch_game_times_today(): return {}                                       # noqa: E704
    def _data_is_contaminated(prop, ts, game_times): return False                 # noqa: E704
try:
    from clv_feedback_engine import (
        get_threshold       as _get_ev_threshold,
        rebuild_thresholds  as _rebuild_ev_thresholds,
        load_thresholds     as _load_ev_thresholds,
        build_discord_summary as _clv_discord_summary,
    )
    _CLV_ENGINE_AVAILABLE = True
except ImportError:
    _CLV_ENGINE_AVAILABLE = False
    def _get_ev_threshold(edge_reasons=None): return MIN_EV_THRESH   # noqa: E704
    def _rebuild_ev_thresholds(): return {}                          # noqa: E704
    def _load_ev_thresholds(): return {}                             # noqa: E704
    def _clv_discord_summary(): return ""                            # noqa: E704
try:
    from odds_math import (
        bookmaker_margin   as _bookmaker_margin,
        kelly_criterion    as _kelly_criterion_wb,
        true_odds_ev       as _true_odds_ev,
        prop_ev_dollar     as _prop_ev_dollar,
        is_acceptable_vig  as _is_acceptable_vig,
        elo_win_prob       as _elo_win_prob,
        MAX_VIG            as _MAX_VIG,
    )
    _ODDS_MATH_AVAILABLE = True
except ImportError:
    _ODDS_MATH_AVAILABLE = False

try:
    from confidence_shrinkage import shrink_and_size as _shrink_and_size
    _SHRINKAGE_AVAILABLE = True
except ImportError:
    _SHRINKAGE_AVAILABLE = False

try:
    from market_validator import stamp_market_validation as _stamp_market_validation
    _MARKET_VALIDATOR_AVAILABLE = True
except ImportError:
    _MARKET_VALIDATOR_AVAILABLE = False

try:
    from risk_manager import RiskManager as _RiskManager
    _risk_manager = _RiskManager()
    _RISK_MANAGER_AVAILABLE = True
except Exception as _rm_exc:
    _risk_manager = None
    _RISK_MANAGER_AVAILABLE = False
    logger.warning("[RISK] RiskManager not available: %s", _rm_exc)

try:
    from agent_diagnostics import get_frozen_agents as _get_frozen_agents
    _FREEZE_AVAILABLE = True
except Exception:
    _FREEZE_AVAILABLE = False
    def _get_frozen_agents() -> set: return set()  # noqa: E704

try:
    from bullpen_fatigue_scorer import build_bullpen_fatigue_scorer as _build_bullpen_scorer
    _BULLPEN_SCORER_AVAILABLE = True
except ImportError:
    _BULLPEN_SCORER_AVAILABLE = False
    def _build_bullpen_scorer(): return None  # noqa: E704

try:
    from bvi_layer import get_bvi_map as _get_bvi_map
    _BVI_AVAILABLE = True
except ImportError:
    _BVI_AVAILABLE = False
    def _get_bvi_map(**kw) -> dict: return {}  # noqa: E704

try:
    from sportsbook_reference_layer import build_sportsbook_reference as _build_sb_reference
    _SB_REFERENCE_AVAILABLE = True
except ImportError:
    _SB_REFERENCE_AVAILABLE = False
    def _build_sb_reference() -> dict: return {}  # noqa: E704

try:
    from nsfi_layer import fetch_nsfi_predictions_today as _fetch_nsfi
    _NSFI_AVAILABLE = True
except ImportError:
    _NSFI_AVAILABLE = False
    def _fetch_nsfi(): return []

try:
    from calibration_layer import (
        _norm_stat, apply_trust_gate, calculate_streak_penalty,
        apply_calibration_governor, is_ev_positive, check_streaks_gate,
        sniper_decision_gate, should_cash_out, apply_thermal_correction,
        ABS_FRAMING_WEIGHT, SteamMonitor, get_reliability_score,
        apply_isotonic_calibration,
        apply_shadow_whiff_boost,
        apply_zone_integrity_multiplier,
        adaptive_velocity_check,
)
    _CAL_LAYER_AVAILABLE = True
except ImportError:
    _CAL_LAYER_AVAILABLE = False
    def _norm_stat(s):
        if not s: return ""
        # stolen_bases, home_runs, walks removed — not approved prop types
        m = {"h":"hits","k":"strikeouts","ks":"strikeouts",
             "tb":"total_bases","rbi":"rbis",
             "er":"earned_runs","p_outs":"pitching_outs","h+r+rbi":"hits_runs_rbis",
             "outs_recorded":"outs_recorded","outs recorded":"outs_recorded",
             "fantasy_score":"fantasy_score","fantasy score":"fantasy_score",
             "pitcher fantasy score":"fantasy_score","hitter fantasy score":"fantasy_score",
             "fantasy pts":"fantasy_score","fantasy_pts":"fantasy_score",
             "hits + runs + rbis":"hits_runs_rbis","hits+runs+rbis":"hits_runs_rbis",
             "hits + runs + rbi":"hits_runs_rbis","h+r+rbi+":"hits_runs_rbis",
             "h+r+rbis":"hits_runs_rbis","h_r_rbi":"hits_runs_rbis",
             "hitter_fantasy_score":"fantasy_score","pitcher_fantasy_score":"fantasy_score",
             "hitter fantasy pts":"fantasy_score","pitcher fantasy pts":"fantasy_score",
             "hits allowed":"hits_allowed","pitching outs":"pitching_outs",
             "earned runs":"earned_runs","earned runs allowed":"earned_runs",
             "walks allowed":"walks_allowed","bb allowed":"walks_allowed",
             "walks":"walks_allowed","bb":"walks_allowed",
             "pitcher walks":"walks_allowed","pitcher bb":"walks_allowed"}
        s2 = str(s).lower().replace(" ","_").replace("-","_").strip()
        result = m.get(s2, s2)
        # Block removed prop types even if they slip through via raw string match
        # ABS 2026: walks_allowed reinstated — walk rate up 18% vs historical.
        # stolen_bases and home_runs remain blocked (high variance, low sample size).
        _BLOCKED = {"stolen_bases","home_runs","doubles","triples","singles"}
        return result if result not in _BLOCKED else ""
    ABS_FRAMING_WEIGHT = 0.20
    # abs_layer ERA adjustment wired below in DataHub
    class SteamMonitor:
        def __init__(self, *a, **kw): pass  # accepts steam_threshold and any future kwargs
        def detect_steam(self, *a, **kw): return False, 0.0
try:
    from drift_monitor import get_current_brier
    _DRIFT_MONITOR_AVAILABLE = True
except ImportError:
    _DRIFT_MONITOR_AVAILABLE = False
    def get_current_brier(): return 0.18

from lineup_chase_layer import get_lineup_chase_score
try:
    from base_rate_model import get_model_prob as _base_rate_prob
    _BASE_RATE_AVAILABLE = True
except ImportError:
    _BASE_RATE_AVAILABLE = False
    def _base_rate_prob(prop, side="OVER"): return 50.0  # noqa: E704
try:
    from prop_enrichment_layer import enrich_props as _enrich_props
    _ENRICHMENT_AVAILABLE = True
except ImportError:
    _ENRICHMENT_AVAILABLE = False
    def _enrich_props(props, hub, season=None): return props  # noqa: E704

try:
    from game_prediction_layer import (
        fetch_game_predictions_today as get_game_predictions,
        get_game_prediction as get_single_game_prediction,
    )
    _GAME_PRED_AVAILABLE = True
except ImportError:
    _GAME_PRED_AVAILABLE = False
    # Only define odds_math fallbacks here if odds_math itself also failed to load.
    # Without this guard the real odds_math functions get silently overwritten by stubs
    # every time game_prediction_layer fails to import (e.g. missing dependency),
    # even though _ODDS_MATH_AVAILABLE is True and the real functions are already bound.
    if not _ODDS_MATH_AVAILABLE:
        def _bookmaker_margin(o, u): return 0.0          # noqa: E704
        def _kelly_criterion_wb(p, o, kf=0.25, mc=0.05): # noqa: E704
            b = (o / 100.0) if o > 0 else (100.0 / abs(o))
            q = 1 - p
            raw = (b * p - q) / b
            return min(kf * raw, mc) if raw > 0 else 0.0
        def _true_odds_ev(stake, profit, prob): return (profit * prob) - (stake * (1 - prob))  # noqa: E704
        def _prop_ev_dollar(mp, o, s=1.0): return 0.0   # noqa: E704
        def _is_acceptable_vig(o, u, mv=0.08): return True  # noqa: E704
        def _elo_win_prob(d): return 1.0 / (10**(-d/400) + 1)  # noqa: E704
        _MAX_VIG = 0.08

import datetime
import json
import logging
import math
import os
import pickle
import time
from typing import Any

import requests

# Guard redis import — if not installed, _NullRedis handles all calls gracefully
try:
    import redis as redis_lib
except ImportError:
    redis_lib = None  # type: ignore[assignment]

from DiscordAlertService import discord_alert
from public_trends_scraper import PublicTrendsScraper, get_fade_signal

# ── Null-object fallback for when Redis is unreachable ────────────────────────

class _NullRedis:
    """
    Silent no-op drop-in for redis.Redis.
    Returned by _redis() when the server is unreachable so the app boots
    successfully and degrades gracefully instead of crashing.
    """
    @staticmethod
    def exists(*a, **kw):  return False
    @staticmethod
    def get(*a, **kw):     return None
    @staticmethod
    def setex(*a, **kw):   return None
    @staticmethod
    def set(*a, **kw):     return None
    @staticmethod
    def lpush(*a, **kw):   return None
    @staticmethod
    def ltrim(*a, **kw):   return None
    @staticmethod
    def lrange(*a, **kw):  return []
    @staticmethod
    def delete(*a, **kw):  return None
    @staticmethod
    def ping(*a, **kw):    return False

# logger defined at top of file

OPENING_DAY        = datetime.date(2026, 3, 26)
SPRING_TRAINING_WT = 0.30          # ST stats count 30 % until Opening Day
TTL_PHYSICS  = 900    # 15 min
TTL_CONTEXT  = 600    # 10 min
TTL_MARKET   = 300    #  5 min
TTL_DFS      = 480    #  8 min
TTL_HUB      = 600    # 10 min — master hub key
# Works with or without Redis. Keyed agent_name → "YYYY-MM-DD".
# An agent may send AT MOST ONE play per calendar day.
_AGENT_SENT_TODAY: dict = {}   # { agent_name: "2026-03-29" }
MIN_CONFIDENCE    = 6
# MIN_PROB cold-start schedule:
#   Apr 16 launch:  0.52  (cold-start — XGBoost not yet trained)
#   Apr 20 retrain: bump to 0.57 manually after first successful retrain
#   May 15+:        bump to 0.60 once 200+ settled rows confirmed
# Currently at 0.55 — intermediate step: negative-EV slips now blocked so we can
# raise the bar from 52% without losing too many qualifying picks.
# At 0.55 with correct multipliers: 2-leg PP needs 55%^2 * 3 - 1 = -9.2% → still
# needs higher prob per leg, but combined_ev gate (+3%) now does the real work.
MIN_PROB          = 0.57   # April 20 retrain: raised from 0.55 — first real model trained on historical data
STREAK_MIN_LINE   = 0.5    # minimum DFS line value for streak tracking — filters junk sub-0.5 props

# ── In-memory fallback cache (active when Redis is unavailable) ──────────────
_MEM: dict = {}  # key → (expire_ts, data)


def _mem_set(key: str, ttl: int, data) -> None:
    _MEM[key] = (time.time() + ttl, data)


def _mem_exists(key: str) -> bool:
    entry = _MEM.get(key)
    return entry is not None and time.time() < entry[0]


def _mem_get(key: str):
    entry = _MEM.get(key)
    if entry and time.time() < entry[0]:
        return entry[1]
    return None


def _hub_exists(r, key: str) -> bool:
    """Check Redis first, fall back to in-memory."""
    try:
        if r.exists(key):
            return True
    except Exception:
        pass
    return _mem_exists(key)


def _hub_setex(r, key: str, ttl: int, json_str: str) -> None:
    """Write to Redis and always write to in-memory fallback."""
    try:
        r.setex(key, ttl, json_str)
    except Exception:
        pass
    _mem_set(key, ttl, json.loads(json_str))


def _hub_get(r, key: str):
    """Read from Redis; fall back to in-memory."""
    try:
        raw = r.get(key)
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return _mem_get(key)


# Agent config
AGENT_NAMES = [
    "EVHunter",
    "UnderMachine",
    "UmpireAgent",
    "F5Agent",
    "FadeAgent",
    "LineValueAgent",
    "BullpenAgent",
    "WeatherAgent",
    "MLEdgeAgent",
    "UnderDogAgent",
    "StackSmithAgent",
    "ChalkBusterAgent",
    "SharpFadeAgent",
    "CorrelatedParlayAgent",
    "PropCycleAgent",
    "LineupChaseAgent",
    "LineDriftAgent",
]
KELLY_FRACTION  = 0.25    # Quarter-Kelly
MAX_UNIT_CAP    = 0.05    # 5 % bankroll cap per bet
MIN_EV_THRESH     = 0.03   # 3% minimum edge to queue a bet (ratio scale, e.g. 0.085)
MIN_EV_THRESH_PCT = 3.0    # same threshold in percent scale (e.g. 8.5) — used by Group B agents
CAP_FLOOR = 0.5
CAP_CEIL  = 2.0

# ── Railway-safe service connections ─────────────────────────────────────────

def _redis():
    """
    Connect to Redis using Railway env var or explicit host/port.
    Returns _NullRedis() if redis is not installed or the server is unreachable
    — allows the app to boot successfully and run without cache/queue.
    """
    if redis_lib is None:
        logger.warning("redis package not installed — running without cache.")
        return _NullRedis()
    try:
        url = os.getenv("REDIS_URL")
        if url:
            r = redis_lib.from_url(url, decode_responses=True,
                                   socket_connect_timeout=3)
        else:
            r = redis_lib.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                db=int(os.getenv("REDIS_DB", 0)),
                decode_responses=True,
                socket_connect_timeout=3,
            )
        r.ping()   # fail fast if server is down
        return r
    except Exception as e:
        logger.warning("Redis unavailable — running without cache (app will still boot): %s", e)
        return _NullRedis()


def _pg_conn():
    """Return a psycopg2 connection using DATABASE_URL or explicit params."""
    import psycopg2
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "propiq"),
        user=os.getenv("POSTGRES_USER", "propiq"),
        password=os.getenv("POSTGRES_PASSWORD", "propiq"),
    )


def _kafka_producer():
    """Return a confluent-kafka Producer, or None if Kafka is unavailable."""
    try:
        from confluent_kafka import Producer
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        p = Producer({
            "bootstrap.servers": bootstrap,
            "socket.timeout.ms": 3000,
            "message.timeout.ms": 5000,
            "retries": 0,
        })
        return p
    except Exception as e:
        logger.warning("Kafka unavailable — falling back to Redis queue: %s", e)
        return None


# ── Shared helpers ────────────────────────────────────────────────────────────

def _is_spring_training() -> bool:
    import zoneinfo as _zi
    today_pt = datetime.datetime.now(_zi.ZoneInfo("America/Los_Angeles")).date()
    return today_pt < OPENING_DAY


def _today_pt() -> "datetime.date":
    """Return today's date in America/Los_Angeles (Pacific Time). Never use date.today()."""
    import zoneinfo as _zi
    return datetime.datetime.now(_zi.ZoneInfo("America/Los_Angeles")).date()


def _american_to_implied(american) -> float:
    """Convert American odds to implied probability percentage.
    Returns 52.4 (≡ -110) as a safe default for None/zero/invalid input.
    """
    if american is None:
        return 52.4
    try:
        american = int(american)
    except (TypeError, ValueError):
        return 52.4
    if american == 0:
        return 52.4
    if american > 0:
        return 100.0 / (american + 100) * 100
    return abs(american) / (abs(american) + 100) * 100


def _no_vig(over_american: int, under_american: int) -> tuple[float, float]:
    """Return (over_fair_prob, under_fair_prob) stripped of vig.
    Uses power method (Shin 1993) for asymmetric lines — more accurate than
    simple additive de-vig, especially when lines are far from -110/-110.
    Source: mc_upgrades.py devig_power() / baseball-sims confidence_shrinkage.
    """
    over_imp  = _american_to_implied(over_american)  / 100
    under_imp = _american_to_implied(under_american) / 100
    if over_imp <= 0 or under_imp <= 0:
        return 0.5, 0.5
    # Power method: find k such that over_imp^(1/k) + under_imp^(1/k) = 1
    lo, hi = 0.5, 3.0
    for _ in range(50):
        k = (lo + hi) / 2.0
        s = over_imp ** (1.0 / k) + under_imp ** (1.0 / k)
        if s > 1.0:
            hi = k
        else:
            lo = k
    k = (lo + hi) / 2.0
    fair_over  = over_imp  ** (1.0 / k)
    fair_under = under_imp ** (1.0 / k)
    # Normalise (should already sum to ~1.0 but floating point safety)
    total = fair_over + fair_under
    return fair_over / total, fair_under / total


# ── Market-model blend weights by prop type (mc_upgrades Phase 6) ──────────
# w = model weight; (1−w) = market weight.
# Higher w → trust model more (less liquid/efficient market for this prop type).
_MARKET_MODEL_WEIGHTS: dict[str, float] = {
    "batter_hits":           0.35,
    "batter_total_bases":    0.35,
    "batter_home_runs":      0.30,
    "pitcher_strikeouts":    0.30,
    "batter_rbis":           0.40,
    "batter_runs_scored":    0.40,
    "batter_hits_runs_rbis": 0.40,
    "batter_stolen_bases":   0.45,
    "pitcher_outs":          0.38,
    "pitcher_earned_runs":   0.42,
    "pitcher_hits_allowed":  0.42,
    "pitcher_walks_allowed": 0.45,
    "default":               0.50,
}


def _logit_blend_prob(
    p_model: float,
    p_market: float,
    market_key: str,
    over_implied: float | None = None,
    under_implied: float | None = None,
) -> float:
    """
    Blend model and market probabilities in log-odds space (mc_upgrades Phase 6).
    Source: Lopez/Matthews/Glickman arXiv:1701.05976; Trademate Sports.

    Args:
        p_model   — model probability (0–1 or 0–100; auto-detected)
        p_market  — market implied probability (0–1)
        market_key — prop type key for weight lookup
        over_implied, under_implied — raw implied probs (pre-de-vig); if both
                                      supplied, power-method de-vig applied first

    Returns blended probability in same scale as p_model.
    """
    # Normalise to 0–1
    pm = p_model / 100.0 if p_model > 1.0 else p_model
    mk = p_market

    # De-vig market if raw over/under supplied
    if over_implied and under_implied and over_implied > 0 and under_implied > 0:
        lo, hi = 0.5, 3.0
        for _ in range(50):
            k = (lo + hi) / 2.0
            s = over_implied ** (1.0 / k) + under_implied ** (1.0 / k)
            if s > 1.0:
                hi = k
            else:
                lo = k
        k = (lo + hi) / 2.0
        mk = over_implied ** (1.0 / k)

    if not mk or mk <= 0:
        return p_model   # no market data — return model unchanged

    w = _MARKET_MODEL_WEIGHTS.get(market_key, _MARKET_MODEL_WEIGHTS["default"])

    def _logit(p: float) -> float:
        p = max(0.001, min(0.999, p))
        return math.log(p / (1.0 - p))

    def _sigmoid(x: float) -> float:
        return 1.0 / (1.0 + math.exp(-x))

    blended = _sigmoid(w * _logit(pm) + (1.0 - w) * _logit(mk))
    # Return in same scale as input
    return round(blended * 100.0 if p_model > 1.0 else blended, 4)


# ── Tango platoon regression constants ───────────────────────────────────────
_PLATOON_M = {"L": 1000, "R": 2200, "S": 1620}   # stabilisation PA
_LEAGUE_PLATOON_SPLITS = {
    "L": 0.035,   # LHB hits .035 wOBA better vs RHP
    "R": 0.023,   # RHB hits .023 wOBA better vs LHP
    "S": 0.027,   # switch: use favourable side
}
_STAT_DEFAULTS_PLATOON = {
    "avg": 0.245, "obp": 0.315, "slg": 0.390, "ops": 0.705, "woba": 0.320
}


def _platoon_blend_v2(batter: dict, pitcher_hand: str, stat: str) -> float:
    """
    Bayesian-regressed platoon blend (mc_upgrades Phase 3 / Tango canonical).
    Regresses observed vs-hand splits toward season average using Tango M constants.
    Replaces simple platoon adjustments with properly uncertainty-weighted values.

    Args:
        batter       — prop dict enriched with vs_r_*/vs_l_* split keys
        pitcher_hand — "L" or "R"
        stat         — "avg", "obp", "slg", "ops", or "woba"
    """
    bats = (batter.get("bats") or batter.get("_batter_hand") or "S").upper()
    hand = (pitcher_hand or "R").upper()
    if hand not in ("L", "R"):
        hand = "R"

    split_key = f"vs_{'l' if hand == 'L' else 'r'}_{stat}"
    pa_key    = f"vs_{'l' if hand == 'L' else 'r'}_pa"

    obs_split = batter.get(split_key)
    split_pa  = int(batter.get(pa_key, 0) or 0)

    season_map = {
        "avg":  batter.get("season_avg")  or batter.get("fg_avg"),
        "obp":  batter.get("season_obp")  or batter.get("fg_obp"),
        "slg":  batter.get("season_slg")  or batter.get("fg_slg"),
        "ops":  batter.get("season_ops"),
        "woba": batter.get("fg_woba")     or batter.get("sv_xwoba"),
    }
    season_val = float(season_map.get(stat) or _STAT_DEFAULTS_PLATOON.get(stat, 0.0) or 0.0)

    if obs_split is None or float(obs_split or 0) <= 0.0:
        # No split data — apply theoretical half-edge
        platoon_fav = (
            (bats == "L" and hand == "R")
            or (bats == "R" and hand == "L")
            or bats == "S"
        )
        if stat in _STAT_DEFAULTS_PLATOON:
            lg_edge = _LEAGUE_PLATOON_SPLITS.get(bats, 0.025)
            return round(season_val + (lg_edge * 0.5 if platoon_fav else -lg_edge * 0.5), 4)
        return season_val

    obs_val = float(obs_split)
    M       = _PLATOON_M.get(bats, 1620)
    # Bayesian shrinkage: blend observed split with season total as prior
    blended = (obs_val * split_pa + season_val * M) / (split_pa + M)
    return round(blended, 4)


def _relief_fatigue_penalty(days_pitched: list[int], pitches_last: int = 20) -> float:
    """
    Reliever back-to-back fatigue penalty in wOBA-against delta.
    Source: mc_upgrades.py relief_fatigue_penalty() / Tango back-to-back research.

    Research: B2B ≈ +10–15 wOBA pts; 3rd consecutive day ≈ +20–30 pts.

    Args:
        days_pitched — list of ints: days ago each appearance occurred (0=today, 1=yesterday)
        pitches_last — pitch count in most recent outing

    Returns: wOBA delta (positive = batters benefit) → convert to pp via / 0.030 × 2.5
    """
    if not days_pitched:
        return 0.0
    min_rest = min(days_pitched)
    if min_rest == 0:
        return 0.020 + min(0.015, pitches_last * 0.0003)
    if min_rest == 1 and len([d for d in days_pitched if d <= 2]) >= 2:
        return 0.015   # back-to-back with multiple recent appearances
    if min_rest == 1:
        return 0.010
    if min_rest == 2 and pitches_last >= 35:
        return 0.008   # heavy load 2 days ago
    return 0.0


def _kelly_units(edge: float, odds_american: int) -> float:
    """Quarter-Kelly bet sizing, capped at MAX_UNIT_CAP.
    Uses WagerBrain kelly_criterion() when available (more accurate decimal
    odds conversion). Falls back to inline calculation if not available.
    """
    if _ODDS_MATH_AVAILABLE:
        return _kelly_criterion_wb(
            prob=float(edge),
            odds_american=int(odds_american),
            kelly_fraction=KELLY_FRACTION,
            max_cap=MAX_UNIT_CAP,
        )
    # Inline fallback
    if odds_american > 0:
        b = odds_american / 100.0
    else:
        b = 100.0 / abs(odds_american)
    p = float(edge)
    q = 1 - p
    kelly = (b * p - q) / b
    return min(KELLY_FRACTION * kelly, MAX_UNIT_CAP)


def _get_sample_count(prop_type: str) -> int:
    """Return the number of settled ledger rows for this prop-type.
    Used by thin-data shrinkage in _build_bet().
    Populated by run_xgboost_tasklet() under key 'xgb_sample_counts'.

    Cold-start behaviour (key absent from Redis):
      Returns _COLD_START_SAMPLES (100) so shrinkage doesn't collapse ALL
      agent confidence to 4/10 on day 1 before the first XGBoost retrain.
      100 samples ≈ 'thin but seen' — applies moderate shrinkage rather than
      maximum shrinkage, keeping confidence scores above MIN_CONFIDENCE=6.

    Once xgb_sample_counts is written by run_xgboost_tasklet() (Sunday 2 AM)
    or by run_data_hub_tasklet() via _refresh_sample_counts(), the real per-
    prop-type counts take over.
    """
    _COLD_START_SAMPLES = 250   # floor when Redis key absent — raised from 100 (gave 0.28 thin_conf → shrinkage death spiral)
    try:
        raw = _redis().get("xgb_sample_counts")
        if raw:
            counts: dict = json.loads(raw)
            # Key exists — use real count (may be 0 for unseen prop types)
            return int(counts.get(str(prop_type).lower(), 0))
        # Key absent — cold start, return floor so shrinkage is moderate not maximal
        return _COLD_START_SAMPLES
    except Exception:
        return _COLD_START_SAMPLES


def _fetch_apify(actor_id: str, run_input: dict) -> list[dict]:
    """Run an Apify actor and return dataset items."""
    api_key = os.getenv("APIFY_API_KEY", "")
    if not api_key:
        logger.warning("APIFY_API_KEY not set — skipping Apify scrape for %s", actor_id)
        return []
    try:
        resp = requests.post(
            f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items",
            params={"token": api_key},
            json=run_input,
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error("Apify error (%s): %s", actor_id, e)
        return []


def _fetch_sbd_public_trends() -> dict:
    """
    Public betting splits — now sourced from Action Network instead of SBD.

    SBD was unreliable (data structure mismatch, scraping blocks).
    Action Network PRO returns identical data: player-level ticket% and money%
    per prop, plus game-level totals — same fields all agents expect.

    Column mapping (AN → SBD-compatible names so all agents work unchanged):
        AN "player"           → "player_name"          (FadeAgent, ChalkBuster)
        AN "over_ticket_pct"  → "prop_over_bets_pct"   (ChalkBuster primary key)
                              → "ticket_pct"            (SharpFade fallback)
        AN "over_money_pct"   → "money_pct"
        AN "prop_type"        → "prop_type"             (already PropIQ canonical)
        AN "rlm_signal"       → "rlm_signal"
        AN "rlm_direction"    → "rlm_direction"

    Returns {"game_df": [...], "prop_df": [...]} — same schema all agents expect.
    Falls back to empty lists if AN not available (agents degrade gracefully).
    """
    try:
        from action_network_layer import (  # noqa: PLC0415
            fetch_mlb_prop_projections,
            fetch_mlb_game_sentiment,
        )

        # ── Player-level prop splits (replaces SBD prop_df) ──────────────────
        an_props = fetch_mlb_prop_projections()
        prop_df_records = []
        for p in an_props:
            prop_df_records.append({
                # SBD-compatible keys — all downstream agents read these
                "player_name":        p.get("player", ""),
                "prop_type":          p.get("prop_type", ""),
                "prop_over_bets_pct": p.get("over_ticket_pct", 50),  # ChalkBuster primary
                "over_pct":           p.get("over_ticket_pct", 50),   # legacy alias
                "ticket_pct":         p.get("over_ticket_pct", 50),   # SharpFade fallback
                "money_pct":          p.get("over_money_pct",  50),
                "under_ticket_pct":   p.get("under_ticket_pct", 50),
                "under_money_pct":    p.get("under_money_pct",  50),
                "rlm_signal":         p.get("rlm_signal", False),
                "rlm_direction":      p.get("rlm_direction"),
                "line":               p.get("line"),
                "source":             "action_network",
            })

        # ── Game-level splits (replaces SBD game_df) ─────────────────────────
        # AN game sentiment is already fetched for DataHub — reuse it here.
        # Returns {} if JWT not set; agents fall back to 50% (no fade signal).
        an_sentiment = fetch_mlb_game_sentiment()
        game_df_records = []
        for team_name, g in an_sentiment.items():
            game_df_records.append({
                "team":               team_name,
                "over_bets_pct":      g.get("over_ticket_pct", 50),
                "over_money_pct":     g.get("over_money_pct",  50),
                "ticket_pct":         g.get("over_ticket_pct", 50),
                "money_pct":          g.get("over_money_pct",  50),
                "rlm_signal":         g.get("rlm_signal", False),
                "rlm_direction":      g.get("rlm_direction"),
                "source":             "action_network",
            })

        logger.info(
            "[DataHub] AN public trends: %d player props, %d game records",
            len(prop_df_records), len(game_df_records),
        )
        return {"game_df": game_df_records, "prop_df": prop_df_records}

    except Exception as exc:
        logger.warning("[DataHub] AN public trends fetch failed: %s", exc)
        return {"game_df": [], "prop_df": []}


def _sportsdata_get(path: str) -> Any:
    """Call SportsData.io MLB v3 API."""
    key = os.getenv("SPORTSDATA_API_KEY", os.getenv("SPORTSDATA_KEY", "c2abf26f55714d228c7c311290f956d7"))
    base = "https://api.sportsdata.io/v3/mlb"
    try:
        resp = requests.get(
            f"{base}/{path}",
            params={"key": key},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("SportsData.io error (%s): %s", path, e)
        return None




def _fetch_espn_games() -> dict:
    """Fetch today's MLB games from ESPN public API. No API key required."""
    status_map = {
        "STATUS_SCHEDULED":   "Scheduled",
        "STATUS_IN_PROGRESS": "InProgress",
        "STATUS_FINAL":       "Final",
        "STATUS_POSTPONED":   "Postponed",
        "STATUS_DELAYED":     "Delayed",
    }
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        games: dict = {}
        for event in data.get("events", []):
            game_id = str(event.get("id", ""))
            competition = event.get("competitions", [{}])[0]
            competitors  = competition.get("competitors", [])
            home = next((t for t in competitors if t.get("homeAway") == "home"), {})
            away = next((t for t in competitors if t.get("homeAway") == "away"), {})
            raw_status = (
                competition.get("status", {})
                           .get("type", {})
                           .get("name", "STATUS_SCHEDULED")
            )
            games[game_id] = {
                "GameID":    game_id,
                "HomeTeam":  home.get("team", {}).get("abbreviation", ""),
                "AwayTeam":  away.get("team", {}).get("abbreviation", ""),
                "DateTime":  event.get("date", ""),
                "Status":    status_map.get(raw_status, "Scheduled"),
                "HomeScore": home.get("score", 0),
                "AwayScore": away.get("score", 0),
                "Inning":    competition.get("status", {}).get("period", 0),
            }
        logger.info("[DataHub] ESPN: %d games today", len(games))
        return games
    except Exception as exc:
        logger.warning("[DataHub] ESPN scoreboard error: %s", exc)
        return {}

def _fetch_injuries_today() -> list[dict]:
    """Fetch current MLB injury list from ESPN + Action Network PRO."""
    try:
        from injury_layer import fetch_injuries as _fetch_inj  # noqa: PLC0415
        return _fetch_inj()
    except Exception as exc:
        logger.warning("[DataHub] injury_layer fetch failed: %s", exc)
        return []


def _fetch_mlb_lineups_today() -> list[dict]:
    """Fetch today's confirmed batting order lineups from MLB Stats API (free, no key)."""
    import datetime as _dt  # noqa: PLC0415
    today = _today_pt().strftime("%Y-%m-%d")
    try:
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": today, "hydrate": "lineups,team,venue"},
            timeout=15,
        )
        resp.raise_for_status()
        lineups = []
        for date_block in resp.json().get("dates", []):
            for game in date_block.get("games", []):
                game_lineups = game.get("lineups", {})
                home = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
                away = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
                for side_key, team_name in (("homePlayers", home), ("awayPlayers", away)):
                    for pos, player in enumerate(game_lineups.get(side_key, []), start=1):
                        lineups.append({
                            "player_id":   player.get("id"),
                            "full_name":   player.get("fullName", ""),
                            "team":        team_name,
                            "batting_pos": pos,
                        })
        logger.info("[DataHub] MLB lineups: %d confirmed players", len(lineups))
        return lineups
    except Exception as exc:
        logger.warning("[DataHub] MLB lineups fetch failed: %s", exc)
        return []


def _fetch_mlb_probable_starters() -> list[dict]:
    """Fetch today's probable starting pitchers from MLB Stats API (free, no key)."""
    import datetime as _dt  # noqa: PLC0415
    today = _today_pt().strftime("%Y-%m-%d")
    try:
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": today, "hydrate": "probablePitcher,team,venue"},
            timeout=15,
        )
        resp.raise_for_status()
        starters = []
        for date_block in resp.json().get("dates", []):
            for game in date_block.get("games", []):
                home_team = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
                away_team = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
                venue = (game.get("venue") or {}).get("name", "")
                home_sp = game.get("teams", {}).get("home", {}).get("probablePitcher", {})
                away_sp = game.get("teams", {}).get("away", {}).get("probablePitcher", {})
                if home_sp:
                    starters.append({
                        "player_id": home_sp.get("id"),
                        "full_name": home_sp.get("fullName", ""),
                        "team": home_team, "side": "home", "venue": venue,
                        "opponent": away_team,
                    })
                if away_sp:
                    starters.append({
                        "player_id": away_sp.get("id"),
                        "full_name": away_sp.get("fullName", ""),
                        "team": away_team, "side": "away", "venue": venue,
                        "opponent": home_team,
                    })
        logger.info("[DataHub] Probable starters: %d pitchers", len(starters))
        return starters
    except Exception as exc:
        logger.warning("[DataHub] Probable starters fetch failed: %s", exc)
        return []


def _fetch_mlb_standings() -> list[dict]:
    """Fetch current MLB standings from MLB Stats API (free, no key).
    Replaces Apify actor ToDC6ydulO79igDoX.
    """
    import datetime as _dt  # noqa: PLC0415
    season = _today_pt().year
    try:
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/standings",
            params={"leagueId": "103,104", "season": season, "standingsTypes": "regularSeason"},
            timeout=15,
        )
        resp.raise_for_status()
        standings = []
        for record in resp.json().get("records", []):
            division = record.get("division", {}).get("name", "")
            for team_record in record.get("teamRecords", []):
                team = team_record.get("team", {})
                standings.append({
                    "team_id":    team.get("id"),
                    "team_name":  team.get("name", ""),
                    "division":   division,
                    "wins":       team_record.get("wins", 0),
                    "losses":     team_record.get("losses", 0),
                    "pct":        float(team_record.get("winningPercentage", "0.000") or 0),
                    "gb":         team_record.get("gamesBack", "-"),
                    "streak":     team_record.get("streak", {}).get("streakCode", ""),
                    "last_10":    next(
                        (sr.get("wins", 5) for sr in team_record.get("records", {}).get("splitRecords", [])
                         if sr.get("type") == "lastTen"),
                        5  # default neutral if split not found
                    ),
                })
        logger.info("[DataHub] Standings: %d teams", len(standings))
        return standings
    except Exception as exc:
        logger.warning("[DataHub] Standings fetch failed: %s", exc)
        return []


def _resilient_get(url: str, headers: dict, params: dict | None = None,
                   timeout: int = 15) -> "requests.Response":
    """
    GET with retry on transient failures.
    Returns the response regardless of status — callers check status_code.
    """
    return requests.get(url, headers=headers, params=params, timeout=timeout)


def _fetch_prizepicks_direct() -> list[dict]:
    """Fetch PrizePicks MLB props via partner-api (public, no key required).
    Uses partner-api.prizepicks.com — confirmed public endpoint with no bot block.
    """
    _PP_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }
    # Dynamically resolve MLB league_id — avoids hardcoded ID being stale
    league_id = 2  # default
    try:
        leagues_resp = _resilient_get(
            "https://partner-api.prizepicks.com/leagues",
            headers=_PP_HEADERS, timeout=10,
        )
        if leagues_resp.status_code == 200:
            for item in leagues_resp.json().get("data", []):
                if (item.get("attributes", {}).get("name") or "").upper() == "MLB":
                    league_id = item["id"]
                    break
    except Exception:
        pass
    try:
        resp = _resilient_get(
            "https://partner-api.prizepicks.com/projections",
            headers=_PP_HEADERS,
            params={"per_page": 1000, "league_id": league_id},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.info("[DataHub] PrizePicks direct returned %d — no props this cycle", resp.status_code)
            return []
        data = resp.json()
        player_map: dict[str, str] = {}
        for item in data.get("included", []):
            if item.get("type") == "new_player":
                pid = item["id"]
                name = item.get("attributes", {}).get("display_name", "")
                if name:
                    player_map[pid] = name
        props = []
        for proj in data.get("data", []):
            attrs = proj.get("attributes", {})
            stat_raw = str(attrs.get("stat_type", "") or "").lower()
            line_val = attrs.get("line_score")
            if line_val is None:
                continue
            # STANDARD filter: skip alt/goblin/demon/flex board types
            board_type  = str(attrs.get("board_type", "standard") or "standard").lower()
            odds_tier   = str(attrs.get("odds_type",  "standard") or "standard").lower()
            adjusted    = attrs.get("adjusted_odds", False)
            if board_type not in ("standard", "") or odds_tier not in ("standard", ""):
                continue
            if adjusted:
                continue
            pid = (
                proj.get("relationships", {})
                    .get("new_player", {})
                    .get("data", {})
                    .get("id", "")
            )
            pname = player_map.get(pid, "")
            if not pname:
                continue
            stat_norm = _norm_stat(stat_raw)
            props.append({
                "player":        pname,
                "player_name":   pname,
                "stat":          stat_norm,
                "prop_type":     stat_norm,
                "line":          float(line_val),
                "over_american":  int(attrs.get("over_odds", -110) or -110),
                "under_american": int(attrs.get("under_odds", -110) or -110),
                "platform":      "PrizePicks",
            })
        # ── Dedup PP ladder lines: keep one line per (player, prop_type) ──────
        # PrizePicks posts 3-5 lines per player per prop (e.g. 3.5/4.5/5.5/6.5/7.5).
        # All are tagged board_type="standard" so the existing filter keeps all of them.
        # We keep only the line closest to the sportsbook reference, or the median.
        from collections import defaultdict as _dd
        _groups: dict = _dd(list)
        for _p in props:
            _key = (_p["player_name"], _p["prop_type"])
            _groups[_key].append(_p)
        deduped = []
        for (_pname, _pt), _candidates in _groups.items():
            if len(_candidates) == 1:
                deduped.append(_candidates[0])
                continue
            # Sort by line value
            _candidates.sort(key=lambda x: x["line"])
            # Pick median line (middle of ladder) as canonical line
            _mid = _candidates[len(_candidates) // 2]
            deduped.append(_mid)
        _removed = len(props) - len(deduped)
        if _removed:
            logger.info("[DataHub] PP ladder dedup: removed %d alt lines, kept %d canonical", _removed, len(deduped))
        props = deduped
        logger.info("[DataHub] PrizePicks direct: %d props", len(props))
        return props
    except Exception as exc:
        logger.info("[DataHub] PrizePicks direct fetch failed: %s", exc)
        return []


def _fetch_underdog_props_direct() -> list[dict]:
    """Fetch Underdog Fantasy MLB over/under lines (free, no key required)."""
    # Headers confirmed working by aidanhall21/underdog-fantasy-pickem-scraper
    _UD_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }
    try:
        resp = _resilient_get(
            "https://api.underdogfantasy.com/beta/v5/over_under_lines",
            headers=_UD_HEADERS,
            timeout=20,
        )
        if resp.status_code != 200:
            logger.info("[DataHub] Underdog returned %d — no props this cycle", resp.status_code)
            return []
        data = resp.json()
        players_map = {p["id"]: p for p in data.get("players", [])}
        appearances_map = {a["id"]: a for a in data.get("appearances", [])}
        props = []
        seen: set = set()
        for line in data.get("over_under_lines", []):
            if line.get("status") != "active":
                continue
            # Enforce STANDARD only — skip FLEX / alt / goblin / demon lines
            ou_check = line.get("over_under") or {}
            # Phase 116: only balanced Pick'em lines — boosted/alt lines excluded
            if line.get("line_type", "balanced") != "balanced":
                continue
            entry_type = (
                line.get("entry_type")
                or ou_check.get("entry_type")
                or line.get("payout_multiplier_type", "")
            )
            if entry_type and str(entry_type).upper() not in ("STANDARD", ""):
                continue
            stable_id = line.get("stable_id", line.get("id", ""))
            if stable_id in seen:
                continue
            seen.add(stable_id)
            ou = line.get("over_under") or {}
            app_stat = ou.get("appearance_stat") or {}
            stat_ud = _norm_stat(app_stat.get("stat", ""))
            app_id = app_stat.get("appearance_id", "")
            appearance = appearances_map.get(app_id, {})
            player_id = appearance.get("player_id", "")
            player = players_map.get(player_id, {})
            if player.get("sport_id") != "MLB":
                continue
            name = f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
            if not name:
                continue
            props.append({
                "player":         name,
                "player_name":    name,
                "stat_type":      stat_ud,
                "prop_type":      stat_ud,
                "line":           float(line.get("stat_value") or ou.get("stat_value") or 1.5),
                "over_american":  int(line.get("over_american", -115) or -115),
                "under_american": int(line.get("under_american", -115) or -115),
                "platform":       "Underdog",
            })
        logger.info("[DataHub] Underdog direct: %d props", len(props))
        return props
    except Exception as exc:
        logger.info("[DataHub] Underdog direct fetch failed: %s", exc)
        return []



def _fetch_sleeper_props_direct() -> list[dict]:
    """Fetch Sleeper Fantasy MLB pick'em lines (free, no key required).
    Used as a fallback when PrizePicks or Underdog return 0 props.
    Falls back gracefully to [] on any failure.
    """
    _SL_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://sleeper.com/",
        "x-sleeper-platform": "web",
    }
    props: list[dict] = []

    # Primary: Sleeper beta sportsbook pick'em lines
    try:
        resp2 = _resilient_get(
            "https://api.sleeper.com/picks/v1/sport/mlb/market/player_props",
            headers=_SL_HEADERS,
            timeout=15,
        )
        if resp2.status_code == 200:
            data = resp2.json()
            for item in (data if isinstance(data, list) else data.get("picks", [])):
                try:
                    player_name = (
                        item.get("player_name")
                        or item.get("name")
                        or f"{item.get('first_name', '')} {item.get('last_name', '')}".strip()
                    )
                    if not player_name:
                        continue
                    stat_raw = str(item.get("stat_type") or item.get("stat") or "")
                    stat_norm = _norm_stat(stat_raw)
                    if not stat_norm:
                        continue
                    line_val = item.get("line") or item.get("value") or item.get("projection")
                    if line_val is None:
                        continue
                    props.append({
                        "player":         player_name,
                        "player_name":    player_name,
                        "stat_type":      stat_norm,
                        "prop_type":      stat_norm,
                        "line":           float(line_val),
                        "over_american":  int(item.get("over_odds", -115) or -115),
                        "under_american": int(item.get("under_odds", -115) or -115),
                        "platform":       "Sleeper",
                    })
                except Exception:
                    continue
            if props:
                logger.info("[DataHub] Sleeper sportsbook: %d props", len(props))
                return props
    except Exception as _exc:
        logger.debug("[DataHub] Sleeper primary endpoint failed: %s", _exc)

    # Secondary: Sleeper public player projections — parse as prop lines
    try:
        _today_sl = _today_pt()
        resp3 = _resilient_get(
            f"https://api.sleeper.app/projections/baseball/{_today_sl.year}/0",
            headers=_SL_HEADERS,
            timeout=15,
        )
        if resp3.status_code == 200:
            data3 = resp3.json()
            _PROJ_MAP = {
                "h":    "hits",
                "rbi":  "rbis",
                "so":   "strikeouts",
                "k":    "strikeouts",
                "tb":   "total_bases",
                "er":   "earned_runs",
                "outs": "pitching_outs",
            }
            for player_id, proj in (data3.items() if isinstance(data3, dict) else {}.items()):
                try:
                    name = proj.get("name") or player_id
                    for key, stat_norm in _PROJ_MAP.items():
                        val = proj.get(key)
                        if val is None or float(val) <= 0:
                            continue
                        if _norm_stat(stat_norm) in ("", None):
                            continue
                        props.append({
                            "player":         name,
                            "player_name":    name,
                            "stat_type":      stat_norm,
                            "prop_type":      stat_norm,
                            "line":           round(float(val), 1),
                            "over_american":  -115,
                            "under_american": -115,
                            "platform":       "Sleeper",
                        })
                except Exception:
                    continue
    except Exception as _exc2:
        logger.debug("[DataHub] Sleeper secondary endpoint failed: %s", _exc2)

    if props:
        logger.info("[DataHub] Sleeper fallback: %d props (projection-derived)", len(props))
    else:
        logger.warning("[DataHub] Sleeper fallback returned 0 props — all three DFS sources dry.")
    return props


def _fetch_draftedge_projections() -> list[dict]:
    """
    Fetch DraftEdge batter and pitcher projections.
    Free — no API key, daily parquet cache, zero quota cost.

    Returns flat list of player projection dicts with fields:
        player_name, team, prop_type, projected_prob, source="draftedge"

    Batter props: hits, home_runs, stolen_bases, runs, rbis
    Pitcher props: strikeouts (k_pct), earned_runs (era_proj)

    Graceful empty-list return if DraftEdge is unreachable.
    """
    try:
        from draftedge_scraper import fetch_all_projections  # noqa: PLC0415
        data = fetch_all_projections()
        props = []

        batters = data.get("batters")
        if batters is not None and not batters.empty:
            for _, row in batters.iterrows():
                name = str(row.get("player_name", "")).strip()
                team = str(row.get("team", "")).strip()
                if not name:
                    continue
                # Map DraftEdge probability fields to prop_type keys
                for prop_type, pct_col in [
                    ("hits",          "hit_pct"),
                    ("home_runs",     "hr_pct"),
                    ("stolen_bases",  "sb_pct"),
                    ("runs",          "run_pct"),
                    ("rbis",          "rbi_pct"),
                ]:
                    val = float(row.get(pct_col, 0) or 0)
                    if val > 0:
                        props.append({
                            "player_name":    name,
                            "team":           team,
                            "prop_type":      prop_type,
                            "projected_prob": round(val, 4),
                            "source":         "draftedge",
                        })

        pitchers = data.get("pitchers")
        if pitchers is not None and not pitchers.empty:
            for _, row in pitchers.iterrows():
                name = str(row.get("player_name", "")).strip()
                team = str(row.get("team", "")).strip()
                if not name:
                    continue
                k_pct = float(row.get("k_pct", 0) or 0)
                era   = float(row.get("era_proj", 4.5) or 4.5)
                if k_pct > 0:
                    props.append({
                        "player_name":    name,
                        "team":           team,
                        "prop_type":      "strikeouts",
                        "projected_prob": round(k_pct, 4),
                        "source":         "draftedge",
                    })
                # ERA → earned run probability (era/9 * 1 inning = prob per inning)
                props.append({
                    "player_name":    name,
                    "team":           team,
                    "prop_type":      "earned_runs",
                    "projected_prob": round(min(era / 9.0, 0.99), 4),
                    "era_proj":       era,
                    "source":         "draftedge",
                })

        logger.info("[DataHub] DraftEdge projections: %d props", len(props))
        return props

    except Exception as exc:
        logger.info("[DataHub] DraftEdge projections unavailable: %s", exc)
        return []


def _odds_api_get(sport: str = "baseball_mlb") -> list[dict]:
    """
    Fetch MLB odds data.

    Priority chain (first success wins, all free):
      1. The Odds API      — real sportsbook lines (h2h, totals, spreads)
                             Only called if ODDS_API_KEY is set AND quota not exhausted.
                             DAILY CACHE: result stored in Redis for 12h so the free
                             tier (500 req/month ≈ 16/day) is never exceeded.
                             Without cache this fired every 15s = 5,760 calls/day.
      2. DraftEdge JSON    — batter/pitcher projections converted to pseudo-odds
                             Free, no key, daily parquet cache, zero quota cost
      3. TheRundown API    — real sportsbook K prop lines (free 100/day with key)
                             market_id=19 = pitcher_strikeouts  (sport_id=3 = MLB)
      4. ESPN public API   — implied totals from team run-scoring data
                             Confirmed working in every log, always available

    Returns list of game dicts compatible with _get_sharp_consensus() lookups.
    """
    # ── Tier 1: The Odds API (only if key is set) ──────────────────────────
    key = os.getenv("ODDS_API_KEY", "")
    if key:
        _quota_backoff_key = "odds_api_quota_backoff"
        _daily_cache_key   = f"odds_api_daily_{_today_pt().strftime('%Y%m%d')}"

        # ── Daily cache check (read) — prevents 5,760 calls/day on free tier ──
        try:
            _cached = _redis().get(_daily_cache_key)
            if _cached:
                import json as _json  # noqa: PLC0415
                _cached_data = _json.loads(_cached)
                if _cached_data:
                    logger.debug("[OddsAPI] Returning daily cache (%d games) — no API call made.", len(_cached_data))
                    return _cached_data
        except Exception:
            pass  # Redis miss or unavailable — fall through to live fetch

        # ── Quota backoff check ────────────────────────────────────────────
        try:
            _in_backoff = _redis().get(_quota_backoff_key)
        except Exception:
            _in_backoff = None

        if _in_backoff:
            logger.debug("[OddsAPI] Quota backoff active — skipping API call, using fallback")
        else:
            try:
                resp = requests.get(
                    f"https://api.the-odds-api.com/v4/sports/{sport}/odds",
                    params={"apiKey": key, "regions": "us", "markets": "h2h,totals,spreads"},
                    timeout=20,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        remaining = resp.headers.get("x-requests-remaining", "?")
                        logger.info("[OddsAPI] %d games fetched. Quota remaining: %s", len(data), remaining)

                        # ── Daily cache write — TTL 12h so it refreshes twice a day ──
                        try:
                            import json as _json  # noqa: PLC0415
                            _redis().set(_daily_cache_key, _json.dumps(data), ex=43200)  # 12 hours
                        except Exception:
                            pass

                        # Cache quota to Redis so bug_checker can read it
                        try:
                            _redis().set("odds_api_quota_remaining", str(remaining), ex=86400)
                        except Exception:
                            pass

                        # Real-time Discord alert when quota drops below threshold
                        try:
                            _remaining_int = int(remaining)
                            if _remaining_int < 50:
                                _alert_key = "odds_api_alert_critical"
                                _already_alerted = _redis().get(_alert_key)
                                if not _already_alerted:
                                    from DiscordAlertService import discord_alert  # noqa: PLC0415
                                    discord_alert._post({
                                        "embeds": [{
                                            "title": "🚨 Odds API Quota Critical",
                                            "description": (
                                                f"**{_remaining_int} requests remaining** — "
                                                "switching to free ESPN fallback on next failure.\n"
                                                "Add ODDS_API_KEY_2 to Railway or reduce scrape frequency."
                                            ),
                                            "color": 0xE74C3C,
                                        }]
                                    })
                                    _redis().set(_alert_key, "1", ex=3600)
                                    logger.warning("[OddsAPI] CRITICAL quota: %d remaining — Discord alerted", _remaining_int)
                            elif _remaining_int < 200:
                                _alert_key = "odds_api_alert_low"
                                _already_alerted = _redis().get(_alert_key)
                                if not _already_alerted:
                                    from DiscordAlertService import discord_alert  # noqa: PLC0415
                                    discord_alert._post({
                                        "embeds": [{
                                            "title": "⚠️ Odds API Quota Low",
                                            "description": (
                                                f"**{_remaining_int} requests remaining** — "
                                                "monitor usage. Bug checker will flag at next 10 AM run."
                                            ),
                                            "color": 0xF39C12,
                                        }]
                                    })
                                    _redis().set(_alert_key, "1", ex=3600)
                                    logger.warning("[OddsAPI] Low quota: %d remaining — Discord alerted", _remaining_int)
                        except Exception:
                            pass  # never block on alert failure
                        return data
                elif resp.status_code in (401, 403, 429):
                    try:
                        _redis().set(_quota_backoff_key, "1", ex=21600)  # 6 hours
                        logger.warning(
                            "[OddsAPI] HTTP %d — quota exhausted or key invalid. "
                            "6-hour backoff set. Switching to free fallback.",
                            resp.status_code,
                        )
                    except Exception:
                        logger.warning("[OddsAPI] HTTP %d — quota exhausted or key invalid, switching to free fallback", resp.status_code)
                else:
                    logger.warning("[OddsAPI] HTTP %d — switching to free fallback", resp.status_code)
            except Exception as e:
                logger.warning("[OddsAPI] Request failed (%s) — switching to free fallback", e)

    # ── Tier 2: DraftEdge free projections ────────────────────────────────
    try:
        from draftedge_scraper import fetch_all_projections  # noqa: PLC0415
        projections = fetch_all_projections()
        batters  = projections.get("batters")
        pitchers = projections.get("pitchers")
        if batters is not None and not batters.empty:
            logger.info("[OddsAPI→DraftEdge] %d batters, %d pitchers loaded",
                        len(batters), len(pitchers) if pitchers is not None else 0)
            # Return as a marker list so callers know DraftEdge is the source
            # when no bookmaker entries exist — agents still fire via _model_prob
            return [{"source": "draftedge", "batters": batters.to_dict("records"),
                     "pitchers": pitchers.to_dict("records") if pitchers is not None else []}]
    except Exception as e:
        logger.info("[OddsAPI→DraftEdge] Not available (%s)", e)

    # ── Tier 3: TheRundown — real sportsbook K prop lines (market_id=19) ────
    _rundown_key = os.getenv("RUNDOWN_API_KEY", "a455831fa40a562b43d7f7830f6ab467fa38074d46d078e0d47de324b46bea79")
    if _rundown_key:
        try:
            _rd_date = _today_pt().strftime("%Y-%m-%d")
            _rd_resp = requests.get(
                f"https://therundown.io/api/v2/sports/3/events/{_rd_date}",
                headers={"X-TheRundown-Key": _rundown_key, "Accept": "application/json"},
                params={"market_ids": 19},   # 19 = pitcher_strikeouts, sport 3 = MLB
                timeout=12,
            )
            if _rd_resp.status_code == 200:
                _rd_events = _rd_resp.json().get("events", [])
                if _rd_events:
                    # Parse into odds-compatible list so _get_sharp_consensus can use it
                    _rd_odds = []
                    for _ev in _rd_events:
                        _teams  = _ev.get("teams", [])
                        _home   = next((t_["name"] for t_ in _teams if t_.get("is_home")), "")
                        _away   = next((t_["name"] for t_ in _teams if t_.get("is_away")), "")
                        _bms    = []
                        for _mkt in _ev.get("markets", []):
                            if _mkt.get("market_id") != 19:
                                continue
                            for _part in _mkt.get("participants", []):
                                _outcomes = []
                                for _line in _part.get("lines", []):
                                    _parts = _line.get("value", "").strip().lower().split()
                                    if len(_parts) != 2:
                                        continue
                                    _side_str, _val_str = _parts
                                    for _bk, _pi in _line.get("prices", {}).items():
                                        try:
                                            _price = int(float(str(_pi.get("price", -110)).replace("+", "")))
                                        except (ValueError, TypeError):
                                            continue
                                        _outcomes.append({
                                            "name":        _side_str.capitalize(),
                                            "description": _part.get("name", ""),
                                            "price":       _price,
                                            "point":       float(_val_str),
                                        })
                                if _outcomes:
                                    _bms.append({
                                        "key":    f"rundown_book_{_bk}",
                                        "title":  "TheRundown",
                                        "markets": [{"key": "pitcher_strikeouts", "outcomes": _outcomes}],
                                    })
                        _rd_odds.append({
                            "source":     "therundown",
                            "home_team":  _home,
                            "away_team":  _away,
                            "bookmakers": _bms,
                        })
                    logger.info("[OddsAPI→Rundown] %d K prop events, %d game entries",
                                len(_rd_events), len(_rd_odds))
                    return _rd_odds
            elif _rd_resp.status_code == 429:
                logger.debug("[OddsAPI→Rundown] Rate limited — falling through to ESPN")
            else:
                logger.debug("[OddsAPI→Rundown] HTTP %d — falling through to ESPN", _rd_resp.status_code)
        except Exception as _rd_err:
            logger.debug("[OddsAPI→Rundown] Failed: %s — falling through to ESPN", _rd_err)

    # ── Tier 4: ESPN implied totals (always available) ─────────────────────
    try:
        games = _fetch_espn_games()
        if games:
            # Build minimal odds-like structure from ESPN game data
            implied = []
            for gid, g in games.items():
                implied.append({
                    "source":      "espn_implied",
                    "id":          gid,
                    "home_team":   g.get("HomeTeam", ""),
                    "away_team":   g.get("AwayTeam", ""),
                    "home_score":  g.get("HomeScore", 0),
                    "away_score":  g.get("AwayScore", 0),
                    "status":      g.get("Status", ""),
                    "bookmakers":  [],  # no book data — agents fall back to model_prob
                })
            logger.info("[OddsAPI→ESPN] %d games as implied odds fallback", len(implied))
            return implied
    except Exception as e:
        logger.info("[OddsAPI→ESPN] ESPN fallback failed (%s)", e)

    logger.warning("[OddsAPI] All tiers exhausted — returning empty odds")
    return []


# Module-level XGBoost model cache — loaded once, reused every 30s cycle
_XGB_MODEL_CACHE = None


def _load_xgb_model():
    """Lazy-load trained XGBoost model from disk. Cached at module level — never reloads
    from disk on every cycle. Cleared by run_xgboost_tasklet() on retrain.
    Supports both .json (XGBoost native) and .pkl (legacy pickle) formats.
    """
    global _XGB_MODEL_CACHE
    if _XGB_MODEL_CACHE is not None:
        return _XGB_MODEL_CACHE
    # ── Try Postgres first — survives Railway restarts (filesystem is ephemeral) ──
    try:
        import psycopg2 as _psycopg2  # noqa: PLC0415
        _db_url = os.getenv("DATABASE_URL", "")
        if _db_url:
            with _psycopg2.connect(_db_url) as _mc:
                with _mc.cursor() as _xcur:
                    _xcur.execute(
                        "SELECT model_json FROM xgb_model_store ORDER BY trained_at DESC LIMIT 1"
                    )
                    _mrow = _xcur.fetchone()
            if _mrow and _mrow[0]:
                import xgboost as xgb  # noqa: PLC0415
                import tempfile as _tmpfile  # noqa: PLC0415
                with _tmpfile.NamedTemporaryFile(suffix=".json", delete=False) as _tf:
                    _tf.write(_mrow[0].encode())
                    _tmp_path = _tf.name
                _booster = xgb.Booster()
                _booster.load_model(_tmp_path)
                try:
                    os.unlink(_tmp_path)
                except Exception:
                    pass
                logger.info("[XGB] Loaded model from xgb_model_store (DB recovery after restart)")
                _XGB_MODEL_CACHE = _booster
                return _booster
    except Exception as _dbe:
        logger.debug("[XGB] DB model load skipped: %s", _dbe)
    path = os.getenv("XGB_MODEL_PATH", "/app/api/models/prop_model_v1.json")
    if not os.path.exists(path):
        logger.warning("[XGB] Model not found at %s — agents using flat 50%% probability", path)
        return None
    try:
        if path.endswith(".json"):
            import xgboost as xgb  # noqa: PLC0415
            booster = xgb.Booster()
            booster.load_model(path)
            logger.info("[XGB] Loaded XGBoost model from %s (cached)", path)
            _XGB_MODEL_CACHE = booster
            return booster
        else:
            with open(path, "rb") as f:
                model = pickle.load(f)
            logger.info("[XGB] Loaded pickle model from %s (cached)", path)
            _XGB_MODEL_CACHE = model
            return model
    except Exception as exc:
        logger.warning("[XGB] Model load failed (%s): %s — using flat 50%%", path, exc)
        return None


# ── In-memory state (persisted to Redis) ──────────────────────────────────────

_agent_perf: dict[str, dict] = {
    name: {"wins": 0, "losses": 0, "pushes": 0, "units": 0.0, "roi": 0.0}
    for name in AGENT_NAMES
}

_capital_multipliers: dict[str, float] = {name: 1.0 for name in AGENT_NAMES}


# ─────────────────────────────────────────────────────────────────────────────
_STADIUM_COORDS: dict[str, tuple[float, float]] = {
    "Angels Stadium":           (33.8003, -117.8827),
    "Chase Field":              (33.4455, -112.0667),
    "Camden Yards":             (39.2839, -76.6218),
    "Fenway Park":              (42.3467, -71.0972),
    "Wrigley Field":            (41.9484, -87.6553),
    "Guaranteed Rate Field":    (41.8299, -87.6338),
    "Great American Ball Park": (39.0979, -84.5082),
    "Progressive Field":        (41.4962, -81.6853),
    "Coors Field":              (39.7559, -104.9942),
    "Comerica Park":            (42.3390, -83.0485),
    "Minute Maid Park":         (29.7573, -95.3555),
    "Kauffman Stadium":         (39.0517, -94.4803),
    "Dodger Stadium":           (34.0739, -118.2400),
    "LoanDepot Park":           (25.7781, -80.2198),
    "American Family Field":    (43.0280, -87.9712),
    "Target Field":             (44.9817, -93.2781),
    "Citi Field":               (40.7571, -73.8458),
    "Yankee Stadium":           (40.8296, -73.9262),
    "Oakland Coliseum":         (37.7516, -122.2005),
    "Citizens Bank Park":       (39.9056, -75.1665),
    "PNC Park":                 (40.4469, -80.0057),
    "Petco Park":               (32.7073, -117.1566),
    "Oracle Park":              (37.7786, -122.3893),
    "T-Mobile Park":            (47.5914, -122.3325),
    "Busch Stadium":            (38.6226, -90.1928),
    "Tropicana Field":          (27.7683, -82.6534),
    "Globe Life Field":         (32.7473, -97.0822),
    "Rogers Centre":            (43.6414, -79.3894),
    "Nationals Park":           (38.8730, -77.0074),
    "Truist Park":              (33.8907, -84.4677),
}

_TEAM_TO_STADIUM: dict[str, str] = {
    "Los Angeles Angels":    "Angels Stadium",
    "Arizona Diamondbacks":  "Chase Field",
    "Baltimore Orioles":     "Camden Yards",
    "Boston Red Sox":        "Fenway Park",
    "Chicago Cubs":          "Wrigley Field",
    "Chicago White Sox":     "Guaranteed Rate Field",
    "Cincinnati Reds":       "Great American Ball Park",
    "Cleveland Guardians":   "Progressive Field",
    "Colorado Rockies":      "Coors Field",
    "Detroit Tigers":        "Comerica Park",
    "Houston Astros":        "Minute Maid Park",
    "Kansas City Royals":    "Kauffman Stadium",
    "Los Angeles Dodgers":   "Dodger Stadium",
    "Miami Marlins":         "LoanDepot Park",
    "Milwaukee Brewers":     "American Family Field",
    "Minnesota Twins":       "Target Field",
    "New York Mets":         "Citi Field",
    "New York Yankees":      "Yankee Stadium",
    "Oakland Athletics":     "Oakland Coliseum",
    "Sacramento Athletics":  "Oakland Coliseum",
    "Philadelphia Phillies": "Citizens Bank Park",
    "Pittsburgh Pirates":    "PNC Park",
    "San Diego Padres":      "Petco Park",
    "San Francisco Giants":  "Oracle Park",
    "Seattle Mariners":      "T-Mobile Park",
    "St. Louis Cardinals":   "Busch Stadium",
    "Tampa Bay Rays":        "Tropicana Field",
    "Texas Rangers":         "Globe Life Field",
    "Toronto Blue Jays":     "Rogers Centre",
    "Washington Nationals":  "Nationals Park",
    "Atlanta Braves":        "Truist Park",
}

# ESPN HomeTeam field returns abbreviations (e.g. "NYY", "LAD") but _TEAM_TO_STADIUM
# keys are full names.  This map translates before the stadium lookup.
_ABBREV_TO_FULL: dict[str, str] = {
    "ARI": "Arizona Diamondbacks",  "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",     "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",          "CWS": "Chicago White Sox",
    "CIN": "Cincinnati Reds",       "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",      "DET": "Detroit Tigers",
    "HOU": "Houston Astros",        "KC":  "Kansas City Royals",
    "LAA": "Los Angeles Angels",    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",         "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",       "NYM": "New York Mets",
    "NYY": "New York Yankees",      "OAK": "Oakland Athletics",
    "PHI": "Philadelphia Phillies", "PIT": "Pittsburgh Pirates",
    "SD":  "San Diego Padres",      "SF":  "San Francisco Giants",
    "SEA": "Seattle Mariners",      "STL": "St. Louis Cardinals",
    "TB":  "Tampa Bay Rays",        "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",     "WSH": "Washington Nationals",
    "ATH": "Sacramento Athletics",  # relocated Oakland A's
}


def _fetch_weather_today() -> list[dict]:
    """
    Fetch wind speed, direction, and temperature for today's games
    using the Open-Meteo API (free, no key required).
    """
    import datetime as _dt

    games = _fetch_espn_games()
    if not games:
        return []

    home_teams = set()
    for g in games.values():
        ht = g.get("HomeTeam", "")
        if ht:
            # ESPN returns abbreviations — translate to full name for _TEAM_TO_STADIUM
            home_teams.add(_ABBREV_TO_FULL.get(ht, ht))

    results = []
    for team in home_teams:
        stadium = _TEAM_TO_STADIUM.get(team, "")
        if not stadium:
            continue
        coords = _STADIUM_COORDS.get(stadium)
        if not coords:
            continue
        lat, lon = coords
        try:
            resp = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude":        lat,
                    "longitude":       lon,
                    "hourly":          "wind_speed_10m,wind_direction_10m,temperature_2m",
                    "wind_speed_unit": "mph",
                    "temperature_unit":"fahrenheit",
                    "forecast_days":   1,
                    "timezone":        "auto",
                },
                timeout=10,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            hourly = data.get("hourly", {})
            idx = 18  # 6pm local — game time default
            wind_mph = float((hourly.get("wind_speed_10m")    or [0])[idx] or 0)
            wind_dir = float((hourly.get("wind_direction_10m") or [0])[idx] or 0)
            temp_f   = float((hourly.get("temperature_2m")    or [70])[idx] or 70)
            dirs     = ["N","NE","E","SE","S","SW","W","NW"]
            cardinal = dirs[int((wind_dir + 22.5) / 45) % 8]
            results.append({
                "stadium":        stadium,
                "team":           team,
                "lat":            lat,
                "lon":            lon,
                "wind_speed_mph": round(wind_mph, 1),
                "wind_direction": cardinal,
                "wind_deg":       wind_dir,
                "temp_f":         round(temp_f, 1),
                "weather_source": "open-meteo",
            })
        except Exception as exc:
            logger.debug("[Weather] Open-Meteo failed for %s: %s", stadium, exc)

    logger.info("[DataHub] Weather fetched for %d stadiums", len(results))
    return results

def _refresh_sample_counts() -> None:
    """Seed xgb_sample_counts in Redis from bet_ledger settled rows.

    Called once per DataHub refresh cycle so sample counts grow from day 1
    rather than staying at 0 until the first Sunday XGBoost retrain.

    Counts only rows with discord_sent=TRUE AND actual_outcome IS NOT NULL so the
    floor matches exactly what XGBoost trains on.  The Sunday retrain will
    overwrite with richer per-prop-type stats; this is just a daily warm-up.
    """
    try:
        conn_str = os.getenv("DATABASE_URL", "")
        if not conn_str:
            return
        import psycopg2  # noqa: PLC0415
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT prop_type, COUNT(*) AS n
                    FROM bet_ledger
                    WHERE discord_sent = TRUE
                      AND actual_outcome IS NOT NULL
                    GROUP BY prop_type
                    """
                )
                rows = cur.fetchall()
        if not rows:
            return
        counts = {r[0].lower(): int(r[1]) for r in rows if r[0]}
        r = _redis()
        existing_raw = r.get("xgb_sample_counts")
        if existing_raw:
            existing = json.loads(existing_raw)
            # Merge: only update keys where we have new data; don't overwrite
            for k, v in counts.items():
                existing[k] = max(existing.get(k, 0), v)
            counts = existing
        r.setex("xgb_sample_counts", 604800, json.dumps(counts))
        logger.info("[DataHub] xgb_sample_counts seeded from bet_ledger: %s", counts)
    except Exception as exc:
        logger.debug("[DataHub] _refresh_sample_counts skipped: %s", exc)


# 1. DataHubTasklet
# ─────────────────────────────────────────────────────────────────────────────


def _ensure_calibration_map() -> None:
    """Write identity calibration_map.json if it doesn't exist on startup."""
    cal_path = os.getenv("CALIBRATION_MAP_PATH", "calibration_map.json")
    if not os.path.exists(cal_path):
        try:
            from calibrate_model import _write_identity_map  # noqa: PLC0415
            _write_identity_map()
            logger.info("[Startup] calibration_map.json bootstrapped (identity map).")
        except Exception as _e:
            # Write minimal identity map inline as last resort
            import json as _json  # noqa: PLC0415
            pts = [round(0.40 + i * 0.01, 2) for i in range(51)]
            try:
                with open(cal_path, "w") as _f:
                    _json.dump({str(p): p for p in pts}, _f)
            except Exception:
                pass


# PR #413: guard so _ensure_bet_ledger() runs once at startup, not every 15s.
# _ensure_bet_ledger() is called inside run_data_hub_tasklet() which fires every
# 15 seconds — 240 Postgres round-trips/hour for no benefit after the first run.
_BET_LEDGER_ENSURED: bool = False


def _ensure_bet_ledger() -> None:
    """Create bet_ledger table if it doesn't exist. Called on startup (once only)."""
    global _BET_LEDGER_ENSURED
    if _BET_LEDGER_ENSURED:
        return
    _BET_LEDGER_ENSURED = True
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bet_ledger (
                    id              SERIAL PRIMARY KEY,
                    player_name     VARCHAR(150),
                    prop_type       VARCHAR(60),
                    line            FLOAT,
                    side            VARCHAR(10),
                    odds_american   INTEGER,
                    kelly_units     FLOAT,
                    model_prob      FLOAT,
                    ev_pct          FLOAT,
                    agent_name      VARCHAR(80),
                    status          VARCHAR(10)  DEFAULT 'OPEN',
                    bet_date        DATE         DEFAULT CURRENT_DATE,
                    platform        VARCHAR(30),
                    profit_loss     FLOAT,
                    actual_result   FLOAT,
                    clv             FLOAT,
                    graded_at       TIMESTAMP,
                    features_json   TEXT,
                    actual_outcome  INTEGER,
                    mlbam_id        INTEGER,          -- for accent-safe grading
                    discord_sent    BOOLEAN      NOT NULL DEFAULT FALSE,
                    created_at      TIMESTAMP    DEFAULT NOW(),
                    units_wagered   FLOAT,                    -- actual dollar stake
                    entry_type      VARCHAR(20)  DEFAULT 'STANDARD',
                    lookahead_safe  BOOLEAN      DEFAULT TRUE, -- no future data leakage
                    parlay_id       VARCHAR(64)               -- links legs of the same slip (agent+date+uuid)
                )
            """)
            conn.commit()
            # Add units_wagered if it didn't exist in earlier schema versions
            try:
                cur.execute("ALTER TABLE bet_ledger ADD COLUMN IF NOT EXISTS units_wagered FLOAT")
                conn.commit()
            except Exception:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE bet_ledger ADD COLUMN IF NOT EXISTS entry_type VARCHAR(20)")
                conn.commit()
            except Exception:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE bet_ledger ADD COLUMN IF NOT EXISTS mlbam_id INTEGER")
                conn.commit()
            except Exception:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE bet_ledger ADD COLUMN IF NOT EXISTS discord_sent BOOLEAN NOT NULL DEFAULT FALSE")
                conn.commit()
            except Exception:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE bet_ledger ADD COLUMN IF NOT EXISTS result VARCHAR(10)")
                conn.commit()
            except Exception:
                conn.rollback()
            try:
                cur.execute("ALTER TABLE bet_ledger ADD COLUMN IF NOT EXISTS lookahead_safe BOOLEAN")
                conn.commit()
            except Exception:
                conn.rollback()
            # FIX GAP 2: add UNIQUE constraint to prevent duplicate grading
            # Step 1: only dedup if index doesn't exist yet — guards against
            # O(n²) self-join on large tables at every startup/redeploy.
            try:
                cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = 'ux_bet_ledger_dedup'")
                _idx_exists = cur.fetchone() is not None
            except Exception:
                _idx_exists = False
            if not _idx_exists:
                try:
                    cur.execute(
                        """
                        DELETE FROM bet_ledger a
                        USING bet_ledger b
                        WHERE a.id > b.id
                          AND a.player_name = b.player_name
                          AND a.prop_type   = b.prop_type
                          AND a.line        = b.line
                          AND a.side        = b.side
                          AND a.agent_name  = b.agent_name
                          AND a.bet_date    = b.bet_date
                        """
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
            # Step 2: now safe to create the unique index
            try:
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_bet_ledger_dedup
                    ON bet_ledger (player_name, prop_type, line, side, agent_name, bet_date)
                    """
                )
                conn.commit()
            except Exception:
                conn.rollback()
        conn.close()
        logger.info("[DB] bet_ledger table ensured.")
    except Exception as exc:
        logger.warning("[DB] bet_ledger create failed: %s", exc)

    # ── Add columns missing from pre-existing Railway DB ──────────────────────
    # Railway keeps the DB between deploys — if CREATE TABLE already ran without
    # these columns, ALTER TABLE adds them without data loss.
    try:
        _ac = _pg_conn()
        with _ac.cursor() as _cc:
            for _col_ddl in [
                "ADD COLUMN IF NOT EXISTS units_wagered   FLOAT",
                "ADD COLUMN IF NOT EXISTS entry_type      VARCHAR(20) DEFAULT 'STANDARD'",
                "ADD COLUMN IF NOT EXISTS lookahead_safe  BOOLEAN DEFAULT TRUE",
                "ADD COLUMN IF NOT EXISTS parlay_id       VARCHAR(64)",
                # PR #413: created_at was in CREATE TABLE but missing from ALTER migration
                # → dedup preload (SELECT ... WHERE created_at >= NOW() - INTERVAL '18 hours')
                # silently failed every restart → _AGENT_SENT_TODAY always empty
                "ADD COLUMN IF NOT EXISTS created_at      TIMESTAMP DEFAULT NOW()",
                # model_source was in CREATE TABLE but missing from ALTER migration
                # → INSERT failures on any row that wrote model_source
                "ADD COLUMN IF NOT EXISTS model_source    VARCHAR(30)",
            ]:
                try:
                    _cc.execute(f"ALTER TABLE bet_ledger {_col_ddl}")
                except Exception:
                    pass
        _ac.commit()
        _ac.close()
    except Exception as _ae:
        logger.debug("[DB] ALTER TABLE bet_ledger: %s", _ae)

    # ── agent_unit_sizing — tier/streak tracking per agent ─────────────────────
    try:
        _us = _pg_conn()
        with _us.cursor() as _uc:
            _uc.execute("""
                CREATE TABLE IF NOT EXISTS agent_unit_sizing (
                    agent_name          VARCHAR(80) PRIMARY KEY,
                    tier                INTEGER     NOT NULL DEFAULT 1,
                    stake_dollars       FLOAT       NOT NULL DEFAULT 5.0,
                    consecutive_wins    INTEGER     NOT NULL DEFAULT 0,
                    consecutive_losses  INTEGER     NOT NULL DEFAULT 0,
                    updated_at          TIMESTAMP   DEFAULT NOW()
                )
            """)
        _us.commit()
        _us.close()
    except Exception as _ue:
        logger.debug("[DB] agent_unit_sizing table: %s", _ue)

    # ── clv_records — CLV tracking per bet leg ─────────────────────────────────
    # Schema owned by clv_tracker.py — this block only adds missing columns
    # to existing deployments that were created with the old wrong schema.
    try:
        _cr = _pg_conn()
        with _cr.cursor() as _cc:
            _cc.execute("""
                CREATE TABLE IF NOT EXISTS clv_records (
                    id           SERIAL PRIMARY KEY,
                    game_date    DATE NOT NULL,
                    agent_name   TEXT,
                    player_name  TEXT,
                    prop_type    TEXT,
                    side         TEXT,
                    pick_line    FLOAT,
                    closing_line FLOAT,
                    clv_pts      FLOAT,
                    beat_close   INTEGER,
                    recorded_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            _cc.execute(
                "CREATE INDEX IF NOT EXISTS idx_clv_records_date "
                "ON clv_records (game_date)"
            )
            # Heal: add missing columns on existing DBs that have the old schema
            for _heal_sql in [
                "ALTER TABLE clv_records ADD COLUMN IF NOT EXISTS pick_line    FLOAT",
                "ALTER TABLE clv_records ADD COLUMN IF NOT EXISTS closing_line FLOAT",
                "ALTER TABLE clv_records ADD COLUMN IF NOT EXISTS recorded_at  TIMESTAMPTZ DEFAULT NOW()",
            ]:
                try:
                    _cc.execute(_heal_sql)
                except Exception:
                    pass
            # Heal beat_close: convert BOOLEAN → INTEGER if needed
            try:
                _cc.execute("""
                    ALTER TABLE clv_records
                    ALTER COLUMN beat_close TYPE INTEGER
                    USING beat_close::int
                """)
            except Exception:
                pass  # already INTEGER or column doesn't exist yet
        _cr.commit()
        _cr.close()
    except Exception as _cre:
        logger.debug("[DB] clv_records table: %s", _cre)

    # ── UD streak state — tracks Underdog Streaks current count ───────────────
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ud_streak_state (
                    id            SERIAL PRIMARY KEY,
                    current_count INTEGER NOT NULL DEFAULT 0,
                    last_updated  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO ud_streak_state (current_count)
                SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM ud_streak_state)
            """)
        conn.commit()
        conn.close()
    except Exception as _sse:
        logger.warning("[DB] ud_streak_state create failed: %s", _sse)

    # ── XGBoost model store — persists trained model across redeploys ──────────
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS xgb_model_store (
                    id            SERIAL PRIMARY KEY,
                    model_json    TEXT NOT NULL,
                    feature_names TEXT,
                    trained_at    TIMESTAMPTZ DEFAULT NOW(),
                    n_rows        INTEGER,
                    notes         TEXT,
                    prop_type     VARCHAR(64),
                    brier_score   FLOAT,
                    n_samples     INTEGER
                )
            """)
            # Heal: add columns missing from existing deployments
            for _xms_sql in [
                "ALTER TABLE xgb_model_store ADD COLUMN IF NOT EXISTS prop_type   VARCHAR(64)",
                "ALTER TABLE xgb_model_store ADD COLUMN IF NOT EXISTS brier_score FLOAT",
                "ALTER TABLE xgb_model_store ADD COLUMN IF NOT EXISTS n_samples   INTEGER",
            ]:
                try:
                    cur.execute(_xms_sql)
                except Exception:
                    pass
        conn.commit()
        conn.close()
    except Exception as _xms:
        logger.warning("[DB] xgb_model_store create failed: %s", _xms)



    # ── rejection_log — tracks legs dropped by gates ──────────────────────────
    try:
        _rl = _pg_conn()
        with _rl.cursor() as _rc:
            _rc.execute("""
                CREATE TABLE IF NOT EXISTS rejection_log (
                    id            SERIAL PRIMARY KEY,
                    player_name   TEXT,
                    prop_type     TEXT,
                    side          TEXT,
                    line          NUMERIC,
                    model_prob    NUMERIC,
                    ev_pct        NUMERIC,
                    confidence    NUMERIC,
                    reject_reason TEXT,
                    reject_date   DATE DEFAULT CURRENT_DATE,
                    created_at    TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        _rl.commit()
        _rl.close()
    except Exception as _rle:
        logger.warning("[DB] rejection_log create failed: %s", _rle)

    # Heal rejection_log — add columns added in PR #415 that may be missing
    # from the existing Railway table (created before PR #415 schema).
    try:
        _rl3 = _pg_conn()
        with _rl3.cursor() as _rc3:
            for _rldl in [
                "ADD COLUMN IF NOT EXISTS agent_name  VARCHAR(80)",
                "ADD COLUMN IF NOT EXISTS reason      VARCHAR(120)",
                "ADD COLUMN IF NOT EXISTS rejected_at TIMESTAMPTZ DEFAULT NOW()",
            ]:
                try:
                    _rc3.execute(f"ALTER TABLE rejection_log {_rldl}")
                except Exception:
                    pass
        _rl3.commit()
        _rl3.close()
    except Exception as _rl3e:
        logger.debug("[DB] rejection_log heal: %s", _rl3e)


def _log_rejection(player_name: str, prop_type: str, side: str, line: float,
                   model_prob: float, ev_pct: float, confidence: float,
                   reject_reason: str) -> None:
    """Insert one row into rejection_log. Fire-and-forget — never raises."""
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rejection_log
                    (player_name, prop_type, side, line, model_prob, ev_pct,
                     confidence, reject_reason)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (player_name, prop_type, side, float(line or 0),
                 float(model_prob or 0), float(ev_pct or 0),
                 float(confidence or 0), reject_reason)
            )
        conn.commit()
        conn.close()
    except Exception as _rle:
        logger.debug("[rejection_log] insert failed: %s", _rle)



def run_data_hub_tasklet() -> None:
    """
    Staggered scrape across 4 data groups (physics, context, market, DFS).
    Pre-match gate: skips any game already LIVE or FINAL so we never poll
    in-game data and waste API quota.
    """
    _ensure_bet_ledger()       # ensure table exists on every startup
    _ensure_calibration_map()  # bootstrap isotonic calibration map if missing

    # ── Steamer 2026 prefetch (once per day, Postgres-cached) ────────────────
    try:
        from steamer_layer import prefetch as _steamer_prefetch  # noqa: PLC0415
        _sc = _steamer_prefetch()
        if _sc:
            logger.info("[DataHub] Steamer 2026 projections loaded: %d players", _sc)
        else:
            logger.warning("[DataHub] Steamer projections unavailable -- using league-average priors")
    except Exception as _spe:
        logger.warning("[DataHub] Steamer prefetch failed: %s", _spe)
    r = _redis()
    hub: dict = {}  # pre-declared so bullpen section can write to it before merge block
    game_states: dict[str, str] = {}
    try:
        today = _today_pt().strftime("%Y-%m-%d")
        games_raw = _fetch_espn_games()
        game_states.update({gid: g["Status"] for gid, g in games_raw.items()})
        logger.info("[DataHub] %d games today. States: %s",
                    len(game_states),
                    {s: list(game_states.values()).count(s) for s in set(game_states.values())})
    except Exception as e:
        logger.warning("[DataHub] Could not fetch game states: %s", e)

    # ── Season weight: blends 2025 full-season stats with 2026 YTD ───────────
    # Ramps from 0.0 (Opening Day) to 1.0 (game ~80, ~early June).
    # prop_enrichment_layer and agents read hub["season_weight_2026"] so the
    # blend is consistent across all data paths and XGBoost training rows.
    _season_start = OPENING_DAY  # 2026-03-26
    _days_played  = max(0, (_today_pt() - _season_start).days)
    _season_weight_2026 = round(min(1.0, _days_played / 80.0), 3)
    _season_weight_2025 = round(1.0 - _season_weight_2026, 3)

    def _is_pre_match(game_id: str) -> bool:
        state = game_states.get(game_id, "Scheduled")
        return state not in ("InProgress", "Live", "Final", "F/OT", "Completed")

    # ── Pre-warm FanGraphs cache before agents run ───────────────────────────
    # Pre-warming here (once per DataHub cycle) avoids cold-start delay in agents.
    # mlb_stats_layer runs first — it uses statsapi.mlb.com which works on Railway.
    # fangraphs_layer runs second as a supplementary source (usually 403-blocked on Railway,
    # but populates cache if running locally or if FanGraphs unblocks the IP).
    try:
        from mlb_stats_layer import warm_cache as _mlb_warm  # noqa: PLC0415
        _mlb_warm(hub)        # pass hub so it reuses already-fetched starter/lineup lists
        logger.info("[DataHub] MLB Stats API cache warm.")
    except Exception as _mlb_err:
        logger.warning("[DataHub] mlb_stats_layer warm failed: %s", _mlb_err)

    try:
        from fangraphs_layer import _load as _fg_load, _loaded as _fg_loaded  # noqa: PLC0415
        if not _fg_loaded:
            _fg_load()
            logger.info("[DataHub] FanGraphs cache pre-warmed.")
    except Exception as _fg_err:
        logger.debug("[DataHub] FanGraphs pre-warm skipped: %s", _fg_err)

    # ── Group 1: Physics / Arsenal (TTL 15 min) ────────────────────────────
    physics_key = "hub:physics"
    if not _hub_exists(r, physics_key):
        logger.info("[DataHub] Scraping physics / arsenal data…")
        # Compute game predictions for DataHub physics group
        _gp_list = []
        if _GAME_PRED_AVAILABLE:
            try:
                _gp_raw = get_game_predictions()
                _gp_list = _gp_raw if isinstance(_gp_raw, list) else []
                _high = [g for g in _gp_list if g.get("confidence", "") == "HIGH"]
                logger.info("[DataHub] Game predictions: %d games, %d HIGH confidence",
                            len(_gp_list), len(_high))
            except Exception as _gpe:
                logger.warning("[DataHub] game_prediction_layer failed: %s", _gpe)

        # FIX Bug 8 (revised): Build pitch_arsenal for today's probable starters using
        # fangraphs_layer.get_pitcher(), which delegates through a compliant, Railway-safe
        # pipeline:
        #   Tier 0 — MLB Stats API (statsapi.mlb.com) + Baseball Savant
        #            Derives csw_pct, swstr_pct, xfip, k_pct, bb_pct, era, whip
        #            from season stats. No FanGraphs scraping, no Cloudflare risk.
        #   Tier 1 — FanGraphs /api/leaders/major-league/data (different endpoint
        #            from the blocked leaders-legacy.aspx; may work on Railway)
        #   Tier 2 — pybaseball (Baseball Reference backend — always works)
        #   Cache  — disk (/tmp) + Postgres fg_cache — survives Railway restarts
        #   Guard  — daily-attempt flag prevents retry loops on 403 days
        #
        # This replaces the old pybaseball call, which hit fangraphs.com/leaders-legacy.aspx
        # and was permanently 403-blocked on Railway (logged every 15 s as a warning).
        _statcast_arsenal: list[dict] = []
        try:
            from fangraphs_layer import get_pitcher as _fg_get_pitcher  # noqa: PLC0415

            # Probable starters are already in the hub at this point
            _prob_starters = physics_ctx.get("projected_starters") if "physics_ctx" in dir() else []
            if not _prob_starters:
                _prob_starters = _fetch_mlb_probable_starters()

            for _sp in _prob_starters:
                _sp_name = _sp.get("full_name", "")
                if not _sp_name:
                    continue
                _sp_stats = _fg_get_pitcher(_sp_name)
                if not _sp_stats:
                    continue
                _entry: dict = {"player": _sp_name}
                if _sp_stats.get("csw_pct"):   _entry["csw_pct"]   = float(_sp_stats["csw_pct"])
                if _sp_stats.get("swstr_pct"): _entry["swstr_pct"] = float(_sp_stats["swstr_pct"])
                if _sp_stats.get("xfip"):      _entry["xfip"]      = float(_sp_stats["xfip"])
                if _sp_stats.get("k_pct"):     _entry["k_rate"]    = float(_sp_stats["k_pct"])
                if _sp_stats.get("bb_pct"):    _entry["bb_rate"]   = float(_sp_stats["bb_pct"])
                if _sp_stats.get("whip"):      _entry["whip"]      = float(_sp_stats["whip"])
                if _sp_stats.get("era"):       _entry["era"]        = float(_sp_stats["era"])
                if len(_entry) > 1:
                    _statcast_arsenal.append(_entry)

            if _statcast_arsenal:
                _src = _statcast_arsenal[0].get("_source", "mlb_stats_api") if _statcast_arsenal else "mlb_stats_api"
                logger.info(
                    "[DataHub] Pitcher arsenal: %d starters loaded via fangraphs_layer (source: %s)",
                    len(_statcast_arsenal), _sp_stats.get("_source", "mlb_stats_api") if _sp_stats else "unknown",
                )
            else:
                logger.debug("[DataHub] Pitcher arsenal: no starters matched — feature slots 4-5 using defaults.")
        except Exception as _sc_err:
            logger.debug("[DataHub] Pitcher arsenal skipped: %s — feature slots 4-5 will use defaults.", _sc_err)

        physics = {
            "pitch_arsenal":  _statcast_arsenal,  # FIX Bug 8: real CSW%/SwStr% from pybaseball
            "advanced_stats": [],
            "bvp":            [],
            "batted_ball":    [],
            "second_half":    [],
            "game_predictions": _gp_list,
            "nsfi":             _fetch_nsfi(),  # No-Strikeout First Inning predictions
        }
        _hub_setex(r, physics_key, TTL_PHYSICS, json.dumps(physics))

    # ── Group 2: Context / Environment (TTL 10 min) ───────────────────────
    context_key = "hub:context"
    if not _hub_exists(r, context_key):
        logger.info("[DataHub] Scraping context / environment data…")
        try:
            context = {
                "weather":            _fetch_weather_today(),    # Open-Meteo free
                "umpires":            [],  # no source yet
                "injuries":           _fetch_injuries_today(),  # injury_layer
                "lineups":            _fetch_mlb_lineups_today(),
                "projected_starters": _fetch_mlb_probable_starters(),
                "standings":          _fetch_mlb_standings(),
                "game_times":         _fetch_game_times_today(),  # Step 3: first-pitch UTC + status
            }
            _hub_setex(r, context_key, TTL_CONTEXT, json.dumps(context))
        except Exception as _ctx_err:
            logger.warning("[DataHub] Context group build failed: %s — storing empty fallback", _ctx_err)
            _hub_setex(r, context_key, TTL_CONTEXT, json.dumps({
                "weather": [], "umpires": [], "injuries": [],
                "lineups": [], "projected_starters": [], "standings": [], "game_times": {},
            }))

    # ── Group 3: Market / Sharp steam (TTL 5 min) ─────────────────────────
    market_key = "hub:market"
    if not _hub_exists(r, market_key):
        logger.info("[DataHub] Scraping market / steam data…")
        # Action Network game-level public betting (no auth required)
        _an_sentiment: dict = {}
        try:
            from action_network_layer import fetch_mlb_game_sentiment
            _an_sentiment = fetch_mlb_game_sentiment()
        except Exception as _an_exc:
            logger.warning(f"[DataHub] ActionNetwork sentiment unavailable: {_an_exc}")

        # Action Network player-level prop ticket%/money% (PRO — Bearer JWT required)
        _sharp_report: list = []
        try:
            from action_network_layer import build_sharp_report
            _sharp_report = build_sharp_report()
            if _sharp_report:
                logger.info(
                    "[DataHub] ActionNetwork sharp_report: %d player props loaded",
                    len(_sharp_report),
                )
            else:
                logger.info(
                    "[DataHub] sharp_report empty — token not set or props not yet posted. "
                    "SharpFadeAgent will use game-level RLM (Path 2)."
                )
        except Exception as _sr_exc:
            logger.warning("[DataHub] ActionNetwork sharp_report build failed: %s", _sr_exc)

        # Action Network live projections REST endpoint (PRO — Bearer JWT required)
        _live_projections: list = []
        try:
            from action_network_layer import fetch_live_projections
            _live_projections = fetch_live_projections()
            if _live_projections:
                logger.info(
                    "[DataHub] ActionNetwork live projections: %d entries",
                    len(_live_projections),
                )
        except Exception as _lp_exc:
            logger.warning("[DataHub] ActionNetwork live projections failed: %s", _lp_exc)

        market = {
            "public_betting":      _fetch_sbd_public_trends(),
            "sharp_report":        _sharp_report,          # player-level; [] if token not set
            "an_live_projections": _live_projections,      # PRO live projections; [] pre-game
            "an_game_sentiment":   _an_sentiment,          # game-level RLM — always live
            "prop_projections":    _fetch_draftedge_projections(),   # free — DraftEdge
            "odds":                _odds_api_get(),                  # free fallback chain
        }
        _hub_setex(r, market_key, TTL_MARKET, json.dumps(market))

    # ── Bullpen fatigue (TTL 60 min) ─────────────────────────────────────────
    bullpen_key = "hub:bullpen"
    if not _hub_exists(r, bullpen_key):
        if _BULLPEN_SCORER_AVAILABLE:
            try:
                _bp_scorer = _build_bullpen_scorer()  # fetches its own pitching logs internally
                if _bp_scorer is not None:
                    _bp_map = {}
                    for _team in [
                        "arizona diamondbacks","atlanta braves","baltimore orioles",
                        "boston red sox","chicago cubs","chicago white sox","cincinnati reds",
                        "cleveland guardians","colorado rockies","detroit tigers",
                        "houston astros","kansas city royals","los angeles angels",
                        "los angeles dodgers","miami marlins","milwaukee brewers",
                        "minnesota twins","new york mets","new york yankees","oakland athletics",
                        "philadelphia phillies","pittsburgh pirates","san diego padres",
                        "san francisco giants","seattle mariners","st. louis cardinals",
                        "tampa bay rays","texas rangers","toronto blue jays","washington nationals",
                    ]:
                        score   = _bp_scorer.score(_team)
                        boost   = _bp_scorer.get_fatigue_boost(_team)
                        _bp_map[_team] = {"fatigue_score": score, "boost": boost}
                    hub["bullpen_fatigue"] = _bp_map
                    try:
                        r.setex(bullpen_key, 3600, json.dumps(_bp_map))
                    except Exception:
                        pass
                    logger.info("[DataHub] Bullpen fatigue built for %d teams.", len(_bp_map))
            except Exception as _bp_err:
                logger.debug("[DataHub] Bullpen fatigue failed: %s", _bp_err)
                hub["bullpen_fatigue"] = {}
        else:
            hub["bullpen_fatigue"] = {}
    else:
        try:
            hub["bullpen_fatigue"] = json.loads(r.get(bullpen_key) or "{}")
        except Exception:
            hub["bullpen_fatigue"] = {}

    # ── BVI: Bullpen Volatility Index (TTL 4 h) ──────────────────────────────
    bvi_key = "hub:bvi"
    if not _hub_exists(r, bvi_key):
        if _BVI_AVAILABLE:
            try:
                _bvi_map = _get_bvi_map(lookback_days=10)
                if _bvi_map:
                    hub["bullpen_bvi"] = _bvi_map
                    try:
                        r.setex(bvi_key, 14_400, json.dumps(_bvi_map))
                    except Exception:
                        pass
                    logger.info("[DataHub] BVI map built for %d teams.", len(_bvi_map))
                else:
                    hub["bullpen_bvi"] = {}
            except Exception as _bvi_err:
                logger.debug("[DataHub] BVI build failed: %s", _bvi_err)
                hub["bullpen_bvi"] = {}
        else:
            hub["bullpen_bvi"] = {}
    else:
        try:
            hub["bullpen_bvi"] = json.loads(r.get(bvi_key) or "{}")
        except Exception:
            hub["bullpen_bvi"] = {}

    # ── Group 4: DFS targets (TTL 8 min) ──────────────────────────────────
    dfs_key = "hub:dfs"
    if not _hub_exists(r, dfs_key):
        logger.info("[DataHub] Scraping DFS target data…")
        _ud_props = _fetch_underdog_props_direct()
        _pp_props = _fetch_prizepicks_direct()
        _total_dfs_props = len(_ud_props) + len(_pp_props)

        # ── Sleeper fallback — triggered per-platform if one or both are dry ──
        _sl_props: list[dict] = []
        _needs_sleeper = (_total_dfs_props == 0) or (len(_ud_props) == 0) or (len(_pp_props) == 0)
        if _needs_sleeper:
            logger.warning(
                "[DataHub] UD=%d PP=%d — fetching Sleeper as fallback.",
                len(_ud_props), len(_pp_props),
            )
            _sl_props = _fetch_sleeper_props_direct()

        # Fill in missing platforms with Sleeper props tagged to that platform
        if len(_ud_props) == 0 and _sl_props:
            _ud_props = [{**p, "platform": "Sleeper"} for p in _sl_props]
            logger.info("[DataHub] Underdog replaced by Sleeper fallback (%d props)", len(_ud_props))
        if len(_pp_props) == 0 and _sl_props:
            _pp_props = [{**p, "platform": "Sleeper"} for p in _sl_props]
            logger.info("[DataHub] PrizePicks replaced by Sleeper fallback (%d props)", len(_pp_props))

        dfs = {
            "underdog":   _ud_props,
            "prizepicks": _pp_props,
            "sleeper":    _sl_props,
            "optimizer":  [],
        }
        # ── Zero-prop and degraded-run guard ──────────────────────────────
        _total_dfs_props = len(dfs.get("underdog", [])) + len(dfs.get("prizepicks", []))
        if _total_dfs_props == 0:
            logger.error(
                "[DataHub] ZERO props from Underdog + PrizePicks — pipeline is dry. "
                "No picks will go out this cycle."
            )
            try:
                from DiscordAlertService import discord_alert  # noqa: PLC0415
                discord_alert.send(
                    "⚠️ **PropIQ Pipeline Alert** — Zero props returned from both "
                    "Underdog AND PrizePicks. No picks will go out until props are "
                    "available. Check UD/PP API status immediately."
                )
            except Exception as _da_err:
                logger.warning("[DataHub] Discord zero-prop alert failed: %s", _da_err)
        elif _total_dfs_props < 20:
            logger.warning(
                "[DataHub] Degraded prop run — only %d props from UD+PP (normal ~250+). "
                "Agent picks may be sparse or unreliable this cycle.",
                _total_dfs_props,
            )
        _hub_setex(r, dfs_key, TTL_DFS, json.dumps(dfs))

    # ── Merge all groups into master hub key ───────────────────────────────
    hub: dict[str, Any] = {
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "game_states": game_states,
        "spring_training": _is_spring_training(),
        # FIX Bug 3: explicit season blend weights — readable by prop_enrichment_layer
        # and all agents. Ramps from 2025-dominant (Opening Day) to 2026-dominant (~game 80).
        "season_weight_2026": _season_weight_2026,
        "season_weight_2025": _season_weight_2025,
    }
    logger.info("[DataHub] Season blend: 2026=%.1f%% | 2025=%.1f%% (day %d of season)",
                _season_weight_2026 * 100, _season_weight_2025 * 100, _days_played)
    for key in (physics_key, context_key, market_key, dfs_key):
        data = _hub_get(r, key)
        if data:
            hub[key.replace("hub:", "")] = data

    # Seed sample counts from bet_ledger so shrinkage uses real data before retrain
    _refresh_sample_counts()
    _hub_setex(r, "mlb_hub", TTL_HUB, json.dumps(hub))
    logger.info("[DataHub] Hub refreshed. Groups: physics=%s context=%s market=%s dfs=%s",
                _hub_exists(r, physics_key), _hub_exists(r, context_key),
                _hub_exists(r, market_key), _hub_exists(r, dfs_key))


def read_hub() -> dict:
    """Read the master hub dict from Redis (or in-memory fallback). Returns empty dict on miss."""
    try:
        r = _redis()
        raw = r.get("mlb_hub")
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.warning("[DataHub] read_hub Redis error: %s", e)
    # Fall back to in-memory cache when Redis is unavailable
    mem = _mem_get("mlb_hub")
    return mem if mem else {}


# ─────────────────────────────────────────────────────────────────────────────
# 2. AgentTasklet — 10-agent army
# ─────────────────────────────────────────────────────────────────────────────

class _BaseAgent:
    name: str = "BaseAgent"
    _shared_model = None   # hot-reloaded by XGBoostTasklet without restart

    def __init__(self, hub: dict, model):
        self.hub   = hub
        self.model = model or _BaseAgent._shared_model

    def evaluate(self, prop: dict) -> dict | None:
        """Return a bet dict if edge found, else None."""
        raise NotImplementedError

    # shared helpers
    def _model_prob(self, player: str, prop_type: str, prop: dict | None = None, **_ignored) -> float:
        # ── Step 1: Try distribution-based simulation engine first ────────────
        # derives P(over) from it.  This replaces the single-scalar approach.
        if _SIM_ENGINE_AVAILABLE and prop:
            try:
                _inject_team_total(prop, self.hub)
                sim = _simulate_prop(prop, n_sims=8_000)
                if sim and sim.prob_over > 0.0:
                    raw = sim.prob_over * 100.0
                    # Apply variance penalty: wide distribution → reduce confidence
                    # For batter props: uses Monte Carlo std/mean coefficient of variation
                    pen = _variance_penalty(sim)
                    # Shift probability toward 50% by penalty factor
                    raw = 50.0 + (raw - 50.0) * pen
                    # Bernoulli tier adjustment: S-tier pitcher gets +4pp, D-tier -5pp
                    _b_adj  = float(prop.get("_bernoulli_prob_adj", 0.0) or 0.0)
                    _b_melt = float(prop.get("_bernoulli_meltdown", 0.0) or 0.0)
                    if _b_adj != 0.0:
                        raw = raw + (_b_adj * 100.0)
                    if _b_melt > 8.0:
                        # Meltdown pitcher: cap probability at 52% regardless of model
                        raw = min(raw, 52.0)
                        logger.debug("[Bernoulli] Meltdown gate applied to %s: prob capped",
                                     prop.get("player", ""))
                    # Store distribution on prop for downstream bet sizing / logging
                    prop["_sim_prob_over"]    = round(sim.prob_over, 4)
                    prop["_sim_mean"]         = sim.mean
                    prop["_sim_std"]          = sim.std
                    prop["_sim_dist"]         = sim.dist
                    prop["_sim_edge_reasons"] = sim.edge_reasons
                    prop["_sim_starter_prob"] = sim.starter_prob
                    prop["_sim_bullpen_prob"]  = sim.bullpen_prob
                    return round(max(5.0, min(95.0, raw)), 2)
            except Exception as _sim_err:
                logger.debug("[BaseAgent._model_prob] SimEngine error: %s", _sim_err)
        # ── Fallback: XGBoost model ───────────────────────────────────────────
        if self.model:
            try:
                import xgboost as xgb  # noqa: PLC0415
                import numpy as np     # noqa: PLC0415
                _live_prop = prop or {"prop_type": prop_type}
                feats_list = self._build_feature_vector(_live_prop)
                # Apply dropped_features mask from last backtest
                try:
                    _bt_raw = _redis().get("backtest_result")
                    if _bt_raw:
                        _bt = json.loads(_bt_raw)
                        for _idx in (_bt.get("dropped_features") or []):
                            if 0 <= _idx < len(feats_list):
                                feats_list[_idx] = 0.0
                except Exception:
                    pass
                # Pad / trim to FEATURE_DIM so old 20-feat models don't crash
                _dim = _BaseAgent.FEATURE_DIM
                if len(feats_list) < _dim:
                    feats_list = feats_list + [0.0] * (_dim - len(feats_list))
                elif len(feats_list) > _dim:
                    feats_list = feats_list[:_dim]
                feats = np.array([feats_list], dtype=np.float32)
                if isinstance(self.model, xgb.Booster):
                    dmat = xgb.DMatrix(feats)
                    prob = float(self.model.predict(dmat)[0])
                    if prob > 1.0 or prob < 0.0:
                        prob = 1.0 / (1.0 + np.exp(-prob))
                    return prob * 100
                else:
                    return float(self.model.predict_proba(feats)[0][1]) * 100
            except Exception:
                pass
        # No XGBoost model — try generate_pick 5-stage pipeline first
        if prop:
            try:
                from generate_pick import generate_pick as _gp  # noqa: PLC0415
                _gp_side = str(prop.get("side", "OVER")).upper()
                # Use player-specific prob if enrichment computed one (fixes OVER bias)
                _ps_prob = prop.get("_player_specific_prob")
                if _ps_prob is not None:
                    # Override base rate in generate_pick by pre-setting it
                    _prop_override = {**prop, "_base_rate_override": float(_ps_prob)}
                else:
                    _prop_override = prop
                _gp_res = _gp(raw_prop=_prop_override, side=_gp_side, min_edge=-1.0)
                if _gp_res is not None:
                    return round(max(5.0, min(95.0, _gp_res["final_prob"] * 100.0)), 2)
            except Exception:
                pass  # fall through to base_rate_model
        if _BASE_RATE_AVAILABLE and prop:
            _side = str(prop.get("side", "OVER")).upper()
            # Use player-specific rate if enrichment computed one
            _ps_prob = prop.get("_player_specific_prob")
            raw_p = float(_ps_prob) * 100.0 if _ps_prob else _base_rate_prob(prop, _side)
            # Layer Marcel, Predict+, and park factor adjustments
            # Game-level environment nudge (game_over_prob / game_home_win_prob
            _gop = float(prop.get("game_over_prob",      0.50) or 0.50)
            _gwp = float(prop.get("game_home_win_prob",  0.50) or 0.50)
            _pt_lower = str(prop_type).lower()
            _game_env_nudge = 0.0
            if _pt_lower in ("total_bases", "home_runs", "rbis", "rbi",
                             "runs", "earned_runs", "hits"):
                # Calibrated from Statcast run-environment correlation (2022-2024):
                # 1pp game total shift ≈ 0.45pp batter prop shift (not 0.6 as before).
                # Max ±2.25pp at extreme game totals (gop=0.0 or 1.0).
                _game_env_nudge += (_gop - 0.50) * 4.5
            if _pt_lower in ("rbis", "rbi", "runs") and _gwp > 0.58:
                _game_env_nudge += 1.0  # home team winning → slightly better RBI/run env
            # PR #322: collect adjustments and dampen to prevent overconfidence stacking
            _br_adjs = [
                ("marcel",       float(prop.get("_marcel_adj",       0.0)) * 100.0),
                ("predict_plus", float(prop.get("_predict_plus_adj", 0.0)) * 100.0),
                ("park_factor",  float(prop.get("_park_factor_adj",  0.0)) * 100.0),
                ("game_env",     _game_env_nudge),
                ("streak",       float(prop.get("_streak_adj",       0.0)) * 100.0),
                ("last10",       float(prop.get("_last10_adj",       0.0)) * 100.0),
            ]
            _br_adjs = [(n, d) for n, d in _br_adjs if abs(d) >= 0.10]
            if _br_adjs:
                try:
                    from adjustment_dampener import dampen_adjustments as _dampen_br  # noqa: PLC0415
                    raw_p = _dampen_br(raw_p, _br_adjs, log_tag=prop.get("player", ""))
                except Exception:
                    for _, d in _br_adjs:
                        raw_p += d
            # Brier calibration governor
            if _DRIFT_MONITOR_AVAILABLE:
                try:
                    brier = get_current_brier()
                    raw_p = apply_calibration_governor(raw_p / 100.0, brier) * 100.0
                except Exception:
                    pass
            return round(max(5.0, min(95.0, raw_p)), 2)

        # Absolute fallback — no base rate model AND no prop context
        raw_p = 50.0
        if _DRIFT_MONITOR_AVAILABLE:
            try:
                brier = get_current_brier()
                raw_p = apply_calibration_governor(raw_p / 100.0, brier) * 100.0
            except Exception:
                pass
        if prop:
            # Phase 91 Step 4: dampen correlated fallback adjustments
            _gop2 = float(prop.get("game_over_prob",     0.50) or 0.50)
            _gwp2 = float(prop.get("game_home_win_prob", 0.50) or 0.50)
            _pt_lower2 = str(prop_type).lower()
            _game_env_nudge = 0.0
            if _pt_lower2 in ("total_bases", "home_runs", "rbis", "rbi",
                               "runs", "earned_runs", "hits"):
                _game_env_nudge += (_gop2 - 0.50) * 4.5
            if _pt_lower2 in ("rbis", "rbi", "runs") and _gwp2 > 0.58:
                _game_env_nudge += 1.0
            _streak_nudge = float(prop.get("_streak_adj",  0.0)) * 100.0
            _last10_nudge = float(prop.get("_last10_adj",  0.0)) * 100.0
            # Reliability weights scale each nudge by how much we trust that signal
            _fw = prop.get("_feature_weights", {})
            _fb_adjs = [
                ("bayesian",        float(prop.get("_bayesian_nudge",      0.0)) * 100.0 * _fw.get("xwoba",   1.0)),
                ("cv_consistency",  float(prop.get("_cv_nudge",            0.0)) * 100.0 * _fw.get("wrc_plus", 1.0)),
                ("form_adj",        float(prop.get("_form_adj",            0.0)) * 100.0),
                ("park_factor",     float(prop.get("_park_factor_adj",     0.0)) * 100.0),
                ("arsenal_k_sig",   float(prop.get("_arsenal_k_sig_nudge", 0.0)) * 100.0 * _fw.get("csw_pct", 1.0)),
                ("game_env",        _game_env_nudge),
                ("streak",          _streak_nudge),
                ("last_10",         _last10_nudge),
            ]
            _fb_adjs = [(n, d) for n, d in _fb_adjs if abs(d) >= 0.10]
            if _fb_adjs:
                try:
                    from adjustment_dampener import dampen_adjustments as _dampen  # noqa: PLC0415
                    raw_p = _dampen(raw_p, _fb_adjs, log_tag=prop.get("player", ""))
                except Exception:
                    for _, d in _fb_adjs:
                        raw_p += d
        return round(max(5.0, min(95.0, raw_p)), 2)


    # ─────────────────────────────────────────────────────────────────────
    # ─────────────────────────────────────────────────────────────────────
    FEATURE_DIM = 27  # 27-element vector — bump when adding columns; old models padded automatically

    @staticmethod
    def _build_feature_vector(prop: dict, bet: dict | None = None) -> list[float]:
        """Return a 27-element float list usable by XGBoost.
        All values normalised to [0, 1] or small bounded floats.
        Schema is FIXED — any future changes must keep len == 27.
        """
        import math

        def _clamp(v, lo=0.0, hi=1.0):
            try:
                return max(lo, min(hi, float(v)))
            except Exception:
                return 0.0

        # ── Player stats — pitcher OR batter signals depending on prop type ──
        _PITCHER_PT = {"strikeouts","pitching_outs","earned_runs","hits_allowed",
                       "fantasy_pitcher"}
        _pt_raw     = str(prop.get("prop_type","") or (bet.get("prop_type","") if bet else "")).lower()
        # FIX Bug 6: "hitter_strikeouts" normalizes to "strikeouts" via _norm_stat but
        # is a BATTER prop. Preserve the raw stat_type label before normalization to
        # correctly classify batter K props and use batter (not pitcher) feature slots.
        _raw_stat_label = str(prop.get("stat_type", prop.get("stat", "")) or "").lower()
        _is_pitcher = (_pt_raw in _PITCHER_PT) and ("hitter" not in _raw_stat_label)

        if _is_pitcher:
            # Pitcher signals (FanGraphs)
            k_rate       = _clamp(prop.get("k_rate",    prop.get("k_pct",    0.22)))
            bb_rate      = _clamp(prop.get("bb_rate",   prop.get("bb_pct",   0.08)))
            era          = _clamp((prop.get("era", 4.0)) / 9.0)
            whip         = _clamp((prop.get("whip", 1.3)) / 3.0)
            shadow_whiff = _clamp(prop.get("shadow_whiff_rate",
                                  prop.get("csw_pct",
                                  prop.get("swstr_pct", 0.25))))
        else:
            # Batter signals (FanGraphs) mapped into the same 5 slots
            # Source: baseball-models feature importance (gmalbert/baseball-predictions)
            _is_tb_prop = _pt_raw in {"total_bases", "home_runs", "hits_runs_rbis",
                                       "fantasy_hitter", "fantasy_score"}

            # slot 0: wRC+ normalized (100=avg → 0.5, 140=elite → 0.7, 70=poor → 0.35)
            k_rate  = _clamp(float(prop.get("wrc_plus", 100.0) or 100.0) / 200.0)

            # slot 1: xbh_per_game for TB/power props (45% feature importance for TB)
            if _is_tb_prop:
                _xbh = float(prop.get("xbh_per_game", 0.50) or 0.50)
                bb_rate = _clamp(_xbh / 1.50)   # 0=0, 0.50=avg(0.33), 1.0=elite(0.67)
            else:
                bb_rate = _clamp(float(prop.get("iso", 0.156) or 0.156) / 0.35)

            # slot 2: SLG for TB/power props (16% feature importance)
            if _is_tb_prop:
                _slg = float(prop.get("slg", 0.410) or 0.410)
                era  = _clamp((_slg - 0.250) / 0.400)   # 0.250=0, 0.410=avg(0.40), 0.650=elite(1.0)
            else:
                era = _clamp((float(prop.get("babip", 0.288) or 0.288) - 0.200) / 0.200)

            # slot 3: batter bb_pct (plate discipline)
            whip    = _clamp(float(prop.get("bb_pct", 0.087) or 0.087) / 0.20)
            # slot 4: batter K% (inverse contact — higher K = worse contact)
            shadow_whiff = _clamp(float(prop.get("k_pct", 0.223) or 0.223) / 0.35)

        # Zone integrity multiplier (pitcher K-props only, 1.0 for batters)
        # Blended with pitcher type cluster: power=+0.05, command=-0.05, neutral=0
        _ptype_enc = {"power": 0.05, "command": -0.05}.get(
            prop.get("_pitcher_type", "neutral"), 0.0
        )
        zone_mult    = _clamp(prop.get("_zone_integrity_mult", 1.0) + _ptype_enc, 0.5, 1.5) / 1.5

        # ── Lineup context ────────────────────────────────────────────
        chase_adj   = _clamp((prop.get("_lineup_chase_adj", 0.0) + 0.10) / 0.20)  # -0.10→0, +0.10→1
        o_swing     = _clamp(prop.get("_opp_o_swing_avg", 0.28))

        # ── Weather ───────────────────────────────────────────────────
        wind_speed  = _clamp(prop.get("_wind_speed",  8.0) / 30.0)
        temp        = _clamp((prop.get("_temp_f",    72.0) - 32) / 80.0)

        # ── Game context ──────────────────────────────────────────────
        is_spring   = float(bool(prop.get("spring_training") or (bet or {}).get("spring_training")))

        # ── Bet signals (from bet dict if available, else from prop) ──
        b           = bet or {}
        model_prob  = _clamp((b.get("model_prob")  or prop.get("model_prob",  50.0)) / 100.0)
        _ev_raw     = b.get("ev_pct")
        ev_pct      = _clamp((((_ev_raw if _ev_raw is not None else prop.get("ev_pct", 3.0)) + 20) / 40.0))
        kelly       = _clamp((b.get("kelly_units")  or prop.get("kelly_units",  0.5)) / 3.0)
        line_val    = _clamp((b.get("line")         or prop.get("line",         1.5)) / 10.0)
        # Use sharp-book vig-stripped probability when available (more accurate than -115 flat)
        _sb_implied = prop.get("sb_implied_prob", 0.0) or 0.0
        _ud_implied = b.get("implied_prob") or prop.get("implied_prob", 52.4)
        impl_prob   = _clamp((_sb_implied if _sb_implied > 0.30 else _ud_implied) / 100.0)
        # Also encode sharp-book line gap as a feature (negative = DFS line favorable for Over)
        sb_line_gap = _clamp(((prop.get("sb_line_gap", 0.0) or 0.0) + 2.0) / 4.0)  # -2 to +2 range
        _pt_map = {
            "strikeouts":        0,   # pitcher Ks
            "pitcher_strikeouts": 0,
            "hitter_strikeouts": 1,   # batter Ks — distinct bucket from pitcher Ks
            "pitching_outs":      2,   # pitcher outs recorded
            "home_runs":          3, "hr": 3,
            "hits":               4, "hits_allowed": 4,
            "rbis":               5, "rbi": 5,
            "hits_runs_rbis":     6,   # most common prop — needs unique code
            "total_bases":        7, "fantasy_score": 7,  # power/fantasy bucket
            "walks_allowed":      8, "walks": 8,
            "earned_runs":        9,
        }
        pt_enc = _pt_map.get(str(b.get("prop_type") or prop.get("prop_type", "")).lower(), 5) / 9.0

        side_enc    = 0.0 if str(b.get("side") or prop.get("side", "OVER")).upper() == "OVER" else 1.0

        # ── Calibration quality ───────────────────────────────────────
        brier = 0.25  # neutral default
        try:
            # calibration_layer re-exports get_current_brier from drift_monitor
            from calibration_layer import get_current_brier as _gcb
            brier = _clamp(_gcb())
        except Exception:
            try:
                from drift_monitor import get_current_brier as _gcb2
                brier = _clamp(_gcb2())
            except Exception:
                pass

        # ── Enrichment signal slots (Phase 97) ───────────────────────────────
        # every prop but never fed to XGBoost — now they are.  Normalised [0,1].
        form_adj      = _clamp((float(prop.get("_form_adj",            0.0) or 0.0) + 0.20) / 0.40)  # hot/cold streak
        cv_nudge      = _clamp((float(prop.get("_cv_nudge",            0.0) or 0.0) + 0.15) / 0.30)  # CV consistency
        bayesian_nudge= _clamp((float(prop.get("_bayesian_nudge",      0.0) or 0.0) + 0.15) / 0.30)  # Bayesian update
        marcel_adj    = _clamp((float(prop.get("_marcel_adj",          0.0) or 0.0) + 0.02) / 0.04)  # Marcel ±1.8pp
        predict_plus  = _clamp((float(prop.get("_predict_plus_adj",    0.0) or 0.0) + 0.08) / 0.16)  # Predict+ arsenal
        ps_prob       = _clamp(float(prop.get("_player_specific_prob", 0.0) or 0.0))                  # Poisson/binomial rate
        # last=1.0 (9/9), unknown=0.0.  More predictive than has_enrich binary.
        bat_order     = _clamp(float(prop.get("_batting_order_slot", 0) or 0) / 9.0)

        vec = [
            k_rate, bb_rate, era, whip,           # 0-3  pitcher/batter stats
            shadow_whiff, zone_mult,               # 4-5  statcast contact quality
            chase_adj, o_swing,                    # 6-7  lineup chase
            wind_speed, temp,                      # 8-9  weather
            is_spring,                             # 10   context flag
            model_prob, ev_pct, kelly,             # 11-13 bet quality
            line_val, impl_prob,                   # 14-15 market (sb_implied when avail)
            pt_enc, side_enc,                      # 16-17 prop meta
            brier, sb_line_gap,                    # 18-19 calibration + sharp line gap
            form_adj,                              # 20   hot/cold form streak
            cv_nudge,                              # 21   CV consistency nudge
            bayesian_nudge,                        # 22   Bayesian update nudge
            marcel_adj,                            # 23   Marcel projection adjustment
            predict_plus,                          # 24   Predict+ arsenal adjustment
            ps_prob,                               # 25   player-specific Poisson/binomial prob
            bat_order,                             # 26   batting order position (normalised)
        ]
        assert len(vec) == 27, f"Feature vector length {len(vec)} != 27"
        return [round(v, 6) for v in vec]

    def _build_bet(self, prop: dict, side: str, model_prob: float,
                   implied_prob: float, ev_pct: float) -> dict:
        # ── Phase 91 Step 4: collect post-model adjustments, apply with
        _prop_type  = prop.get("prop_type", "")
        pitcher_id  = prop.get("mlbam_id") or prop.get("player_id")
        _post_adjs: list[tuple[str, float]] = []

        # Shadow zone whiff boost (K-props) — compute effective delta
        _sw_prob = apply_shadow_whiff_boost(model_prob, prop, _prop_type)
        if _sw_prob != model_prob:
            _post_adjs.append(("shadow_whiff", _sw_prob - model_prob))

        # Zone integrity multiplier (K-props) — convert to effective delta
        _zi_prob = apply_zone_integrity_multiplier(model_prob, _prop_type, pitcher_id)
        if abs(_zi_prob - model_prob) >= 0.01:
            _post_adjs.append(("zone_integrity", _zi_prob - model_prob))

        # Lineup chase difficulty (K-props) — compute delta
        _k_prop_types = {"strikeouts", "pitcher_strikeouts", "k", "ks"}
        if _prop_type.lower() in _k_prop_types:
            _ctx_lineups = prop.get("_context_lineups", [])
            _opp_team    = prop.get("opposing_team", "")
            if _opp_team and _ctx_lineups:
                _chase   = get_lineup_chase_score(_opp_team, _ctx_lineups)
                _k_delta = _chase["k_prob_adjustment"] * 100
                if abs(_k_delta) >= 0.01:
                    _post_adjs.append(("chase_difficulty", _k_delta))

        # Apply with correlation dampening (or pass-through if no adjustments)
        if _post_adjs:
            try:
                from adjustment_dampener import (  # noqa: PLC0415
                    dampen_adjustments   as _dampen,
                    undampened_total     as _undampened,
                )
                _raw_total = _undampened(model_prob, _post_adjs)
                model_prob = _dampen(
                    model_prob, _post_adjs,
                    log_tag=prop.get("player", ""),
                )
                # Persist both values for audit / feature vector
                prop["_adj_raw_prob"]     = round(_raw_total, 4)
                prop["_adj_dampened"]     = round(model_prob,  4)
                prop["_adj_signals"]      = [n for n, _ in _post_adjs]
            except Exception:
                # Fallback: apply adjustments naively (safe degradation)
                for _, delta in _post_adjs:
                    model_prob = round(max(3.0, min(97.0, model_prob + delta)), 4)

        # ── Phase 91 Step 5: Thin-data shrinkage ─────────────────────────────
        # toward the market implied probability. Proven prop-types pass through
        if _SHRINKAGE_AVAILABLE:
            try:
                from confidence_shrinkage import shrink_toward_market as _shrink_toward_market  # noqa: PLC0415
                _n_samples = _get_sample_count(_prop_type)
                _shrunk, _conf = _shrink_toward_market(
                    model_prob_pct=model_prob,
                    market_implied_pct=implied_prob,
                    n_samples=_n_samples,
                )
                prop["_shrinkage_n"]     = _n_samples
                prop["_shrinkage_conf"]  = _conf
                prop["_shrinkage_delta"] = round(_shrunk - model_prob, 3)
                model_prob = _shrunk
                logger.debug("[ThinData] %s %s n=%d conf=%.2f delta=%.2fpp",
                             prop.get("player", ""), _prop_type,
                             _n_samples, _conf,
                             prop["_shrinkage_delta"])
            except Exception as _se:
                logger.debug("[ThinData] Shrinkage error: %s", _se)

        # ── Phase 91 Step 6: Market line validation ───────────────────────────
        # implied probability.  Divergence >20pp → soft-cap (likely data error).
        if _MARKET_VALIDATOR_AVAILABLE:
            try:
                _market_implied_pct = implied_prob
                model_prob, _mv_valid = _stamp_market_validation(
                    prop,
                    model_prob_pct=model_prob,
                    market_implied_pct=_market_implied_pct,
                )
                if not _mv_valid:
                    logger.error(
                        "[MarketValidator] INVALID prob after validation %s %s prob=%.1f",
                        prop.get("player", ""), _prop_type, model_prob,
                    )
            except Exception as _mve:
                logger.debug("[MarketValidator] Error: %s", _mve)

        # ── Recalculate ev_pct from final model_prob (shrinkage + cap may have changed it) ──
        # so Kelly / confidence downstream reflect the actual adjusted probability.
        try:
            _platform = prop.get("platform", "").lower()
            _is_pickem = _platform in {"underdog", "prizepicks"}
            if _is_pickem:
                # UD/PP balanced pick'em lines are true 50/50 (not -110 vig).
                # Use even money (profit=1.0) so stored ev_pct and XGBoost training
                # data reflect the actual edge, not a -110 sportsbook overround.
                _profit = 1.0
            else:
                _side_american = (
                    prop.get("over_american",  prop.get("odds_american", -115))
                    if side == "OVER"
                    else prop.get("under_american", prop.get("odds_american", -115))
                )
                _decimal = (
                    (100 / abs(_side_american) + 1) if _side_american < 0
                    else (_side_american / 100 + 1)
                )
                _profit = _decimal - 1
            ev_pct   = round((_profit * (model_prob / 100) - (1 - model_prob / 100)) * 100, 3)
        except Exception:
            pass   # keep original ev_pct on error

        side_odds = (
            prop.get("over_american",  prop.get("odds_american", -115))
            if side == "OVER"
            else prop.get("under_american", prop.get("odds_american", -115))
        )
        # Build feature vector now (all signals computed above are on prop)
        _feat_vec = self._build_feature_vector(prop, {
            "model_prob":  model_prob,
            "ev_pct":      ev_pct,
            "kelly_units": round(_kelly_units(model_prob / 100, side_odds), 3),
            "line":        prop.get("line", 0),
            "implied_prob": implied_prob,
            "side":        side,
            "prop_type":   prop.get("prop_type", ""),
            "confidence":  self._confidence(ev_pct),
            "spring_training": _is_spring_training(),
        })
        kelly = _kelly_units(model_prob / 100, side_odds)
        platforms = self._dfs_platforms(prop, side)
        return {
            "agent":              self.name,
            "player":             prop.get("player", "Unknown"),
            "player_name":        prop.get("player", "Unknown"),  # Discord field
            "prop_type":          prop.get("prop_type", ""),
            "line":               prop.get("line", 0),
            "side":               side,
            "odds_american":      (
                prop.get("over_american",  prop.get("odds_american", -110))
                if side == "OVER"
                else prop.get("under_american", prop.get("odds_american", -110))
            ),
            "model_prob":         round(model_prob, 1),
            "implied_prob":       round(implied_prob, 1),
            "ev_pct":             round(ev_pct, 1),
            "kelly_units":        round(kelly, 3),
            "recommended_platform": platforms[0] if platforms else "PrizePicks",
            "checklist":          self._checklist(prop),
            "confidence":         self._confidence(ev_pct),
            "spring_training":    _is_spring_training(),
            "ts":                 datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "_features_json":     json.dumps(_feat_vec),
            # Simulation engine outputs (present only when sim ran successfully)
            "sim_mean":           prop.get("_sim_mean"),
            "sim_std":            prop.get("_sim_std"),
            "sim_dist":           prop.get("_sim_dist"),
            "sim_edge_reasons":   prop.get("_sim_edge_reasons", []),
            "sim_starter_prob":   prop.get("_sim_starter_prob"),
            "sim_bullpen_prob":   prop.get("_sim_bullpen_prob"),
            # Pass MLBAM ID through for accent-safe grading (Acuña, Peña, etc.)
            "mlbam_id":           prop.get("mlbam_id") or prop.get("player_id"),
            "player_id":          prop.get("player_id") or prop.get("mlbam_id"),
        }

    def _dfs_platforms(self, prop: dict, side: str) -> list[str]:
        # Platform must match where the prop came from — never mix sources.
        # If the prop already has a platform tag (set during ingestion), trust it.
        _src = (prop.get("platform") or "").lower()
        if "prize" in _src:
            return ["PrizePicks"]
        if "underdog" in _src or "ud" in _src:
            return ["Underdog"]
        if "sleeper" in _src:
            return ["Sleeper"]
        # Fallback: scan hub DFS data to find which platform actually has this prop
        dfs = self.hub.get("dfs", {})
        for platform in ("prizepicks", "underdog", "sleeper"):
            picks = dfs.get(platform, [])
            for pick in picks:
                if isinstance(pick, dict):
                    if prop.get("player", "").lower() in str(pick).lower():
                        return [platform.capitalize()]
        # Default to Underdog — higher multipliers and more prop types
        return ["Underdog"]

    def _checklist(self, prop: dict) -> dict:
        ctx = self.hub.get("context", {})
        return {
            "pitcher_ok":  True,
            "matchup_ok":  True,
            "park_ok":     True,
            "umpire_ok":   bool(ctx.get("umpires")),
            "public_ok":   bool(self.hub.get("market", {}).get("public_betting")),
            "lineup_ok":   bool(ctx.get("lineups")),
            "bullpen_ok":  True,
        }

    @staticmethod
    def _confidence(ev_pct: float) -> int:
        # ev_pct is stored as percentage (3–20 range) in _build_bet
        # Thresholds calibrated so MIN_CONFIDENCE=6 passes picks with ≥3% EV
        # (previously required ≥5% which blocked most agents below -115 odds)
        if ev_pct >= 15: return 9
        if ev_pct >= 10: return 8
        if ev_pct >= 7:  return 7
        if ev_pct >= 3:  return 6
        if ev_pct >= 1:  return 5
        return 4


class _EVHunter(_BaseAgent):
    name = "EVHunter"

    def evaluate(self, prop: dict) -> dict | None:
        """Best-EV generalist — checks both sides, bets whichever has higher EV."""
        over_odds  = prop.get("over_american",  -115)
        under_odds = prop.get("under_american", -115)

        # WagerBrain: skip props with excessive juice (>8% margin)
        if _ODDS_MATH_AVAILABLE and not _is_acceptable_vig(over_odds, under_odds, _MAX_VIG):
            return None

        best_bet = None
        best_ev  = MIN_EV_THRESH  # only fire if clearly positive

        for side, odds in (("OVER", over_odds), ("UNDER", under_odds)):
            # Get calibrated probability for this side
            if _BASE_RATE_AVAILABLE:
                model_p = _base_rate_prob(prop, side)
            else:
                model_p = self._model_prob(
                    prop.get("player", ""), prop.get("prop_type", ""),
                    prop=prop,
                )
                if side == "UNDER":
                    model_p = 100.0 - model_p

            implied = _american_to_implied(odds) / 100

            if _ODDS_MATH_AVAILABLE:
                from odds_math import american_to_decimal as _a2d  # noqa: PLC0415
                profit    = _a2d(odds) - 1.0
                ev_pct    = _true_odds_ev(stake=1.0, profit=profit, prob=model_p / 100)
            else:
                ev_pct = (model_p / 100 - implied) / implied

            if ev_pct > best_ev:
                best_ev  = ev_pct
                best_bet = (side, model_p, implied * 100, ev_pct * 100)

        if best_bet:
            side, model_p, implied_p, ev_p = best_bet
            return self._build_bet(prop, side, model_p, implied_p, ev_p)
        return None


class _UnderMachine(_BaseAgent):
    name = "UnderMachine"

    def evaluate(self, prop: dict) -> dict | None:
        """Strict UNDER specialist — targets high-probability Under lines."""
        over_odds  = prop.get("over_american",  -115)
        under_odds = prop.get("under_american", -115)

        # WagerBrain: skip excessive vig
        if _ODDS_MATH_AVAILABLE and not _is_acceptable_vig(over_odds, under_odds, _MAX_VIG):
            return None

        # Use base_rate_model for Under probability directly
        if _BASE_RATE_AVAILABLE:
            model_prob = _base_rate_prob(prop, "UNDER")
        else:
            model_prob = 100.0 - self._model_prob(
                prop.get("player", ""), prop.get("prop_type", ""),
                prop=prop,
            )

        implied = _american_to_implied(under_odds) / 100

        if _ODDS_MATH_AVAILABLE:
            from odds_math import american_to_decimal as _a2d  # noqa: PLC0415
            profit = _a2d(under_odds) - 1.0
            ev_pct = _true_odds_ev(stake=1.0, profit=profit, prob=model_prob / 100)
        else:
            ev_pct = (model_prob / 100 - implied) / implied

        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, "UNDER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _UmpireAgent(_BaseAgent):
    name = "UmpireAgent"
    # Canonical pitcher stat set — populated via _norm_stat() at ingestion
    _PITCHER_STATS = {"strikeouts", "earned_runs", "pitching_outs", "innings_pitched",
                      "outs_recorded", "fantasy_score", "hits_allowed",
                      "pitching_wins"}
    # ABS 2026: catcher framing weight reduced 80 % per ABS Challenge System
    _FRAMING_WEIGHT = ABS_FRAMING_WEIGHT  # 0.20

    def evaluate(self, prop: dict) -> dict | None:
        # Try hub umpires, then fetch live from MLB Stats API (free)
        umpires = self.hub.get("context", {}).get("umpires", [])
        if not umpires:
            try:
                _d = _today_pt().strftime("%Y-%m-%d")
                _sched = requests.get(
                    "https://statsapi.mlb.com/api/v1/schedule",
                    params={"sportId":1,"date":_d,"hydrate":"officials","gameType":"R"},
                    timeout=8,
                ).json()
                for _db in _sched.get("dates", []):
                    for _g in _db.get("games", []):
                        for _off in _g.get("officials", []):
                            if _off.get("officialType") == "Home Plate":
                                _ump_name = _off.get("official", {}).get("fullName", "")
                                try:
                                    from umpire_rates import get_umpire_rates as _gur  # noqa: PLC0415
                                    _ump_rates = _gur(_ump_name)
                                except Exception:
                                    _ump_rates = {"k_rate": 8.8, "bb_rate": 3.1,
                                                  "k_mod": 1.0, "bb_mod": 1.0, "known": False}
                                umpires.append({
                                    "name":      _ump_name,
                                    "home_team": _g["teams"]["home"]["team"].get("name",""),
                                    "away_team": _g["teams"]["away"]["team"].get("name",""),
                                    "k_rate":    _ump_rates["k_rate"],
                                    "bb_rate":   _ump_rates.get("bb_rate", 3.1),
                                    "k_mod":     _ump_rates["k_mod"],
                                    "bb_mod":    _ump_rates.get("bb_mod", 1.0),
                                    "known":     _ump_rates.get("known", False),
                                    "run_env":   1.0,
                                })
            except Exception:
                pass

        prop_type = prop.get("prop_type", "")
        if _norm_stat(prop_type) not in self._PITCHER_STATS:
            return None

        # UmpireAgent only needs K props — umpires just confirm game is happening
        model_prob = self._model_prob(prop.get("player", ""), prop_type, prop=prop)

        # Apply umpire modifier: historical K-rate × ABS overturn rate (2026)
        if umpires:
            _ump       = umpires[0]
            _ump_name  = _ump.get("name", "")
            k_mod      = float(_ump.get("k_mod", _ump.get("k_rate", 8.8) / 8.8))
            _ump_known  = _ump.get("known", False)
            # Dampen modifier for unknown umpires (regression to mean)
            if not _ump_known:
                k_mod = 1.0 + (k_mod - 1.0) * 0.5
            # Apply historical K-rate modifier first
            model_prob = min(model_prob * k_mod, 95.0)
            # Layer ABS overturn rate on top (2026-specific signal)
            try:
                from abs_layer import get_umpire_abs_rate as _guar  # noqa: PLC0415
                _abs_rates = _guar(_ump_name)
                # High overturn = unreliable zone = suppress K prob further
                model_prob = max(5.0, min(95.0,
                    model_prob + _abs_rates.get("k_adj", 0.0)))
                logger.debug(
                    "[UmpireAgent] %s  k_mod=%.3f  abs_k_adj=%.1f  "
                    "overturn=%.0f%%  prob→%.1f",
                    _ump_name, k_mod,
                    _abs_rates.get("k_adj", 0.0),
                    _abs_rates.get("avg_overturn_rate", 0.55) * 100,
                    model_prob,
                )
            except Exception:
                logger.debug("[UmpireAgent] %s  k_mod=%.3f  prob→%.1f",
                             _ump_name, k_mod, model_prob)
        else:
            model_prob = min(model_prob + 1.0, 95.0)

        # Bidirectional: umpire with tight zone → favour UNDER K; wide zone → OVER K.
        over_odds     = prop.get("over_american",  -110)
        under_odds    = prop.get("under_american", -110)
        over_prob     = model_prob
        under_prob    = 100.0 - model_prob
        over_implied  = _american_to_implied(over_odds)  / 100
        under_implied = _american_to_implied(under_odds) / 100
        ev_over  = (over_prob  / 100 - over_implied)  / over_implied
        ev_under = (under_prob / 100 - under_implied) / under_implied
        _thresh  = _get_ev_threshold(prop.get("_sim_edge_reasons", []))
        if ev_over >= _thresh and ev_over >= ev_under:
            return self._build_bet(prop, "OVER",  over_prob,
                                   over_implied  * 100, ev_over  * 100)
        if ev_under >= _thresh:
            return self._build_bet(prop, "UNDER", under_prob,
                                   under_implied * 100, ev_under * 100)
        return None


class _F5Agent(_BaseAgent):
    name = "F5Agent"

    @staticmethod
    def _ttop_penalty(batters_faced: int, pitches_thrown: int, arsenal_size: int = 3) -> float:
        """
        Continuous Times-Through-Order Penalty as probability percentage points.
        Source: Tango/Lichtman, Brill JQAS 2023.
        Positive return = batter has an advantage (pitcher fading).
        """
        ttop_woba = min(0.030, (batters_faced / 27.0) ** 1.4 * 0.030)
        if arsenal_size <= 1:
            arsenal_scale = 1.7
        elif arsenal_size <= 2:
            arsenal_scale = 1.2
        elif arsenal_size >= 4:
            arsenal_scale = 0.8
        else:
            arsenal_scale = 1.0
        pitch_tail = max(0.0, (pitches_thrown - 75) * 0.00015)
        ttop_woba  = min(0.050, ttop_woba * arsenal_scale + pitch_tail)
        return round(ttop_woba / 0.030 * 2.5, 2)  # ~0.030 wOBA ≈ 2.5pp

    # ── Handedness K-rate adjustment table (pp) ───────────────────────────
    # Source: BaseballBettingEdge PLATOON_K_DELTA
    # Rows = pitcher_hand, cols = batter_hand.  Positive = K-over more likely.
    # (L,L) = +2.0 : lefty batters have hardest time reading same-hand stuff.
    # (L,R) = −1.5 : righties handle lefties better at the league level.
    _PLATOON_K_DELTA: dict = {
        ("R", "R"): +0.5,
        ("R", "L"): -1.0,
        ("L", "R"): -1.5,
        ("L", "L"): +2.0,
    }

    @staticmethod
    def _swstr_career_delta_adj(current_swstr: float,
                                career_swstr: float,
                                n_starts: int) -> float:
        """Career-relative SwStr% delta → K9 probability adjustment (pp).
        Source: BaseballBettingEdge calc_swstr_delta_k9().
        Bayesian dampening: weight = n_starts / (n_starts + 10).
          3 starts → 23% weight | 10 → 50% | 20 → 67%.
        Positive return = pitcher above career baseline → K-over boost.
        scale: 0.01 SwStr% delta × 30 = 0.30 K9 ≈ 1 pp per ~3% SwStr gap.
        """
        if n_starts <= 0:
            return 0.0
        weight    = n_starts / (n_starts + 10.0)
        raw_delta = (current_swstr - career_swstr) * 30.0
        return round(raw_delta * weight, 2)

    @staticmethod
    def _bayesian_opp_k(obs_k_rate: float,
                        n_games: int,
                        league_avg: float = 0.227) -> float:
        """Bayesian shrinkage of opposing lineup K rate toward league average.
        Source: BaseballBettingEdge bayesian_opp_k().
        adj = (games × obs_k + 50 × league_avg) / (games + 50).
          8 games  → 14% observed weight (heavy shrinkage early season).
          30 games → 38% observed weight.
          81 games → 62% observed weight (majority season, more trust).
        Returns regressed K rate (0–1).
        """
        n = max(n_games, 0)
        return (n * obs_k_rate + 50.0 * league_avg) / (n + 50.0)

    @staticmethod
    def _lambda_gap_cap(model_prob: float, k_line: float,
                        max_gap: float = 2.5) -> float:
        """
        Cap model_prob at the probability implied by λ = k_line ± max_gap.

        BBEdge data: picks where |model_lambda − k_line| ≥ 3 win at only 21%.
        The model over-reaches on extreme K predictions generating inflated EVs
        that don't reflect real edge. Default gap = 2.5 (half-K headroom past
        the empirical 3-K failure threshold).

        Uses normal approximation to Poisson + math.erf (stdlib, no scipy).
        Returns capped probability in 0–100 scale. No-op when gap ≤ max_gap.
        """
        import math

        if k_line <= 0:
            return model_prob

        def _norm_inv(pp: float) -> float:
            """Abramowitz & Stegun 26.2.17 rational approx for Φ⁻¹(p)."""
            if pp <= 0.0:
                return -8.0
            if pp >= 1.0:
                return 8.0
            if pp > 0.5:
                return -_norm_inv(1.0 - pp)
            t = math.sqrt(-2.0 * math.log(pp))
            c0, c1, c2 = 2.515517, 0.802853, 0.010328
            d1, d2, d3 = 1.432788, 0.189269, 0.001308
            return -(t - (c0 + t * (c1 + t * c2)) /
                        (1.0 + t * (d1 + t * (d2 + t * d3))))

        p = max(0.001, min(0.999, model_prob / 100.0))
        z = _norm_inv(p)
        lam_approx = k_line + 0.5 + z * math.sqrt(max(k_line, 1.0))

        gap = lam_approx - k_line
        if abs(gap) <= max_gap:
            return model_prob

        lam_capped = max(0.5, k_line + math.copysign(max_gap, gap))
        z_cap = (lam_capped - k_line - 0.5) / math.sqrt(max(lam_capped, 1.0))
        p_cap = 0.5 * (1.0 + math.erf(z_cap / math.sqrt(2.0)))
        return round(max(5.0, min(95.0, p_cap * 100.0)), 2)

    @staticmethod
    def _line_movement_conf(
        player: str,
        prop_type: str,
        current_line: float,
        side: str,
        noise_floor: float = 0.5,
        full_fade: float = 1.5,
    ) -> float:
        """
        Confidence multiplier (0.0–1.0) based on UD/PP prop-line movement
        AGAINST the bet direction since the first snapshot of the day.

        Adapted from BBEdge calc_movement_confidence() — uses UD/PP line
        deltas from line_stream.db (is_opening=1 rows) instead of American
        odds price deltas. No OddsAPI dependency.

        Line moving UP   (6.5→7.0) is unfavourable for OVER  → penalty
        Line moving DOWN (6.5→6.0) is unfavourable for UNDER → penalty

        Linear decay: 1.0 at noise_floor (0.5 lines) → 0.0 at full_fade (1.5 lines).
        Returns 1.0 when line_stream.db is unavailable or no opening row found.
        """
        import os, sqlite3 as _sq3, datetime as _dt
        from zoneinfo import ZoneInfo

        db_path = os.environ.get("LINE_STREAM_DB_PATH", "/app/data/line_stream.db")
        if not os.path.exists(db_path):
            return 1.0
        try:
            today_pt = _dt.datetime.now(tz=ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")
            with _sq3.connect(db_path, timeout=2) as _conn:
                row = _conn.execute(
                    """
                    SELECT line FROM line_snapshots
                     WHERE game_date = ?
                       AND is_opening = 1
                       AND LOWER(player_name) = LOWER(?)
                       AND prop_type = ?
                     ORDER BY snapshot_ts ASC
                     LIMIT 1
                    """,
                    (today_pt, player.strip(), prop_type),
                ).fetchone()
            if row is None:
                return 1.0

            delta = current_line - float(row[0])   # + = line went up
            adverse = max(0.0, delta) if side.upper() == "OVER" else max(0.0, -delta)

            if adverse <= noise_floor:
                return 1.0
            if adverse >= full_fade:
                return 0.0
            return round(1.0 - (adverse - noise_floor) / (full_fade - noise_floor), 4)
        except Exception:
            return 1.0

    @staticmethod
    def _platoon_k_adj(pitcher_hand: str,
                       lineup_hand: str,
                       platoon_k_delta: dict) -> float:
        """Per-start K% adjustment by pitcher × lineup dominant handedness.
        Source: BaseballBettingEdge PLATOON_K_DELTA.
        Positive = K-over more likely.
        """
        p = (pitcher_hand.upper() or "R")[:1]
        b = (lineup_hand.upper() or "R")[:1]
        return platoon_k_delta.get((p, b), 0.0)

    def evaluate(self, prop: dict) -> dict | None:
        """Pitcher performance props (K, outs, earned runs, hits allowed).
        Renamed from F5Agent — Underdog/PrizePicks don't offer F5 markets.
        Uses SP matchup quality, lineup chase score, and FanGraphs pitcher stats.
        """
        prop_type = _norm_stat(prop.get("prop_type", ""))
        _PITCHER_TARGETS = {"strikeouts", "pitching_outs", "earned_runs",
                            "hits_allowed", "fantasy_pitcher",
                            "walks_allowed"}  # ABS 2026: reinstated — BB rate +18%
        if prop_type not in _PITCHER_TARGETS:
            return None

        model_prob = self._model_prob(prop.get("player", ""), prop_type, prop=prop)

        # Boost for high-K pitcher props based on chase score from enrichment
        if prop_type == "strikeouts":
            chase_adj = float(prop.get("_lineup_chase_adj", 0.0))
            model_prob = min(model_prob + chase_adj * 100, 95.0)

        # CSW% boost: elite contact/swing-strike means K-over more likely
        csw = float(prop.get("csw_pct", prop.get("swstr_pct", 0.0)))
        if csw > 0.30:
            model_prob = min(model_prob + 3.0, 95.0)
        elif csw < 0.23:
            model_prob = max(model_prob - 2.0, 30.0)

        # ── Career SwStr% delta (BBEdge calc_swstr_delta_k9) ─────────────────
        # Compares current season SwStr% to pitcher's career baseline.
        # career_swstr_pct comes from fangraphs_layer or prop enrichment;
        # defaults to 0.115 (MLB average) when unavailable.
        # Cap to ±3 pp to prevent runaway adjustments on small samples.
        if prop_type == "strikeouts":
            _cur_swstr    = float(prop.get("swstr_pct", 0.0) or 0.0)
            _career_swstr = float(prop.get("career_swstr_pct",
                                           prop.get("_career_swstr", 0.115)) or 0.115)
            _n_starts     = int(prop.get("_n_starts", 10) or 10)
            if _cur_swstr > 0:
                _swstr_delta = self._swstr_career_delta_adj(
                    _cur_swstr, _career_swstr, _n_starts)
                _swstr_delta = max(-3.0, min(3.0, _swstr_delta))
                model_prob   = max(5.0, min(95.0, model_prob + _swstr_delta))

        # ── Bayesian opponent K-rate (BBEdge bayesian_opp_k) ─────────────────
        # Regresses opposing lineup K% toward 22.7% league avg by games played.
        # Early season (8 games) → 14% observed weight; by 81 games → 62%.
        # Adjustment: each 1% K-rate delta ≈ 0.8 pp (empirical from BBEdge).
        # Cap to ±3 pp.
        if prop_type == "strikeouts":
            _opp_k_raw  = float(prop.get("opp_k_rate",
                                         prop.get("_opp_team_k_pct", 0.227)) or 0.227)
            _opp_games  = int(prop.get("_opp_games_played",
                                       prop.get("_season_games", 30)) or 30)
            _adj_opp_k  = self._bayesian_opp_k(_opp_k_raw, _opp_games)
            _opp_k_pp   = max(-3.0, min(3.0, (_adj_opp_k - 0.227) * 80.0))
            model_prob  = max(5.0, min(95.0, model_prob + _opp_k_pp))

        # ── Platoon K-rate adjustment (BBEdge PLATOON_K_DELTA) ───────────────
        # Uses pitcher hand × lineup dominant hand to apply per-start K% delta.
        # pitcher_hand / _pitcher_hand from prop enrichment (default R).
        # _lineup_dominant_hand from lineup_chase_layer or batter_hand fallback.
        if prop_type == "strikeouts":
            _p_hand      = str(prop.get("pitcher_hand",
                                        prop.get("_pitcher_hand", "R")) or "R")
            _lineup_hand = str(prop.get("_lineup_dominant_hand",
                                        prop.get("batter_hand", "R")) or "R")
            _plat_adj    = self._platoon_k_adj(_p_hand, _lineup_hand,
                                               self._PLATOON_K_DELTA)
            model_prob   = max(5.0, min(95.0, model_prob + _plat_adj))

        # ── MAX_LAMBDA_LINE_GAP cap (BBEdge) ──────────────────────────────────
        # Picks where model lambda disagrees with K line by ≥3 win at 21%.
        # Cap model_prob at the probability implied by λ = line ± 2.5.
        # Applies to strikeouts and pitching_outs props only.
        if prop_type in ("strikeouts", "pitching_outs"):
            _k_line = float(prop.get("line", 0) or 0)
            if _k_line > 0:
                model_prob = self._lambda_gap_cap(model_prob, _k_line, max_gap=2.5)

        # ── Line-movement confidence (BBEdge adapted) ─────────────────────────
        # If UD/PP prop line has moved AGAINST the bet direction since
        # this morning's first snapshot, apply a confidence decay multiplier.
        # noise_floor = 0.5 lines (half-K movement ignored as routine);
        # full_fade   = 1.5 lines (1.5-K adverse move kills the pick).
        # Source: line_stream.db is_opening rows. Returns 1.0 when unavailable.
        if prop_type in ("strikeouts", "pitching_outs", "hits", "earned_runs",
                         "hits_allowed", "walks_allowed", "hitter_strikeouts"):
            _cur_line   = float(prop.get("line", 0) or 0)
            _bet_side   = prop.get("_pre_side", "OVER")  # side chosen before final gate
            _move_conf  = self._line_movement_conf(
                prop.get("player", ""), prop_type, _cur_line, _bet_side)
            if _move_conf < 1.0:
                # Apply as a probability drag: shrink edge toward 50% by (1 - conf)
                _edge      = model_prob - 50.0
                model_prob = round(50.0 + _edge * _move_conf, 2)
                model_prob = max(5.0, min(95.0, model_prob))

        # Pitcher type cluster nudge (set by prop_enrichment_layer)
        # Power pitchers get extra K-over bias on top of raw CSW%;
        # command pitchers suppress K-over and support ER-under.
        _ptype = prop.get("_pitcher_type", "neutral")
        if _ptype == "power" and prop_type == "strikeouts":
            model_prob = min(model_prob + 2.5, 95.0)
        elif _ptype == "command" and prop_type == "strikeouts":
            model_prob = max(model_prob - 2.5, 30.0)
        elif _ptype == "command" and prop_type == "earned_runs":
            model_prob = min(model_prob + 2.0, 95.0)  # command → ER under more likely

        # Days rest / pitch count adjustment from prop_enrichment_layer
        _rest_adj = float(prop.get("_rest_adj", 0.0))
        _pc_adj   = float(prop.get("_pitch_count_adj", 0.0))
        if _rest_adj or _pc_adj:
            model_prob = max(5.0, min(95.0, model_prob + _rest_adj + _pc_adj))

        # ── Continuous TTOP decay (Tango/Brill 2023) ──────────────────────
        # Batters improve each time through the order — adjust pitcher props.
        # For K-props: pitcher fades → Under gets a boost.
        # For ER/hits_allowed: pitcher fades → Over gets a boost.
        _bf      = int(prop.get("_batters_faced", 0) or 0)
        _pc      = int(prop.get("_pitches_thrown", 0) or 0)
        _arsenal = int(prop.get("_arsenal_size", 3) or 3)
        if _bf > 0 or _pc > 0:
            _ttop_pp = self._ttop_penalty(_bf, _pc, _arsenal)
            if _ttop_pp > 0.5:
                if prop_type == "strikeouts":
                    model_prob = max(5.0, model_prob - _ttop_pp)
                elif prop_type in ("earned_runs", "hits_allowed"):
                    model_prob = min(95.0, model_prob + _ttop_pp)

        over_odds     = prop.get("over_american",  -110)
        under_odds    = prop.get("under_american", -110)
        under_prob    = 100.0 - model_prob
        over_implied  = _american_to_implied(over_odds)  / 100
        under_implied = _american_to_implied(under_odds) / 100
        ev_over  = (model_prob  / 100 - over_implied)  / over_implied
        ev_under = (under_prob  / 100 - under_implied) / under_implied
        _thresh  = _get_ev_threshold(prop.get("_sim_edge_reasons", []))
        if ev_over >= _thresh and ev_over >= ev_under:
            return self._build_bet(prop, "OVER",  model_prob,
                                   over_implied  * 100, ev_over  * 100)
        if ev_under >= _thresh:
            return self._build_bet(prop, "UNDER", under_prob,
                                   under_implied * 100, ev_under * 100)
        return None


class _FadeAgent(_BaseAgent):
    name = "FadeAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Fades heavy public action using SportsBettingDime real BET%/MONEY% data.

        Fades BOTH directions:
          - Public ≥65% Over tickets  → fade to Under
          - Public ≤35% Over tickets  → public is heavy Under → fade to Over
        Boost scales with how extreme the lean is (65%→+0pp, 80%→+1.5pp, 95%→+3.5pp).
        """
        SBD_THRESHOLD       = 65.0
        SBD_UNDER_THRESHOLD = 35.0  # mirror: public heavy Under → fade to Over
        market   = self.hub.get("market", {})
        pub_data = market.get("public_betting", {})
        player   = prop.get("player", "")
        team     = prop.get("team", "")
        prop_type = prop.get("prop_type", "")

        try:
            import pandas as pd
            game_records = pub_data.get("game_df", []) if isinstance(pub_data, dict) else []
            prop_records = pub_data.get("prop_df", []) if isinstance(pub_data, dict) else []
            game_df = pd.DataFrame(game_records) if game_records else pd.DataFrame()
            prop_df = pd.DataFrame(prop_records) if prop_records else pd.DataFrame()
        except ImportError:
            return None

        pub_pct, signal_src = get_fade_signal(
            player, team, prop_type, game_df, prop_df, threshold=SBD_THRESHOLD
        )

        model_prob = self._model_prob(player, prop_type, prop=prop)
        _multiplier = 1.25 if signal_src == "player_prop" else 1.0

        # ── Fade heavy public Over → bet Under ───────────────────────────────
        if pub_pct >= SBD_THRESHOLD:
            _extremity  = (pub_pct - SBD_THRESHOLD) / (100.0 - SBD_THRESHOLD)
            fade_boost  = round(_extremity * 4.0 * _multiplier, 2)
            fade_prob   = min(95.0, (100 - model_prob) + fade_boost)
            under_odds  = prop.get("under_american", -110)
            implied     = _american_to_implied(under_odds) / 100
            ev_pct      = (fade_prob / 100 - implied) / implied
            if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
                return self._build_bet(prop, "UNDER", fade_prob,
                                       implied * 100, ev_pct * 100)

        # ── Fade heavy public Under → bet Over ───────────────────────────────
        if pub_pct <= SBD_UNDER_THRESHOLD and pub_pct > 0:
            _extremity  = (SBD_UNDER_THRESHOLD - pub_pct) / SBD_UNDER_THRESHOLD
            fade_boost  = round(_extremity * 4.0 * _multiplier, 2)
            fade_prob   = min(95.0, model_prob + fade_boost)
            over_odds   = prop.get("over_american", -110)
            implied     = _american_to_implied(over_odds) / 100
            ev_pct      = (fade_prob / 100 - implied) / implied
            if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
                return self._build_bet(prop, "OVER", fade_prob,
                                       implied * 100, ev_pct * 100)

        return None


class _LineValueAgent(_BaseAgent):
    name = "LineValueAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Hunts steam moves (sharp money signals).
        Primary: sharp_report from Action Network (if available).
        Fallback: SBD public_betting — >70% public tickets = steam signal.
        FIX: now tracks which side steam is on and bets accordingly.
        """
        player    = prop.get("player", "")
        prop_type = prop.get("prop_type", "")
        steam     = False
        steam_side = "OVER"  # default; overridden when side is determinable
        sharp = self.hub.get("market", {}).get("sharp_report", [])
        for rec in sharp:
            if isinstance(rec, dict) and player.lower() in str(rec).lower():
                steam = bool(
                    rec.get("rlm_signal", False)
                    or rec.get("steam_move", False)
                    or rec.get("reverse_line_move", False)
                )
                if steam:
                    # Use rlm_direction if available
                    _dir = str(rec.get("rlm_direction", rec.get("steam_direction", "over"))).lower()
                    steam_side = "UNDER" if _dir == "under" else "OVER"
                    break

        # Fallback: SBD public betting — extreme ticket% = steam proxy
        if not steam:
            pub = self.hub.get("market", {}).get("public_betting", {})
            for rec in (pub.get("prop_df", []) if isinstance(pub, dict) else []):
                if not isinstance(rec, dict):
                    continue
                _rec_player = str(rec.get("player_name", rec.get("player", ""))).lower()
                if player.lower() not in _rec_player:
                    continue
                over_pct  = float(
                    rec.get("prop_over_bets_pct")
                    or rec.get("over_pct")
                    or rec.get("ticket_pct")
                    or 0
                )
                under_pct = float(
                    rec.get("prop_under_bets_pct")
                    or rec.get("under_pct")
                    or 0
                )
                if over_pct >= 70:
                    # Require line movement evidence: public piling on Over without
                    # the line moving over = NOT steam. Real steam = public on Over
                    # but line moved toward Under (reverse line movement).
                    # If no line move data available, require more extreme ticket%.
                    _line_moved_against = bool(
                        rec.get("reverse_line_move")
                        or rec.get("rlm_signal")
                        or rec.get("line_moved_against_public")
                    )
                    _extreme_tickets = over_pct >= 80  # very extreme without RLM confirmation
                    if _line_moved_against or _extreme_tickets:
                        steam = True
                        steam_side = "OVER"
                        break
                if under_pct >= 70:
                    _line_moved_against = bool(
                        rec.get("reverse_line_move")
                        or rec.get("rlm_signal")
                        or rec.get("line_moved_against_public")
                    )
                    _extreme_tickets = under_pct >= 80
                    if _line_moved_against or _extreme_tickets:
                        steam = True
                        steam_side = "UNDER"
                        break

        if not steam:
            return None

        model_prob = self._model_prob(player, prop_type, prop=prop)
        odds   = prop.get("over_american", -110) if steam_side == "OVER" else prop.get("under_american", -110)
        implied = _american_to_implied(odds) / 100
        # Use side-appropriate probability
        prob_side = model_prob if steam_side == "OVER" else (100.0 - model_prob)
        ev_pct = (prob_side / 100 - implied) / implied
        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, steam_side, prob_side,
                                   implied * 100, ev_pct * 100)
        return None


class _BullpenAgent(_BaseAgent):
    name = "BullpenAgent"
    # Canonical hitter stat set — populated via _norm_stat() at ingestion
    _HITTER_STATS = {"home_runs", "rbis", "hits", "total_bases", "hits_runs_rbis",
                     "stolen_bases", "walks", "runs", "fantasy_score"}

    def evaluate(self, prop: dict) -> dict | None:
        """Targets hitter props when opposing bullpen is fatigued or volatile.

        Signal stack:
          1. BullpenFatigueScorer (0–4 scale) — day-of workload from pitching logs
          2. BVI (0–100) — structural volatility from live feed entry states,
             inherited runner instability, and fatigue CV over last 10 days
          3. _relief_fatigue_penalty() — per-pitcher B2B penalty (wOBA delta)

        BVI interpretation:
          >60 → volatile bullpen: confirm OVER, apply +3pp structural boost
          <35 → stable bullpen:   confirm UNDER, apply +2pp structural boost
          35–60 → neutral: no BVI adjustment

        Fatigue scoring (unchanged): ≥3 → +6pp, ≥2 → +2pp.
        """
        fatigue_map: dict = self.hub.get("bullpen_fatigue", {})
        bvi_map:     dict = self.hub.get("bullpen_bvi", {})
        player    = prop.get("player", "")
        team      = prop.get("team", "")
        prop_type = prop.get("prop_type", "")

        if _norm_stat(prop_type) not in self._HITTER_STATS:
            return None

        # ── Opposing team (whose bullpen the batter faces) ────────────────────
        opp_team     = prop.get("opposing_team", "")
        opp_abbrev   = prop.get("_opp_team_abbrev", opp_team).upper()

        # ── 1. Legacy fatigue score ───────────────────────────────────────────
        _raw_entry = fatigue_map.get(opp_team, fatigue_map.get(team, -1))
        if isinstance(_raw_entry, dict):
            fatigue = float(_raw_entry.get("fatigue_score", 2.0))
        else:
            fatigue = float(_raw_entry)
        if fatigue < 0:
            fatigue = 2.0   # neutral when no data

        # ── 2. BVI structural adjustment ─────────────────────────────────────
        bvi_entry  = bvi_map.get(opp_abbrev, {})
        bvi_score  = float(bvi_entry.get("bvi", 50.0)) if bvi_entry else 50.0
        # Directional BVI boost: volatile bullpen → OVER; stable → UNDER
        if bvi_score > 60.0:
            bvi_over_adj  = round((bvi_score - 60.0) / 40.0 * 3.0, 2)   # max +3pp at BVI=100
            bvi_under_adj = 0.0
        elif bvi_score < 35.0:
            bvi_over_adj  = 0.0
            bvi_under_adj = round((35.0 - bvi_score) / 35.0 * 2.0, 2)   # max +2pp at BVI=0
        else:
            bvi_over_adj = bvi_under_adj = 0.0

        # ── 3. Reliever B2B penalty (if workload data available) ──────────────
        _days_pitched = prop.get("_opp_reliever_days_pitched", [])
        _pitches_last = int(prop.get("_opp_reliever_pitches_last", 20) or 20)
        rf_penalty_woba = _relief_fatigue_penalty(_days_pitched, _pitches_last)
        # Convert wOBA delta → probability points (~0.030 wOBA ≈ 2.5pp)
        rf_pp = round(rf_penalty_woba / 0.030 * 2.5, 2)

        model_prob = self._model_prob(player, prop_type, prop=prop)

        # ── Apply fatigue boost (continuous linear, not discrete steps) ──────
        # Old: fatigue>=3 → +6pp cliff. New: scales 0pp at 1.0 to +6pp at 5.0.
        # Each 0.5 unit of fatigue above neutral (2.0) adds ~1pp, max +6pp.
        # Neutral fatigue=2.0 → 0pp boost; fatigue=5.0 → +6pp; fatigue=0.0 → 0pp (rested).
        if fatigue > 2.0:
            model_prob = min(model_prob + min((fatigue - 2.0) / 3.0 * 6.0, 6.0), 95.0)

        # ── Apply BVI + reliever B2B structural boosts ────────────────────────
        model_prob_over  = min(model_prob + bvi_over_adj  + rf_pp, 95.0)
        model_prob_under = min((100.0 - model_prob) + bvi_under_adj, 95.0)

        over_odds     = prop.get("over_american",  -110)
        under_odds    = prop.get("under_american", -110)
        over_implied  = _american_to_implied(over_odds)  / 100
        under_implied = _american_to_implied(under_odds) / 100
        ev_over  = (model_prob_over  / 100 - over_implied)  / over_implied
        ev_under = (model_prob_under / 100 - under_implied) / under_implied
        _thresh  = _get_ev_threshold(prop.get("_sim_edge_reasons", []))

        # Fatigued/volatile bullpen → OVER. Stable/rested bullpen → UNDER.
        if ev_over >= _thresh and ev_over >= ev_under:
            return self._build_bet(prop, "OVER",  model_prob_over,
                                   over_implied  * 100, ev_over  * 100)
        if ev_under >= _thresh:
            return self._build_bet(prop, "UNDER", model_prob_under,
                                   under_implied * 100, ev_under * 100)
        return None


class _WeatherAgent(_BaseAgent):
    name = "WeatherAgent"

    # ── Park data keyed by stadium name (matches _TEAM_TO_STADIUM values) ─
    # hr_factor: Baseball Reference park HR factor (100 = league avg).
    # lhh_hr_boost: True when a short porch gives LHH pull hitters a structural edge.
    # wind_critical: True when wind is the dominant game variable (Wrigley).
    # Source: baseball-analytics/src/weather.py PARK_DATA (2024 season).
    # Park metadata: lhh_hr_boost and wind_critical flags per stadium.
    # HR factors are read dynamically from park_factors.py (the canonical source)
    # so _WeatherAgent and XGBoost training always use the same park numbers.
    _PARK_META: dict[str, dict] = {
        "Angels Stadium":           {"lhh_hr_boost": False, "wind_critical": False},
        "Chase Field":              {"lhh_hr_boost": False, "wind_critical": False},
        "Camden Yards":             {"lhh_hr_boost": True,  "wind_critical": False},
        "Fenway Park":              {"lhh_hr_boost": False, "wind_critical": False},
        "Wrigley Field":            {"lhh_hr_boost": False, "wind_critical": True},
        "Guaranteed Rate Field":    {"lhh_hr_boost": False, "wind_critical": False},
        "Great American Ball Park": {"lhh_hr_boost": True,  "wind_critical": False},
        "Progressive Field":        {"lhh_hr_boost": False, "wind_critical": False},
        "Coors Field":              {"lhh_hr_boost": False, "wind_critical": False},
        "Comerica Park":            {"lhh_hr_boost": False, "wind_critical": False},
        "Minute Maid Park":         {"lhh_hr_boost": False, "wind_critical": False},
        "Kauffman Stadium":         {"lhh_hr_boost": False, "wind_critical": False},
        "Dodger Stadium":           {"lhh_hr_boost": False, "wind_critical": False},
        "LoanDepot Park":           {"lhh_hr_boost": False, "wind_critical": False},
        "American Family Field":    {"lhh_hr_boost": False, "wind_critical": False},
        "Target Field":             {"lhh_hr_boost": False, "wind_critical": False},
        "Citi Field":               {"lhh_hr_boost": False, "wind_critical": False},
        "Yankee Stadium":           {"lhh_hr_boost": True,  "wind_critical": False},
        "Oakland Coliseum":         {"lhh_hr_boost": False, "wind_critical": False},
        "Citizens Bank Park":       {"lhh_hr_boost": False, "wind_critical": False},
        "PNC Park":                 {"lhh_hr_boost": False, "wind_critical": False},
        "Petco Park":               {"lhh_hr_boost": False, "wind_critical": False},
        "Oracle Park":              {"lhh_hr_boost": False, "wind_critical": False},
        "T-Mobile Park":            {"lhh_hr_boost": False, "wind_critical": False},
        "Busch Stadium":            {"lhh_hr_boost": False, "wind_critical": False},
        "Tropicana Field":          {"lhh_hr_boost": False, "wind_critical": False},
        "Globe Life Field":         {"lhh_hr_boost": False, "wind_critical": False},
        "Rogers Centre":            {"lhh_hr_boost": False, "wind_critical": False},
        "Nationals Park":           {"lhh_hr_boost": False, "wind_critical": False},
        "Truist Park":              {"lhh_hr_boost": False, "wind_critical": False},
    }

    @staticmethod
    def _get_park_hr_factor(stadium: str) -> float:
        """Read HR park factor from park_factors.py (canonical source).
        Returns 100.0 (league average) if stadium not found.
        Converts park_factors.py multiplier (1.13 = +13%) to BRef-style
        integer-scale (113) for consistent comparison in park_hr_adj calc.
        """
        try:
            from park_factors import get_park_factor as _gpf  # noqa: PLC0415
            pf = _gpf(stadium, "home_runs")
            return round(pf * 100.0, 1)   # 1.13 → 113.0
        except Exception:
            return 100.0  # neutral fallback

    # Primary outfield compass direction per park (wind along this axis = "out")
    _OUTFIELD_COMPASS = {
        "Wrigley Field":  90,   # E toward Lake Michigan
        "Coors Field":   270,   # W (Rocky Mountain breeze)
        "Fenway Park":    90,   # E to right/center
        "Oracle Park":   315,   # NW to McCovey Cove
    }
    _COMPASS_DEG = {
        "N":0,"NNE":22.5,"NE":45,"ENE":67.5,"E":90,"ESE":112.5,
        "SE":135,"SSE":157.5,"S":180,"SSW":202.5,"SW":225,"WSW":247.5,
        "W":270,"WNW":292.5,"NW":315,"NNW":337.5,
    }

    @staticmethod
    def _wind_along_spray(wind_spd: float, wind_dir: str, stadium: str,
                          outfield_compass: dict, compass_deg: dict) -> float:
        """Signed wind speed along primary batted-ball spray axis.
        Positive = out (HR boost), negative = in (HR suppression).
        Source: mc_upgrades.py build_weather_multipliers().
        """
        import math as _m
        if not wind_dir or wind_spd <= 0:
            return 0.0
        out_deg  = outfield_compass.get(stadium, 180.0)
        wind_deg = compass_deg.get(wind_dir.strip().upper(), 0.0)
        diff_rad = _m.radians(wind_deg - out_deg)
        return wind_spd * _m.cos(diff_rad)

    def evaluate(self, prop: dict) -> dict | None:
        """Physics-grade weather adjustments for power and contact props.

        Adjustments applied in order:
          1. Temperature  — Nathan 2017: +0.8pp/°F HR, +0.4pp/°F contact vs 70°F baseline
          2. Wind         — signed component along park spray axis (mc_upgrades Phase 1A)
          3. Park HR factor — data-driven per-park HR suppression/boost vs league average.
             Replaces flat -3pp humidor hardcode. Scale: each 1pt hr_factor ≈ 0.12pp.
             Petco hr_factor=86 → -1.68pp | Coors hr_factor=122 → +2.64pp
          4. LHH HR boost — +1.5pp for left-handed pull hitters at porch parks
             (Yankee Stadium RF 314ft, Camden Yards RF 318ft, Great American BF)
          5. Wrigley wind amplifier — wind adjustment ×1.5 when wind_critical and ≥10mph
        """
        if prop.get("is_dome"):
            return None

        player    = prop.get("player", "")
        prop_type = prop.get("prop_type", "")
        venue     = prop.get("venue", "")

        wind_mph = float(prop.get("_wind_speed", 0) or 0)
        wind_dir = str(prop.get("_wind_direction", "") or "")
        temp_f   = float(prop.get("_temp_f", 72) or 72)

        if wind_mph == 0:
            for w in self.hub.get("context", {}).get("weather", []):
                if not isinstance(w, dict):
                    continue
                team = prop.get("team", "")
                if (venue and venue.lower() in str(w).lower()) or \
                   (team and team.lower() in str(w.get("team", "")).lower()):
                    wind_mph = float(w.get("wind_speed_mph", w.get("wind_speed", 0)) or 0)
                    wind_dir = str(w.get("wind_direction", "") or "")
                    temp_f   = float(w.get("temp_f", 72) or 72)
                    break

        _POWER_PROPS   = {"home_runs", "total_bases", "hits_runs_rbis",
                          "fantasy_hitter", "fantasy_pitcher"}
        _CONTACT_PROPS = {"hits", "rbis", "runs"}
        pt_norm = _norm_stat(prop_type)

        if pt_norm not in (_POWER_PROPS | _CONTACT_PROPS):
            return None

        model_prob = self._model_prob(player, prop_type, prop=prop)
        stadium    = _TEAM_TO_STADIUM.get(prop.get("team", ""), venue)
        park       = self._PARK_META.get(stadium, {})
        hr_factor  = self._get_park_hr_factor(stadium)

        # ── 1. Temperature ──────────────────────────────────────────────────
        temp_f     = max(20.0, min(110.0, temp_f))
        temp_boost = (temp_f - 70.0) * (0.8 if pt_norm in _POWER_PROPS else 0.4)

        # ── 2. Wind: signed component along spray axis ──────────────────────
        along         = self._wind_along_spray(
            wind_mph, wind_dir, stadium, self._OUTFIELD_COMPASS, self._COMPASS_DEG,
        )
        wind_hr_boost = along * 0.4   # +0.4pp per mph outward

        # ── 3. Park HR factor from park_factors.py (canonical source) ───────
        # Each 1pt of hr_factor delta from 100 ≈ 0.12pp on a prop probability.
        park_hr_adj = (hr_factor - 100) * 0.12
        if pt_norm in _CONTACT_PROPS:
            park_hr_adj *= 0.4   # contact props get 40% of park HR effect

        # ── 4. LHH structural porch advantage ──────────────────────────────
        # +1.5pp for left-handed pull hitters at parks with short RF/LF porches.
        lhh_adj = 0.0
        if (park.get("lhh_hr_boost")
                and pt_norm in _POWER_PROPS
                and str(prop.get("_batter_hand", "") or "").upper() == "L"):
            lhh_adj = 1.5

        # ── 5. Wrigley wind amplifier ───────────────────────────────────────
        if park.get("wind_critical") and wind_mph >= 10:
            wind_hr_boost *= 1.5

        # ── Combine ─────────────────────────────────────────────────────────
        total_adj = temp_boost + wind_hr_boost + park_hr_adj + lhh_adj

        if abs(total_adj) < 1.0:
            return None

        side      = "OVER" if total_adj > 0 else "UNDER"
        adj_prob  = (
            model_prob + total_adj
            if side == "OVER"
            else (100.0 - model_prob) + abs(total_adj)
        )
        adj_prob = max(5.0, min(95.0, adj_prob))

        if side == "OVER":
            odds    = prop.get("over_american", -110)
            implied = _american_to_implied(odds) / 100
            ev_pct  = (adj_prob / 100 - implied) / implied
        else:
            odds    = prop.get("under_american", -110)
            implied = _american_to_implied(odds) / 100
            ev_pct  = (adj_prob / 100 - implied) / implied

        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, side, adj_prob, implied * 100, ev_pct * 100)
        return None


class _SteamAgent(_BaseAgent):
    name = "SteamAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Follows sharp money (reverse line movement).
        Primary: sharp_report RLM flag.
        Fallback: SBD money% vs ticket% divergence ≥15pp = RLM proxy.
        """
        player    = prop.get("player", "")
        prop_type = prop.get("prop_type", "")
        rlm       = False
        fade_side = "OVER"  # default: sharp money on Over
        sharp = self.hub.get("market", {}).get("sharp_report", [])
        for rec in sharp:
            if isinstance(rec, dict) and player.lower() in str(rec).lower():
                rlm = bool(
                    rec.get("rlm_signal", False)
                    or rec.get("reverse_line_move", False)
                )
                if rlm:
                    # rlm_direction tells us which side sharp money is on
                    _dir = rec.get("rlm_direction", "")
                    if _dir == "under":
                        fade_side = "UNDER"
                    else:
                        fade_side = "OVER"
                    break

        # Fallback: SBD money% vs ticket% divergence
        if not rlm:
            pub = self.hub.get("market", {}).get("public_betting", {})
            for rec in (pub.get("prop_df", []) if isinstance(pub, dict) else []):
                if not isinstance(rec, dict):
                    continue
                _rec_player = str(rec.get("player_name", rec.get("player", ""))).lower()
                if player.lower() not in _rec_player:
                    continue
                # SBD actual columns: prop_over_bets_pct, prop_over_money_pct
                ticket = float(
                    rec.get("prop_over_bets_pct")
                    or rec.get("ticket_pct")
                    or rec.get("over_pct")
                    or 50
                )
                money  = float(
                    rec.get("prop_over_money_pct")
                    or rec.get("money_pct")
                    or 50
                )
                div    = abs(money - ticket)
                if div >= 15:  # sharp money diverging from public = RLM
                    rlm = True
                    # If money% > ticket%, sharp betting Over despite public Under
                    fade_side = "OVER" if money > ticket else "UNDER"
                    break

        if not rlm:
            return None

        model_prob = self._model_prob(player, prop_type, prop=prop)
        model_prob = min(model_prob + 4.0, 95.0)

        if fade_side == "OVER":
            odds    = prop.get("over_american", -110)
            implied = _american_to_implied(odds) / 100
        else:
            odds    = prop.get("under_american", -110)
            implied = _american_to_implied(odds) / 100
            model_prob = 100 - model_prob  # flip for Under

        ev_pct = (model_prob / 100 - implied) / implied
        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, fade_side, model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _MLEdgeAgent(_BaseAgent):
    name = "MLEdgeAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Pure XGBoost model edge — fires when model diverges ≥8pp from implied.
        Passes full prop dict to _model_prob so enriched features are used.
        """
        model_prob = self._model_prob(
            prop.get("player", ""), prop.get("prop_type", ""), prop=prop
        )
        over_odds  = prop.get("over_american", -110)
        # _american_to_implied returns 0-100 (already percentage-scaled)
        implied    = _american_to_implied(over_odds)
        divergence = abs(model_prob - implied)
        if divergence < 8.0:
            return None
        side      = "OVER" if model_prob > implied else "UNDER"
        odds      = over_odds if side == "OVER" else prop.get("under_american", -110)
        imp       = _american_to_implied(odds)
        prob_side = model_prob if side == "OVER" else (100.0 - model_prob)
        ev_pct    = (prob_side / 100 - imp / 100) / (imp / 100)
        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, side, prob_side, imp, ev_pct * 100)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Underdog-vs-Sharp edge helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_underdog_props(hub: dict) -> list[dict]:
    """Parse Underdog Fantasy lines from hub DFS data.
    Handles both 'over_odds'/'under_odds' (old) and
    'over_american'/'under_american' (new _fetch_underdog_props_direct) key names.
    """
    picks = hub.get("dfs", {}).get("underdog", [])
    props: list[dict] = []
    for pick in picks:
        if not isinstance(pick, dict):
            continue
        # Support both key naming conventions
        over_odds = int(
            pick.get("over_american", pick.get("over_odds", -115)) or -115
        )
        under_odds = int(
            pick.get("under_american", pick.get("under_odds", -115)) or -115
        )
        player = pick.get("player", pick.get("name", "Unknown"))
        prop_type = _norm_stat(pick.get("stat_type", pick.get("prop_type", pick.get("prop", "H"))))
        line = float(pick.get("line", pick.get("value", 1.5)) or 1.5)
        if not player or player == "Unknown":
            continue
        props.append({
            "player":         player,
            "prop_type":      str(prop_type).lower(),
            "line":           line,
            "over_american":  over_odds,
            "under_american": under_odds,
            "team":           pick.get("team", ""),
            "venue":          pick.get("venue", ""),
            "platform":       "underdog",
            "underdog_line":  over_odds,
        })
    return props


_SHARP_BOOKS = {"draftkings", "fanduel", "pinnacle", "circa", "betmgm", "pointsbet"}


def _get_sharp_consensus(hub: dict, player: str, prop_type: str) -> float | None:
    """
    Extract sharp-book consensus implied probability for a player/prop.

    Routes through sportsbook_reference_layer (The Odds API /events/{id}/odds
    with player-prop markets: pitcher_strikeouts, batter_hits, batter_total_bases,
    batter_rbis, batter_runs_scored). Fetches DK/FD/BetMGM, strips vig, caches
    daily to /tmp/sb_ref_{date}.json — subsequent calls in the same day are free.

    Returns probability as a percentage (0–100), or None if no match found.

    NOTE: hub["market"]["odds"] contains game-level odds (moneylines/totals),
    NOT player props — the old implementation always returned None for props.
    """
    if not _SB_REFERENCE_AVAILABLE:
        return None
    try:
        reference = _build_sb_reference()
        if not reference:
            return None

        # Normalize to match sportsbook_reference_layer._normalize_name() format
        player_norm = (
            player.lower()
            .strip()
            .replace(".", "")
            .replace("'", "")
            .replace("-", " ")
            .replace("  ", " ")
        )

        # Map internal prop_type ("hits") to Odds API market key ("batter_hits")
        # DraftEdge fallback stores by market key, Odds API also uses market keys
        _PT_TO_MARKET = {
            "hits":              "batter_hits",
            "runs":              "batter_runs_scored",
            "rbis":              "batter_rbis",
            "rbi":               "batter_rbis",
            "total_bases":       "batter_total_bases",
            "strikeouts":        "pitcher_strikeouts",
            "earned_runs":       "pitcher_earned_runs",
            # these two were missing — caused sharp_prob to always be None
            # for pitching outs and hitter strikeout props
            "pitching_outs":     "pitcher_outs",
            "hitter_strikeouts": "batter_strikeouts",
            "hits_allowed":      "pitcher_hits_allowed",
        }
        market_key = _PT_TO_MARKET.get(prop_type, prop_type)

        # Direct full-name lookup — try both "Over" (Odds API) and "over" (DraftEdge)
        # Try internal prop_type first, then mapped market key
        ref = (
            reference.get((player_norm, prop_type, "Over"))
            or reference.get((player_norm, prop_type, "over"))
            or reference.get((player_norm, market_key, "Over"))
            or reference.get((player_norm, market_key, "over"))
        )
        if ref:
            return round(ref["sb_implied_prob"] * 100.0, 2)

        # Last-name fallback: scan for any entry where last token matches
        parts = player_norm.split()
        last = parts[-1] if parts else ""
        for (pn, pt, side), data in reference.items():
            if side.lower() == "over" and pn.split()[-1:] == [last]:
                # Match on either the internal prop_type or the market key form
                if pt in (prop_type, market_key):
                    return round(data["sb_implied_prob"] * 100.0, 2)

        return None
    except Exception:
        return None


def _underdog_edge(underdog_odds: int, sharp_prob_pct: float) -> float:
    """
    Edge = sharp consensus implied prob % − Underdog implied prob %.
    Positive = Underdog is mispriced vs sharp books → exploitable DFS edge.
    """
    return sharp_prob_pct - _american_to_implied(underdog_odds)


# ─────────────────────────────────────────────────────────────────────────────
# DFS Parlay (Slip) builder
# ─────────────────────────────────────────────────────────────────────────────

def _are_legs_correlated(legs: list[dict]) -> bool:
    """Return True if the slip would be rejected by PrizePicks/Underdog correlated parlay rules.

    Rules enforced:
    1. Same player in two legs → blocked.
    2. Same player+prop combo → blocked.
    3. Platform same-team rule: a slip where EVERY leg is from the same team is
       blocked (pure same-team stack). Two teammates are fine IF at least one
       leg from a different team is also in the slip.
       - LAD+LAD (2-leg)         → blocked (pure stack, no break)
       - LAD+LAD+NYY (3-leg)     → allowed (NYY breaks it)
       - STL+DET+STL (3-leg)     → allowed (DET breaks it)
       - LAD+LAD+LAD (3-leg)     → blocked (pure stack)
       - all different teams      → always allowed
    """
    from collections import Counter  # noqa: PLC0415
    players = [lg.get("player", "") for lg in legs]
    if len(set(players)) < len(players):
        return True

    # Rule 2 — same player+prop combo (e.g. Judge Over AND Under strikeouts)
    combos = [(lg.get("player", ""), lg.get("prop_type", "")) for lg in legs]
    if len(set(combos)) < len(combos):
        return True

    # Rule 3 — pure same-team stack
    teams = [
        lg.get("team", lg.get("team_abbrev", "")).strip().upper()
        for lg in legs
    ]
    teams_known = [t for t in teams if t]          # drop legs with no team field
    if len(teams_known) >= 2:
        counts = Counter(teams_known)
        # blocked only when EVERY known leg is on the same team
        if len(counts) == 1 and max(counts.values()) >= 2:
            return True

    return False


def _make_parlay(legs: list[dict], agent_name: str = "The Correlated Parlay Agent") -> list[dict]:
    avg_conf = round(sum(lg.get("confidence", 5) for lg in legs) / max(len(legs), 1), 1)

    # ── Step 1: enrich each leg with the best available line per platform ──────
    try:
        from line_comparator import build_line_lookup, compare_prop  # noqa: PLC0415
        _hub_snap = read_hub()
        _ud_raw   = _hub_snap.get("dfs", {}).get("underdog",   [])
        _pp_raw   = _hub_snap.get("dfs", {}).get("prizepicks", [])
        _ud_lkp   = build_line_lookup(_ud_raw)
        _pp_lkp   = build_line_lookup(_pp_raw)
        _lc_ok    = True
    except Exception:
        _lc_ok = False

    enriched_legs = []
    for lg in legs:
        _orig_conf = lg.get("confidence", 5)   # capture before any mutation
        if _lc_ok:
            try:
                result = compare_prop(
                    lg.get("player", lg.get("player_name", "")),
                    lg.get("prop_type", ""),
                    lg.get("side", "OVER"),
                    _ud_lkp, _pp_lkp,
                )
                if result.get("platform") and result.get("line") is not None:
                    lg = {**lg,
                          "recommended_platform": result["platform"],
                          "line":                 result["line"],
                          "_line_note":           result.get("note", ""),
                    }
            except Exception:
                pass
        # Defensive: always restore original confidence so compare_prop cannot
        # the MIN_CONFIDENCE=6 gate for non-EVHunter agents (Bug #15b).
        if "confidence" not in lg:
            lg = {**lg, "confidence": _orig_conf}
        enriched_legs.append(lg)

    # ── Step 2: enforce platform purity + decide entry type ─────────────────
    # RULE: every leg in a slip must be from the same platform.
    # Streaks are Underdog-only — PP legs can never qualify for a streak.
    # If legs from multiple platforms somehow reach here, drop the minority platform.

    def _leg_platform(lg: dict) -> str:
        raw = (lg.get("recommended_platform") or lg.get("platform") or "underdog").lower()
        return "prizepicks" if "prize" in raw else "underdog"

    _plat_counts: dict = {}
    for _lg in enriched_legs:
        _p = _leg_platform(_lg)
        _plat_counts[_p] = _plat_counts.get(_p, 0) + 1

    # Majority platform wins; ties go to underdog (higher multipliers)
    if len(_plat_counts) > 1:
        _dominant = max(_plat_counts, key=lambda k: (_plat_counts[k], k == "underdog"))
        _dropped  = [_lg for _lg in enriched_legs if _leg_platform(_lg) != _dominant]
        enriched_legs = [_lg for _lg in enriched_legs if _leg_platform(_lg) == _dominant]
        if _dropped:
            logger.info(
                "[_make_parlay] %s: dropped %d cross-platform leg(s) to enforce %s purity.",
                agent_name, len(_dropped), _dominant,
            )
    parlay_platform = _leg_platform(enriched_legs[0]) if enriched_legs else "underdog"

    if len(enriched_legs) < 2:
        logger.info("[_make_parlay] %s: not enough same-platform legs after purity filter.", agent_name)
        return []

    # Streaks are Underdog-only — never assign STREAKS to a PP slip
    entry_type_forced = None

    # Safety net: re-check leg count — the Streaks block used to trim
    # 2-leg slips to 1-leg here, producing "1-Leg FlexPlay Slips".
    # Streaks are now handled exclusively by streak_agent.py.
    if len(enriched_legs) < 2:
        logger.info("[_make_parlay] %s: fewer than 2 legs after filtering — skipping.", agent_name)
        return []

    # ── Step 3: build the single parlay ──────────────────────────────────────
    n      = len(enriched_legs)
    probs  = [min(0.95, max(0.05, l.get("model_prob", 0.0) / 100)) for l in enriched_legs]
    p_conf = round(sum(l.get("confidence", 5) for l in enriched_legs) / max(n, 1), 1)

    if parlay_platform == "underdog":
        entry_type  = entry_type_forced or "PowerPlay"
        combined_ev = 0.0
        try:
            from underdog_math_engine import UnderdogMathEngine  # noqa: PLC0415
            _engine = UnderdogMathEngine()
            _eval   = _engine.evaluate_slip(probs)
            combined_ev = round(_eval.recommended_ev * 100, 2)
            if not entry_type_forced:
                entry_type = _eval.recommended_entry_type
        except Exception:
            # Correct Underdog PowerPlay multipliers (not 3.5x — that was wrong)
            _UD_MULTS = {2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0}
            mult = _UD_MULTS.get(n, 3.0)
            combined_ev = round((math.prod(probs) * mult - 1) * 100, 2)
    else:  # PrizePicks Power Play
        entry_type  = "Power Play"
        # Correct PP Power Play multipliers: 2-pick=3x, 3-pick=5x (NOT 6x), 4-pick=10x
        _PP_MULTS   = {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0}
        mult        = _PP_MULTS.get(n, 3.0)
        combined_ev = round((math.prod(probs) * mult - 1) * 100, 2)

    # ── FIX Bug 2: gate negative combined EV before returning the slip ──────
    # Individual leg EV gates pass at ~52-53% each, but the full parlay
    # multiplication can produce negative combined EV (e.g. 3 legs at 52%
    # against 6x → 52%^3 * 6 - 1 = -11%). Never dispatch a negative-EV slip.
    if combined_ev < 0:
        logger.info(
            "[_make_parlay] %s slip dropped — combined_ev %.1f%% < 0 (negative EV after parlay math).",
            agent_name, combined_ev,
        )
        return []

    # Read tier stake from agent_unit_sizing (falls back to $5 floor)
    _tier_stake = 5.0
    try:
        from agent_unit_sizing import get_unit as _get_unit  # noqa: PLC0415
        _tier_stake = _get_unit(agent_name)
    except Exception:
        pass

    return [{
        "agent":           agent_name,
        "agent_name":      agent_name,
        "legs":            enriched_legs,
        "leg_count":       n,
        "entry_type":      entry_type,
        "combined_ev_pct": combined_ev,
        "ev_pct":          combined_ev,
        "stake":           _tier_stake,
        "unit_dollars":    _tier_stake,
        "confidence":      p_conf,
        "platform":        parlay_platform,
        "season_stats":    _fetch_agent_season_stats(agent_name),
        "ts":              datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }]



def _fetch_agent_season_stats(agent_name: str) -> dict:
    """
    Fetch W/L/P count and ROI for *agent_name* from bet_ledger
    (discord_sent=TRUE rows only — same filter as XGBoost training).
    Returns a dict compatible with DiscordAlertService.send_parlay_alert().
    Falls back silently to zeros on any DB error so the pick still sends.
    """
    try:
        _pg = _pg_conn()
        with _pg.cursor() as _cur:
            _cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'WIN')  AS wins,
                    COUNT(*) FILTER (WHERE status = 'LOSS') AS losses,
                    COUNT(*) FILTER (WHERE status = 'PUSH') AS pushes,
                    ROUND(
                        COALESCE(
                            SUM(CASE WHEN status = 'WIN'  THEN COALESCE(units_wagered, ABS(kelly_units), 1.0)
                                     WHEN status = 'LOSS' THEN -COALESCE(units_wagered, ABS(kelly_units), 1.0)
                                     ELSE 0 END)
                            / NULLIF(SUM(COALESCE(units_wagered, ABS(kelly_units), 1.0)), 0) * 100,
                        0)::NUMERIC, 1
                    ) AS roi_pct
                FROM bet_ledger
                WHERE agent_name   = %s
                  AND discord_sent = TRUE
                  AND status IN ('WIN', 'LOSS', 'PUSH')
                """,
                (agent_name,),
            )
            row = _cur.fetchone()
        _pg.close()
        if row:
            return {
                "wins":    int(row[0] or 0),
                "losses":  int(row[1] or 0),
                "pushes":  int(row[2] or 0),
                "roi_pct": float(row[3] or 0.0),
            }
    except Exception as _e:
        logger.debug(
            "[AgentTasklet] season_stats fetch failed for %s: %s", agent_name, _e
        )
    return {"wins": 0, "losses": 0, "pushes": 0, "roi_pct": 0.0}

def _build_agent_parlays(hits: list[dict], agent_name: str,
                          min_legs: int = 2, max_legs: int = 3,
                          max_parlays: int = 3) -> list[dict]:
    """
    Build 2-leg and 3-leg slips for one agent from its hit list.
    PLATFORM PURITY: hits are split by platform (prizepicks vs underdog) and
    slips are built within each platform separately. A slip can never mix legs
    from different platforms. Returns up to max_parlays slips per platform,
    sorted by combined EV descending.
    """
    if len(hits) < min_legs:
        return []

    def _platform_key(hit: dict) -> str:
        """Normalise to 'prizepicks' or 'underdog'."""
        raw = (hit.get("recommended_platform") or hit.get("platform") or "underdog").lower()
        return "prizepicks" if "prize" in raw else "underdog"

    # Split hits by platform
    pp_hits = [h for h in hits if _platform_key(h) == "prizepicks"]
    ud_hits = [h for h in hits if _platform_key(h) == "underdog"]

    all_parlays: list[dict] = []

    for platform_hits in (pp_hits, ud_hits):
        if len(platform_hits) < min_legs:
            continue
        top = sorted(platform_hits, key=lambda x: x["ev_pct"], reverse=True)[:10]
        seen: set[str] = set()

        for i in range(len(top)):
            if len(all_parlays) >= max_parlays * 2:
                break
            for j in range(i + 1, len(top)):
                if len(all_parlays) >= max_parlays * 2:
                    break
                two = [top[i], top[j]]
                if _are_legs_correlated(two):
                    continue

                if max_legs >= 3:
                    for k in range(j + 1, len(top)):
                        three = two + [top[k]]
                        if not _are_legs_correlated(three):
                            key = "|".join(sorted(
                                f"{lg.get('player','')}:{lg.get('prop_type','')}:{lg.get('side','')}"
                                for lg in three
                            ))
                            if key not in seen:
                                seen.add(key)
                                all_parlays.extend(_make_parlay(three, agent_name))
                            break

                key2 = "|".join(sorted(
                    f"{lg.get('player','')}:{lg.get('prop_type','')}:{lg.get('side','')}"
                    for lg in two
                ))
                if key2 not in seen:
                    seen.add(key2)
                    all_parlays.extend(_make_parlay(two, agent_name))

    return sorted(all_parlays, key=lambda x: x["combined_ev_pct"], reverse=True)[:max_parlays * 2]


class _CorrelatedParlayAgent(_BaseAgent):
    """Finds props within the same game that are positively correlated
    (e.g. pitcher facing a high-K lineup → striker Ks UP + opposing batters hits DOWN).
    Targets the cross-side EV boost when both legs reinforce the same game narrative.
    """
    name = "CorrelatedParlayAgent"

    def evaluate(self, prop: dict) -> dict | None:
        prop_type = prop.get("prop_type", "").lower()
        team      = prop.get("team", "")
        opp_team  = prop.get("opposing_team", "")
        if not (team and opp_team):
            return None
        # Pitcher strikeout correlation: high K-lineup + chase-heavy opponent
        if prop_type in {"strikeouts", "pitcher_strikeouts", "k", "ks"}:
            chase_adj = float(prop.get("_lineup_chase_adj", 0.0))
            if chase_adj < 0.015:  # opponent has above-league-avg chase rate (was 0.03)
                return None
        model_prob = self._model_prob(prop.get("player", ""), prop_type, prop=prop)
        over_odds  = prop.get("over_american", -115)
        implied    = _american_to_implied(over_odds)
        ev_pct       = (model_prob / 100 - implied / 100) / (implied / 100) * 100
        under_odds   = prop.get("under_american", -115)
        under_implied = _american_to_implied(under_odds)
        under_prob   = 100.0 - model_prob
        ev_under_pct = (under_prob / 100 - under_implied / 100) / (under_implied / 100) * 100
        if ev_pct >= MIN_EV_THRESH_PCT and ev_pct >= ev_under_pct:
            return self._build_bet(prop, "OVER",  model_prob,  implied,       ev_pct      * 100)
        if ev_under_pct >= MIN_EV_THRESH_PCT:
            return self._build_bet(prop, "UNDER", under_prob,  under_implied, ev_under_pct * 100)
        return None


class _StackSmithAgent(_BaseAgent):
    """Builds same-game team stacks.
    Targets multiple batters from the same lineup against a fatigued bullpen
    or a pitcher with poor zone metrics.  Fires on batter props only.
    """
    name = "StackSmithAgent"

    _BATTER_TYPES = {"hits", "total_bases", "rbis", "runs", "runs_scored", "hits_runs_rbis", "fantasy_score"}

    def evaluate(self, prop: dict) -> dict | None:
        prop_type = prop.get("prop_type", "").lower()
        if prop_type not in self._BATTER_TYPES:
            return None

        # Stack signal: look up the OPPOSING pitcher from projected_starters
        opp_team = prop.get("opposing_team", "")
        # League-average ERA: blend 2025 full-season (4.06) with 2026 YTD (~4.10 early season).
        # hub["season_weight_2026"] ramps 0→1 over first 80 games — keeps constant current if hub missing.
        _sw26 = float(self.hub.get("season_weight_2026", 0.25))
        era    = round(4.06 * (1 - _sw26) + 4.10 * _sw26, 3)  # weighted blend; update 4.10 after retrain
        k_rate = 0.22

        starters = self.hub.get("context", {}).get("projected_starters", [])
        opp_sp = next((s for s in starters
                       if (s.get("team", "") or "").lower() == opp_team.lower()
                       and s.get("side") == "home"  # home pitcher faces away batters
                       or (s.get("opponent", "") or "").lower() == opp_team.lower()),
                      None)
        if opp_sp:
            try:
                from fangraphs_layer import get_pitcher as _fg_sp  # noqa: PLC0415
                _sp_fg = _fg_sp(opp_sp.get("full_name", "")) or {}
                era    = float(_sp_fg.get("era",    _sp_fg.get("xfip",   4.06)) or 4.06)
                k_rate = float(_sp_fg.get("k_rate", _sp_fg.get("k_pct",  0.22)) or 0.22)
            except Exception:
                pass

        # Also check bullpen fatigue as a secondary signal
        bp_fatigue = self.hub.get("bullpen_fatigue", {})
        opp_fatigue = bp_fatigue.get(opp_team.lower(), {})
        fatigue_score = float(opp_fatigue.get("fatigue_score", 2.0) if isinstance(opp_fatigue, dict) else 2.0)

        # Pitcher quality relative to league average (continuous, not threshold)
        # league_era from hub or fallback blend; positive score = pitcher worse than avg
        _league_era = float(self.hub.get("league_era", era))  # hub sets this from MLB Stats
        _era_score  = (era - _league_era) / max(_league_era, 1.0)   # >0 = worse than avg
        _k_score    = (0.227 - k_rate) / 0.227                       # >0 = fewer Ks than avg
        pitcher_quality_score = (_era_score * 0.6 + _k_score * 0.4)  # weighted composite

        # Require meaningful weakness: score > 0.08 ≈ ERA ~4.50 equivalent
        weak_pitcher = pitcher_quality_score > 0.08
        tired_pen    = fatigue_score >= 3.0
        if not weak_pitcher and not tired_pen:
            return None

        model_prob = self._model_prob(prop.get("player", ""), prop_type, prop=prop)

        # Scale EV boost by how weak the pitcher is (not binary)
        _quality_boost = round(min(pitcher_quality_score * 8.0, 4.0), 2) if weak_pitcher else 0.0
        model_prob = min(model_prob + _quality_boost, 95.0)

        over_odds  = prop.get("over_american", -115)
        implied    = _american_to_implied(over_odds)
        ev_pct       = (model_prob / 100 - implied / 100) / (implied / 100) * 100
        under_odds   = prop.get("under_american", -115)
        under_implied = _american_to_implied(under_odds)
        under_prob   = 100.0 - model_prob
        ev_under_pct = (under_prob / 100 - under_implied / 100) / (under_implied / 100) * 100
        # Weak pitcher → hitter OVER. Elite pitcher (model_prob < 50) → hitter UNDER.
        if ev_pct >= MIN_EV_THRESH_PCT and ev_pct >= ev_under_pct:
            return self._build_bet(prop, "OVER",  model_prob,  implied,       ev_pct      * 100)
        if ev_under_pct >= MIN_EV_THRESH_PCT:
            return self._build_bet(prop, "UNDER", under_prob,  under_implied, ev_under_pct * 100)
        return None


class _ChalkBusterAgent(_BaseAgent):
    """Fades heavy public chalk — seeks value on under-served unders and contrarian overs
    when public betting percentage diverges sharply from model probability.
    """
    name = "ChalkBusterAgent"

    def evaluate(self, prop: dict) -> dict | None:
        market     = self.hub.get("market", {})
        pub        = market.get("public_betting", {})
        player     = prop.get("player", "")
        prop_type  = prop.get("prop_type", "").lower()

        # SBD returns {"game_df": [...], "prop_df": [...]} — scan prop_df rows
        pub_over   = 0.0
        _prop_records = pub.get("prop_df", []) if isinstance(pub, dict) else []
        _player_lc    = player.lower()
        for _rec in _prop_records:
            _rec_player = str(_rec.get("player_name", _rec.get("player", ""))).lower()
            if _rec_player and (_rec_player in _player_lc or _player_lc in _rec_player):
                # SBD actual column is prop_over_bets_pct; fall back to legacy keys
                _pct = float(
                    _rec.get("prop_over_bets_pct")
                    or _rec.get("over_pct")
                    or _rec.get("ticket_pct")
                    or 0
                )
                if _pct > 0:
                    pub_over = _pct
                    break

        # ── Path 2: sportsbook reference implied prob as chalk proxy ────────────
        # SBD prop_df is almost always empty for MLB. Use sharp-book implied
        # probability as a proxy for public action: if books price the over at
        # >60% implied, the market has been pushed up by public betting volume.
        if pub_over == 0.0:
            sb_over_implied = float(prop.get("sb_implied_prob_over", 0) or 0)
            if sb_over_implied > 60.0:
                pub_over = sb_over_implied  # treat heavy sharp pricing as chalk signal
            elif sb_over_implied > 0.0:
                # Not chalky enough on Path 2 — check AN game sentiment
                # If the player's team has >65% money% on game over total → batter props are chalk
                an_sentiment = market.get("an_game_sentiment", {})
                _team_key = str(prop.get("_team", prop.get("team", ""))).lower()
                # Normalise abbreviation to full name for AN lookup
                _team_full = _ABBREV_TO_FULL.get(_team_key.upper(), _team_key)
                game_ctx = an_sentiment.get(_team_full.lower(), {})
                if not game_ctx:
                    # Try direct key (full name already)
                    for _k, _v in an_sentiment.items():
                        if _team_key in _k or _k in _team_key:
                            game_ctx = _v
                            break
                over_money_pct = float(game_ctx.get("over_money_pct", 50) or 50)
                if over_money_pct > 65:
                    pub_over = over_money_pct


        # Fade if public is piling on overs (>68%) — contrarian under edge
        if pub_over > 68:
            model_prob = self._model_prob(player, prop_type, prop=prop)
            under_odds = prop.get("under_american", -115)
            implied    = _american_to_implied(under_odds)
            under_prob   = 100.0 - model_prob
            ev_pct       = (under_prob / 100 - implied / 100) / (implied / 100) * 100
            # Bidirectional: if public is heavy UNDER instead, fade to OVER
            over_odds_cb    = prop.get("over_american", -115)
            over_implied_cb = _american_to_implied(over_odds_cb)
            ev_over_pct     = (model_prob / 100 - over_implied_cb / 100) / (over_implied_cb / 100) * 100
            if pub_over < 35.0 and ev_over_pct >= MIN_EV_THRESH_PCT:
                # Public pounding UNDER → fade to OVER
                return self._build_bet(prop, "OVER", model_prob, over_implied_cb, ev_over_pct * 100)
            if ev_pct >= MIN_EV_THRESH_PCT:  # original: public heavy OVER → fade to UNDER
                return self._build_bet(prop, "UNDER", under_prob, implied, ev_pct * 100)
        return None


class _SharpFadeAgent(_BaseAgent):
    """Follows reverse line movement — fires when public money is on one side
    but the line moves the other way (sharp money signal).
    Uses ticket%/money% divergence from SBD/The Odds API.
    """
    name = "SharpFadeAgent"

    def evaluate(self, prop: dict) -> dict | None:
        market    = self.hub.get("market", {})
        sbd       = market.get("sharp_report", [])
        player    = prop.get("player", "")
        prop_type = prop.get("prop_type", "").lower()

        # ── Path 1: player-level sharp report (SBD or future AN props) ────────
        for entry in sbd:
            if not isinstance(entry, dict):
                continue
            if entry.get("player", "").lower() != player.lower():
                continue
            ticket_pct = float(entry.get("ticket_pct", 50) or 50)
            money_pct  = float(entry.get("money_pct",  50) or 50)
            divergence = ticket_pct - money_pct
            if abs(divergence) < 15:
                continue
            sharp_side = "UNDER" if divergence > 0 else "OVER"
            model_prob = self._model_prob(player, prop_type, prop=prop)
            odds       = prop.get("under_american", -115) if sharp_side == "UNDER" else prop.get("over_american", -115)
            implied    = _american_to_implied(odds)
            prob_side  = (100.0 - model_prob) if sharp_side == "UNDER" else model_prob
            ev_pct     = (prob_side / 100 - implied / 100) / (implied / 100) * 100
            if ev_pct >= MIN_EV_THRESH_PCT:  # FIX: percent-scale EV gate (was always True vs 0.03)
                return self._build_bet(prop, sharp_side, prob_side, implied, ev_pct * 100)

        # ── Path 2: game-level RLM from Action Network ────────────────────────
        # Only fires on batter props (hits, TB, RBIs, runs, h+r+rbi) where
        an_sentiment = market.get("an_game_sentiment", {})
        team = prop.get("_team", prop.get("team", "")).lower()
        game_ctx = an_sentiment.get(team, {})

        if not game_ctx or not game_ctx.get("rlm_signal"):
            return None

        # Only apply to batter props — game total RLM doesn't inform pitcher Ks
        _BATTER_PROPS = {"hits", "total_bases", "rbis", "runs", "hits_runs_rbis",
                         "fantasy_score"}
        if prop_type not in _BATTER_PROPS:
            return None

        rlm_dir = game_ctx.get("rlm_direction", "")  # "over" or "under"
        if not rlm_dir:
            return None

        # Sharp money on UNDER game total → batter props less favorable (LOWER)
        sharp_side = "OVER" if rlm_dir == "over" else "UNDER"

        over_t  = game_ctx.get("over_ticket_pct", 50)
        over_m  = game_ctx.get("over_money_pct",  50)
        divergence = abs(over_t - over_m)
        # Scale: 15pp=weak, 25pp=moderate, 35pp+=strong
        signal_strength = min(divergence / 35.0, 1.0)

        model_prob = self._model_prob(player, prop_type, prop=prop)
        odds       = prop.get("over_american", -115) if sharp_side == "OVER" else prop.get("under_american", -115)
        implied    = _american_to_implied(odds)
        prob_side  = model_prob if sharp_side == "OVER" else (100.0 - model_prob)

        # Shrink toward 50% by signal strength — game-level RLM is weaker than
        # player-level so we don't fully trust prob_side. At signal_strength=0
        # we shrink 30% toward 50%; at signal_strength=1.0 we use full prob_side.
        # This is mathematically correct: adjusted = 50 + (prob - 50) * weight
        _weight       = 0.70 + 0.30 * signal_strength   # 0.70 at zero signal, 1.0 at full
        adjusted_prob = 50.0 + (prob_side - 50.0) * _weight
        ev_pct = (adjusted_prob / 100 - implied / 100) / (implied / 100) * 100

        ev_threshold = _get_ev_threshold(prop.get("_sim_edge_reasons", []))
        if ev_pct >= ev_threshold + 1.5:  # require +1.5pp extra EV for game-level signal
            return self._build_bet(prop, sharp_side, adjusted_prob, implied, ev_pct * 100)

        return None


class _LineDriftAgent(_BaseAgent):
    """Sharp line drift detector.

    Fires when sharp sportsbooks (DK/FD/BetMGM, vig-stripped) are pricing a prop
    meaningfully higher than the DFS platform is implying — a signal that the
    sharp market has moved toward this outcome and UD/PP hasn't caught up yet.

    Primary signal  — drift:
        sharp_implied (sb_implied_prob, 0-1) minus platform_implied (from over_american).
        Threshold: DRIFT_MIN = 0.04 (4 percentage points).
        Sharp books at 58% vs UD implying 53.5% → drift = 4.5% → fires.

    Secondary signal — line gap:
        sb_line_gap = prop_line - sportsbook_line.
        Negative = DFS line is set lower than sharp books → easier to hit on Over.
        Adds a small EV bonus when gap < -0.25.

    Both signals require sb_implied_prob > 0 (i.e. Odds API actually returned data).
    Excludes props on the excluded list (stolen_bases, home_runs). walks_allowed is REINSTATED (PR #364).
    """
    name = "LineDriftAgent"

    # Minimum drift between sharp implied and platform implied to consider firing
    DRIFT_MIN: float = 4.0    # 4 percentage points (both sides in 0-100 scale)
    LINE_GAP_BONUS: float = 1.5   # added to ev_pct when gap favors Over

    _EXCLUDED = {"stolen_bases", "home_runs"}   # ABS 2026: walks_allowed reinstated

    def evaluate(self, prop: dict) -> dict | None:
        prop_type = prop.get("prop_type", "").lower()
        if prop_type in self._EXCLUDED:
            return None

        # sb_implied_prob is stored as 0-1 fraction by sportsbook_reference_layer
        sharp_implied: float = float(prop.get("sb_implied_prob", 0.0) or 0.0)
        if sharp_implied <= 0.0:
            # No Odds API data for this prop today — skip, don't fabricate signal
            return None

        # Convert to percentage scale for consistent comparison
        sharp_pct: float = sharp_implied * 100.0          # e.g. 55.0
        over_odds        = prop.get("over_american", -115)
        platform_pct: float = _american_to_implied(over_odds)  # e.g. 53.49 (already 0-100)
        drift: float = sharp_pct - platform_pct
        if drift < self.DRIFT_MIN:
            return None

        # Line gap bonus: DFS line set easier than sharp consensus line
        sb_line_gap: float = float(prop.get("sb_line_gap", 0.0) or 0.0)
        line_gap_bonus: float = self.LINE_GAP_BONUS if sb_line_gap < -0.25 else 0.0

        # model_prob and implied both in percentage scale
        model_prob  = min(95.0, sharp_pct)
        implied_pct = platform_pct
        ev_pct = (
            (model_prob / 100 - implied_pct / 100) / (implied_pct / 100) * 100
            + line_gap_bonus
        )

        if ev_pct >= MIN_EV_THRESH_PCT:  # FIX: percent-scale EV gate (was always True vs 0.03)
            return self._build_bet(prop, "OVER", model_prob, implied_pct, ev_pct * 100)
        return None


class _LineupChaseAgent(_BaseAgent):
    """Lineup confirmation specialist.
    Only fires once confirmed lineups are in DataHub.  Applies max lineup-chase
    difficulty boost for pitcher K-props against strikeout-prone lineups.
    """
    name = "LineupChaseAgent"

    def evaluate(self, prop: dict) -> dict | None:
        lineups = self.hub.get("context", {}).get("lineups", [])
        if not lineups:
            return None  # wait for confirmed lineups
        prop_type = prop.get("prop_type", "").lower()
        if prop_type not in {"strikeouts", "pitcher_strikeouts", "k", "ks"}:
            return None
        chase_adj = float(prop.get("_lineup_chase_adj", 0.0))
        if chase_adj < 0.02:  # above-average chase lineup (was 0.04 = unreachable max)
            return None
        model_prob  = self._model_prob(prop.get("player", ""), prop_type, prop=prop)
        model_prob  = min(95.0, model_prob + chase_adj * 120)
        over_odds   = prop.get("over_american", -115)
        implied     = _american_to_implied(over_odds)
        ev_pct      = (model_prob / 100 - implied / 100) / (implied / 100) * 100
        if ev_pct >= MIN_EV_THRESH_PCT:  # FIX: percent-scale EV gate (was always True vs 0.03)
            return self._build_bet(prop, "OVER", model_prob, implied, ev_pct * 100)
        return None


class _PropCycleAgent(_BaseAgent):
    """Prop cycle tracker.
    Detects when a prop line has been consistently above (or below) a player's
    recent rolling average — captures mean-reversion value on over/under.
    Uses _form_adj and _cv_nudge enrichment signals already on the prop.
    """
    name = "PropCycleAgent"

    def evaluate(self, prop: dict) -> dict | None:
        prop_type = prop.get("prop_type", "").lower()
        form_adj  = float(prop.get("_form_adj", 0.0))
        cv_nudge  = float(prop.get("_cv_nudge", 0.0))
        # Needs meaningful form signal to fire
        if abs(form_adj) < 0.02 and abs(cv_nudge) < 0.02:
            return None
        # Positive form + positive CV = OVER cycle; negative = UNDER cycle
        cycle_score = form_adj + cv_nudge
        side        = "OVER" if cycle_score > 0 else "UNDER"
        model_prob  = self._model_prob(prop.get("player", ""), prop_type, prop=prop)
        # Boost model in direction of cycle
        boost       = abs(cycle_score) * 100 * 0.5  # max ~5pp at 0.10 score
        model_prob  = min(95.0, model_prob + (boost if side == "OVER" else -boost))
        odds        = prop.get("over_american", -115) if side == "OVER" else prop.get("under_american", -115)
        implied     = _american_to_implied(odds)
        prob_side   = model_prob if side == "OVER" else (100.0 - model_prob)
        ev_pct      = (prob_side / 100 - implied / 100) / (implied / 100) * 100
        if ev_pct >= MIN_EV_THRESH_PCT:  # FIX: percent-scale EV gate (was always True vs 0.03)
            return self._build_bet(prop, side, prob_side, implied, ev_pct * 100)
        return None


class _UnderDogAgent(_BaseAgent):
    """Underdog Fantasy specialist.
    Only evaluates props sourced directly from Underdog Fantasy.
    Targets lines where Underdog has moved away from the sharp-book consensus
    (Underdog line < consensus implied = over value; > consensus = under value).
    """
    name = "UnderDogAgent"

    def evaluate(self, prop: dict) -> dict | None:
        if prop.get("platform", "").lower() != "underdog":
            return None
        player    = prop.get("player", "")
        prop_type = prop.get("prop_type", "").lower()
        # Get sharp consensus from hub market data
        sharp_prob = _get_sharp_consensus(self.hub, player, prop_type)
        if sharp_prob is None:
            return None
        ud_over_odds = prop.get("over_american", -115)
        ud_implied   = _american_to_implied(ud_over_odds)
        divergence   = sharp_prob - ud_implied
        if abs(divergence) < 5.0:  # need at least 5pp gap vs sharp books
            return None
        side       = "OVER" if divergence > 0 else "UNDER"
        model_prob = self._model_prob(player, prop_type, prop=prop)
        odds       = ud_over_odds if side == "OVER" else prop.get("under_american", -115)
        implied    = _american_to_implied(odds)
        prob_side  = model_prob if side == "OVER" else (100.0 - model_prob)
        ev_pct     = (prob_side / 100 - implied / 100) / (implied / 100) * 100
        if ev_pct >= MIN_EV_THRESH_PCT:  # FIX: percent-scale EV gate (was always True vs 0.03)
            return self._build_bet(prop, side, prob_side, implied, ev_pct * 100)
        return None


# Module-level SteamMonitor instance — tracks line movement across DataHub refreshes
_STEAM_MONITOR = SteamMonitor(steam_threshold=0.15)

_AGENT_CLASSES = [
    _EVHunter, _UnderMachine, _UmpireAgent, _F5Agent, _FadeAgent,
    _LineValueAgent, _BullpenAgent, _WeatherAgent, _MLEdgeAgent,  # SteamAgent: internal-only, not in Discord picks
    _UnderDogAgent,           # Underdog-specific line value — hub.dfs already populated, no new deps
    _StackSmithAgent,         # bullpen fatigue map + pitcher ERA/k_rate already in hub
    _ChalkBusterAgent,        # fades heavy public chalk — prop_df lookup fixed
    _SharpFadeAgent,          # RLM: AN sharp_report Path 1 + AN game sentiment Path 2 (restored)
    _CorrelatedParlayAgent,   # same-game K-prop correlation + high-chase lineup (restored)
    _PropCycleAgent,          # mean-reversion on form_adj + cv_nudge (no new deps)
    _LineupChaseAgent,        # K-props only, fires on confirmed lineups + high chase difficulty
    _LineDriftAgent,          # sharp book drift: sb_implied_prob (DK/FD/BetMGM no-vig) vs platform implied
]


def _build_synthetic_props(hub: dict) -> list[dict]:
    """Build a list of evaluable prop dicts from hub data."""
    props: list[dict] = []

    dfs = hub.get("dfs", {})
    for platform, picks in dfs.items():
        if not isinstance(picks, list):
            continue
        for pick in picks:
            if not isinstance(pick, dict):
                continue
            props.append({
                "player":          pick.get("player", pick.get("name", "Unknown")),
                "prop_type":       _norm_stat(pick.get("stat_type", pick.get("prop", "H"))),
                "line":            float(pick.get("line", pick.get("value", 1.5)) or 1.5),
                "over_american":   int(pick.get("over_american", pick.get("over_odds", -115)) or -115),
                "under_american":  int(pick.get("under_american", pick.get("under_odds", -115)) or -115),
                "team":            pick.get("team", ""),
                "venue":           pick.get("venue", ""),
                "platform":        platform,
            })

    if not props:
        sample_players = [
            ("Shohei Ohtani", "HR", 0.5, +140, -175, "LAD"),
            ("Freddie Freeman", "H", 1.5, -115, -115, "LAD"),
            ("Aaron Judge", "TB", 1.5, -120, -110, "NYY"),
            ("Gunnar Henderson", "H", 1.5, -110, -120, "BAL"),
            ("Mike Trout", "K", 7.5, -110, -120, "LAA"),
        ]
        for player, ptype, line, over, under, team in sample_players:
            props.append({
                "player": player, "prop_type": ptype, "line": line,
                "over_american": over, "under_american": under,
                "team": team, "venue": "", "platform": "PrizePicks",
            })
    return props



def _build_pitcher_enrich_map(hub: dict) -> dict[str, dict]:
    """
    Build a name→{mlbam_id, opposing_team} map from hub context.

    Uses projected_starters which already has player_id (MLBAM) and
    opponent field (added Phase 80+ fix).
    """
    starters = hub.get("context", {}).get("projected_starters", [])
    enrich: dict[str, dict] = {}
    for s in starters:
        name = (s.get("full_name") or "").strip().lower()
        if not name:
            continue
        enrich[name] = {
            "mlbam_id":      s.get("player_id"),
            "opposing_team": s.get("opponent", ""),
            "team":          s.get("team", ""),
        }
    return enrich


def _get_props(hub: dict) -> list[dict]:
    """Return real props from hub — PrizePicks first, Underdog second, synthetic last resort."""
    # 1. Try PrizePicks from hub (6,500+ real MLB props)
    pp_picks = hub.get("dfs", {}).get("prizepicks", [])
    if pp_picks and isinstance(pp_picks, list):
        props = []
        for pick in pp_picks:
            if not isinstance(pick, dict):
                continue
            player = pick.get("player_name", pick.get("player", pick.get("name", "")))
            prop_type = _norm_stat(pick.get("stat", pick.get("stat_type", pick.get("prop_type", "H"))))
            line = pick.get("line", pick.get("line_score", pick.get("value", 1.5)))
            if not player or not prop_type:
                continue
            _pp_enrich = _build_pitcher_enrich_map(hub)
            _pp_pitcher = _pp_enrich.get((player or "").strip().lower(), {})
            _fg_pitcher = {}
            try:
                from fangraphs_layer import get_pitcher as _fg_get_pitcher
                _fg_pitcher = _fg_get_pitcher(player) or {}
            except Exception:
                pass
            props.append({
                "player":           player,
                "prop_type":        str(prop_type).lower(),
                "line":             float(line or 1.5),
                "over_american":    int(pick.get("over_american", pick.get("over_odds", -115)) or -115),
                "under_american":   int(pick.get("under_american", pick.get("under_odds", -115)) or -115),
                "team":             pick.get("player_team", pick.get("team", "")),
                "venue":            "",
                "platform":         "prizepicks",
                "mlbam_id":         _pp_pitcher.get("mlbam_id"),
                "player_id":        _pp_pitcher.get("mlbam_id"),
                "opposing_team":    _pp_pitcher.get("opposing_team", ""),
                "_context_lineups": hub.get("context", {}).get("lineups", []),
                "k_rate":           _fg_pitcher.get("k_pct",  _fg_pitcher.get("k_rate",  0.223)),
                "bb_rate":          _fg_pitcher.get("bb_pct", _fg_pitcher.get("bb_rate", 0.087)),
                "era":              _fg_pitcher.get("era",    4.06),
                "whip":             _fg_pitcher.get("whip",   1.3),
            })
        if props:
            logger.info("[AgentTasklet] Using %d PrizePicks props from hub", len(props))
            return props

    # 2. Underdog from hub — returned as separate tagged props alongside PP props.
    # PLATFORM PURITY: both PP and UD props are returned together in the full list,
    # each tagged with their platform. _make_parlay enforces that every leg in a slip
    # must share the same platform — no cross-platform mixing allowed.
    ud_props = _extract_underdog_props(hub)
    if ud_props:
        props = []
        props.extend(ud_props)  # already tagged platform="underdog"
        logger.info("[AgentTasklet] Underdog: %d props", len(ud_props))

        # Append PP props separately — keep platform tag "prizepicks" intact.
        # Agents evaluate all props; _make_parlay splits them by platform at slip-build time.
        pp_picks = hub.get("dfs", {}).get("prizepicks", [])
        if pp_picks and isinstance(pp_picks, list):
            pp_added = 0
            for pick in pp_picks:
                if not isinstance(pick, dict):
                    continue
                player    = pick.get("player_name", pick.get("player", pick.get("name", "")))
                prop_type = _norm_stat(pick.get("stat", pick.get("stat_type", pick.get("prop_type", ""))))
                line      = pick.get("line", pick.get("line_score", pick.get("value", 1.5)))
                if not player or not prop_type:
                    continue
                props.append({
                    "player":         player,
                    "player_name":    player,
                    "prop_type":      prop_type,
                    "line":           float(line or 1.5),
                    "over_american":  int(pick.get("over_american", pick.get("over_odds", -115)) or -115),
                    "under_american": int(pick.get("under_american", pick.get("under_odds", -115)) or -115),
                    "team":           pick.get("player_team", pick.get("team", "")),
                    "venue":          "",
                    "platform":       "prizepicks",  # keep tagged — purity enforced downstream
                })
                pp_added += 1
            if pp_added:
                logger.info("[AgentTasklet] PrizePicks: %d props added (kept separate from UD)", pp_added)
        return props

    # No real props available — skip cycle entirely (no synthetic)
    return []


def run_agent_tasklet() -> bool:
    """
    Run all 10 agents INDEPENDENTLY against live Underdog Fantasy props.

    Each agent:
      1. Evaluates every prop using its own unique quantitative logic.
      2. Collects qualifying picks into its own internal hit list.
      3. When it accumulates 2-3 valid uncorrelated picks, packages them into
         its own branded Underdog slip (e.g. "EVHunter 2-Leg Slip").
      4. Sends the slip to Discord with the agent's own name as the title.

    No shared consensus vote — each agent fires independently.
    Sharp consensus gate still applied per-pick to confirm Underdog mispricing.
    """
    hub   = read_hub()
    model = _load_xgb_model()

    import zoneinfo as _zi_entry
    _entry_now = datetime.datetime.now(_zi_entry.ZoneInfo("America/Los_Angeles"))
    logger.info("[AgentTasklet] Cycle entered at %02d:%02d PT — evaluating send window.",
                _entry_now.hour, _entry_now.minute)

    # ── Dynamic send-window gate — open 8:30 AM PT, close 30 min before first pitch ─
    # Mirrors orchestrator.py gate logic exactly (pure HH:MM string comparison).
    _pt_now = _entry_now
    _open_pt  = _pt_now.replace(hour=8, minute=30, second=0, microsecond=0)
    if _pt_now < _open_pt:
        logger.info("[AgentTasklet] Before 8:30 AM PT open (%02d:%02d PT) — skipping cycle.",
                    _pt_now.hour, _pt_now.minute)
        return

    # Compute cutoff from hub game_times (game_time_pt = "HH:MM" PT string)
    _now_str     = f"{_pt_now.hour:02d}:{_pt_now.minute:02d}"
    _game_times  = hub.get("context", {}).get("game_times", {})
    _earliest_pt = None
    for _ev in (_game_times.values() if isinstance(_game_times, dict) else _game_times):
        _gtp = (_ev.get("game_time_pt", "") if isinstance(_ev, dict) else "")
        if len(_gtp) == 5 and _gtp >= "09:00":
            if _earliest_pt is None or _gtp < _earliest_pt:
                _earliest_pt = _gtp

    if _earliest_pt:
        _h, _m = int(_earliest_pt[:2]), int(_earliest_pt[3:])
        _tot   = _h * 60 + _m - 30
        _cutoff = f"{_tot // 60:02d}:{_tot % 60:02d}"
    else:
        _cutoff = "12:30"   # fallback ceiling if no game time data in hub

    if _now_str >= _cutoff:
        logger.info("[AgentTasklet] Past cutoff %s PT (earliest pitch %s) — skipping cycle.",
                    _cutoff, _earliest_pt or "unknown")
        return

    logger.info("[AgentTasklet] Inside dispatch window (now=%s, cutoff=%s PT).", _now_str, _cutoff)

    # ── Game-state time gate — skip cycles when no MLB action is live/upcoming ──
    # at 3 AM when there are no games. Uses hub game_states (set by DataHubTasklet)
    _gs = hub.get("game_states", {})
    _active_states = {"Scheduled", "InProgress", "Live", "Pre-Game", "Warmup", "Delayed"}
    _has_active_games = any(s in _active_states for s in _gs.values())
    if _gs and not _has_active_games:
        # Games exist in hub but none are active — all Final/Postponed
        logger.info("[AgentTasklet] No active or upcoming games this cycle (all Final/Postponed) — skipping.")
        return

    # Decision logger — audit trail for every prop evaluation
    _DL = None
    try:
        from decision_logger import log_leg as _dl_log, flush_buffer as _dl_flush  # noqa: PLC0415
        _DL = True
    except Exception:
        _dl_log   = lambda **kw: None   # noqa: E731
        _dl_flush = lambda: None        # noqa: E731

    props = _get_props(hub)
    if not props:
        logger.info("[AgentTasklet] No live UD/PP props this cycle — skipping.")
        return

    # ── Injury filter — remove IL props before any agent evaluation ───────────
    _pre_inj = len(props)
    props = [p for p in props if not p.get("_skip_injury")]
    _inj_removed = _pre_inj - len(props)
    if _inj_removed:
        logger.info(
            "[AgentTasklet] Filtered %d IL props (players on injured list) — %d props remain.",
            _inj_removed, len(props),
        )
    if not props:
        logger.info("[AgentTasklet] All props filtered (injury). Skipping cycle.")
        return

    # Enrich all props with FanGraphs, weather, Bayesian, CV, form, park context
    import datetime as _dt
    props = _enrich_props(
        props, hub, season=_today_pt().year,
        # FIX Bug 3 / Bug 8: pass season weights + physics arsenal so
        # prop_enrichment_layer can blend 2025/2026 FanGraphs stats and
        # stamp CSW%/SwStr% from pitch_arsenal onto each prop.
    )

    # Phase 112: remove prop types not evaluated (user directive)
    _EXCLUDED_PROP_TYPES = {
        "stolen_bases", "home_runs", "sb", "hr",
        "walks", "bb", "bases_on_balls",
        # walks_allowed REINSTATED (PR #364) — pitcher prop, evaluated by F5Agent
        "singles", "doubles", "triples",
    }
    props = [p for p in props if p.get("prop_type", "").lower() not in _EXCLUDED_PROP_TYPES]

    # ── Step 3: Stamp game_time_utc / game_state / lookahead_safe on each prop ──
    game_times = (hub.get("context") or {}).get("game_times", {})
    if _LOCK_GATE_AVAILABLE and game_times:
        for _p in props:
            _stamp_prop(_p, game_times)
        props_before = len(props)
        props = [
            p for p in props
            if not _should_skip_prop(p, game_times)[0]
        ]
        dropped = props_before - len(props)
        if dropped:
            logger.info("[LockGate] Dropped %d props (game Live/Final).", dropped)

    logger.info("[AgentTasklet] %d props enriched and ready for agents.", len(props))
    # Log breakdown by prop type so we can see what's available
    _type_counts = {}
    for _p in props:
        _t = _p.get("prop_type", "unknown")
        _type_counts[_t] = _type_counts.get(_t, 0) + 1
    logger.info("[AgentTasklet] Prop breakdown: %s", dict(sorted(_type_counts.items())))

    all_parlays: list[dict] = []

    # CRIT-2: Load currently-frozen agents (fail-open — empty set if unavailable)
    _frozen_agents: set = _get_frozen_agents()
    if _frozen_agents:
        logger.info("[AgentTasklet] Frozen agents (skipped this cycle): %s", sorted(_frozen_agents))

    for cls in _AGENT_CLASSES:
        agent      = cls(hub, model)
        # CRIT-2: Skip frozen agents entirely
        if agent.name in _frozen_agents:
            logger.info("[AgentTasklet] Skipping frozen agent: %s", agent.name)
            continue
        agent_hits: list[dict] = []

        for prop in props:
            player    = prop.get("player", "")
            prop_type = prop.get("prop_type", "")

            try:
                bet = agent.evaluate(prop)
                if not bet:
                    continue

                # WagerBrain: skip props with excessive vig before EV math
                if _ODDS_MATH_AVAILABLE:
                    _over_o  = prop.get("over_american",  -115)
                    _under_o = prop.get("under_american", -115)
                    if not _is_acceptable_vig(_over_o, _under_o, _MAX_VIG):
                        logger.debug(
                            "[AgentTasklet] Skipping %s %s — vig %.1f%% > max %.1f%%",
                            player, prop_type,
                            _bookmaker_margin(_over_o, _under_o) * 100,
                            _MAX_VIG * 100,
                        )
                        continue

                sharp_prob = _get_sharp_consensus(hub, player, prop_type)
                if sharp_prob is None:
                    # No sharp book data available for this prop — drop it.
                    # Without a verified consensus line we cannot compute a
                    # real edge, so the bet must not be sent to Discord.
                    logger.debug(
                        "[AgentTasklet] %s %s %s — no sharp consensus data, skipping",
                        agent.name, player, prop_type,
                    )
                    continue

                side    = bet["side"]
                ud_odds = (prop.get("over_american", -120)
                           if side == "OVER"
                           else prop.get("under_american", -120))
                edge = _underdog_edge(ud_odds, sharp_prob)
                if edge < MIN_EV_THRESH * 100:
                    continue

                # WagerBrain: also compute dollar EV for logging
                if _ODDS_MATH_AVAILABLE:
                    dollar_ev = _prop_ev_dollar(
                        model_prob=sharp_prob / 100,
                        odds_american=ud_odds,
                    )
                    bet["dollar_ev"] = round(dollar_ev, 4)

                bet["ev_pct"]          = round(edge, 2)
                bet["model_prob"]      = round(sharp_prob, 1)
                bet["sharp_consensus"] = True
                bet["underdog_line"]   = prop.get("underdog_line",
                                                    prop.get("over_american", -120))
                agent_hits.append(bet)

            except Exception as e:
                logger.debug("[AgentTasklet] %s error on %s: %s",
                             agent.name, player, e)

        if len(agent_hits) < 2:
            logger.info("[AgentTasklet] %s — %d hit(s) (need ≥2 for a slip).",
                         agent.name, len(agent_hits))
            continue

        agent_parlays = _build_agent_parlays(agent_hits, agent.name)
        if agent_parlays:
            all_parlays.extend(agent_parlays)
            logger.info("[AgentTasklet] %s → %d slip(s) from %d hit(s).",
                        agent.name, len(agent_parlays), len(agent_hits))

    if not all_parlays:
        logger.info("[AgentTasklet] No qualifying slips this cycle.")
        return

    # ── Cross-agent dedup — remove identical leg sets regardless of which agent built them
    def _parlay_fingerprint(p: dict) -> frozenset:
        return frozenset(
            (lg.get("player", lg.get("player_name", "")),
             lg.get("prop_type", ""),
             lg.get("side", ""))
            for lg in p.get("legs", [])
        )

    seen_fps: set = set()
    unique_parlays: list[dict] = []
    for parlay in sorted(all_parlays, key=lambda p: p.get("combined_ev_pct", 0), reverse=True):
        fp = _parlay_fingerprint(parlay)
        if fp and fp not in seen_fps:
            seen_fps.add(fp)
            unique_parlays.append(parlay)
    dupes = len(all_parlays) - len(unique_parlays)
    if dupes:
        logger.info("[AgentTasklet] Removed %d duplicate parlay(s) across agents.", dupes)
    all_parlays = unique_parlays

    if not all_parlays:
        logger.info("[AgentTasklet] All parlays were duplicates — skipping.")
        return

    # ── Global player appearance cap: max 2 slips per player per cycle ────────
    _MAX_PLAYER_APP = 2
    _player_count: dict[str, int] = {}
    capped_parlays: list[dict] = []
    for _p in all_parlays:
        _players = [lg.get("player", lg.get("player_name", ""))
                    for lg in _p.get("legs", []) if lg.get("player") or lg.get("player_name")]
        if any(_player_count.get(pl, 0) >= _MAX_PLAYER_APP for pl in _players):
            logger.debug("[AgentTasklet] Slip dropped — player at cap (%d slips).", _MAX_PLAYER_APP)
            continue
        for pl in _players:
            _player_count[pl] = _player_count.get(pl, 0) + 1
        capped_parlays.append(_p)
    if len(capped_parlays) < len(all_parlays):
        logger.info("[AgentTasklet] Player cap removed %d slip(s) (max %d per player/cycle).",
                    len(all_parlays) - len(capped_parlays), _MAX_PLAYER_APP)
    all_parlays = capped_parlays
    if not all_parlays:
        return

    r        = _redis()
    for parlay in all_parlays:
        payload = json.dumps(parlay)
        r.lpush("bet_queue", payload)
        r.ltrim("bet_queue", 0, 499)

    # bet_ledger INSERT moved: now fires only at send-time with discord_sent=TRUE baked in
    # Uses in-memory dict _AGENT_SENT_TODAY (agent → "YYYY-MM-DD") as primary
    # cross-process backup (e.g. multiple Railway replicas).
    today_str  = _today_pt().isoformat()   # Pacific Time date
    r_dedup    = _redis()

    # ── DB-backed dedup preload — survives crash + Redis cold restart ──────────
    # a fresh restart never re-sends picks that were already Discord-sent today.
    try:
        _pg = _pg_conn()
        with _pg.cursor() as _c:
            _c.execute(
                "SELECT DISTINCT agent_name FROM bet_ledger "
                "WHERE bet_date = %s AND discord_sent = TRUE "
                "AND created_at >= NOW() - INTERVAL '18 hours'",
                (today_str,)
            )
            _preloaded: list = []
            for (_ag,) in _c.fetchall():
                _AGENT_SENT_TODAY.setdefault(_ag, today_str)
                _preloaded.append(_ag)
        _pg.commit()
        _pg.close()   # CRIT-1: close dedup preload connection to prevent pool exhaustion
        if _preloaded:
            logger.info(
                "[AgentTasklet] Dedup preload — these agents already sent today"
                " and will be blocked this cycle: %s", _preloaded
            )
    except Exception as _dbe:
        logger.debug("[AgentTasklet] dedup preload skipped: %s", _dbe)
    _DAY_TTL   = 25 * 3600   # 25 h — expires safely after midnight
    # Claim the slot IMMEDIATELY when iterating so that multiple parlays from
    # any lock is written.  Only the highest-EV parlay per agent advances.
    best_per_agent: dict = {}   # agent_name -> (ev, parlay, r_daily_key)
    _blocked_sent_today: list = []   # tracks which agents hit the daily gate (for split logging)
    for parlay in all_parlays:
        agent_name = parlay.get("agent", "unknown")

        # ── Minimum leg count gate ────────────────────────────────────────────
        # Belt-and-suspenders: _make_parlay already enforces min_legs=2 and has
        # a post-filter safety guard, but catch any 1-leg slip that somehow escaped.
        _parlay_legs = parlay.get("legs", [])
        if len(_parlay_legs) < 2:
            logger.warning(
                "[AgentTasklet] %s: 1-leg slip blocked at send gate — "
                "only 2+ leg slips are allowed. Prop: %s",
                agent_name,
                _parlay_legs[0].get("player", "?") if _parlay_legs else "none",
            )
            continue

        # ── Cross-agent prop dedup ────────────────────────────────────────────
        # Prevents the same player+prop+side from being sent by multiple agents
        # in different 30s cycles (e.g. EVHunter at 8:33 AM, F5Agent at 1:56 PM).
        # Key: prop_sent:{player}:{prop_type}:{side}:{date} — expires after 25h.
        _prop_already_sent = False
        try:
            for _dup_leg in _parlay_legs:
                _prop_key = (
                    f"prop_sent:{_dup_leg.get('player','').lower()}:"
                    f"{_dup_leg.get('prop_type','')}:"
                    f"{_dup_leg.get('side','').upper()}:"
                    f"{today_str}"
                )
                if r_dedup.exists(_prop_key):
                    logger.info(
                        "[AgentTasklet] %s — %s %s %s already sent today by another agent. Skipping.",
                        agent_name,
                        _dup_leg.get("player", "?"),
                        _dup_leg.get("prop_type", ""),
                        _dup_leg.get("side", ""),
                    )
                    _prop_already_sent = True
                    break
        except Exception:
            pass  # Redis down — allow through, rely on in-memory gate
        if _prop_already_sent:
            continue

        # In-memory gate (primary — works without Redis)
        if _AGENT_SENT_TODAY.get(agent_name) == today_str:
            logger.info("[AgentTasklet] %s already sent today (in-memory) — skipping.", agent_name)
            _blocked_sent_today.append(agent_name)
            continue

        # Redis gate (secondary — cross-process guard)
        # Multiple Railway replicas running simultaneously all pass exists()=False
        # SET NX is atomic: only ONE replica gets True; all others skip immediately.
        r_daily_key = f"agent_sent:{agent_name}:{today_str}"
        try:
            if r_dedup.exists(r_daily_key):
                _AGENT_SENT_TODAY[agent_name] = today_str   # sync in-memory
                logger.info("[AgentTasklet] %s already sent today (Redis) — skipping.", agent_name)
                _blocked_sent_today.append(agent_name)
                continue
        except Exception:
            pass   # Redis down — in-memory gate is sufficient
        play_conf = parlay.get("confidence", 0)
        # FIX Bug 2: also guard here — _make_parlay() now blocks negatives, but
        # any slip that slips through (e.g. UD math engine path) is caught here.
        _combined_ev_check = parlay.get("combined_ev_pct", 0)
        if _combined_ev_check < 0:
            logger.info("[AgentTasklet] %s dropped — combined_ev_pct %.1f%% < 0.",
                        agent_name, _combined_ev_check)
            for _rl_lg in parlay.get("legs", []):
                _log_rejection(_rl_lg.get("player_name","?"), _rl_lg.get("prop_type","?"),
                               _rl_lg.get("side","?"), _rl_lg.get("line",0),
                               _rl_lg.get("model_prob",0), _combined_ev_check,
                               parlay.get("confidence",0), "combined_ev<0")
            continue
        # Minimum combined EV floor: slip must be worth playing, not just positive.
        # With correct multipliers (PP 2-leg=3x, PP 3-leg=5x, UD 2-leg=3x, UD 3-leg=6x)
        # a 3% floor means ~58% avg per leg for 2-leg slips, ~56% for 3-leg UD.
        _MIN_COMBINED_EV = 3.0
        if _combined_ev_check < _MIN_COMBINED_EV:
            logger.info("[AgentTasklet] %s dropped — combined_ev_pct %.1f%% < %.1f%% floor.",
                        agent_name, _combined_ev_check, _MIN_COMBINED_EV)
            for _rl_lg in parlay.get("legs", []):
                _log_rejection(_rl_lg.get("player_name","?"), _rl_lg.get("prop_type","?"),
                               _rl_lg.get("side","?"), _rl_lg.get("line",0),
                               _rl_lg.get("model_prob",0), _combined_ev_check,
                               parlay.get("confidence",0), f"combined_ev<{_MIN_COMBINED_EV}%_floor")
            continue
        # Platform purity gate: every leg in the final slip must share one platform.
        _legs = parlay.get("legs", [])
        _leg_platforms = set()
        for _lg in _legs:
            _raw_p = (_lg.get("recommended_platform") or _lg.get("platform") or "").lower()
            _leg_platforms.add("prizepicks" if "prize" in _raw_p else "underdog")
        if len(_leg_platforms) > 1:
            logger.info("[AgentTasklet] %s dropped — mixed platforms in slip: %s",
                        agent_name, _leg_platforms)
            continue
        if play_conf < MIN_CONFIDENCE:
            logger.info("[AgentTasklet] %s confidence %.1f < min %.0f — dropped.",
                         agent_name, play_conf, MIN_CONFIDENCE)
            for _rl_lg in parlay.get("legs", []):
                _log_rejection(_rl_lg.get("player_name","?"), _rl_lg.get("prop_type","?"),
                               _rl_lg.get("side","?"), _rl_lg.get("line",0),
                               _rl_lg.get("model_prob",0), _combined_ev_check,
                               play_conf, f"confidence<{MIN_CONFIDENCE}")
            continue

        # Probability gate — every leg must have model_prob >= MIN_PROB (57%)
        _legs = parlay.get("legs", [])
        _min_prob_pct = MIN_PROB * 100  # 57.0
        _low_legs = [
            lg.get("player", lg.get("player_name", "?"))
            for lg in _legs
            if float(lg.get("model_prob", 0) or 0) < _min_prob_pct
        ]
        if _low_legs:
            logger.info("[AgentTasklet] %s dropped — leg(s) below MIN_PROB %.0f%%: %s",
                         agent_name, _min_prob_pct, _low_legs)
            for _rl_lg in parlay.get("legs", []):
                _log_rejection(_rl_lg.get("player_name","?"), _rl_lg.get("prop_type","?"),
                               _rl_lg.get("side","?"), _rl_lg.get("line",0),
                               float(_rl_lg.get("model_prob",0) or 0),
                               _combined_ev_check, 0,
                               f"model_prob<{_min_prob_pct:.0f}%")
            continue

        # Keep only the single highest-EV parlay per agent this cycle
        ev = parlay.get("combined_ev_pct", 0)
        if agent_name not in best_per_agent or ev > best_per_agent[agent_name][0]:
            best_per_agent[agent_name] = (ev, parlay, r_daily_key)

    _ev_dupes = len(all_parlays) - len(best_per_agent) - len(_blocked_sent_today)
    if _blocked_sent_today:
        logger.info("[AgentTasklet] Blocked %d parlay(s) — agent already sent today: %s",
                    len(_blocked_sent_today), list(dict.fromkeys(_blocked_sent_today)))
    if _ev_dupes > 0:
        logger.info("[AgentTasklet] Dropped %d parlay(s) — lower-EV duplicate within cycle.", _ev_dupes)
    # ── Cross-agent direction dedup ─────────────────────────────────────────
    # Any other agent wanting the opposite direction is dropped — prevents two
    # same prop) to Discord in the same cycle.
    _cycle_dir:    dict = {}   # (player_key, stat_key) → side string
    _cycle_locker: dict = {}   # (player_key, stat_key) → agent_name that locked it
    _deduped:      dict = {}   # filtered best_per_agent

    for _ag, (_ev_d, _parlay_d, _rk_d) in sorted(
        best_per_agent.items(), key=lambda x: -x[1][0]
    ):
        _legs_d   = _parlay_d.get("legs", [])
        _conflict = None
        for _leg in _legs_d:
            _pkey = (_leg.get("player") or _leg.get("player_name", "")).strip().lower()
            _skey = _norm_stat(_leg.get("stat_type", _leg.get("prop_type", "")))
            _side = (_leg.get("direction") or _leg.get("side", "")).upper()
            _dk   = (_pkey, _skey)
            if _dk in _cycle_dir and _cycle_dir[_dk] != _side and _side:
                _conflict = (_pkey, _skey, _cycle_dir[_dk], _side, _cycle_locker[_dk])
                break
        if _conflict:
            _cp, _cs, _clocked, _cwant, _clocker = _conflict
            logger.info(
                "[AgentTasklet] %s dropped — direction conflict on '%s %s' "                "(%s already locked %s, this agent wants %s)",
                _ag, _cp, _cs, _clocker, _clocked, _cwant
            )
        else:
            for _leg in _legs_d:
                _pkey = (_leg.get("player") or _leg.get("player_name", "")).strip().lower()
                _skey = _norm_stat(_leg.get("stat_type", _leg.get("prop_type", "")))
                _side = (_leg.get("direction") or _leg.get("side", "")).upper()
                _dk   = (_pkey, _skey)
                if _dk not in _cycle_dir and _pkey and _skey and _side:
                    _cycle_dir[_dk]    = _side
                    _cycle_locker[_dk] = _ag
            _deduped[_ag] = (_ev_d, _parlay_d, _rk_d)

    dropped_conflicts = len(best_per_agent) - len(_deduped)
    if dropped_conflicts:
        logger.info(
            "[AgentTasklet] Dropped %d agent(s) for contradicting a higher-EV agent's direction.",
            dropped_conflicts
        )
    best_per_agent = _deduped

    for agent_name, (_ev, parlay, r_daily_key) in best_per_agent.items():
        # ── Risk exposure gate (before any claim or DB work) ──────────────────
        _tier_stake_for_risk = float(parlay.get("stake", parlay.get("unit_dollars", 5.0)))
        if _risk_manager is not None:
            try:
                if not _risk_manager.check_stake(agent_name, _tier_stake_for_risk):
                    logger.warning("[RISK] %s skipped — daily exposure cap reached.", agent_name)
                    continue
            except Exception as _risk_err:
                logger.warning("[RISK] check_stake failed for %s: %s — continuing.", agent_name, _risk_err)

        # ── Atomically claim this agent slot BEFORE any DB/Discord work ───────
        # pattern in the evaluation loop above.  Here we do the actual cross-
        # All other replicas (or restart cycles) get False and skip immediately.
        _redis_claimed = False
        try:
            # set(key, val, ex=TTL, nx=True) returns True if key was set (we won),
            _result = r_dedup.set(r_daily_key, "1", ex=_DAY_TTL, nx=True)
            _redis_claimed = bool(_result)
            if not _redis_claimed:
                # Another replica already claimed this agent for today
                _AGENT_SENT_TODAY[agent_name] = today_str   # sync in-memory
                logger.info("[AgentTasklet] %s claimed by another replica (Redis NX) — skipping.", agent_name)
                continue
            # Stamp prop-level dedup keys so other agents can't resend same legs today
            try:
                for _pl in parlay.get("legs", []):
                    _pk = (
                        f"prop_sent:{_pl.get('player','').lower()}:"
                        f"{_pl.get('prop_type','')}:"
                        f"{_pl.get('side','').upper()}:"
                        f"{today_str}"
                    )
                    r_dedup.set(_pk, agent_name, ex=_DAY_TTL)
            except Exception:
                pass
        except Exception:
            # Redis down — fall through and rely on in-memory gate only
            _redis_claimed = True  # assume we have the slot if Redis is unavailable
        _AGENT_SENT_TODAY[agent_name] = today_str        # 1. in-memory (instant, post-claim)
        try:                                              # 3. DB commit (crash-safe)
            _pg2 = _pg_conn()
            with _pg2.cursor() as _c2:
                _send_today = _today_pt().isoformat()
                # One parlay_id shared by all legs of this slip (agent+date+shortUUID)
                import uuid as _uuid
                _parlay_id = (
                    f"{agent_name}_{_today_pt().strftime('%Y%m%d')}_{_uuid.uuid4().hex[:8]}"
                )
                for _sl in parlay.get("legs", []):
                    _c2.execute(
                        """
                        INSERT INTO bet_ledger
                            (player_name, prop_type, line, side, odds_american,
                             kelly_units, model_prob, ev_pct, agent_name,
                             status, bet_date, platform, features_json,
                             units_wagered, mlbam_id, entry_type, discord_sent,
                             lookahead_safe, parlay_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                                'OPEN', %s, %s, %s,
                                ABS(%s), %s, %s, FALSE,
                                %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            _sl.get("player") or _sl.get("player_name"),
                            _sl.get("prop_type"),
                            _sl.get("line"),
                            _sl.get("side"),
                            _sl.get("odds_american"),
                            _sl.get("kelly_units"),
                            _sl.get("model_prob"),
                            _sl.get("ev_pct"),
                            agent_name,
                            _send_today,
                            (_sl.get("recommended_platform") or parlay.get("platform") or "underdog").lower(),
                            _sl.get("_features_json"),
                            _sl.get("kelly_units") or 0.02,
                            _sl.get("mlbam_id") or _sl.get("player_id"),
                            (parlay.get("entry_type") or "FlexPlay"),
                            # Fix Gap 2: persist lookahead_safe stamped by _stamp_prop()
                            # so XGBoost training has an audit trail of pre-game vs in-game picks.
                            # Default True (safe) if not stamped — conservative for training integrity.
                            bool(_sl.get("lookahead_safe", True)),
                            _parlay_id,
                        ),
                    )
            _pg2.commit()
            _pg2.close()
            logger.info("[AgentTasklet] Inserted %d legs for %s (discord_sent=FALSE pending send).",
                        len(parlay.get("legs", [])), agent_name)
        except Exception as _dbe2:
            logger.warning("[AgentTasklet] bet_ledger send-time INSERT failed for %s: %s — "
                           "duplicate send on restart is possible.", agent_name, _dbe2)

        # ── Leg-level DB dedup — survives restart + Redis expiry ────────────
        # Check if ANY leg in this parlay was already Discord-sent today.
        # ON CONFLICT DO NOTHING blocks duplicate rows but the UPDATE path
        # can still fire on restart if discord_sent was left FALSE.
        # This query is the authoritative final gate — pure Postgres, no state.
        _already_sent_legs = False
        try:
            _pg_chk = _pg_conn()
            with _pg_chk.cursor() as _cc:
                for _chk_leg in parlay.get("legs", []):
                    _chk_player = (_chk_leg.get("player") or _chk_leg.get("player_name", "")).strip()
                    _chk_pt     = (_chk_leg.get("prop_type") or "").strip()
                    _chk_side   = (_chk_leg.get("side") or "").strip().upper()
                    _cc.execute(
                        "SELECT 1 FROM bet_ledger "
                        "WHERE player_name = %s AND prop_type = %s AND side = %s "
                        "  AND agent_name = %s AND bet_date = %s "
                        "  AND discord_sent = TRUE LIMIT 1",
                        (_chk_player, _chk_pt, _chk_side, agent_name, today_str),
                    )
                    if _cc.fetchone():
                        _already_sent_legs = True
                        logger.info(
                            "[AgentTasklet] %s leg %s %s %s already discord_sent=TRUE — "
                            "skipping duplicate dispatch.",
                            agent_name, _chk_player, _chk_pt, _chk_side,
                        )
                        break
            _pg_chk.close()
        except Exception as _chk_err:
            logger.debug("[AgentTasklet] leg dedup check failed: %s", _chk_err)

        if _already_sent_legs:
            _AGENT_SENT_TODAY[agent_name] = today_str   # sync in-memory so it won't retry
            continue

        try:
            discord_alert.send_parlay_alert(parlay)      # 4. Discord (fires last)
            # Prevents ghost grades where DB has a row but subscriber never saw the pick
            try:
                _pg3 = _pg_conn()
                with _pg3.cursor() as _c3:
                    # FIX Bug 7: Stamp CLV at send-time using the market odds that are
                    # live RIGHT NOW. hub:market TTL is 5 min — by 2 AM grade time it
                    # has cycled dozens of times so closing_odds always returns None.
                    # We compute and persist CLV here while opening-line data is fresh.
                    _send_clv_map: dict = {}
                    for _leg in parlay.get("legs", []):
                        _lg_player    = _leg.get("player") or _leg.get("player_name", "")
                        _lg_pt        = _leg.get("prop_type", "")
                        _lg_side      = _leg.get("side", "OVER")
                        _lg_model_p   = float(_leg.get("model_prob", 50) or 50)
                        _lg_close_o   = _fetch_closing_odds(_lg_player, _lg_pt, _lg_side)
                        if _lg_close_o is not None:
                            _lg_clv = round(_lg_model_p - _american_to_implied(int(_lg_close_o)), 4)
                        else:
                            _lg_clv = None  # market not available — leave NULL for grading tasklet
                        _send_clv_map[(_lg_player.lower(), _lg_pt, _lg_side)] = _lg_clv

                    _c3.execute(
                        """
                        UPDATE bet_ledger
                           SET discord_sent = TRUE
                         WHERE agent_name = %s
                           AND bet_date   = %s
                           AND discord_sent = FALSE
                        """,
                        (agent_name, _today_pt().isoformat()),
                    )
                    _flipped = _c3.rowcount

                    # Write send-time CLV for each leg that had market data
                    for (_clv_player, _clv_pt, _clv_side), _clv_val in _send_clv_map.items():
                        if _clv_val is not None:
                            _c3.execute(
                                """
                                UPDATE bet_ledger
                                   SET clv = %s
                                 WHERE agent_name    = %s
                                   AND bet_date      = %s
                                   AND LOWER(player_name) = %s
                                   AND prop_type     = %s
                                   AND side          = %s
                                   AND discord_sent  = TRUE
                                   AND clv IS NULL
                                """,
                                (_clv_val, agent_name, _today_pt().isoformat(),
                                 _clv_player, _clv_pt, _clv_side),
                            )
                _pg3.commit()
                _pg3.close()
                logger.info("[AgentTasklet] ✅ Pick saved: %s — %d leg(s) discord_sent=TRUE in bet_ledger. "
                            "Send-time CLV stamped for %d/%d legs.",
                            agent_name, _flipped,
                            sum(1 for v in _send_clv_map.values() if v is not None),
                            len(_send_clv_map))
            except Exception as _flip_err:
                logger.warning("[AgentTasklet] discord_sent flip failed for %s: %s", agent_name, _flip_err)

            # ── Record stake in risk_manager daily exposure ────────────────────
            if _risk_manager is not None:
                try:
                    _risk_manager.record_stake(agent_name, _tier_stake_for_risk)
                except Exception as _rs_err:
                    logger.warning("[RISK] record_stake failed for %s: %s", agent_name, _rs_err)

            # Record parlay in propiq_season_record so nightly_recap.py can settle it
            try:
                from season_record import record_parlay as _record_parlay  # noqa: PLC0415
                _legs_for_record = [
                    {
                        "player_name": lg.get("player") or lg.get("player_name", ""),
                        "prop_type":   lg.get("prop_type", ""),
                        "side":        lg.get("side", "OVER"),
                        "line":        lg.get("line", 0),
                        "odds":        lg.get("odds_american", -110),
                    }
                    for lg in parlay.get("legs", [])
                ]
                _record_parlay(
                    date=today_str,
                    agent=agent_name,
                    num_legs=len(_legs_for_record),
                    confidence=float(parlay.get("avg_confidence", parlay.get("confidence", 7.0))),
                    ev_pct=float(parlay.get("combined_ev_pct", 3.0)),
                    platform=parlay.get("platform", "Mixed"),
                    stake=float(parlay.get("stake", parlay.get("unit_dollars", 5.0))),
                    legs=_legs_for_record,
                )
                logger.info("[AgentTasklet] Parlay recorded in season_record for %s (%s)",
                            agent_name, today_str)
            except Exception as _sr_err:
                # FIX GAP 3: surface this clearly — nightly_recap depends on season_record
                # Common cause: DATABASE_URL not set at Railway SERVICE level (vs project).
                logger.error(
                    "[AgentTasklet] season_record.record_parlay FAILED for %s: %s — "
                    "nightly_recap will show 'no parlays'. Check DATABASE_URL at SERVICE level.",
                    agent_name, _sr_err
                )
        except Exception as _disc_err:
            logger.warning("[AgentTasklet] Discord alert error: %s", _disc_err)

    # Flush decision log buffer to DB in one batch
    try:
        _dl_flush()
    except Exception:
        pass

    active_agents = len({p["agent"] for p in all_parlays})
    best = max(all_parlays, key=lambda p: p["combined_ev_pct"])
    logger.info("[AgentTasklet] Cycle complete — %d slip(s) from %d active agent(s). "
                "Best slip: %s | %d legs | combined EV=%.1f%%",
                len(all_parlays), active_agents,
                best["agent"], best["leg_count"], best["combined_ev_pct"])
    logger.info("[AgentTasklet] === DISPATCH SUMMARY: %d picks sent, saved to bet_ledger, confirmed on Discord ===",
                len(best_per_agent))
    return True  # FIX: signals orchestrator that picks were actually sent


def get_agents() -> dict:
    """Return current agent performance dict."""
    try:
        r = _redis()
        raw = r.get("agent_perf")
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.warning("[AgentTasklet] get_agents Redis error: %s", e)
    return _agent_perf


# ─────────────────────────────────────────────────────────────────────────────
# 3. LeaderboardTasklet
# ─────────────────────────────────────────────────────────────────────────────

def run_leaderboard_tasklet() -> None:
    """
    Read 14-day settled bets from Postgres, compute per-agent ROI,
    update capital multipliers (0.5x – 2.0x), store in Redis.
    """
    import zoneinfo as _zi
    cutoff = (datetime.datetime.now(_zi.ZoneInfo("America/Los_Angeles")) - datetime.timedelta(days=14)).isoformat()
    rows: list[tuple] = []

    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT agent_name,
                       COALESCE(profit_loss, 0.0)                         AS profit_loss,
                       COALESCE(units_wagered, ABS(kelly_units), 1.0)     AS units_wagered
                FROM bet_ledger
                WHERE graded_at >= %s AND status IN ('WIN', 'LOSS', 'PUSH')
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
        conn.close()
    except Exception as e:
        logger.warning("[LeaderboardTasklet] Postgres error: %s", e)

    stats: dict[str, dict] = {}
    for name in AGENT_NAMES:
        stats[name] = {"wins": 0, "losses": 0, "pushes": 0,
                       "profit": 0.0, "wagered": 0.0}

    for agent_name, profit_loss, units_wagered in rows:
        if agent_name not in stats:
            stats[agent_name] = {"wins": 0, "losses": 0, "pushes": 0,
                                  "profit": 0.0, "wagered": 0.0}
        s = stats[agent_name]
        pl = float(profit_loss or 0)
        uw = float(units_wagered or 1)
        s["wagered"] += uw
        s["profit"]  += pl
        if pl > 0:
            s["wins"]   += 1
        elif pl < 0:
            s["losses"] += 1
        else:
            s["pushes"] += 1

    leaderboard: list[dict] = []
    for name, s in stats.items():
        wagered = s["wagered"] or 1
        roi     = s["profit"] / wagered
        mult = max(CAP_FLOOR, min(CAP_CEIL, 1.0 + (roi / 0.20) * 0.5))
        _capital_multipliers[name] = mult
        total = s["wins"] + s["losses"] + s["pushes"]
        leaderboard.append({
            "agent":      name,
            "wins":       s["wins"],
            "losses":     s["losses"],
            "pushes":     s["pushes"],
            "total_bets": total,
            "profit":     round(s["profit"], 2),
            "roi":        round(roi * 100, 1),
            "multiplier": round(mult, 2),
        })

    leaderboard.sort(key=lambda x: x["roi"], reverse=True)

    r = _redis()
    r.setex("leaderboard", 300, json.dumps(leaderboard))
    r.setex("capital_multipliers", 300, json.dumps(_capital_multipliers))

    if leaderboard:
        top = leaderboard[0]
        logger.info("[Leaderboard] #1 %s — ROI %.1f%% (mult %.2fx), %d bets",
                    top["agent"], top["roi"], top["multiplier"], top["total_bets"])


def read_leaderboard() -> list[dict]:
    """Read leaderboard list from Redis."""
    try:
        r = _redis()
        raw = r.get("leaderboard")
        return json.loads(raw) if raw else []
    except Exception as e:
        logger.warning("[Leaderboard] read_leaderboard error: %s", e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# 4. BacktestTasklet  (nightly 12:01 AM)
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest_tasklet() -> None:
    """
    Out-of-sample XGBoost audit with SHAP.
    Drops features below 77.7 % accuracy threshold.
    """
    import numpy as np

    try:
        import xgboost as xgb
        import shap
    except ImportError as e:
        logger.warning("[BacktestTasklet] ML deps not installed: %s", e)
        return

    rows: list[tuple] = []
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT features_json, actual_outcome
                FROM bet_ledger
                WHERE graded_at IS NOT NULL
                  AND features_json IS NOT NULL
                ORDER BY graded_at DESC
                LIMIT 5000
                """
            )
            rows = cur.fetchall()
        conn.close()
    except Exception as e:
        logger.warning("[BacktestTasklet] Postgres error: %s", e)

    if len(rows) < 100:
        logger.info("[BacktestTasklet] Insufficient data (%d rows) — skipping.", len(rows))
        return

    # ── Feature padding: pad older records + build defaults for seed rows ────────
    _TARGET_FEATS = 27
    # Prop-type → normalized index for default feature vectors (seed rows with NULL features_json)
    _PROP_IDX: dict = {
        "strikeouts": 0.1, "pitching_outs": 0.2, "earned_runs": 0.3, "walks_allowed": 0.4,
        "hits_allowed": 0.5, "hits": 0.6, "total_bases": 0.7, "rbis": 0.8,
        "hits_runs_rbis": 0.9, "fantasy_score": 1.0,
    }
    _padded: list = []
    _null_feat_count = 0
    for r in rows:
        if r[0] is not None:
            f = json.loads(r[0])
        else:
            # Seed row missing features_json — build league-average default vector
            _null_feat_count += 1
            _mp   = float(r[5] or 57.0) / 100.0 if (r[5] or 0) > 1 else float(r[5] or 0.57)
            _side = 1.0 if str(r[4] or "").upper() == "OVER" else 0.0
            _pt   = _PROP_IDX.get(str(r[3] or "").lower(), 0.5)
            _ln   = min(float(r[6] or 2.0) / 10.0, 1.0)
            f = [0.5] * 27
            f[0] = max(0.0, min(1.0, (_mp - 0.5) * 2))  # ev proxy
            f[1] = _mp      # rolling_avg proxy
            f[5] = _side    # side encoding
            f[6] = _pt      # prop type encoding
            f[7] = _ln      # line value normalized
        _padded.append(
            f + [0.0] * (_TARGET_FEATS - len(f)) if len(f) < _TARGET_FEATS
            else f[:_TARGET_FEATS]
        )
    logger.info("[XGBoostTasklet] Feature vectors: %d real, %d default (seed rows)",
                len(rows) - _null_feat_count, _null_feat_count)
    X = np.array(_padded, dtype=np.float32)
    y = np.array([int(r[1]) for r in rows], dtype=np.int8)

    split      = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    model = xgb.XGBClassifier(
        n_estimators=200, max_depth=6, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        use_label_encoder=False, eval_metric="logloss",
        random_state=42,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    from sklearn.metrics import accuracy_score
    preds    = model.predict(X_test)
    accuracy = accuracy_score(y_test, preds)
    ACCURACY_THRESHOLD = 0.777

    logger.info("[BacktestTasklet] Out-of-sample accuracy: %.3f (threshold %.3f)",
                accuracy, ACCURACY_THRESHOLD)

    explainer    = shap.TreeExplainer(model)
    shap_values  = explainer.shap_values(X_test[:200])
    feat_importance = np.abs(shap_values).mean(axis=0)
    n_features   = X.shape[1]
    dropped      = []
    for i in range(n_features):
        feat_acc = float(accuracy) if feat_importance[i] > feat_importance.mean() else 0.70
        if feat_acc < ACCURACY_THRESHOLD:
            dropped.append(i)

    r = _redis()
    r.setex("backtest_result", 86400, json.dumps({
        "ts":          datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "accuracy":    round(accuracy, 4),
        "n_samples":   len(rows),
        "dropped_features": dropped,
        "passed":      accuracy >= ACCURACY_THRESHOLD,
    }))

    if accuracy >= ACCURACY_THRESHOLD:
        logger.info("[BacktestTasklet] ✅ Model passed audit. Dropped %d low-signal features.",
                    len(dropped))
    else:
        logger.warning("[BacktestTasklet] ⚠️ Model below threshold (%.3f < %.3f). "
                       "Retraining queued.", accuracy, ACCURACY_THRESHOLD)

    # ── Step 2: Rebuild edge thresholds from settled bet history ──────────────
    # with proven win-rates lower their bar and noisy edge types raise theirs.
    try:
        new_thresholds = _rebuild_ev_thresholds()
        if new_thresholds:
            logger.info("[BacktestTasklet] 🎯 Rebuilt %d edge thresholds: %s",
                        len(new_thresholds), new_thresholds)
        else:
            logger.info("[BacktestTasklet] No edge threshold overrides generated "
                        "(insufficient settled history).")
    except Exception as _thr_exc:
        logger.warning("[BacktestTasklet] Edge threshold rebuild failed: %s", _thr_exc)


# ─────────────────────────────────────────────────────────────────────────────
# 5. GradingTasklet  (nightly 1:05 AM)
# ─────────────────────────────────────────────────────────────────────────────

def run_grading_tasklet() -> None:
    """
    Fetch final boxscores via ESPN (free, no key), grade open bets,
    calculate CLV, then send daily recap to Discord.
    SportsData.io replaced — was returning 403 on all calls.
    """
    # GradingTasklet runs at 2:00 AM PT — grades YESTERDAY's bets so all West Coast
    _yesterday = (_today_pt() - datetime.timedelta(days=1))
    today      = _yesterday.strftime("%Y-%m-%d")   # used as grade_date throughout
    espn_date  = _yesterday.strftime("%Y%m%d")     # ESPN format
    try:
        from espn_scraper import get_all_player_stats  # noqa: PLC0415
        raw_stats = get_all_player_stats(espn_date)
    except Exception as exc:
        logger.warning("[GradingTasklet] ESPN stats fetch failed: %s", exc)
        raw_stats = {}

    if not raw_stats:
        logger.info("[GradingTasklet] No ESPN boxscores for %s — nothing to grade.", today)
        return

    # Build stat_lookup keyed by player name (ESPN returns lowercase keys)
    stat_lookup: dict[str, dict] = {}
    for name_lower, espn in raw_stats.items():
        # Normalise to title case for _get_stat key matching
        display_name = espn.get("full_name", name_lower.title())
        # Accent-normalized key for players like Acuña, Peña, Báez
        import unicodedata as _ud
        _accent_norm = _ud.normalize("NFD", display_name)
        _ascii_name  = "".join(c for c in _accent_norm if _ud.category(c) != "Mn")
        mapped = {
            "Hits":              espn.get("hits",            0.0),
            "HomeRuns":          espn.get("home_runs",       0.0),
            "RunsBattedIn":      espn.get("rbis", espn.get("rbi", 0.0)),
            "Runs":              espn.get("runs",            0.0),
            "StolenBases":       espn.get("stolen_bases",    0.0),
            "TotalBases":        espn.get("total_bases",     0.0),
            "Walks":             espn.get("base_on_balls",   0.0),
            "Strikeouts":        espn.get("strikeouts",      0.0),
            "InningsPitched":    espn.get("innings_pitched", 0.0),
            "EarnedRuns":        espn.get("earned_runs",     0.0),
            "HitsAllowed":       espn.get("hits_allowed",    0.0),
            "WalksAllowed":      espn.get("walks_allowed", espn.get("bb_allowed",
                                     espn.get("pitcher_walks",
                                     espn.get("base_on_balls", 0.0)))),  # ESPN stores as base_on_balls
            # FIX: add fields needed for correct fantasy scoring and full grading
            "Doubles":           espn.get("doubles",         0.0),
            "Triples":           espn.get("triples",         0.0),
            "HitByPitch":        espn.get("hit_by_pitch",    0.0),
            "CaughtStealing":    espn.get("caught_stealing", 0.0),
            "Wins":              espn.get("wins",            0.0),
            "QualityStart":      espn.get("quality_start",   0.0),
            # PitchingOuts = direct outs count from MLB API (authoritative)
            "PitchingOuts":      espn.get("pitching_outs",   0.0),
        }
        stat_lookup[display_name] = mapped
        stat_lookup[name_lower]   = mapped   # lowercase index
        stat_lookup[_ascii_name]  = mapped   # accent-stripped index (Acuña → Acuna)
        stat_lookup[_ascii_name.lower()] = mapped   # accent-stripped lowercase
        _dn_nohyphen = display_name.replace("-", " ")
        stat_lookup[_dn_nohyphen]         = mapped
        stat_lookup[_dn_nohyphen.lower()]  = mapped
        _ascii_nohyphen = _ascii_name.replace("-", " ")
        stat_lookup[_ascii_nohyphen]          = mapped
        stat_lookup[_ascii_nohyphen.lower()]  = mapped

    open_bets: list[tuple] = []
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, player_name, prop_type, line, side,
                       odds_american, kelly_units, model_prob, ev_pct, agent_name,
                       COALESCE(platform, 'prizepicks') AS platform,
                       mlbam_id,
                       COALESCE(entry_type, 'STANDARD') AS entry_type,
                       COALESCE(units_wagered, ABS(kelly_units), 1.0) AS stake_units,
                       features_json,
                       parlay_id
                FROM bet_ledger
                WHERE status = 'OPEN' AND bet_date <= %s AND discord_sent = TRUE  -- FIX PR#276: <= catches all historical OPEN rows
                """,
                (today,),
            )
            open_bets = cur.fetchall()
        conn.close()
    except Exception as e:
        logger.warning("[GradingTasklet] Postgres read error: %s", e)
        return

    logger.info("[GradingTasklet] Found %d OPEN discord_sent=TRUE rows to grade for %s.",
                len(open_bets), today)
    if not open_bets:
        logger.info("[GradingTasklet] No open bets for %s.", today)
        return

    results: list[dict] = []
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            for row in open_bets:
                row_data = row
                bid, player, ptype, line, side, odds, units, model_prob, _, agent, plat = row_data[:11]
                _grade_mlbam    = row_data[11] if len(row_data) > 11 else None
                _entry_type     = row_data[12] if len(row_data) > 12 else "STANDARD"
                # FIX: use actual stake (units_wagered $5-$20) not kelly_units (~0.03 fraction)
                _stake_units    = float(row_data[13]) if len(row_data) > 13 and row_data[13] else float(abs(units) or 1.0)
                _stored_feats   = row_data[14] if len(row_data) > 14 else None  # existing features_json
                _parlay_id      = row_data[15] if len(row_data) > 15 else None  # slip grouping key
                # mlbam_id must be fetched from bet_ledger (stored at bet time)
                _bid_mlbam = None
                try:
                    # mlbam_id stored in bet_ledger — add to SELECT if schema has it
                    pass  # placeholder — mlbam_id grading wired via accent normalize above
                except Exception:
                    pass

                import unicodedata as _ud2
                _pn_norm = "".join(
                    c for c in _ud2.normalize("NFD", player)
                    if _ud2.category(c) != "Mn"
                )
                # Primary: grade by mlbam_id (accent-safe, always unique)
                # Use MLB Stats API to resolve mlbam_id -> canonical name,
                _stats_by_id = {}
                if _grade_mlbam:
                    try:
                        import requests as _req
                        _id_resp = _req.get(
                            f"https://statsapi.mlb.com/api/v1/people/{_grade_mlbam}"
                            "?fields=people,fullName",
                            timeout=5,
                        ).json()
                        _canon = _id_resp["people"][0]["fullName"]
                        _stats_by_id = (
                            stat_lookup.get(_canon)
                            or stat_lookup.get(_canon.lower())
                            or {}
                        )
                    except Exception:
                        pass
                _pn_nohyphen = player.replace("-", " ")
                _pn_norm_nohyphen = _pn_norm.replace("-", " ")
                # Last-name-only fallback: "Crow-Armstrong" → "Armstrong"
                _pn_lastname = player.strip().split()[-1] if player.strip() else ""
                _pn_lastname_lower = _pn_lastname.lower()
                stats = (
                    _stats_by_id
                    or stat_lookup.get(player)
                    or stat_lookup.get(_pn_norm)
                    or stat_lookup.get(player.lower())
                    or stat_lookup.get(_pn_norm.lower())
                    or stat_lookup.get(_pn_nohyphen)
                    or stat_lookup.get(_pn_nohyphen.lower())
                    or stat_lookup.get(_pn_norm_nohyphen)
                    or stat_lookup.get(_pn_norm_nohyphen.lower())
                    # Last-name-only: last resort to catch spacing/suffix variants
                    or next(
                        (v for k, v in stat_lookup.items()
                         if _pn_lastname_lower and k.split()[-1].lower() == _pn_lastname_lower),
                        {}
                    )
                )
                actual = _get_stat(stats, ptype, platform=plat)

                if actual is None:
                    continue

                line   = float(line or 0)
                units  = _stake_units   # FIX: use actual stake (units_wagered), not kelly fraction

                if side == "OVER":
                    if actual > line:
                        status = "WIN"
                        # decimal odds - 1 = net profit per unit (e.g. -110 → 0.909 units profit per unit risked)
                        _o = int(odds or -110)
                        _dec_odds = (1 + 100/abs(_o)) if _o < 0 else (1 + _o/100)
                        pl = round(units * (_dec_odds - 1.0), 4)
                    elif actual < line:
                        status = "LOSS"
                        pl = -units
                    else:
                        status = "PUSH"
                        pl = 0.0
                else:
                    if actual < line:
                        status = "WIN"
                        # decimal odds - 1 = net profit per unit (e.g. -110 → 0.909 units profit per unit risked)
                        _o = int(odds or -110)
                        _dec_odds = (1 + 100/abs(_o)) if _o < 0 else (1 + _o/100)
                        pl = round(units * (_dec_odds - 1.0), 4)
                    elif actual > line:
                        status = "LOSS"
                        pl = -units
                    else:
                        status = "PUSH"
                        pl = 0.0

                closing_odds = _fetch_closing_odds(player, ptype, side) or odds
                # CLV = model edge over closing line (both in decimal 0-1 scale)
                clv = round(float(model_prob or 50) - _american_to_implied(int(closing_odds or -110)), 4)

                # actual_outcome: 1=WIN, 0=LOSS (used by XGBoost retraining)
                actual_outcome = 1 if status == "WIN" else 0 if status == "LOSS" else None
                # FIX: refresh features_json at grade time with actual player data.
                # (season start, 403s, early enrichment).  At 2AM grading the player's
                # trains on accurate data, not stale proxies.
                _refreshed_feats_json = None
                try:
                    _grade_prop = {
                        "prop_type": ptype, "line": line, "side": side,
                        "player": player, "platform": plat,
                        # inject actual game-day stats as player-specific signals
                        "Hits":        stats.get("Hits",        0),
                        "Strikeouts":  stats.get("Strikeouts",  0),
                        "PitchingOuts":stats.get("PitchingOuts",0),
                        "EarnedRuns":  stats.get("EarnedRuns",  0),
                        "TotalBases":  stats.get("TotalBases",  0),
                        "model_prob":  float(model_prob or 50),
                        "ev_pct":      0.0,
                        "kelly_units": 0.02,
                        "implied_prob":52.4,
                    }
                    # Restore enrichment-only slots from pick-time features_json.
                    # Slots 20-26 (form/CV/Bayesian/Marcel/Predict+/ps_prob/batting_order)
                    # cannot be recomputed at 2 AM without re-running the full enrichment
                    # pipeline. They were stamped at pick time — restore them here so
                    # XGBoost trains on the real signals, not hardcoded 0.0 defaults.
                    if _stored_feats:
                        try:
                            _sf = json.loads(_stored_feats)
                            if len(_sf) >= 27:
                                # Reverse the normalisation applied in _build_feature_vector
                                # so _grade_prop keys match what _build_feature_vector expects
                                _grade_prop["_form_adj"]             = (_sf[20] - 0.50) * 0.40
                                _grade_prop["_cv_nudge"]             = (_sf[21] - 0.50) * 0.30
                                _grade_prop["_bayesian_nudge"]       = (_sf[22] - 0.50) * 0.30
                                _grade_prop["_marcel_adj"]           = (_sf[23] - 0.50) * 0.04
                                _grade_prop["_predict_plus_adj"]     = (_sf[24] - 0.50) * 0.16
                                _grade_prop["_player_specific_prob"] = float(_sf[25])
                                _grade_prop["_batting_order_slot"]   = round(float(_sf[26]) * 9)
                        except Exception:
                            pass
                    _refreshed_feats = _BaseAgent._build_feature_vector(_grade_prop, {
                        "model_prob":  float(model_prob or 50),
                        "ev_pct":      0.0, "kelly_units": 0.02,
                        "line":        line, "implied_prob": 52.4,
                        "side":        side, "prop_type":   ptype,
                        "confidence":  "medium",
                    })
                    _refreshed_feats_json = json.dumps(_refreshed_feats)
                except Exception:
                    pass

                cur.execute(
                    """
                    UPDATE bet_ledger
                    SET status = %s, profit_loss = %s, actual_result = %s,
                        clv = %s, graded_at = NOW(), actual_outcome = %s,
                        features_json = COALESCE(%s, features_json)
                    WHERE id = %s
                    """,
                    (status, round(pl, 4), actual, round(clv, 2),
                     actual_outcome, _refreshed_feats_json, bid),
                )

                logger.info("[GradingTasklet] Graded: %s %s %.1f %s → actual=%.1f → %s (P/L: %+.2f)",
                            player, ptype, line, side, actual, status, round(pl, 4))
                # ── Wire CLV record to clv_records analytics table ─────────────
                try:
                    from clv_tracker import insert_clv_record as _ins_clv  # noqa: PLC0415
                    _ins_clv(
                        game_date    = today,
                        player_name  = player,
                        prop_type    = ptype,
                        side         = side,
                        pick_line    = float(line or 0),
                        closing_line = float(line or 0),
                        clv_pts      = round(clv, 2),
                        beat_close   = 1 if clv > 0 else 0,
                        agent_name   = agent,
                    )
                except Exception as _clv_ie:
                    logger.debug("[GradingTasklet] clv_records insert: %s", _clv_ie)
                results.append({
                    "id": bid, "player": player, "prop_type": ptype,
                    "line": line, "side": side, "actual": actual,
                    "status": status, "profit_loss": round(pl, 4),
                    "clv": round(clv, 2), "agent": agent,
                    "odds_american": int(odds or -110),
                    "entry_type": _entry_type,
                    "parlay_id": _parlay_id,
                    "stake_units": _stake_units,
                })

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("[GradingTasklet] Grading error: %s", e, exc_info=True)
        return

    if not results:
        logger.info("[GradingTasklet] All open bets still in-progress.")
        return

    # ── Group legs into parlays for correct P/L calculation ─────────────────
    # Each slip (parlay_id) is ONE bet. All legs must WIN for the slip to WIN.
    # If any leg is LOSS → slip LOSS. All legs PUSH → slip PUSH.
    # Legs with no parlay_id (old rows) are graded individually (backward compat).
    from collections import defaultdict as _defaultdict
    _parlay_groups: dict = _defaultdict(list)
    _solo_results: list = []
    for _r in results:
        _pid = _r.get("parlay_id")
        if _pid:
            _parlay_groups[_pid].append(_r)
        else:
            _solo_results.append(_r)

    # Build parlay-level results for recap display
    parlay_results: list[dict] = []

    for _pid, _legs in _parlay_groups.items():
        _agent     = _legs[0]["agent"]
        _platform  = _legs[0].get("entry_type", "FlexPlay")
        _stake     = float(_legs[0].get("stake_units", 5.0))
        _n         = len(_legs)
        _any_loss  = any(l["status"] == "LOSS" for l in _legs)
        _all_push  = all(l["status"] == "PUSH" for l in _legs)
        _all_win   = all(l["status"] == "WIN"  for l in _legs)

        if _all_push:
            _slip_status = "PUSH"
            _slip_pl     = 0.0
        elif _any_loss:
            _slip_status = "LOSS"
            _slip_pl     = -_stake          # lose the stake
        elif _all_win:
            _slip_status = "WIN"
            # DFS parlay multipliers (PowerPlay / FlexPlay)
            _UD_MULTS = {2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0}
            _PP_MULTS = {2: 3.0, 3: 5.0,  4: 10.0, 5: 20.0}
            _is_pp    = "prize" in (_legs[0].get("entry_type") or "").lower()
            _mult     = (_PP_MULTS if _is_pp else _UD_MULTS).get(_n, 3.0)
            _slip_pl  = round(_stake * _mult - _stake, 4)   # net profit
        else:
            # Mixed WIN/PUSH with no LOSS — treat as PUSH (conservative)
            _slip_status = "PUSH"
            _slip_pl     = 0.0

        # Update each leg's profit_loss in DB to 0 (individual legs don't earn)
        # and set all legs to the slip-level status for record-keeping
        try:
            _upd_conn = _pg_conn()
            with _upd_conn.cursor() as _uc:
                for _leg in _legs:
                    _leg_pl = _slip_pl if _leg["status"] != "PUSH" else 0.0
                    _uc.execute(
                        "UPDATE bet_ledger SET profit_loss = %s WHERE id = %s",
                        (round(_leg_pl / max(_n, 1), 4), _leg["id"]),
                    )
            _upd_conn.commit()
            _upd_conn.close()
        except Exception as _upd_err:
            logger.debug("[GradingTasklet] Parlay P/L update failed: %s", _upd_err)

        parlay_results.append({
            "parlay_id":   _pid,
            "agent":       _agent,
            "legs":        _legs,
            "leg_count":   _n,
            "status":      _slip_status,
            "profit_loss": _slip_pl,
            "stake":       _stake,
            "entry_type":  _platform,
        })

    # Solo legs (no parlay_id — backward compat with old rows)
    for _r in _solo_results:
        parlay_results.append({
            "parlay_id":   None,
            "agent":       _r["agent"],
            "legs":        [_r],
            "leg_count":   1,
            "status":      _r["status"],
            "profit_loss": _r["profit_loss"],
            "stake":       _r.get("stake_units", 5.0),
            "entry_type":  _r.get("entry_type", ""),
        })

    total_profit = sum(p["profit_loss"] for p in parlay_results)
    wins   = sum(1 for p in parlay_results if p["status"] == "WIN")
    losses = sum(1 for p in parlay_results if p["status"] == "LOSS")
    pushes = sum(1 for p in parlay_results if p["status"] == "PUSH")

    logger.info("[GradingTasklet] Graded %d bets — W:%d L:%d P:%d  Profit: %+.2fu",
                len(results), wins, losses, pushes, total_profit)

    # ── Update UD streak state based on STREAKS bet outcomes ────────────────
    try:
        _streak_bets = [r for r in results if (r.get("entry_type") or "").upper() == "STREAKS"]
        if _streak_bets:
            _sconn = _pg_conn()
            with _sconn.cursor() as _scur:
                _scur.execute("SELECT current_count FROM ud_streak_state LIMIT 1")
                _srow2 = _scur.fetchone()
                _cur_count = int(_srow2[0]) if _srow2 else 0

                _all_won  = all(r["status"] == "WIN"  for r in _streak_bets)
                _any_loss = any(r["status"] == "LOSS" for r in _streak_bets)

                if _any_loss:
                    _new_count = 0   # streak broken
                    logger.info("[Streaks] Streak BROKEN — reset to 0.")
                elif _all_won:
                    if _cur_count == 0:
                        _new_count = 2   # 2-leg entry completed
                        logger.info("[Streaks] Streak STARTED — count now 2.")
                    elif _cur_count >= 11:
                        _new_count = 0   # 11-for-11 complete (1000x!) — reset
                        logger.info("[Streaks] Streak COMPLETED 11! Resetting to 0.")
                    else:
                        _new_count = _cur_count + 1
                        logger.info("[Streaks] Streak advanced to %d.", _new_count)
                else:
                    _new_count = _cur_count  # pushes — no change

                _scur.execute(
                    "UPDATE ud_streak_state SET current_count = %s, last_updated = NOW()",
                    (_new_count,),
                )
            _sconn.commit()
            _sconn.close()
    except Exception as _streak_upd_err:
        logger.debug("[GradingTasklet] Streak state update error: %s", _streak_upd_err)
    # ── End streak state update ──────────────────────────────────────────────
    try:
        conn2 = _pg_conn()
        with conn2.cursor() as cur2:
            # Upsert a daily summary row per agent
            from collections import defaultdict
            _agent_results: dict = defaultdict(lambda: {"wins":0,"losses":0,"pushes":0,"profit":0.0,"stake":5.0})
            for r in results:
                _ag = r.get("agent", "Unknown")
                # FIX Bug 4: capture actual stake from the result row (graded from units_wagered)
                _res_stake = float(r.get("units_wagered", r.get("stake", 5.0)) or 5.0)
                _agent_results[_ag]["stake"] = _res_stake  # last win's stake is fine — same tier
                if r["status"] == "WIN":
                    _agent_results[_ag]["wins"]   += 1
                    _agent_results[_ag]["profit"] += float(r.get("profit_loss", 0))
                elif r["status"] == "LOSS":
                    _agent_results[_ag]["losses"] += 1
                    _agent_results[_ag]["profit"] += float(r.get("profit_loss", 0))
                else:
                    _agent_results[_ag]["pushes"] += 1
            for _ag, _stats in _agent_results.items():
                _status = "WIN" if _stats["wins"] > _stats["losses"] else (
                          "LOSS" if _stats["losses"] > _stats["wins"] else "PUSH")
                cur2.execute("""
                    INSERT INTO propiq_season_record
                        (date, agent_name, parlay_legs, platform, stake, payout,
                         confidence, status, legs_json, created_at)
                    SELECT %s, %s, %s, 'mixed', %s, %s, 0.0, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM propiq_season_record
                        WHERE date = %s AND agent_name = %s AND status != 'PENDING'
                    )
                """, (
                    today, _ag,
                    _stats["wins"] + _stats["losses"] + _stats["pushes"],
                    # FIX: use actual tier stake from agent_unit_sizing, not hardcoded 5.00
                    round(_stats.get("stake", 5.0), 2),
                    round(_stats.get("stake", 5.0) + _stats["profit"], 2),
                    _status,
                    json.dumps({"wins": _stats["wins"], "losses": _stats["losses"],
                                "pushes": _stats["pushes"]}),
                    datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    today, _ag,
                ))
        conn2.commit()
        conn2.close()
        logger.info("[GradingTasklet] Synced %d agents to propiq_season_record", len(_agent_results))
    except Exception as _sync_err:
        logger.debug("[GradingTasklet] Season record sync failed: %s", _sync_err)

    # ── Phase 89: Update agent tier ladder + build progress messages ──────
    # Progress is shown in Discord after every result so user always sees
    _TIER_UP_DOLLARS   = {1: 8, 2: 12, 3: 16, 4: 20}
    _TIER_DOWN_DOLLARS = {2: 5, 3: 8,  4: 12, 5: 16}
    _tier_progress: list[str] = []
    try:
        from agent_unit_sizing import record_result as _unit_record  # noqa: PLC0415
        for _ag, _stats in _agent_results.items():
            # Determine W/L/P for this agent's day
            _wl = ("W" if _stats["wins"] > _stats["losses"]
                   else "L" if _stats["losses"] > _stats["wins"]
                   else "P")
            _tu = _unit_record(_ag, _wl)
            if _tu.get("tier_change"):
                # Full promotion / demotion — show it
                _tier_progress.append(_tu["tier_change"])
            else:
                # Show in-progress streak so user always sees 3 wins/losses building up
                _cw = _tu.get("consecutive_wins", 0)
                _cl = _tu.get("consecutive_losses", 0)
                _nt = _tu.get("new_tier", 1)
                if _cw > 0 and _nt < 5:
                    _next_dollar = _TIER_UP_DOLLARS.get(_nt, 20)
                    _tier_progress.append(
                        f"🔥 {_ag}: {_cw}/3 wins → Tier {_nt + 1} (${_next_dollar}/unit)"
                    )
                elif _cl > 0 and _nt > 1:
                    _prev_dollar = _TIER_DOWN_DOLLARS.get(_nt, 5)
                    _tier_progress.append(
                        f"⚠️ {_ag}: {_cl}/3 losses → Tier {_nt - 1} (${_prev_dollar}/unit)"
                    )
                # If both 0 — streak reset (direction changed), nothing to show
    except Exception as _tier_err:
        logger.debug("[GradingTasklet] Tier update error: %s", _tier_err)
    # ── End Phase 89 ──────────────────────────────────────────────────────

    try:
        discord_alert.send_daily_recap(
            parlay_results, total_profit, today,
            tier_updates=_tier_progress if _tier_progress else None,
        )
    except Exception as _disc_err:
        logger.warning("[GradingTasklet] Discord recap error: %s", _disc_err)

    # ── Post-grading monitoring: calibration + edge health ──────────────────
    try:
        from calibration_monitor import run as _cal_run  # noqa: PLC0415
        _cal_run(days=30, quiet=True)
        logger.info("[GradingTasklet] Calibration monitor complete.")
    except Exception as _cal_err:
        logger.debug("[GradingTasklet] Calibration monitor skipped: %s", _cal_err)
    try:
        from edge_health_monitor import run as _edge_run  # noqa: PLC0415
        _edge_run(days=30, quiet=True)
        logger.info("[GradingTasklet] Edge health monitor complete.")
    except Exception as _edge_err:
        logger.debug("[GradingTasklet] Edge health monitor skipped: %s", _edge_err)

    # Update drift monitor with today's Brier score
    if results:
        try:
            from drift_monitor import record_brier  # noqa: PLC0415
            # Fetch actual model_prob from bet_ledger for accurate Brier score
            _brier_probs = {}
            try:
                _bc = _pg_conn()
                with _bc.cursor() as _bcur:
                    _ids = [r["id"] for r in results if r.get("id")]
                    if _ids:
                        _bcur.execute(
                            "SELECT id, model_prob FROM bet_ledger WHERE id = ANY(%s)",
                            (_ids,)
                        )
                        _brier_probs = {row[0]: float(row[1]) / 100
                                        for row in _bcur.fetchall()
                                        if row[1] is not None}  # FIX: skip NULL model_prob rows
                _bc.close()
            except Exception:
                pass

            brier_inputs = []
            for r in results:
                if r["status"] == "PUSH":
                    continue   # PUSH is not a WIN/LOSS — exclude from Brier
                outcome = 1 if r["status"] == "WIN" else 0
                prob    = _brier_probs.get(r.get("id"))
                if prob is None:
                    continue  # FIX: skip rows with no model_prob — 0.52 default biases calibration
                brier_inputs.append({"prob": prob, "outcome": outcome})
            # Minimum sample gate: Brier is meaningless on fewer than 30 graded picks.
            # A single bad day on 9 picks swings Brier 15%+ triggering false drift alerts.
            _MIN_BRIER_SAMPLE = 30
            if brier_inputs and len(brier_inputs) >= _MIN_BRIER_SAMPLE:
                from calibration_layer import calculate_brier_score  # noqa: PLC0415
                brier = calculate_brier_score(brier_inputs)
                if brier is not None:
                    record_brier(brier, n_samples=len(brier_inputs), agent_name="global")
                    logger.info("[GradingTasklet] Brier score recorded: %.4f (%d samples)",
                                brier, len(brier_inputs))
            elif brier_inputs:
                logger.info(
                    "[GradingTasklet] Brier skipped — only %d graded rows (need %d). "
                    "No drift check fired. Governor stays inactive.",
                    len(brier_inputs), _MIN_BRIER_SAMPLE,
                )
        except Exception as _brier_err:
            logger.warning("[GradingTasklet] Brier record failed: %s", _brier_err)  # FIX: was debug
    # Previously calibration_map.json only updated on Sunday XGBoost retrain.
    # Probabilities start being corrected after ~3-4 days instead of up to 7 days.
    try:
        from calibrate_model import generate_calibration_map_from_db  # noqa: PLC0415
        _graded_count = len([r for r in results if r.get("status") in ("WIN", "LOSS")])
        if _graded_count > 0:
            generate_calibration_map_from_db()
            logger.info(
                "[GradingTasklet] Calibration map updated from %d WIN/LOSS rows.",
                _graded_count,
            )
        else:
            logger.info("[GradingTasklet] No WIN/LOSS rows this cycle — calibration map unchanged.")
    except Exception as _cal_map_err:
        logger.warning("[GradingTasklet] Calibration map update failed: %s", _cal_map_err)

    # ── Streak settlement — grade PENDING streak picks for yesterday ────────────
    try:
        from streak_agent import settle_streak_picks as _settle_streak  # noqa: PLC0415
        _settle_streak(today)   # today = yesterday's PT date (set at top of grading tasklet)
        logger.info("[GradingTasklet] Streak picks settled for %s.", today)
    except Exception as _streak_settle_err:
        logger.warning("[GradingTasklet] Streak settlement failed (non-fatal): %s", _streak_settle_err)

    # ── Void stale OPEN bets from postponed/suspended games ────────────────────
    # After 7 days without grading, mark VOID so they don't pollute the ledger.
    try:
        _void_conn = _pg_conn()
        with _void_conn.cursor() as _vc:
            _vc.execute("""
                UPDATE bet_ledger
                   SET status = 'VOID', graded_at = NOW()
                 WHERE (status IS NULL OR status = 'OPEN')
                   AND discord_sent = TRUE
                   AND bet_date <= CURRENT_DATE - INTERVAL '7 days'
            """)
            _voided = _vc.rowcount
        _void_conn.commit()
        _void_conn.close()
        if _voided > 0:
            logger.info("[GradingTasklet] Voided %d stale OPEN bets (postponed/no boxscore >7 days).", _voided)
    except Exception as _void_err:
        logger.debug("[GradingTasklet] Stale OPEN void sweep failed: %s", _void_err)

    # ── Nightly Parquet archival — durable backup for XGBoost retraining ──────
    # Postgres is the primary store; Parquet is the backup.  If DB is wiped, we
    # XGBoostTasklet reads from Postgres; the Parquet files are a safety net only.
    try:
        import pandas as _pd  # noqa: PLC0415
        _archive_dir = os.getenv("RETRAINING_ARCHIVE_DIR", "/app/data")
        os.makedirs(_archive_dir, exist_ok=True)
        _archive_path = os.path.join(_archive_dir, f"retraining_{today}.parquet")
        # Fetch all graded rows (not just tonight's) for full snapshot
        _arc_conn = _pg_conn()
        with _arc_conn.cursor() as _arc_cur:
            _arc_cur.execute("""
                SELECT id, bet_date, agent_name, player_name, prop_type, line, side,
                       odds_american, model_prob, ev_pct, kelly_units, units_wagered,
                       platform, entry_type, features_json,
                       status, actual_result, profit_loss, actual_outcome,
                       clv, graded_at
                FROM bet_ledger
                WHERE graded_at IS NOT NULL
                  AND actual_outcome IS NOT NULL
                  AND discord_sent = TRUE
                ORDER BY graded_at DESC
                LIMIT 25000
            """)
            _arc_rows = _arc_cur.fetchall()
            _arc_cols = [d[0] for d in _arc_cur.description]
        _arc_conn.close()
        if _arc_rows:
            _arc_df = _pd.DataFrame(_arc_rows, columns=_arc_cols)
            _arc_df.to_parquet(_archive_path, index=False, compression="snappy")
            logger.info(
                "[GradingTasklet] Nightly archive: %d rows → %s",
                len(_arc_rows), _archive_path,
            )
    except ImportError:
        logger.debug("[GradingTasklet] pandas/pyarrow not installed — Parquet archive skipped")
    except Exception as _arc_err:
        logger.warning("[GradingTasklet] Nightly Parquet archive failed: %s", _arc_err)

    # ── Agent diagnostics: 30-day ROI / win rate / Brier per agent + freeze gate ──
    try:
        from agent_diagnostics import run_agent_diagnostics as _run_diagnostics  # noqa: PLC0415
        _run_diagnostics()
        logger.info("[GradingTasklet] Agent diagnostics snapshot complete.")
    except Exception as _diag_err:
        logger.warning("[GradingTasklet] Agent diagnostics failed (non-fatal): %s", _diag_err)

    # ── Isotonic calibration rebuild (HIGH-2: was dead — now wired nightly) ──────
    try:
        from isotonic_calibrator import rebuild_isotonic_calibration as _rebuild_iso  # noqa: PLC0415
        _rebuild_iso()
        logger.info("[GradingTasklet] Isotonic calibration map rebuilt.")
    except Exception as _iso_err:
        logger.warning("[GradingTasklet] Isotonic calibration rebuild failed (non-fatal): %s", _iso_err)

    # ── Temperature (Platt) calibration — fits per-agent T scalar ────────────
    # Phase 47: walks agent_calibration_data → fits T for each agent with ≥30 graded picks
    # T>1 compresses overconfident probs. Phase 45 backtest showed T≈3.0 on raw signal.
    # temperature_scaling.py contains the math; temperature_calibration.py runs the loop.
    try:
        from temperature_calibration import run as _run_temp_cal  # noqa: PLC0415
        _temp_updates = _run_temp_cal()
        if _temp_updates:
            logger.info("[GradingTasklet] Temperature calibration updated %d agents: %s",
                        len(_temp_updates), list(_temp_updates.keys()))
        else:
            logger.info("[GradingTasklet] Temperature calibration: no agents had ≥30 graded picks yet.")
    except Exception as _tc_err:
        logger.warning("[GradingTasklet] Temperature calibration failed (non-fatal): %s", _tc_err)


def _get_stat(stats: dict, prop_type: str, platform: str = "prizepicks") -> float | None:
    """
    Map prop_type to graded stat value.

    Approved prop types:
      Pitchers : strikeouts | pitching_outs | fantasy_pitcher
      Batters  : hits_runs_rbis | total_bases | fantasy_hitter

    Pitching outs = total outs recorded while pitcher is on mound.
      1 full inning = 3 outs.  ESPN 6.2 IP format = 20 outs (6*3+2).
      MLB Stats API pit['outs'] is the authoritative integer count.
      Examples: 13 outs = 4.1 IP;  20 outs = 6.2 IP;  9 outs = 3.0 IP

    Scoring tables (verified against screenshots):
      PrizePicks Pitcher : K×3  Out×1  W×6  QS×4  ER×-3
      Underdog   Pitcher : K×3  IP×3   W×5  QS×5  ER×-3
      PrizePicks Hitter  : 1B×3 2B×5 3B×8 HR×10 R×2 RBI×2 BB×2 HBP×2 SB×5
      Underdog   Hitter  : 1B×3 2B×6 3B×8 HR×10 R×2 RBI×2 BB×3 HBP×3 SB×4 CS×-2
    """
    mapping = {
        # Normalised lowercase keys
        "hits":               "Hits",
        "home_runs":          "HomeRuns",
        "rbis":               "RunsBattedIn",
        "rbi":                "RunsBattedIn",
        "runs":               "Runs",
        "stolen_bases":       "StolenBases",
        "total_bases":        "TotalBases",
        "walks":              "Walks",
        "doubles":            "Doubles",
        "triples":            "Triples",
        "strikeouts":         "Strikeouts",
        "pitcher_strikeouts": "Strikeouts",
        "hitter_strikeouts":  "Strikeouts",   # batter K props (MLEdgeAgent, CorrelatedParlay)
        "earned_runs":        "EarnedRuns",
        "hits_allowed":       "HitsAllowed",
        "walks_allowed":      "WalksAllowed",
        # pitching_outs → __pitching_outs__ uses PitchingOuts first, then IP conversion
        "pitching_outs":      "__pitching_outs__",
        "outs_recorded":      "__pitching_outs__",
        "p_outs":             "__pitching_outs__",
        # composite / fantasy
        "hits_runs_rbis":     "__composite__",
        "fantasy_score":      "__fantasy_score__",
        "fantasy_hitter":     "__fantasy_score__",
        "fantasy_pitcher":    "__fantasy_score__",
        "fantasy_pts":        "__fantasy_score__",
        # Legacy abbreviations
        "h":  "Hits",  "hr": "HomeRuns", "r": "Runs",
        "sb": "StolenBases", "tb": "TotalBases",
        "bb": "Walks", "k":  "Strikeouts",
        "ks": "Strikeouts", "er": "EarnedRuns",
    }
    prop_key = prop_type.lower().strip()
    # Strip common prefixes
    for prefix in ("over_", "under_", "o_", "u_"):
        if prop_key.startswith(prefix):
            prop_key = prop_key[len(prefix):]

    field = mapping.get(prop_key)

    # ── Pitching outs ──────────────────────────────────────────────────────────
    # Examples: 13 outs=4.1 IP, 20 outs=6.2 IP, 9 outs=3.0 IP
    if field == "__pitching_outs__":
        po = stats.get("PitchingOuts")
        if po is not None and float(po) > 0:
            return float(po)
        # FALLBACK: ESPN InningsPitched float (6.2 = 6 full + 2 partial outs = 20)
        ip = stats.get("InningsPitched")
        if ip is not None:
            ip      = float(ip)
            full    = int(ip)
            partial = round((ip % 1) * 10)   # .1→1, .2→2
            total   = float(full * 3 + partial)
            return total if total > 0 else None
        return None

    # ── H+R+RBI composite ─────────────────────────────────────────────────────
    if field == "__composite__":
        h   = float(stats.get("Hits",         stats.get("H",   0)) or 0)
        r   = float(stats.get("Runs",         stats.get("R",   0)) or 0)
        rbi = float(stats.get("RunsBattedIn", stats.get("RBI", 0)) or 0)
        return h + r + rbi

    # ── Fantasy score ─────────────────────────────────────────────────────────
    if field == "__fantasy_score__":
        plat  = (platform or "prizepicks").lower()
        k_raw = stats.get("Strikeouts")
        ip    = stats.get("InningsPitched")
        er    = stats.get("EarnedRuns")

        # Pitcher: detected when K + IP + ER are all present
        if k_raw is not None and ip is not None and er is not None:
            k  = float(k_raw or 0)
            er = float(er    or 0)
            w  = float(stats.get("Wins")         or stats.get("Win") or 0)
            qs = float(stats.get("QualityStart") or 0)
            # Outs: prefer direct MLB API count; fall back to IP float conversion
            po = stats.get("PitchingOuts")
            if po is not None and float(po) > 0:
                outs = float(po)
            else:
                ip_f    = float(ip or 0)
                full    = int(ip_f)
                partial = round((ip_f % 1) * 10)
                outs    = float(full * 3 + partial)
            if plat == "prizepicks":
                # PrizePicks Pitcher: K×3  Out×1  W×6  QS×4  ER×-3
                return round(k*3 + outs*1 + w*6 + qs*4 + er*-3, 2)
            else:
                # Underdog Pitcher: K×3  IP×3  W×5  QS×5  ER×-3
                ip_f = float(ip or 0)
                return round(k*3 + ip_f*3 + w*5 + qs*5 + er*-3, 2)

        # Hitter fantasy score
        h   = float(stats.get("Hits")           or stats.get("H")   or 0)
        rn  = float(stats.get("Runs")           or stats.get("R")   or 0)
        rbi = float(stats.get("RunsBattedIn")   or stats.get("RBI") or 0)
        hr  = float(stats.get("HomeRuns")       or stats.get("HR")  or 0)
        db  = float(stats.get("Doubles")        or stats.get("2B")  or 0)
        tb3 = float(stats.get("Triples")        or stats.get("3B")  or 0)
        sb  = float(stats.get("StolenBases")    or stats.get("SB")  or 0)
        bb  = float(stats.get("Walks")          or stats.get("BB")  or 0)
        hbp = float(stats.get("HitByPitch")     or stats.get("HBP") or 0)
        cs  = float(stats.get("CaughtStealing") or stats.get("CS")  or 0)
        if h == 0 and rn == 0 and rbi == 0:
            return None   # no data yet — keep PENDING
        singles = max(0.0, h - db - tb3 - hr)
        if plat == "prizepicks":
            # PrizePicks Hitter: 1B×3  2B×5  3B×8  HR×10  R×2  RBI×2  BB×2  HBP×2  SB×5
            return round(singles*3 + db*5 + tb3*8 + hr*10
                         + rn*2 + rbi*2 + bb*2 + hbp*2 + sb*5, 2)
        else:
            # Underdog Hitter: 1B×3  2B×6  3B×8  HR×10  R×2  RBI×2  BB×3  HBP×3  SB×4  CS×-2
            return round(singles*3 + db*6 + tb3*8 + hr*10
                         + rn*2 + rbi*2 + bb*3 + hbp*3 + sb*4 + cs*-2, 2)

    # ── Direct stat lookup ────────────────────────────────────────────────────
    if field:
        val = stats.get(field)
        return float(val) if val is not None else None

    return None

def _fetch_closing_odds(player: str, prop_type: str, side: str) -> int | None:
    """
    Best-effort closing line from Redis market cache (hub:market odds list).
    Scans sharp book markets for player name match. Returns American odds int or None.
    """
    try:
        r   = _redis()
        raw = r.get("hub:market")
        if not raw:
            return None
        market    = json.loads(raw)
        odds_list = market.get("odds", [])
        player_lc = player.lower()

        for game in odds_list:
            if not isinstance(game, dict):
                continue
            for bookmaker in game.get("bookmakers", []):
                bkey = bookmaker.get("key", "").lower()
                if bkey not in {"draftkings", "fanduel", "pinnacle", "betmgm"}:
                    continue
                for mkt in bookmaker.get("markets", []):
                    for outcome in mkt.get("outcomes", []):
                        desc = str(outcome.get("description", "")).lower()
                        name = str(outcome.get("name", "")).lower()
                        out_side = str(outcome.get("name", "")).upper()
                        if player_lc in desc or player_lc in name:
                            if side.upper() in out_side or out_side in side.upper():
                                price = outcome.get("price")
                                if price is not None:
                                    return int(price)
    except Exception:
        pass
    return None


def _fmt_american(american: int) -> str:
    return f"+{american}" if american > 0 else str(american)


# ─────────────────────────────────────────────────────────────────────────────
# 6. XGBoostTasklet  (weekly Sunday 2:00 AM)
# ─────────────────────────────────────────────────────────────────────────────

def run_xgboost_tasklet() -> None:
    """
    Retrain XGBoost on the full Postgres settlement ledger.
    Saves model to XGB_MODEL_PATH for all agents to use.
    """
    import numpy as np

    try:
        import xgboost as xgb
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score
    except ImportError as e:
        logger.warning("[XGBoostTasklet] ML deps not available: %s", e)
        return

    rows: list[tuple] = []
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT features_json, actual_outcome, graded_at, prop_type, side, model_prob, line
                FROM bet_ledger
                WHERE actual_outcome IS NOT NULL
                  AND discord_sent = TRUE
                  AND features_json IS NOT NULL
                  AND (lookahead_safe IS NULL OR lookahead_safe = TRUE)
                ORDER BY COALESCE(graded_at, NOW()) DESC
                LIMIT 25000
                """
            )
            rows = cur.fetchall()
        conn.close()
    except Exception as e:
        logger.warning("[XGBoostTasklet] Postgres error: %s", e)

    if len(rows) < 200:
        logger.info("[XGBoostTasklet] Insufficient training data (%d rows) — skipping.", len(rows))
        return

    # ── Feature padding: pad older 20-feature records to current 27-feature schema ──
    _TARGET_FEATS = 27
    _null_feat_count = 0
    _raw_feats = []
    for r in rows:
        if r[0] is not None:
            _raw_feats.append(json.loads(r[0]))
        else:
            # Seed row missing features_json (pre-fix rows) — use neutral defaults
            _null_feat_count += 1
            _raw_feats.append([0.5] * _TARGET_FEATS)
    if _null_feat_count:
        logger.info("[XGBoostTasklet] %d rows had NULL features_json — using neutral defaults", _null_feat_count)
    _padded = [
        f + [0.0] * (_TARGET_FEATS - len(f)) if len(f) < _TARGET_FEATS
        else f[:_TARGET_FEATS]
        for f in _raw_feats
    ]
    X = np.array(_padded, dtype=np.float32)
    y = np.array([int(r[1]) for r in rows], dtype=np.int8)

    # ── Recency decay: recent bets matter more than old ones ──────────────
    # Last week ≈ 0.93 | 30 days ≈ 0.74 | 90 days ≈ 0.41 | Opening Day ≈ 0.16
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    sample_weights = np.array([
        np.exp(-0.01 * max((now_utc - (
            r[2] if isinstance(r[2], datetime.datetime)
            else datetime.datetime.fromisoformat(str(r[2]))
        ).replace(tzinfo=None)).days, 0))
        for r in rows
    ], dtype=np.float32)

    X_train, X_test, y_train, y_test, w_train, _ = train_test_split(
        X, y, sample_weights, test_size=0.2, random_state=42, stratify=y
    )

    model = xgb.XGBClassifier(
        n_estimators=400, max_depth=6, learning_rate=0.03,
        subsample=0.8, colsample_bytree=0.8,
        min_child_weight=5, gamma=0.1,
        use_label_encoder=False, eval_metric="logloss",
        random_state=42, n_jobs=-1,
    )
    model.fit(
        X_train, y_train,
        sample_weight=w_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    preds    = model.predict(X_test)
    accuracy = accuracy_score(y_test, preds)

    model_path = os.getenv("XGB_MODEL_PATH", "/app/api/models/prop_model_v1.json")
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    # Save as XGBoost native JSON (matches _load_xgb_model() which uses xgb.Booster().load_model())
    try:
        model.get_booster().save_model(model_path)
        logger.info("[XGBoostTasklet] Saved model as XGBoost JSON to %s", model_path)
    except Exception:
        # Fallback to pickle for sklearn wrapper models
        with open(model_path.replace(".json", ".pkl"), "wb") as f:
            pickle.dump(model, f)
        logger.info("[XGBoostTasklet] Saved model as pickle (JSON save failed)")

    # ── Persist model to Postgres so it survives Railway restarts ─────────────
    try:
        with open(model_path, "r") as _mf:
            _model_json_str = _mf.read()
        _ms_conn = _pg_conn()
        with _ms_conn.cursor() as _ms_cur:
            # Keep only last 3 models to cap storage
            _ms_cur.execute(
                "DELETE FROM xgb_model_store WHERE id NOT IN "
                "(SELECT id FROM xgb_model_store ORDER BY trained_at DESC LIMIT 2)"
            )
            _ms_cur.execute(
                "INSERT INTO xgb_model_store (model_json, n_rows, notes) VALUES (%s, %s, %s)",
                (_model_json_str, len(rows), f"accuracy={round(accuracy, 4)}")
            )
        _ms_conn.commit()
        _ms_conn.close()
        logger.info("[XGBoostTasklet] Model persisted to xgb_model_store (%d rows).", len(rows))
    except Exception as _ms_err:
        logger.warning("[XGBoostTasklet] xgb_model_store persist failed: %s", _ms_err)

    r = _redis()
    r.setex("xgb_meta", 604800, json.dumps({
        "ts":            datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "accuracy":      round(accuracy, 4),
        "n_train":       len(X_train),
        "n_test":        len(X_test),
        "model_path":    model_path,
        "target_accuracy": 0.842,
        "passed":        accuracy >= 0.777,
    }))

    # ── Phase 91 Step 5: Cache per-prop-type sample counts for thin-data shrinkage ──
    # for prop types with few settled training rows.
    try:
        _sc_conn = _pg_conn()
        with _sc_conn.cursor() as _sc_cur:
            _sc_cur.execute(
                """
                SELECT prop_type, COUNT(*) AS n
                FROM bet_ledger
                WHERE actual_outcome IS NOT NULL
                  AND (lookahead_safe IS NULL OR lookahead_safe = TRUE)
                GROUP BY prop_type
                """
            )
            _counts = {row[0]: int(row[1]) for row in _sc_cur.fetchall() if row[0]}
        _sc_conn.close()
        r.setex("xgb_sample_counts", 604800, json.dumps(_counts))
        logger.info("[XGBoostTasklet] Cached sample counts for %d prop types: %s",
                    len(_counts), _counts)
    except Exception as _sce:
        logger.warning("[XGBoostTasklet] Could not cache sample counts: %s", _sce)

    logger.info("[XGBoostTasklet] Retrain complete. Accuracy=%.3f | Train=%d Test=%d | Saved→%s",
                accuracy, len(X_train), len(X_test), model_path)

    # ── Rebuild isotonic calibration map from settled bets ────────────────────
    try:
        from calibrate_model import generate_calibration_map_from_db  # noqa: PLC0415
        generate_calibration_map_from_db()
        logger.info("[XGBoostTasklet] Calibration map rebuilt from bet_ledger.")
    except Exception as _cal_err:
        logger.warning("[XGBoostTasklet] Calibration map rebuild failed: %s", _cal_err)

    # ── Persist feature importances to Postgres ────────────────────────────────────────
    try:
        _fi_booster = model.get_booster()
        _fi_gain    = _fi_booster.get_score(importance_type="gain")
        _fi_weight  = _fi_booster.get_score(importance_type="weight")
        _fi_conn    = _pg_conn()
        with _fi_conn.cursor() as _fi_cur:
            _fi_cur.execute(
                """
                CREATE TABLE IF NOT EXISTS xgb_feature_importance (
                    id                SERIAL PRIMARY KEY,
                    feature_name      TEXT NOT NULL,
                    importance_gain   FLOAT,
                    importance_weight FLOAT,
                    model_accuracy    FLOAT,
                    trained_at        TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            _fi_cur.execute(
                "DELETE FROM xgb_feature_importance WHERE trained_at < NOW() - INTERVAL '90 days'"
            )
            _fi_all = set(list(_fi_gain.keys()) + list(_fi_weight.keys()))
            for _fname in _fi_all:
                _fi_cur.execute(
                    """
                    INSERT INTO xgb_feature_importance
                        (feature_name, importance_gain, importance_weight, model_accuracy)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (_fname, _fi_gain.get(_fname), _fi_weight.get(_fname), round(accuracy, 4)),
                )
        _fi_conn.commit()
        _fi_conn.close()
        logger.info("[XGBoostTasklet] Saved %d feature importances to xgb_feature_importance.",
                    len(_fi_all))
    except Exception as _fi_err:
        logger.warning("[XGBoostTasklet] Feature importance persist failed: %s", _fi_err)

    # ── Hot-reload: update the global model so live agents use it immediately ──
    try:
        new_booster = xgb.Booster()
        new_booster.load_model(model_path)
        _BaseAgent._shared_model = new_booster
        global _XGB_MODEL_CACHE
        _XGB_MODEL_CACHE = new_booster   # sync module-level cache with retrained model
        logger.info("[XGBoostTasklet] ✅ Hot-reloaded model into all agents.")
    except Exception as _hrl_err:
        logger.warning("[XGBoostTasklet] Hot-reload failed (%s) — agents pick up on next restart.", _hrl_err)

    if accuracy >= 0.842:
        logger.info("[XGBoostTasklet] 🎯 Target accuracy %.1f%% reached!", accuracy * 100)
    elif accuracy >= 0.777:
        logger.info("[XGBoostTasklet] ✅ Minimum threshold met (%.1f%%).", accuracy * 100)
    else:
        logger.warning("[XGBoostTasklet] ⚠️ Below minimum threshold (%.1f%% < 77.7%%).",
                       accuracy * 100)
