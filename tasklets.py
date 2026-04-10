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

# ── Lock-Time Gate (Step 3 upgrade: prevent lookahead bias) ───────────────────
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

# ── CLV Feedback Engine (Step 2 upgrade: per-edge-tag adaptive thresholds) ────
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

# WagerBrain-enhanced odds math (bookmaker_margin, kelly_criterion, true_odds_ev)
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
    from bullpen_fatigue_scorer import build_bullpen_fatigue_scorer as _build_bullpen_scorer
    _BULLPEN_SCORER_AVAILABLE = True
except ImportError:
    _BULLPEN_SCORER_AVAILABLE = False
    def _build_bullpen_scorer(): return None  # noqa: E704

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
             "earned runs":"earned_runs","earned runs allowed":"earned_runs"}
        s2 = str(s).lower().replace(" ","_").replace("-","_").strip()
        result = m.get(s2, s2)
        # Block removed prop types even if they slip through via raw string match
        _BLOCKED = {"stolen_bases","home_runs","walks","walks_allowed","doubles","triples"}
        return result if result not in _BLOCKED else ""
    ABS_FRAMING_WEIGHT = 0.20
    class SteamMonitor:
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

# ── Constants ─────────────────────────────────────────────────────────────────

OPENING_DAY        = datetime.date(2026, 3, 26)
SPRING_TRAINING_WT = 0.30          # ST stats count 30 % until Opening Day

# Data TTLs (seconds) — 4 scraper groups
TTL_PHYSICS  = 900    # 15 min
TTL_CONTEXT  = 600    # 10 min
TTL_MARKET   = 300    #  5 min
TTL_DFS      = 480    #  8 min
TTL_HUB      = 600    # 10 min — master hub key

# ── Per-agent daily send gate (in-memory, resets at midnight) ────────────────
# Works with or without Redis. Keyed agent_name → "YYYY-MM-DD".
# An agent may send AT MOST ONE play per calendar day.
_AGENT_SENT_TODAY: dict = {}   # { agent_name: "2026-03-29" }
MIN_CONFIDENCE    = 6
MIN_PROB          = 0.57   # Phase 121: minimum XGBoost model probability (57%)          # plays below 6/10 are never sent to Discord (matches live_dispatcher conf gate)

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
    "SteamAgent",
    "MLEdgeAgent",
]
KELLY_FRACTION  = 0.25    # Quarter-Kelly
MAX_UNIT_CAP    = 0.05    # 5 % bankroll cap per bet
MIN_EV_THRESH     = 0.03   # 3% minimum edge to queue a bet (ratio scale, e.g. 0.085)
MIN_EV_THRESH_PCT = 3.0    # same threshold in percent scale (e.g. 8.5) — used by Group B agents

# Capital allocation bounds (14-day ROI → multiplier)
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


def _american_to_implied(american: int) -> float:
    if american > 0:
        return 100.0 / (american + 100) * 100
    return abs(american) / (abs(american) + 100) * 100


def _no_vig(over_american: int, under_american: int) -> tuple[float, float]:
    """Return (over_fair_prob, under_fair_prob) stripped of vig."""
    over_imp  = _american_to_implied(over_american)  / 100
    under_imp = _american_to_implied(under_american) / 100
    juice = over_imp + under_imp
    return over_imp / juice, under_imp / juice


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
    _COLD_START_SAMPLES = 100   # floor when Redis key absent entirely
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
    """Fetch SportsBettingDime public betting splits.

    Returns dict with keys: game_df, prop_df (pandas DataFrames as dicts for JSON storage).
    Caches via PublicTrendsScraper daily Parquet cache — zero re-hits after first fetch.
    """
    try:
        scraper = PublicTrendsScraper()
        game_df, prop_df = scraper.fetch()
        return {
            "game_df": game_df.to_dict(orient="records") if not game_df.empty else [],
            "prop_df": prop_df.to_dict(orient="records") if not prop_df.empty else [],
        }
    except Exception as exc:
        logger.warning("[DataHub] SBD public trends fetch failed: %s", exc)
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
    GET with automatic ScraperAPI fallback on 403/429.
    If SCRAPERAPI_KEY env var is set and direct call fails, retries via proxy.
    ScraperAPI free tier: 1,000 calls/month — only used as fallback.
    """
    import os as _os
    resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    if resp.status_code in (403, 429, 407):
        scraper_key = _os.getenv("SCRAPERAPI_KEY", "")
        if scraper_key:
            proxy_url = f"http://scraperapi:{scraper_key}@proxy-server.scraperapi.com:8001"
            proxies = {"http": proxy_url, "https": proxy_url}
            logger.info("[DataHub] Direct fetch %d — retrying via ScraperAPI proxy", resp.status_code)
            resp = requests.get(url, headers=headers, params=params,
                                timeout=30, proxies=proxies, verify=False)
    return resp


def _fetch_prizepicks_direct() -> list[dict]:
    """Fetch PrizePicks MLB props via partner-api (public, no key required).
    Uses partner-api.prizepicks.com — confirmed public endpoint with no bot block.
    Falls back to ScraperAPI proxy on 403 if SCRAPERAPI_KEY env var is set.
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
        logger.info("[DataHub] PrizePicks direct: %d props", len(props))
        return props
    except Exception as exc:
        logger.info("[DataHub] PrizePicks direct fetch failed: %s", exc)
        return []


def _fetch_underdog_props_direct() -> list[dict]:
    """Fetch Underdog Fantasy MLB over/under lines (free, no key required).
    Falls back to ScraperAPI proxy on 403 if SCRAPERAPI_KEY env var is set.
    """
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
    # Excluded props: home_runs, stolen_bases, walks, walks_allowed (Phase 112 + 118)
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
                             Only called if ODDS_API_KEY is set AND quota not exhausted
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
                    # Cache quota to Redis so bug_checker can read it without a live API call
                    try:
                        _redis().set("odds_api_quota_remaining", str(remaining), ex=86400)
                    except Exception:
                        pass
                    return data
            elif resp.status_code in (401, 403, 422, 429):
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
            # _get_sharp_consensus uses hub.market.odds but gracefully returns None
            # when no bookmaker entries exist — agents still fire via _model_prob
            return [{"source": "draftedge", "batters": batters.to_dict("records"),
                     "pitchers": pitchers.to_dict("records") if pitchers is not None else []}]
    except Exception as e:
        logger.info("[OddsAPI→DraftEdge] Not available (%s)", e)

    # ── Tier 3: TheRundown — real sportsbook K prop lines (market_id=19) ────
    # Free 100 requests/day with key. Covers pitcher_strikeouts for all MLB games.
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

# MLB stadium coordinates for Open-Meteo weather fetch
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
            # XGBoost-written counts that may be higher (retrain uses more rows)
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


def _ensure_bet_ledger() -> None:
    """Create bet_ledger table if it doesn't exist. Called on startup."""
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
                    created_at      TIMESTAMP    DEFAULT NOW()
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
        conn.close()
        logger.info("[DB] bet_ledger table ensured.")
    except Exception as exc:
        logger.warning("[DB] bet_ledger create failed: %s", exc)

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


def run_data_hub_tasklet() -> None:
    """
    Staggered scrape across 4 data groups (physics, context, market, DFS).
    Pre-match gate: skips any game already LIVE or FINAL so we never poll
    in-game data and waste API quota.
    """
    _ensure_bet_ledger()       # ensure table exists on every startup
    _ensure_calibration_map()  # bootstrap isotonic calibration map if missing
    r = _redis()
    hub: dict = {}  # pre-declared so bullpen section can write to it before merge block

    # ── Pre-match gate: fetch today's game states ──────────────────────────
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

    def _is_pre_match(game_id: str) -> bool:
        state = game_states.get(game_id, "Scheduled")
        return state not in ("InProgress", "Live", "Final", "F/OT", "Completed")

    # ── Pre-warm FanGraphs cache before agents run ───────────────────────────
    # FG data is lazy-loaded on first get_pitcher/get_batter call.
    # Pre-warming here (once per DataHub cycle) avoids cold-start delay in agents.
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

        physics = {
            "pitch_arsenal":  [],  # no Statcast actor yet
            "advanced_stats": [],  # no actor yet
            "bvp":            [],  # no actor yet
            "batted_ball":    [],  # no actor yet
            "second_half":    [],  # no actor yet
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
                "injuries":           [],  # no source yet
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
        "ts": datetime.datetime.utcnow().isoformat(),
        "game_states": game_states,
        "spring_training": _is_spring_training(),
    }
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
        # The simulation engine returns a proper outcome distribution and
        # derives P(over) from it.  This replaces the single-scalar approach.
        # Falls back to XGBoost / base_rate if sim engine unavailable.
        if _SIM_ENGINE_AVAILABLE and prop:
            try:
                _inject_team_total(prop, self.hub)
                sim = _simulate_prop(prop, n_sims=8_000)
                if sim and sim.prob_over > 0.0:
                    raw = sim.prob_over * 100.0
                    # Apply variance penalty: wide distribution → reduce confidence
                    pen = _variance_penalty(sim)
                    # Shift probability toward 50% by penalty factor
                    raw = 50.0 + (raw - 50.0) * pen
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
                # Build live feature vector from the actual prop (not zeros)
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

        # Fallback: base_rate_model (calibrated historical rates + FanGraphs + context signals)
        if _BASE_RATE_AVAILABLE and prop:
            _side = str(prop.get("side", "OVER")).upper()
            # Use player-specific rate if enrichment computed one
            _ps_prob = prop.get("_player_specific_prob")
            raw_p = float(_ps_prob) * 100.0 if _ps_prob else _base_rate_prob(prop, _side)
            # Layer Marcel, Predict+, and park factor adjustments
            raw_p += float(prop.get("_marcel_adj",       0.0)) * 100.0
            raw_p += float(prop.get("_predict_plus_adj", 0.0)) * 100.0
            raw_p += float(prop.get("_park_factor_adj",  0.0)) * 100.0
            # Game-level environment nudge (game_over_prob / game_home_win_prob
            # written to prop by prop_enrichment_layer Step 9)
            _gop = float(prop.get("game_over_prob",      0.50) or 0.50)
            _gwp = float(prop.get("game_home_win_prob",  0.50) or 0.50)
            _pt_lower = str(prop_type).lower()
            if _pt_lower in ("total_bases", "home_runs", "rbis", "rbi",
                             "runs", "earned_runs", "hits"):
                # High-scoring game env → boost over props; low-scoring → suppress
                raw_p += (_gop - 0.50) * 6.0   # ±3pp max at 100% confidence
            if _pt_lower in ("rbis", "rbi", "runs") and _gwp > 0.58:
                raw_p += 1.0  # home team winning → slightly better RBI/run env
            raw_p += float(prop.get("_streak_adj",  0.0)) * 100.0
            raw_p += float(prop.get("_last10_adj",  0.0)) * 100.0
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
                _game_env_nudge += (_gop2 - 0.50) * 6.0
            if _pt_lower2 in ("rbis", "rbi", "runs") and _gwp2 > 0.58:
                _game_env_nudge += 1.0
            _streak_nudge = float(prop.get("_streak_adj",  0.0)) * 100.0
            _last10_nudge = float(prop.get("_last10_adj",  0.0)) * 100.0
            _fb_adjs = [
                ("bayesian",        float(prop.get("_bayesian_nudge",   0.0)) * 100.0),
                ("cv_consistency",  float(prop.get("_cv_nudge",         0.0)) * 100.0),
                ("form_adj",        float(prop.get("_form_adj",         0.0)) * 100.0),
                ("park_factor",     float(prop.get("_park_factor_adj",  0.0)) * 100.0),
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
    # Feature vector (23 signals) — identical schema at INSERT and predict
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
        _pt_raw     = str(prop.get("prop_type","") or bet.get("prop_type","") if bet else "").lower()
        _is_pitcher = _pt_raw in _PITCHER_PT

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
            # Prop-type-aware: TB props use xbh_per_game (#1 feature, 45% importance)
            # Source: baseball-models feature importance (gmalbert/baseball-predictions)
            _is_tb_prop = _pt_raw in {"total_bases", "home_runs", "hits_runs_rbis",
                                       "fantasy_hitter", "fantasy_score"}

            # slot 0: wRC+ normalized (100=avg → 0.5, 140=elite → 0.7, 70=poor → 0.35)
            k_rate  = _clamp(float(prop.get("wrc_plus", 100.0) or 100.0) / 200.0)

            # slot 1: xbh_per_game for TB/power props (45% feature importance for TB)
            #         ISO for all other batter props
            if _is_tb_prop:
                _xbh = float(prop.get("xbh_per_game", 0.50) or 0.50)
                bb_rate = _clamp(_xbh / 1.50)   # 0=0, 0.50=avg(0.33), 1.0=elite(0.67)
            else:
                bb_rate = _clamp(float(prop.get("iso", 0.155) or 0.155) / 0.35)

            # slot 2: SLG for TB/power props (16% feature importance)
            #         BABIP for all other batter props
            if _is_tb_prop:
                _slg = float(prop.get("slg", 0.405) or 0.405)
                era  = _clamp((_slg - 0.250) / 0.400)   # 0.250=0, 0.405=avg(0.39), 0.650=elite(1.0)
            else:
                era = _clamp((float(prop.get("babip", 0.300) or 0.300) - 0.200) / 0.200)

            # slot 3: batter bb_pct (plate discipline)
            whip    = _clamp(float(prop.get("bb_pct", 0.085) or 0.085) / 0.20)
            # slot 4: batter K% (inverse contact — higher K = worse contact)
            shadow_whiff = _clamp(float(prop.get("k_pct", 0.224) or 0.224) / 0.35)

        # Zone integrity multiplier (pitcher K-props only, 1.0 for batters)
        zone_mult    = _clamp(prop.get("_zone_integrity_mult", 1.0), 0.5, 1.5) / 1.5

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

        # ── Prop type encoding ────────────────────────────────────────
        _pt_map = {"strikeouts": 0, "pitcher_strikeouts": 0,
                   "home_runs": 1, "hr": 1,
                   "hits": 2, "hits_allowed": 2,
                   "rbis": 3, "rbi": 3,
                   "hits_runs_rbis": 4,                   # most common prop — needs unique code
                   "total_bases": 5, "fantasy_score": 5,  # power/fantasy bucket
                  }
        pt_enc = _pt_map.get(str(b.get("prop_type") or prop.get("prop_type", "")).lower(), 6) / 6.0

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

        # ── Confidence encoding ───────────────────────────────────────
        _conf_map = {"low": 0.0, "medium": 0.33, "high": 0.67, "elite": 1.0}
        conf_enc = _conf_map.get(str(b.get("confidence") or "medium").lower(), 0.33)

        # ── Enrichment signal slots (Phase 97) ───────────────────────────────
        # These 7 signals were computed by prop_enrichment_layer and attached to
        # every prop but never fed to XGBoost — now they are.  Normalised [0,1].
        form_adj      = _clamp((float(prop.get("_form_adj",            0.0) or 0.0) + 0.20) / 0.40)  # hot/cold streak
        cv_nudge      = _clamp((float(prop.get("_cv_nudge",            0.0) or 0.0) + 0.15) / 0.30)  # CV consistency
        bayesian_nudge= _clamp((float(prop.get("_bayesian_nudge",      0.0) or 0.0) + 0.15) / 0.30)  # Bayesian update
        marcel_adj    = _clamp((float(prop.get("_marcel_adj",          0.0) or 0.0) + 0.02) / 0.04)  # Marcel ±1.8pp
        predict_plus  = _clamp((float(prop.get("_predict_plus_adj",    0.0) or 0.0) + 0.08) / 0.16)  # Predict+ arsenal
        ps_prob       = _clamp(float(prop.get("_player_specific_prob", 0.0) or 0.0))                  # Poisson/binomial rate
        # Batting order position: leadoff=0.111 (1/9), cleanup=0.444 (4/9),
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
        #    correlation dampening to prevent stacked-signal inflation ──────
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
        # If this prop-type has few settled training rows, shrink model_prob
        # toward the market implied probability. Proven prop-types pass through
        # at full strength; cold-start prop-types are heavily dampened.
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
        # After shrinkage, check how far model_prob departs from the sportsbook's
        # implied probability.  Divergence >20pp → soft-cap (likely data error).
        # Divergence 12-20pp → WIDE flag (monitor).  Logs + stamps prop for audit.
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
        # ev_pct was computed from raw model_p before _build_bet(); recalculate
        # so Kelly / confidence downstream reflect the actual adjusted probability.
        try:
            _side_american = (
                prop.get("over_american",  prop.get("odds_american", -115))
                if side == "OVER"
                else prop.get("under_american", prop.get("odds_american", -115))
            )
            _decimal = (
                (100 / abs(_side_american) + 1) if _side_american < 0
                else (_side_american / 100 + 1)
            )
            _profit  = _decimal - 1
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
            "odds_american":      prop.get("odds_american", -110),
            "model_prob":         round(model_prob, 1),
            "implied_prob":       round(implied_prob, 1),
            "ev_pct":             round(ev_pct, 1),
            "kelly_units":        round(kelly, 3),
            "recommended_platform": platforms[0] if platforms else "PrizePicks",
            "checklist":          self._checklist(prop),
            "confidence":         self._confidence(ev_pct),
            "spring_training":    _is_spring_training(),
            "ts":                 datetime.datetime.utcnow().isoformat(),
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
        dfs = self.hub.get("dfs", {})
        matched = []
        for platform in ("prizepicks", "underdog", "sleeper"):
            picks = dfs.get(platform, [])
            for pick in picks:
                if isinstance(pick, dict):
                    if prop.get("player", "").lower() in str(pick).lower():
                        matched.append(platform.capitalize())
                        break
        return matched or ["PrizePicks"]

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
        if ev_pct >= 15: return 9
        if ev_pct >= 10: return 8
        if ev_pct >= 7:  return 7
        if ev_pct >= 5:  return 6
        if ev_pct >= 3:  return 5
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
                                umpires.append({
                                    "name":      _off.get("official", {}).get("fullName", ""),
                                    "home_team": _g["teams"]["home"]["team"].get("name",""),
                                    "away_team": _g["teams"]["away"]["team"].get("name",""),
                                    "k_rate": 8.8, "run_env": 1.0,
                                })
            except Exception:
                pass

        prop_type = prop.get("prop_type", "")
        if _norm_stat(prop_type) not in self._PITCHER_STATS:
            return None

        # UmpireAgent only needs K props — umpires just confirm game is happening
        # If we got umpires from MLB API we know games are scheduled
        model_prob = self._model_prob(prop.get("player", ""), prop_type, prop=prop)

        # Apply umpire K-rate modifier if we have umpire data
        if umpires:
            k_mod = float(umpires[0].get("k_rate", 8.8)) / 8.8  # vs league avg
            model_prob = min(model_prob * k_mod, 95.0)
        else:
            # No umpire data — still evaluate but without umpire boost
            model_prob = min(model_prob + 3.0, 95.0)

        # FIX: _model_prob returns P(OVER). Flip to P(UNDER) before EV calc.
        under_prob = 100.0 - model_prob
        under_odds = prop.get("under_american", -110)
        implied    = _american_to_implied(under_odds) / 100
        ev_pct     = (under_prob / 100 - implied) / implied
        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, "UNDER", under_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _F5Agent(_BaseAgent):
    name = "F5Agent"

    def evaluate(self, prop: dict) -> dict | None:
        """Pitcher performance props (K, outs, earned runs, hits allowed).
        Renamed from F5Agent — Underdog/PrizePicks don't offer F5 markets.
        Uses SP matchup quality, lineup chase score, and FanGraphs pitcher stats.
        """
        prop_type = _norm_stat(prop.get("prop_type", ""))
        _PITCHER_TARGETS = {"strikeouts", "pitching_outs", "earned_runs",
                            "hits_allowed", "fantasy_pitcher"}
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

        over_odds  = prop.get("over_american", -110)
        implied    = _american_to_implied(over_odds) / 100
        ev_pct     = (model_prob / 100 - implied) / implied
        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, "OVER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _FadeAgent(_BaseAgent):
    name = "FadeAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Fades heavy public action using SportsBettingDime real BET%/MONEY% data.

        Threshold: 65% public tickets on Over → fade signal.
        """
        SBD_THRESHOLD = 65.0
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
            return None  # pandas not available

        pub_pct, signal_src = get_fade_signal(
            player, team, prop_type, game_df, prop_df, threshold=SBD_THRESHOLD
        )

        if pub_pct < SBD_THRESHOLD:
            return None

        model_prob = self._model_prob(player, prop_type, prop=prop)
        fade_boost = 6.0 if signal_src == "player_prop" else 5.0
        fade_prob  = 100 - model_prob + fade_boost
        under_odds = prop.get("under_american", -110)
        implied    = _american_to_implied(under_odds) / 100
        ev_pct     = (fade_prob / 100 - implied) / implied
        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, "UNDER", fade_prob,
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

        # Primary: Action Network sharp_report — uses rlm_signal (AN schema key)
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
        # FIX: determine side from which bucket triggered (over vs under)
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
                    steam = True
                    steam_side = "OVER"   # public piling on Over → steam on Over
                    break
                if under_pct >= 70:
                    steam = True
                    steam_side = "UNDER"  # public piling on Under → steam on Under
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
                     "stolen_bases", "singles", "walks", "runs", "fantasy_score"}

    def evaluate(self, prop: dict) -> dict | None:
        """Targets hitter props when opposing bullpen is fatigued (0-4 scale).
        Fatigue from context_modifiers.BullpenFatigueScorer when available,
        otherwise defaults to neutral (2).
        """
        # Try hub bullpen_fatigue, then context_modifiers live calculation
        fatigue_map: dict = self.hub.get("bullpen_fatigue", {})
        player    = prop.get("player", "")
        team      = prop.get("team", "")
        prop_type = prop.get("prop_type", "")

        if _norm_stat(prop_type) not in self._HITTER_STATS:
            return None

        # Get fatigue for opposing team (batter faces opponent's bullpen).
        # Hub stores {"fatigue_score": float, "boost": float} per team.
        opp_team    = prop.get("opposing_team", "")
        _raw_entry  = fatigue_map.get(opp_team, fatigue_map.get(team, -1))
        if isinstance(_raw_entry, dict):
            fatigue = float(_raw_entry.get("fatigue_score", 2.0))
        else:
            fatigue = float(_raw_entry)  # legacy scalar or -1 sentinel

        # If not in hub, fall back to neutral (BullpenFatigueScorer needs full
        # pitching_logs DataFrame + target_date which aren't available here)
        if fatigue < 0:
            fatigue = 2  # neutral — no fatigue data available this cycle

        model_prob = self._model_prob(player, prop_type, prop=prop)

        # Fatigue boost: tired bullpen = more runs for batters
        if fatigue >= 3:
            model_prob = min(model_prob + 6.0, 95.0)
        elif fatigue >= 2:
            model_prob = min(model_prob + 2.0, 95.0)

        over_odds = prop.get("over_american", -110)
        implied   = _american_to_implied(over_odds) / 100
        ev_pct    = (model_prob / 100 - implied) / implied
        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, "OVER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _WeatherAgent(_BaseAgent):
    name = "WeatherAgent"
    # Note: apply_thermal_correction() from calibration_layer used for HR/total props.
    # Temp data arrives via hub["context"]["weather"] → WeatherAgent enriches game totals.

    def evaluate(self, prop: dict) -> dict | None:
        """Wind/park/temperature combos for power hitter props.
        Wind data from prop dict (set by prop_enrichment_layer) — no hub lookup needed.
        Dome games are skipped (no wind effect indoors).
        """
        # Skip dome games — prop_enrichment_layer sets is_dome
        if prop.get("is_dome"):
            return None

        player    = prop.get("player", "")
        prop_type = prop.get("prop_type", "")
        venue     = prop.get("venue", "")

        # Get wind from enriched prop dict first (fastest), then hub weather list
        wind_mph = float(prop.get("_wind_speed", 0) or 0)
        wind_dir = str(prop.get("_wind_direction", "") or "")

        # Fallback: scan hub weather list
        if wind_mph == 0:
            for w in self.hub.get("context", {}).get("weather", []):
                if not isinstance(w, dict):
                    continue
                team = prop.get("team", "")
                if (venue and venue.lower() in str(w).lower()) or                    (team and team.lower() in str(w.get("team","")).lower()):
                    wind_mph = float(w.get("wind_speed_mph", w.get("wind_speed", 0)) or 0)
                    wind_dir = str(w.get("wind_direction", "") or "")
                    break

        _POWER_PROPS  = {"home_runs", "total_bases", "hits_runs_rbis",
                         "fantasy_hitter", "fantasy_pitcher"}
        _CONTACT_PROPS = {"hits", "rbis", "runs"}

        pt_norm = _norm_stat(prop_type)

        # Wind blowing out ≥10mph → boost power props
        if wind_mph >= 10 and "out" in wind_dir.lower() and pt_norm in _POWER_PROPS:
            model_prob = self._model_prob(player, prop_type, prop=prop)
            # Scale boost: 10mph=+4pp, 15mph=+6pp, 20mph+=+8pp
            boost = min(8.0, (wind_mph - 10) * 0.4 + 4.0)
            model_prob = min(model_prob + boost, 95.0)
            over_odds  = prop.get("over_american", -110)
            implied    = _american_to_implied(over_odds) / 100
            ev_pct     = (model_prob / 100 - implied) / implied
            if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
                return self._build_bet(prop, "OVER", model_prob,
                                       implied * 100, ev_pct * 100)

        # Hot temperature (>85°F) → boost all hitter props
        temp_f = float(prop.get("_temp_f", 72) or 72)
        if temp_f >= 85 and pt_norm in (_POWER_PROPS | _CONTACT_PROPS):
            model_prob = self._model_prob(player, prop_type, prop=prop)
            model_prob = min(model_prob + 3.0, 95.0)
            over_odds  = prop.get("over_american", -110)
            implied    = _american_to_implied(over_odds) / 100
            ev_pct     = (model_prob / 100 - implied) / implied
            if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
                return self._build_bet(prop, "OVER", model_prob,
                                       implied * 100, ev_pct * 100)

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

        # Primary: Action Network RLM flag — uses rlm_signal (actual AN schema key)
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
        side   = "OVER" if model_prob > implied else "UNDER"
        odds   = over_odds if side == "OVER" else prop.get("under_american", -110)
        imp    = _american_to_implied(odds)
        ev_pct = (model_prob / 100 - imp / 100) / (imp / 100)
        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
            return self._build_bet(prop, side, model_prob, imp, ev_pct * 100)
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

        # Direct full-name lookup — try both "Over" (Odds API) and "over" (DraftEdge)
        ref = (
            reference.get((player_norm, prop_type, "Over"))
            or reference.get((player_norm, prop_type, "over"))
        )
        if ref:
            return round(ref["sb_implied_prob"] * 100.0, 2)

        # Last-name fallback: scan for any entry where last token matches
        parts = player_norm.split()
        last = parts[-1] if parts else ""
        for (pn, pt, side), data in reference.items():
            # Accept both "Over" (Odds API) and "over" (DraftEdge) casing
            if side.lower() == "over" and pt == prop_type and pn.split()[-1:] == [last]:
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

    # Rule 1 — duplicate player
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
        # silently drop it, preventing p_conf from defaulting to 5 and blocking
        # the MIN_CONFIDENCE=6 gate for non-EVHunter agents (Bug #15b).
        if "confidence" not in lg:
            lg = {**lg, "confidence": _orig_conf}
        enriched_legs.append(lg)

    # ── Step 2: decide ONE platform for the whole parlay ─────────────────────
    # Priority 1 — Underdog Streaks rules (Phase 112):
    #   • No active streak (count == 0):  need 2 legs BOTH clearing pick-2 hurdle (0.5774)
    #   • Active streak (count >= 2):     need 1 leg clearing pick-1 hurdle (0.5336)
    #   • Max streak: 11 (1000x)

    _ud_streak_count = 0
    try:
        _sc = _pg_conn()
        with _sc.cursor() as _scc:
            _scc.execute("SELECT current_count FROM ud_streak_state LIMIT 1")
            _srow = _scc.fetchone()
            if _srow:
                _ud_streak_count = int(_srow[0])
        _sc.close()
    except Exception:
        pass

    if _ud_streak_count == 0:
        _streak_phase       = "pick-2"
        _streak_legs_needed = 2
    else:
        _streak_phase       = "pick-1"
        _streak_legs_needed = 1

    _qualifying_streak_legs = [
        lg for lg in enriched_legs
        if check_streaks_gate(
            min(0.95, max(0.05, lg.get("model_prob", 0.0) / 100)),  # Fix 30: default 0 → fails MIN_PROB gate explicitly
            phase=_streak_phase,
        )[0]
    ]
    has_ud_streak = len(_qualifying_streak_legs) >= _streak_legs_needed

    if has_ud_streak:
        enriched_legs     = _qualifying_streak_legs[:_streak_legs_needed]
        parlay_platform   = "underdog"
        entry_type_forced = "STREAKS"
    else:
        # Priority 2 — tiebreaker: PrizePicks wins only when EVERY leg
        #              independently voted PrizePicks (line_comparator returns
        #              PrizePicks on tied lines).  Any Underdog vote → Underdog.
        pp_votes = sum(
            1 for lg in enriched_legs
            if "prize" in lg.get("recommended_platform", "Underdog").lower()
        )
        parlay_platform   = "prizepicks" if pp_votes == len(enriched_legs) else "underdog"
        entry_type_forced = None

    # ── Step 3: build the single parlay ──────────────────────────────────────
    n      = len(enriched_legs)
    probs  = [min(0.95, max(0.05, l.get("model_prob", 0.0) / 100)) for l in enriched_legs]  # Fix 30: default 0 → fails MIN_PROB gate explicitly
    p_conf = round(sum(l.get("confidence", 5) for l in enriched_legs) / max(n, 1), 1)

    if parlay_platform == "underdog":
        entry_type  = entry_type_forced or "STANDARD"
        combined_ev = 0.0
        try:
            from underdog_math_engine import UnderdogMathEngine  # noqa: PLC0415
            _engine = UnderdogMathEngine()
            _eval   = _engine.evaluate_slip(probs)
            combined_ev = round(_eval.recommended_ev * 100, 2)
            if not entry_type_forced:
                entry_type = _eval.recommended_entry_type
        except Exception:
            _UD_MULTS = {2: 3.5, 3: 6.0, 4: 10.0, 5: 20.0}
            mult = _UD_MULTS.get(n, 3.5)
            combined_ev = round((math.prod(probs) * mult - 1) * 100, 2)
    else:  # PrizePicks — Power mode
        entry_type  = "POWER"
        _PP_MULTS   = {2: 3.0, 3: 6.0, 4: 10.0}
        mult        = _PP_MULTS.get(n, 3.0)
        combined_ev = round((math.prod(probs) * mult - 1) * 100, 2)

    return [{
        "agent":           agent_name,
        "agent_name":      agent_name,
        "legs":            enriched_legs,
        "leg_count":       n,
        "entry_type":      entry_type,
        "combined_ev_pct": combined_ev,
        "ev_pct":          combined_ev,
        "stake":           10.0,
        "confidence":      p_conf,
        "platform":        parlay_platform,
        "season_stats":    {},
        "ts":              datetime.datetime.utcnow().isoformat(),
    }]


def _build_agent_parlays(hits: list[dict], agent_name: str,
                          min_legs: int = 2, max_legs: int = 3,
                          max_parlays: int = 3) -> list[dict]:
    """
    Build 2-leg and 3-leg Underdog slips for one specific agent from its own
    hit list.  Avoids same-player correlation.  Returns up to max_parlays
    slips sorted by combined EV descending, each branded with agent_name.
    """
    if len(hits) < min_legs:
        return []

    top = sorted(hits, key=lambda x: x["ev_pct"], reverse=True)[:10]
    parlays: list[dict] = []
    seen: set[str] = set()

    for i in range(len(top)):
        if len(parlays) >= max_parlays:
            break
        for j in range(i + 1, len(top)):
            if len(parlays) >= max_parlays:
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
                            parlays.extend(_make_parlay(three, agent_name))
                        break

            key2 = "|".join(sorted(
                f"{lg.get('player','')}:{lg.get('prop_type','')}:{lg.get('side','')}"
                for lg in two
            ))
            if key2 not in seen:
                seen.add(key2)
                parlays.extend(_make_parlay(two, agent_name))

    return sorted(parlays, key=lambda x: x["combined_ev_pct"], reverse=True)[:max_parlays * 2]


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
            if chase_adj < 0.03:  # opponent isn't a high-chase lineup
                return None
        model_prob = self._model_prob(prop.get("player", ""), prop_type, prop=prop)
        over_odds  = prop.get("over_american", -115)
        implied    = _american_to_implied(over_odds)
        ev_pct     = (model_prob / 100 - implied / 100) / (implied / 100) * 100
        if ev_pct >= MIN_EV_THRESH_PCT:  # FIX: percent-scale EV gate (was always True vs 0.03)
            return self._build_bet(prop, "OVER", model_prob, implied, ev_pct)
        return None


class _StackSmithAgent(_BaseAgent):
    """Builds same-game team stacks.
    Targets multiple batters from the same lineup against a fatigued bullpen
    or a pitcher with poor zone metrics.  Fires on batter props only.
    """
    name = "StackSmithAgent"

    _BATTER_TYPES = {"hits", "total_bases", "home_runs", "rbis", "runs_scored", "singles"}

    def evaluate(self, prop: dict) -> dict | None:
        prop_type = prop.get("prop_type", "").lower()
        if prop_type not in self._BATTER_TYPES:
            return None

        # Stack signal: look up the OPPOSING pitcher from projected_starters
        # (prop is a batter prop — it carries batter stats, not pitcher stats)
        opp_team = prop.get("opposing_team", "")
        era    = 4.20  # league-average default
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
                era    = float(_sp_fg.get("era",    _sp_fg.get("xfip",   4.20)) or 4.20)
                k_rate = float(_sp_fg.get("k_rate", _sp_fg.get("k_pct",  0.22)) or 0.22)
            except Exception:
                pass

        # Also check bullpen fatigue as a secondary signal
        bp_fatigue = self.hub.get("bullpen_fatigue", {})
        opp_fatigue = bp_fatigue.get(opp_team.lower(), {})
        fatigue_score = float(opp_fatigue.get("fatigue_score", 2.0) if isinstance(opp_fatigue, dict) else 2.0)

        # Fire when opposing pitcher is weak OR bullpen is fatigued
        weak_pitcher = era > 4.50 or k_rate < 0.20
        tired_pen    = fatigue_score >= 3.0
        if not weak_pitcher and not tired_pen:
            return None
        model_prob = self._model_prob(prop.get("player", ""), prop_type, prop=prop)
        over_odds  = prop.get("over_american", -115)
        implied    = _american_to_implied(over_odds)
        ev_pct     = (model_prob / 100 - implied / 100) / (implied / 100) * 100
        if ev_pct >= MIN_EV_THRESH_PCT:  # FIX: percent-scale EV gate (was always True vs 0.03)
            return self._build_bet(prop, "OVER", model_prob, implied, ev_pct)
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
        # for a player match rather than expecting a player-keyed dict.
        pub_over   = 50.0
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

        # Fade if public is piling on overs (>68%) — contrarian under edge
        if pub_over > 68:
            model_prob = self._model_prob(player, prop_type, prop=prop)
            under_odds = prop.get("under_american", -115)
            implied    = _american_to_implied(under_odds)
            under_prob = 100.0 - model_prob
            ev_pct     = (under_prob / 100 - implied / 100) / (implied / 100) * 100
            if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):
                # FIX: pass under_prob (not model_prob) so bet_ledger stores P(UNDER)
                # and Brier/XGBoost retraining uses the correct outcome probability.
                return self._build_bet(prop, "UNDER", under_prob, implied, ev_pct)
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
                return self._build_bet(prop, sharp_side, model_prob, implied, ev_pct)

        # ── Path 2: game-level RLM from Action Network ────────────────────────
        # If no player-level entry, use game-level ticket%/money% divergence.
        # Only fires on batter props (hits, TB, RBIs, runs, h+r+rbi) where
        # the game total direction implies a scoring environment mismatch.
        an_sentiment = market.get("an_game_sentiment", {})
        team = prop.get("_team", prop.get("team", "")).lower()
        game_ctx = an_sentiment.get(team, {})

        if not game_ctx or not game_ctx.get("rlm_signal"):
            return None

        # Only apply to batter props — game total RLM doesn't inform pitcher Ks
        _BATTER_PROPS = {"hits", "total_bases", "rbis", "runs", "hits_runs_rbis",
                         "fantasy_score", "singles"}
        if prop_type not in _BATTER_PROPS:
            return None

        rlm_dir = game_ctx.get("rlm_direction", "")  # "over" or "under"
        if not rlm_dir:
            return None

        # Sharp money on UNDER game total → batter props less favorable (LOWER)
        # Sharp money on OVER game total  → batter props more favorable (HIGHER)
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

        # Apply a signal-strength discount — game-level signal is weaker than player-level
        adjusted_prob = prob_side * (0.85 + 0.15 * signal_strength)
        ev_pct = (adjusted_prob / 100 - implied / 100) / (implied / 100) * 100

        ev_threshold = _get_ev_threshold(prop.get("_sim_edge_reasons", []))
        if ev_pct >= ev_threshold + 1.5:  # require +1.5pp extra EV for game-level signal
            return self._build_bet(prop, sharp_side, model_prob, implied, ev_pct)

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
    Excludes props on the excluded list (stolen_bases, home_runs, walks, walks_allowed).
    """
    name = "LineDriftAgent"

    # Minimum drift between sharp implied and platform implied to consider firing
    DRIFT_MIN: float = 4.0    # 4 percentage points (both sides in 0-100 scale)
    # Line gap bonus: if DFS line is 0.25+ lower than sportsbook line → small EV bonus
    LINE_GAP_BONUS: float = 1.5   # added to ev_pct when gap favors Over

    _EXCLUDED = {"stolen_bases", "home_runs", "walks", "walks_allowed"}

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

        # Core drift signal: sharp books vs DFS platform — both percentage scale
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
            return self._build_bet(prop, "OVER", model_prob, implied_pct, ev_pct)
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
        if chase_adj < 0.04:  # only high-chase lineups
            return None
        model_prob  = self._model_prob(prop.get("player", ""), prop_type, prop=prop)
        model_prob  = min(95.0, model_prob + chase_adj * 120)
        over_odds   = prop.get("over_american", -115)
        implied     = _american_to_implied(over_odds)
        ev_pct      = (model_prob / 100 - implied / 100) / (implied / 100) * 100
        if ev_pct >= MIN_EV_THRESH_PCT:  # FIX: percent-scale EV gate (was always True vs 0.03)
            return self._build_bet(prop, "OVER", model_prob, implied, ev_pct)
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
            return self._build_bet(prop, side, model_prob, implied, ev_pct)
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
            return self._build_bet(prop, side, model_prob, implied, ev_pct)
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
                "k_rate":           _fg_pitcher.get("k_pct",  _fg_pitcher.get("k_rate",  0.22)),
                "bb_rate":          _fg_pitcher.get("bb_pct", _fg_pitcher.get("bb_rate", 0.08)),
                "era":              _fg_pitcher.get("era",    4.0),
                "whip":             _fg_pitcher.get("whip",   1.3),
            })
        if props:
            logger.info("[AgentTasklet] Using %d PrizePicks props from hub", len(props))
            return props

    # 2. Underdog from hub — combined with PrizePicks if both available
    ud_props = _extract_underdog_props(hub)
    if ud_props:
        props = []
        props.extend(ud_props)
        logger.info("[AgentTasklet] Underdog: %d props", len(ud_props))

        # Add PrizePicks on top (deduped)
        pp_picks = hub.get("dfs", {}).get("prizepicks", [])
        if pp_picks and isinstance(pp_picks, list):
            seen = {(p["player"].lower(), p["prop_type"]) for p in props}
            pp_added = 0
            for pick in pp_picks:
                if not isinstance(pick, dict):
                    continue
                player    = pick.get("player_name", pick.get("player", pick.get("name", "")))
                prop_type = _norm_stat(pick.get("stat", pick.get("stat_type", pick.get("prop_type", ""))))
                line      = pick.get("line", pick.get("line_score", pick.get("value", 1.5)))
                if not player or not prop_type:
                    continue
                key = (player.lower(), prop_type)
                if key in seen:
                    continue
                seen.add(key)
                props.append({
                    "player":         player,
                    "player_name":    player,
                    "prop_type":      prop_type,
                    "line":           float(line or 1.5),
                    "over_american":  int(pick.get("over_american", pick.get("over_odds", -115)) or -115),
                    "under_american": int(pick.get("under_american", pick.get("under_odds", -115)) or -115),
                    "team":           pick.get("player_team", pick.get("team", "")),
                    "venue":          "",
                    "platform":       "prizepicks",
                })
                pp_added += 1
            if pp_added:
                logger.info("[AgentTasklet] PrizePicks: %d props added", pp_added)
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

    # ── Send-window clock gate — only dispatch picks 9:00–10:00 AM PT ──────────
    _pt_now = _entry_now
    if not (9 <= _pt_now.hour < 10):
        logger.info("[AgentTasklet] Outside 9–10 AM PT send window (%02d:%02d PT) — skipping cycle.",
                    _pt_now.hour, _pt_now.minute)
        return

    # ── Game-state time gate — skip cycles when no MLB action is live/upcoming ──
    # Avoids burning API quota, writing empty bet_ledger rows, and spamming logs
    # at 3 AM when there are no games. Uses hub game_states (set by DataHubTasklet)
    # rather than a hardcoded clock check so rain delays and doubleheaders are handled.
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

    # Enrich all props with FanGraphs, weather, Bayesian, CV, form, park context
    # This populates the fields _build_feature_vector() reads (k_rate, shadow_whiff, etc.)
    import datetime as _dt
    props = _enrich_props(props, hub, season=_today_pt().year)

    # Phase 112: remove prop types not evaluated (user directive)
    _EXCLUDED_PROP_TYPES = {
        "stolen_bases", "home_runs", "sb", "hr",
        "walks", "bb", "bases_on_balls",
        "walks_allowed",
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

    all_parlays: list[dict] = []

    for cls in _AGENT_CLASSES:
        agent      = cls(hub, model)
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
                if sharp_prob is not None:
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

                bet["underdog_line"] = prop.get("underdog_line",
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
    # Fingerprint = frozenset of (player, prop_type, side) tuples across all legs
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

    # ── Per-agent daily gate — each agent sends AT MOST ONE play per calendar day ──
    # Uses in-memory dict _AGENT_SENT_TODAY (agent → "YYYY-MM-DD") as primary
    # gate so it works with or without Redis.  Redis is also written as a
    # cross-process backup (e.g. multiple Railway replicas).
    today_str  = _today_pt().isoformat()   # Pacific Time date
    r_dedup    = _redis()

    # ── DB-backed dedup preload — survives crash + Redis cold restart ──────────
    # On cycle start, restore _AGENT_SENT_TODAY from bet_ledger for today so
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
        if _preloaded:
            logger.info(
                "[AgentTasklet] Dedup preload — these agents already sent today"
                " and will be blocked this cycle: %s", _preloaded
            )
    except Exception as _dbe:
        logger.debug("[AgentTasklet] dedup preload skipped: %s", _dbe)
    _DAY_TTL   = 25 * 3600   # 25 h — expires safely after midnight

    # ── One play per agent per day — hard gate ───────────────────────────────────────
    # Claim the slot IMMEDIATELY when iterating so that multiple parlays from
    # the same agent in a single 30-second cycle cannot all slip through before
    # any lock is written.  Only the highest-EV parlay per agent advances.
    best_per_agent: dict = {}   # agent_name -> (ev, parlay, r_daily_key)
    _blocked_sent_today: list = []   # tracks which agents hit the daily gate (for split logging)
    for parlay in all_parlays:
        agent_name = parlay.get("agent", "unknown")

        # In-memory gate (primary — works without Redis)
        if _AGENT_SENT_TODAY.get(agent_name) == today_str:
            logger.info("[AgentTasklet] %s already sent today (in-memory) — skipping.", agent_name)
            _blocked_sent_today.append(agent_name)
            continue

        # Redis gate (secondary — cross-process guard)
        r_daily_key = f"agent_sent:{agent_name}:{today_str}"
        try:
            if r_dedup.exists(r_daily_key):
                _AGENT_SENT_TODAY[agent_name] = today_str   # sync in-memory
                logger.info("[AgentTasklet] %s already sent today (Redis) — skipping.", agent_name)
                _blocked_sent_today.append(agent_name)
                continue
        except Exception:
            pass   # Redis down — in-memory gate is sufficient

        # Confidence gate — MIN_CONFIDENCE minimum, nothing lower reaches Discord
        play_conf = parlay.get("confidence", 0)
        if play_conf < MIN_CONFIDENCE:
            logger.info("[AgentTasklet] %s confidence %.1f < min %.0f — dropped.",
                         agent_name, play_conf, MIN_CONFIDENCE)
            continue

        # Probability gate — every leg must have model_prob >= MIN_PROB (57%)
        # MIN_PROB is stored as fraction (0.57); model_prob is stored as percentage (57.0)
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
    # Highest-EV agent locks direction for each (player, stat) pair this cycle.
    # Any other agent wanting the opposite direction is dropped — prevents two
    # agents sending contradictory picks (e.g. one UNDER and one OVER on the
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
        # ── Claim all three dedup stores BEFORE sending ──────────────────────
        # Order matters: in-memory → Redis → DB → Discord.
        # If the process crashes after the DB commit but before Discord fires,
        # the next restart's dedup preload will find discord_sent=TRUE and block
        # the re-send. This is intentional — a missed send is better than a
        # duplicate send spamming subscribers every restart.
        _AGENT_SENT_TODAY[agent_name] = today_str        # 1. in-memory (instant)
        try:
            r_dedup.setex(r_daily_key, _DAY_TTL, "1")   # 2. Redis (cross-process)
        except Exception:
            pass
        try:                                              # 3. DB commit (crash-safe)
            _pg2 = _pg_conn()
            with _pg2.cursor() as _c2:
                _send_today = _today_pt().isoformat()
                for _sl in parlay.get("legs", []):
                    _c2.execute(
                        """
                        INSERT INTO bet_ledger
                            (player_name, prop_type, line, side, odds_american,
                             kelly_units, model_prob, ev_pct, agent_name,
                             status, bet_date, platform, features_json,
                             units_wagered, mlbam_id, entry_type, discord_sent)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                                'OPEN', %s, %s, %s,
                                ABS(%s), %s, %s, TRUE)
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
                        ),
                    )
            _pg2.commit()
            _pg2.close()
            logger.info("[AgentTasklet] discord_sent=TRUE inserted %d legs for %s.",
                        len(parlay.get("legs", [])), agent_name)
        except Exception as _dbe2:
            logger.warning("[AgentTasklet] bet_ledger send-time INSERT failed for %s: %s — "
                           "duplicate send on restart is possible.", agent_name, _dbe2)

        try:
            discord_alert.send_parlay_alert(parlay)      # 4. Discord (fires last)

            # Record parlay in propiq_season_record so nightly_recap.py can settle it
            # Without this, recap always shows "No parlays sent today" even when plays fire
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
                    stake=5.0,
                    legs=_legs_for_record,
                )
                logger.info("[AgentTasklet] Parlay recorded in season_record for %s (%s)",
                            agent_name, today_str)
            except Exception as _sr_err:
                logger.warning("[AgentTasklet] season_record insert skipped: %s", _sr_err)
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

    # ── Feature padding: pad older 20-feature records to current 27-feature schema ──
    _TARGET_FEATS = 27
    _raw_feats = [json.loads(r[0]) for r in rows]
    _padded    = [
        f + [0.0] * (_TARGET_FEATS - len(f)) if len(f) < _TARGET_FEATS
        else f[:_TARGET_FEATS]
        for f in _raw_feats
    ]
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
        "ts":          datetime.datetime.utcnow().isoformat(),
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
    # After every weekly retrain, update per-tag EV thresholds so edge types
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
    # games (ending ~10:30 PM PT) have complete ESPN boxscores available.
    _yesterday = (_today_pt() - datetime.timedelta(days=1))
    today      = _yesterday.strftime("%Y-%m-%d")   # used as grade_date throughout
    espn_date  = _yesterday.strftime("%Y%m%d")     # ESPN format

    # Use ESPN box score scraper (same source as nightly_recap.py)
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
    # Map ESPN stat dict to the format _get_stat() expects
    stat_lookup: dict[str, dict] = {}
    for name_lower, espn in raw_stats.items():
        # Normalise to title case for _get_stat key matching
        display_name = espn.get("full_name", name_lower.title())
        # Accent-normalized key for players like Acuña, Peña, Báez
        import unicodedata as _ud
        _accent_norm = _ud.normalize("NFD", display_name)
        _ascii_name  = "".join(c for c in _accent_norm if _ud.category(c) != "Mn")
        mapped = {
            "Hits":           espn.get("hits", 0.0),
            "HomeRuns":       espn.get("home_runs", 0.0),
            "RunsBattedIn":   espn.get("rbis", espn.get("rbi", 0.0)),
            "Runs":           espn.get("runs", 0.0),
            "StolenBases":    espn.get("stolen_bases", 0.0),
            "TotalBases":     espn.get("total_bases", 0.0),
            "Walks":          espn.get("base_on_balls", 0.0),
            "Strikeouts":     espn.get("strikeouts", 0.0),
            "InningsPitched": espn.get("innings_pitched", 0.0),
            "EarnedRuns":     espn.get("earned_runs", 0.0),
            "HitsAllowed":    espn.get("hits_allowed", 0.0),
            "WalksAllowed":   espn.get("base_on_balls", 0.0),
        }
        stat_lookup[display_name] = mapped
        stat_lookup[name_lower]   = mapped   # lowercase index
        stat_lookup[_ascii_name]  = mapped   # accent-stripped index (Acuña → Acuna)
        stat_lookup[_ascii_name.lower()] = mapped   # accent-stripped lowercase
        # Hyphen-normalized keys (ESPN uses "Crow-Armstrong"; PP/UD may store "Crow Armstrong")
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
                       COALESCE(entry_type, 'STANDARD') AS entry_type
                FROM bet_ledger
                WHERE status = 'OPEN' AND bet_date = %s AND discord_sent = TRUE
                """,
                (today,),
            )
            open_bets = cur.fetchall()
        conn.close()
    except Exception as e:
        logger.warning("[GradingTasklet] Postgres read error: %s", e)
        return

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
                _grade_mlbam  = row_data[11] if len(row_data) > 11 else None
                _entry_type   = row_data[12] if len(row_data) > 12 else "STANDARD"

                # Grade by mlbam_id when available — accent-safe, always unique
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
                # stat_lookup is name-keyed (ESPN doesn't embed mlbam_id).
                # Use MLB Stats API to resolve mlbam_id -> canonical name,
                # then look up that name in stat_lookup.
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
                units  = float(units or 1)

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
                # CLV = model edge over closing line, both in percentage scale
                clv = round(float(model_prob or 50) - _american_to_implied(int(closing_odds or -110)), 4)

                # actual_outcome: 1=WIN, 0=LOSS (used by XGBoost retraining)
                actual_outcome = 1 if status == "WIN" else 0 if status == "LOSS" else None
                cur.execute(
                    """
                    UPDATE bet_ledger
                    SET status = %s, profit_loss = %s, actual_result = %s,
                        clv = %s, graded_at = NOW(), actual_outcome = %s
                    WHERE id = %s
                    """,
                    (status, round(pl, 4), actual, round(clv, 2),
                     actual_outcome, bid),
                )

                results.append({
                    "id": bid, "player": player, "prop_type": ptype,
                    "line": line, "side": side, "actual": actual,
                    "status": status, "profit_loss": round(pl, 4),
                    "clv": round(clv, 2), "agent": agent,
                    "odds_american": int(odds or -110),
                    "entry_type": _entry_type,
                })

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("[GradingTasklet] Grading error: %s", e, exc_info=True)
        return

    if not results:
        logger.info("[GradingTasklet] All open bets still in-progress.")
        return

    total_profit = sum(r["profit_loss"] for r in results)
    wins   = sum(1 for r in results if r["status"] == "WIN")
    losses = sum(1 for r in results if r["status"] == "LOSS")
    pushes = sum(1 for r in results if r["status"] == "PUSH")

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

    # Sync results into propiq_season_record so /propiq/record endpoint has data
    try:
        conn2 = _pg_conn()
        with conn2.cursor() as cur2:
            # Upsert a daily summary row per agent
            from collections import defaultdict
            _agent_results: dict = defaultdict(lambda: {"wins":0,"losses":0,"pushes":0,"profit":0.0})
            for r in results:
                _ag = r.get("agent", "Unknown")
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
                    SELECT %s, %s, %s, 'mixed', 5.00, %s, 0.0, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM propiq_season_record
                        WHERE date = %s AND agent_name = %s AND status != 'PENDING'
                    )
                """, (
                    today, _ag,
                    _stats["wins"] + _stats["losses"] + _stats["pushes"],
                    round(5.0 + _stats["profit"], 2),
                    _status,
                    json.dumps({"wins": _stats["wins"], "losses": _stats["losses"],
                                "pushes": _stats["pushes"]}),
                    datetime.datetime.utcnow().isoformat(),
                    today, _ag,
                ))
        conn2.commit()
        conn2.close()
        logger.info("[GradingTasklet] Synced %d agents to propiq_season_record", len(_agent_results))
    except Exception as _sync_err:
        logger.debug("[GradingTasklet] Season record sync failed: %s", _sync_err)

    # ── Phase 89: Update agent tier ladder + build progress messages ──────
    # Requirements: need 3 consecutive W or L before tier moves.
    # Progress is shown in Discord after every result so user always sees
    # where each agent stands (e.g. "2/3 wins → Tier 2").
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
            results, total_profit, today,
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
            # Brier score: mean((model_prob/100 - outcome)^2) across graded bets
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
                        _brier_probs = {row[0]: float(row[1] or 52) / 100
                                        for row in _bcur.fetchall()}
                _bc.close()
            except Exception:
                pass

            brier_inputs = []
            for r in results:
                if r["status"] == "PUSH":
                    continue   # PUSH is not a WIN/LOSS — exclude from Brier
                outcome = 1 if r["status"] == "WIN" else 0
                prob    = _brier_probs.get(r.get("id"), 0.52)
                brier_inputs.append({"prob": prob, "outcome": outcome})
            if brier_inputs:
                from calibration_layer import calculate_brier_score  # noqa: PLC0415
                brier = calculate_brier_score(brier_inputs)
                if brier is not None:
                    record_brier(brier)
                    logger.info("[GradingTasklet] Brier score recorded: %.4f", brier)
        except Exception as _brier_err:
            logger.debug("[GradingTasklet] Brier record failed: %s", _brier_err)


def _get_stat(stats: dict, prop_type: str, platform: str = "prizepicks") -> float | None:
    """Map prop_type string to SportsData.io stat field."""
    mapping = {
        # Normalised lowercase keys (current pipeline)
        "hits":          "Hits",
        "home_runs":     "HomeRuns",
        "rbis":          "RunsBattedIn",
        "rbi":           "RunsBattedIn",
        "runs":          "Runs",
        "stolen_bases":  "StolenBases",
        "total_bases":   "TotalBases",
        "walks":         "Walks",
        "strikeouts":    "Strikeouts",
        "earned_runs":   "EarnedRuns",
        "hits_allowed":  "HitsAllowed",
        "walks_allowed": "WalksAllowed",
        "pitching_outs": "InningsPitched",
        "outs_recorded": "__outs_recorded__",  # computed below
        "hits_runs_rbis": "__composite__",      # computed below
        "fantasy_score":  "__fantasy_score__",  # computed below
        # Legacy uppercase abbreviations (fallback)
        "h":  "Hits",    "hr": "HomeRuns",  "r":  "Runs",
        "sb": "StolenBases", "tb": "TotalBases",
        "bb": "Walks",   "k":  "Strikeouts",
        # _norm_stat aliases that must round-trip through grading
        "ks":           "Strikeouts",       # alternate K abbreviation
        "er":           "EarnedRuns",       # alternate earned_runs abbreviation
        "p_outs":       "InningsPitched",   # alternate pitching_outs abbreviation
        "fantasy_pts":  "__fantasy_score__", # alternate fantasy_score label
    }
    prop_key = prop_type.lower().strip()
    # Strip common prefixes
    for prefix in ("over_", "under_", "o_", "u_"):
        if prop_key.startswith(prefix):
            prop_key = prop_key[len(prefix):]

    field = mapping.get(prop_key)
    if field == "__outs_recorded__":
        # IP stored as e.g. 6.2 = 6 innings 2 outs (NOT 6.67)
        ip = stats.get("InningsPitched")
        if ip is not None:
            ip = float(ip)
            full = int(ip)
            partial = round((ip % 1) * 10)  # 6.2 → partial=2
            return float(full * 3 + partial)
        return None
    if field == "__composite__":
        # H + R + RBI composite
        h   = stats.get("Hits",          stats.get("H", 0)) or 0
        r   = stats.get("Runs",          stats.get("R", 0)) or 0
        rbi = stats.get("RunsBattedIn",  stats.get("RBI", 0)) or 0
        return float(h) + float(r) + float(rbi)
    if field == "__fantasy_score__":
        # Official platform scoring tables (2026)
        # PrizePicks Pitcher:  K×3, Out×1, W×6, QS×4, ER×-3
        # Underdog  Pitcher:   K×3, IP×3,  W×5, QS×5, ER×-3
        # PrizePicks Hitter:   1B×3, 2B×5, 3B×8, HR×10, R×2, RBI×2, BB×2, HBP×2, SB×5
        # Underdog  Hitter:    1B×3, 2B×5, 3B×8, HR×10, R×2, RBI×2, BB×3, HBP×3, SB×4, CS×-2
        plat = (platform or "prizepicks").lower()
        k  = stats.get("Strikeouts")
        ip = stats.get("InningsPitched")
        er = stats.get("EarnedRuns")
        if k is not None and ip is not None and er is not None:
            # Pitcher fantasy score
            k  = float(k  or 0)
            ip = float(ip or 0)
            er = float(er or 0)
            w  = float(stats.get("Wins") or stats.get("Win") or 0)
            qs = float(stats.get("QualityStart") or 0)
            if plat == "prizepicks":
                # Outs = floor(ip)*3 + tenths digit (6.2 IP = 20 outs)
                full, frac = divmod(ip, 1)
                outs = int(full) * 3 + round(frac * 10)
                return round(k * 3 + outs * 1 + w * 6 + qs * 4 + er * (-3), 2)
            else:  # underdog
                return round(k * 3 + ip * 3 + w * 5 + qs * 5 + er * (-3), 2)
        # Hitter fantasy score
        h   = float(stats.get("Hits")          or stats.get("H")   or 0)
        rn  = float(stats.get("Runs")          or stats.get("R")   or 0)
        rbi = float(stats.get("RunsBattedIn")  or stats.get("RBI") or 0)
        hr  = float(stats.get("HomeRuns")      or stats.get("HR")  or 0)
        db  = float(stats.get("Doubles")       or stats.get("2B")  or 0)
        tb3 = float(stats.get("Triples")       or stats.get("3B")  or 0)
        sb  = float(stats.get("StolenBases")   or stats.get("SB")  or 0)
        bb  = float(stats.get("Walks")         or stats.get("BB")  or 0)
        hbp = float(stats.get("HitByPitch")    or stats.get("HBP") or 0)
        cs  = float(stats.get("CaughtStealing")or stats.get("CS")  or 0)
        if h == 0 and rn == 0 and rbi == 0:
            return None   # insufficient data — settlement marks pending
        singles = max(0.0, h - db - tb3 - hr)
        if plat == "prizepicks":
            fs = (singles*3 + db*5 + tb3*8 + hr*10 +
                  rn*2 + rbi*2 + bb*2 + hbp*2 + sb*5)
        else:  # underdog
            fs = (singles*3 + db*5 + tb3*8 + hr*10 +
                  rn*2 + rbi*2 + bb*3 + hbp*3 + sb*4 + cs*(-2))
        return round(fs, 2)
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
                SELECT features_json, actual_outcome, graded_at
                FROM bet_ledger
                WHERE graded_at IS NOT NULL
                  AND features_json IS NOT NULL
                  AND actual_outcome IS NOT NULL
                  AND discord_sent = TRUE
                  AND (lookahead_safe IS NULL OR lookahead_safe = TRUE)
                ORDER BY graded_at DESC
                LIMIT 20000
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
    _raw_feats = [json.loads(r[0]) for r in rows]
    _padded    = [
        f + [0.0] * (_TARGET_FEATS - len(f)) if len(f) < _TARGET_FEATS
        else f[:_TARGET_FEATS]
        for f in _raw_feats
    ]
    X = np.array(_padded, dtype=np.float32)
    y = np.array([int(r[1]) for r in rows], dtype=np.int8)

    # ── Recency decay: recent bets matter more than old ones ──────────────
    # weight = e^(-0.01 × days_ago)
    # Last week ≈ 0.93 | 30 days ≈ 0.74 | 90 days ≈ 0.41 | Opening Day ≈ 0.16
    now_utc = datetime.datetime.utcnow()
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
    # pickle.dump would save as sklearn wrapper — incompatible with xgb.Booster.load_model()
    try:
        model.get_booster().save_model(model_path)
        logger.info("[XGBoostTasklet] Saved model as XGBoost JSON to %s", model_path)
    except Exception:
        # Fallback to pickle for sklearn wrapper models
        with open(model_path.replace(".json", ".pkl"), "wb") as f:
            pickle.dump(model, f)
        logger.info("[XGBoostTasklet] Saved model as pickle (JSON save failed)")

    r = _redis()
    r.setex("xgb_meta", 604800, json.dumps({
        "ts":            datetime.datetime.utcnow().isoformat(),
        "accuracy":      round(accuracy, 4),
        "n_train":       len(X_train),
        "n_test":        len(X_test),
        "model_path":    model_path,
        "target_accuracy": 0.842,
        "passed":        accuracy >= 0.777,
    }))

    # ── Phase 91 Step 5: Cache per-prop-type sample counts for thin-data shrinkage ──
    # Agents read this at bet-evaluation time to know how much to trust the model
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
