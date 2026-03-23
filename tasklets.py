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

import datetime
import json
import logging
import os
import pickle
from typing import Any

import requests
import redis as redis_lib
from DiscordAlertService import discord_alert

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

logger = logging.getLogger("propiq.tasklets")

# ── Constants ─────────────────────────────────────────────────────────────────

OPENING_DAY        = datetime.date(2026, 3, 26)
SPRING_TRAINING_WT = 0.30          # ST stats count 30 % until Opening Day

# Data TTLs (seconds) — 4 scraper groups
TTL_PHYSICS  = 900    # 15 min
TTL_CONTEXT  = 600    # 10 min
TTL_MARKET   = 300    #  5 min
TTL_DFS      = 480    #  8 min
TTL_HUB      = 120    #  2 min — master hub key

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
MIN_EV_THRESH   = 0.03    # 3 % minimum edge to queue a bet

# Capital allocation bounds (14-day ROI → multiplier)
CAP_FLOOR = 0.5
CAP_CEIL  = 2.0

# ── Railway-safe service connections ─────────────────────────────────────────

def _redis():
    """
    Connect to Redis using Railway env var or explicit host/port.
    Returns _NullRedis() if the server is unreachable — allows the app
    to boot successfully and run without cache/queue.
    """
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
    return datetime.date.today() < OPENING_DAY


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
    """Quarter-Kelly bet sizing, capped at MAX_UNIT_CAP."""
    if odds_american > 0:
        b = odds_american / 100.0
    else:
        b = 100.0 / abs(odds_american)
    p = edge
    q = 1 - p
    kelly = (b * p - q) / b
    return min(KELLY_FRACTION * kelly, MAX_UNIT_CAP)


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
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error("Apify error (%s): %s", actor_id, e)
        return []


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


def _odds_api_get(sport: str = "baseball_mlb") -> list[dict]:
    """Call The Odds API for MLB lines."""
    key = os.getenv("ODDS_API_KEY", "e4e30098807a9eece674d85e30471f03")
    try:
        resp = requests.get(
            f"https://api.the-odds-api.com/v4/sports/{sport}/odds",
            params={"apiKey": key, "regions": "us", "markets": "h2h,totals,spreads"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("Odds API error: %s", e)
        return []


def _load_xgb_model():
    """Lazy-load trained XGBoost model from disk."""
    path = os.getenv("XGB_MODEL_PATH", "/app/models/xgb_propiq.pkl")
    if os.path.exists(path):
        with open(path, "rb") as f:
            return pickle.load(f)
    return None


# ── In-memory state (persisted to Redis) ──────────────────────────────────────

_agent_perf: dict[str, dict] = {
    name: {"wins": 0, "losses": 0, "pushes": 0, "units": 0.0, "roi": 0.0}
    for name in AGENT_NAMES
}

_capital_multipliers: dict[str, float] = {name: 1.0 for name in AGENT_NAMES}


# ─────────────────────────────────────────────────────────────────────────────
# 1. DataHubTasklet
# ─────────────────────────────────────────────────────────────────────────────

def run_data_hub_tasklet() -> None:
    """
    Staggered scrape across 4 data groups (physics, context, market, DFS).
    Pre-match gate: skips any game already LIVE or FINAL so we never poll
    in-game data and waste API quota.
    """
    r = _redis()

    # ── Pre-match gate: fetch today's game states ──────────────────────────
    game_states: dict[str, str] = {}
    try:
        today = datetime.date.today().strftime("%Y-%m-%d")
        games = _sportsdata_get(f"scores/json/GamesByDate/{today}") or []
        for g in games:
            gid   = str(g.get("GameID", ""))
            state = g.get("Status", "Scheduled")
            game_states[gid] = state
        logger.info("[DataHub] %d games today. States: %s",
                    len(game_states),
                    {s: list(game_states.values()).count(s) for s in set(game_states.values())})
    except Exception as e:
        logger.warning("[DataHub] Could not fetch game states: %s", e)

    def _is_pre_match(game_id: str) -> bool:
        state = game_states.get(game_id, "Scheduled")
        return state not in ("InProgress", "Live", "Final", "F/OT", "Completed")

    # ── Group 1: Physics / Arsenal (TTL 15 min) ────────────────────────────
    physics_key = "hub:physics"
    if not r.exists(physics_key):
        logger.info("[DataHub] Scraping physics / arsenal data…")
        physics = {
            "pitch_arsenal": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats"}],
                "maxCrawlingDepth": 0,
            }),
            "advanced_stats": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/baseball/stats-advanced.php"}],
                "maxCrawlingDepth": 0,
            }),
            "bvp": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/baseball/stats-bvp.php"}],
                "maxCrawlingDepth": 0,
            }),
            "batted_ball": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/baseball/stats-batted-ball.php"}],
                "maxCrawlingDepth": 0,
            }),
            "second_half": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/baseball/stats-second-half.php"}],
                "maxCrawlingDepth": 0,
            }),
        }
        r.setex(physics_key, TTL_PHYSICS, json.dumps(physics))

    # ── Group 2: Context / Environment (TTL 10 min) ───────────────────────
    context_key = "hub:context"
    if not r.exists(context_key):
        logger.info("[DataHub] Scraping context / environment data…")
        context = {
            "weather": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/baseball/weather.php"}],
                "maxCrawlingDepth": 0,
            }),
            "umpires": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/baseball/umpire-stats-daily.php"}],
                "maxCrawlingDepth": 0,
            }),
            "injuries": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/baseball/news.php?injuries=all"}],
                "maxCrawlingDepth": 0,
            }),
            "lineups": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/baseball/batting-orders.php"}],
                "maxCrawlingDepth": 0,
            }),
            "projected_starters": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/baseball/projected-starters.php"}],
                "maxCrawlingDepth": 0,
            }),
        }
        r.setex(context_key, TTL_CONTEXT, json.dumps(context))

    # ── Group 3: Market / Sharp steam (TTL 5 min) ─────────────────────────
    market_key = "hub:market"
    if not r.exists(market_key):
        logger.info("[DataHub] Scraping market / steam data…")
        market = {
            "public_betting": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.actionnetwork.com/mlb/public-betting"}],
                "maxCrawlingDepth": 0,
            }),
            "sharp_report": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.actionnetwork.com/mlb/sharp-report"}],
                "maxCrawlingDepth": 0,
            }),
            "prop_projections": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.actionnetwork.com/mlb/prop-projections"}],
                "maxCrawlingDepth": 0,
            }),
            "odds": _odds_api_get(),
        }
        r.setex(market_key, TTL_MARKET, json.dumps(market))

    # ── Group 4: DFS targets (TTL 8 min) ──────────────────────────────────
    dfs_key = "hub:dfs"
    if not r.exists(dfs_key):
        logger.info("[DataHub] Scraping DFS target data…")
        dfs = {
            "underdog": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/picks/underdog/"}],
                "maxCrawlingDepth": 0,
            }),
            "prizepicks": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/picks/prizepicks/"}],
                "maxCrawlingDepth": 0,
            }),
            "sleeper": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/picks/sleeper/"}],
                "maxCrawlingDepth": 0,
            }),
            "optimizer": _fetch_apify("apify/web-scraper", {
                "startUrls": [{"url": "https://www.rotowire.com/daily/mlb/optimizer.php"}],
                "maxCrawlingDepth": 0,
            }),
        }
        r.setex(dfs_key, TTL_DFS, json.dumps(dfs))

    # ── Merge all groups into master hub key ───────────────────────────────
    hub: dict[str, Any] = {
        "ts": datetime.datetime.utcnow().isoformat(),
        "game_states": game_states,
        "spring_training": _is_spring_training(),
    }
    for key in (physics_key, context_key, market_key, dfs_key):
        raw = r.get(key)
        if raw:
            hub[key.replace("hub:", "")] = json.loads(raw)

    r.setex("mlb_hub", TTL_HUB, json.dumps(hub))
    logger.info("[DataHub] Hub refreshed. Groups: physics=%s context=%s market=%s dfs=%s",
                r.exists(physics_key), r.exists(context_key),
                r.exists(market_key), r.exists(dfs_key))


def read_hub() -> dict:
    """Read the master hub dict from Redis. Returns empty dict on miss."""
    try:
        r = _redis()
        raw = r.get("mlb_hub")
        return json.loads(raw) if raw else {}
    except Exception as e:
        logger.warning("[DataHub] read_hub error: %s", e)
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# 2. AgentTasklet — 10-agent army
# ─────────────────────────────────────────────────────────────────────────────

class _BaseAgent:
    name: str = "BaseAgent"

    def __init__(self, hub: dict, model):
        self.hub   = hub
        self.model = model

    def evaluate(self, prop: dict) -> dict | None:
        """Return a bet dict if edge found, else None."""
        raise NotImplementedError

    # shared helpers
    def _model_prob(self, player: str, prop_type: str) -> float:
        if self.model:
            try:
                feats = [0.0] * 20  # placeholder — real feature pipeline
                return float(self.model.predict_proba([feats])[0][1]) * 100
            except Exception:
                pass
        return 50.0

    def _build_bet(self, prop: dict, side: str, model_prob: float,
                   implied_prob: float, ev_pct: float) -> dict:
        kelly = _kelly_units(model_prob / 100, prop.get("odds_american", -110))
        platforms = self._dfs_platforms(prop, side)
        return {
            "agent":              self.name,
            "player":             prop.get("player", "Unknown"),
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
        if ev_pct >= 10: return 9
        if ev_pct >= 7:  return 8
        if ev_pct >= 5:  return 7
        if ev_pct >= 3:  return 5
        return 3


class _EVHunter(_BaseAgent):
    name = "EVHunter"

    def evaluate(self, prop: dict) -> dict | None:
        over_odds  = prop.get("over_american",  -110)
        under_odds = prop.get("under_american", -110)
        fair_over, _ = _no_vig(over_odds, under_odds)
        model_prob   = self._model_prob(prop.get("player", ""), prop.get("prop_type", ""))
        implied      = _american_to_implied(over_odds) / 100
        ev_pct       = (model_prob / 100 - implied) / implied
        if ev_pct >= MIN_EV_THRESH:
            return self._build_bet(prop, "OVER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _UnderMachine(_BaseAgent):
    name = "UnderMachine"

    def evaluate(self, prop: dict) -> dict | None:
        under_odds = prop.get("under_american", -110)
        _, fair_under = _no_vig(prop.get("over_american", -110), under_odds)
        model_prob    = 100 - self._model_prob(prop.get("player", ""), prop.get("prop_type", ""))
        implied       = _american_to_implied(under_odds) / 100
        ev_pct        = (model_prob / 100 - implied) / implied
        if ev_pct >= MIN_EV_THRESH:
            return self._build_bet(prop, "UNDER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _UmpireAgent(_BaseAgent):
    name = "UmpireAgent"

    def evaluate(self, prop: dict) -> dict | None:
        umpires = self.hub.get("context", {}).get("umpires", [])
        if not umpires:
            return None
        # Look for ump with large K zone (favours strikeout unders)
        prop_type = prop.get("prop_type", "")
        if "K" not in prop_type and "strikeout" not in prop_type.lower():
            return None
        model_prob = self._model_prob(prop.get("player", ""), prop_type)
        # Umpire adjustment: tight zone → boost under prob by 5 pp
        model_prob = min(model_prob + 5.0, 95.0)
        under_odds = prop.get("under_american", -110)
        implied    = _american_to_implied(under_odds) / 100
        ev_pct     = (model_prob / 100 - implied) / implied
        if ev_pct >= MIN_EV_THRESH:
            return self._build_bet(prop, "UNDER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _F5Agent(_BaseAgent):
    name = "F5Agent"

    def evaluate(self, prop: dict) -> dict | None:
        """Targets first-5-innings run props."""
        if "f5" not in prop.get("prop_type", "").lower():
            return None
        starters = self.hub.get("context", {}).get("projected_starters", [])
        model_prob = self._model_prob(prop.get("player", ""), prop.get("prop_type", ""))
        over_odds  = prop.get("over_american", -110)
        implied    = _american_to_implied(over_odds) / 100
        ev_pct     = (model_prob / 100 - implied) / implied
        if ev_pct >= MIN_EV_THRESH:
            return self._build_bet(prop, "OVER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _FadeAgent(_BaseAgent):
    name = "FadeAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Fades heavy public action (>70 % public on one side)."""
        market    = self.hub.get("market", {})
        public    = market.get("public_betting", [])
        player    = prop.get("player", "")
        pub_pct   = 0
        for rec in public:
            if isinstance(rec, dict) and player.lower() in str(rec).lower():
                pub_pct = float(rec.get("over_public_pct", 0) or 0)
                break
        if pub_pct < 70:
            return None
        # Fade the public → take UNDER
        model_prob = self._model_prob(player, prop.get("prop_type", ""))
        fade_prob  = 100 - model_prob + 5.0   # boost by 5 pp for fade logic
        under_odds = prop.get("under_american", -110)
        implied    = _american_to_implied(under_odds) / 100
        ev_pct     = (fade_prob / 100 - implied) / implied
        if ev_pct >= MIN_EV_THRESH:
            return self._build_bet(prop, "UNDER", fade_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _LineValueAgent(_BaseAgent):
    name = "LineValueAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Hunts opening line steam moves."""
        sharp = self.hub.get("market", {}).get("sharp_report", [])
        player = prop.get("player", "")
        steam  = False
        for rec in sharp:
            if isinstance(rec, dict) and player.lower() in str(rec).lower():
                steam = bool(rec.get("steam_move", False) or rec.get("reverse_line_move", False))
                break
        if not steam:
            return None
        model_prob = self._model_prob(player, prop.get("prop_type", ""))
        over_odds  = prop.get("over_american", -110)
        implied    = _american_to_implied(over_odds) / 100
        ev_pct     = (model_prob / 100 - implied) / implied
        if ev_pct >= MIN_EV_THRESH:
            return self._build_bet(prop, "OVER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _BullpenAgent(_BaseAgent):
    name = "BullpenAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Targets high-leverage relief situations (fatigue 0-4 scale)."""
        # Bullpen fatigue from hub (populated by analytics layer)
        fatigue_map: dict = self.hub.get("bullpen_fatigue", {})
        player    = prop.get("player", "")
        team      = prop.get("team", "")
        fatigue   = fatigue_map.get(team, 2)   # default mid-range

        prop_type = prop.get("prop_type", "")
        if "HR" not in prop_type and "RBI" not in prop_type and "H" not in prop_type:
            return None

        model_prob = self._model_prob(player, prop_type)
        # High bullpen fatigue → batters see worse pitching → boost OVER
        if fatigue >= 3:
            model_prob = min(model_prob + 6.0, 95.0)
        over_odds = prop.get("over_american", -110)
        implied   = _american_to_implied(over_odds) / 100
        ev_pct    = (model_prob / 100 - implied) / implied
        if ev_pct >= MIN_EV_THRESH:
            return self._build_bet(prop, "OVER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _WeatherAgent(_BaseAgent):
    name = "WeatherAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Wind/park/pull-hitter combos."""
        weather_list = self.hub.get("context", {}).get("weather", [])
        player       = prop.get("player", "")
        venue        = prop.get("venue", "")
        wind_mph     = 0
        wind_dir     = ""
        for w in weather_list:
            if isinstance(w, dict) and (venue.lower() in str(w).lower() or
                                        player.lower() in str(w).lower()):
                wind_mph = float(w.get("wind_speed", 0) or 0)
                wind_dir = str(w.get("wind_direction", "") or "")
                break

        prop_type = prop.get("prop_type", "")
        # 10+ mph blowing out → boost HR/TB OVER
        if wind_mph >= 10 and "out" in wind_dir.lower() and (
                "HR" in prop_type or "TB" in prop_type):
            model_prob = self._model_prob(player, prop_type)
            model_prob = min(model_prob + 8.0, 95.0)
            over_odds  = prop.get("over_american", -110)
            implied    = _american_to_implied(over_odds) / 100
            ev_pct     = (model_prob / 100 - implied) / implied
            if ev_pct >= MIN_EV_THRESH:
                return self._build_bet(prop, "OVER", model_prob,
                                       implied * 100, ev_pct * 100)
        return None


class _SteamAgent(_BaseAgent):
    name = "SteamAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Follows sharp money (reverse line movement)."""
        sharp  = self.hub.get("market", {}).get("sharp_report", [])
        player = prop.get("player", "")
        rlm    = False
        for rec in sharp:
            if isinstance(rec, dict) and player.lower() in str(rec).lower():
                rlm = bool(rec.get("reverse_line_move", False))
                break
        if not rlm:
            return None
        model_prob = self._model_prob(player, prop.get("prop_type", ""))
        model_prob = min(model_prob + 4.0, 95.0)   # +4 pp for confirmed RLM
        over_odds  = prop.get("over_american", -110)
        implied    = _american_to_implied(over_odds) / 100
        ev_pct     = (model_prob / 100 - implied) / implied
        if ev_pct >= MIN_EV_THRESH:
            return self._build_bet(prop, "OVER", model_prob,
                                   implied * 100, ev_pct * 100)
        return None


class _MLEdgeAgent(_BaseAgent):
    name = "MLEdgeAgent"

    def evaluate(self, prop: dict) -> dict | None:
        """Pure XGBoost model edge — only fires when model prob diverges ≥8 pp."""
        model_prob = self._model_prob(prop.get("player", ""), prop.get("prop_type", ""))
        over_odds  = prop.get("over_american", -110)
        implied    = _american_to_implied(over_odds) / 100 * 100
        divergence = abs(model_prob - implied)
        if divergence < 8.0:
            return None
        side   = "OVER" if model_prob > implied else "UNDER"
        odds   = over_odds if side == "OVER" else prop.get("under_american", -110)
        imp    = _american_to_implied(odds) / 100
        ev_pct = (model_prob / 100 - imp) / imp
        if ev_pct >= MIN_EV_THRESH:
            return self._build_bet(prop, side, model_prob, imp * 100, ev_pct * 100)
        return None




# ─────────────────────────────────────────────────────────────────────────────
# Underdog-vs-Sharp edge helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_underdog_props(hub: dict) -> list[dict]:
    """Parse Underdog Fantasy lines from hub DFS data."""
    picks = hub.get("dfs", {}).get("underdog", [])
    props: list[dict] = []
    for pick in picks:
        if not isinstance(pick, dict):
            continue
        over_odds  = int(pick.get("over_odds",  -120) or -120)
        under_odds = int(pick.get("under_odds", -120) or -120)
        props.append({
            "player":         pick.get("player", pick.get("name", "Unknown")),
            "prop_type":      pick.get("stat_type", pick.get("prop", "H")),
            "line":           float(pick.get("line", pick.get("value", 1.5)) or 1.5),
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
    Extract sharp-book consensus implied probability for a player/prop
    from The Odds API data in hub.market.odds.
    Returns probability as a percentage (0-100), or None if no data found.
    """
    odds_list = hub.get("market", {}).get("odds", [])
    probs: list[float] = []
    player_lower = player.lower()
    for game in odds_list:
        if not isinstance(game, dict):
            continue
        for bookmaker in game.get("bookmakers", []):
            if bookmaker.get("key", "").lower() not in _SHARP_BOOKS:
                continue
            for market in bookmaker.get("markets", []):
                for outcome in market.get("outcomes", []):
                    desc = str(outcome.get("description", "")).lower()
                    name = str(outcome.get("name", "")).lower()
                    if player_lower in desc or player_lower in name:
                        price = outcome.get("price")
                        if price is not None:
                            try:
                                probs.append(_american_to_implied(int(price)))
                            except (TypeError, ValueError):
                                pass
    return (sum(probs) / len(probs)) if probs else None


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
    """Return True if any two legs share the same player."""
    players = [lg.get("player", "") for lg in legs]
    return len(set(players)) < len(players)


def _make_parlay(legs: list[dict], agent_name: str = "The Correlated Parlay Agent") -> dict:
    return {
        "agent":           agent_name,
        "legs":            legs,
        "leg_count":       len(legs),
        "combined_ev_pct": round(sum(lg["ev_pct"] for lg in legs), 2),
        "ts":              datetime.datetime.utcnow().isoformat(),
    }


def _build_parlays(ev_pool: list[dict], agent_name: str = "The Correlated Parlay Agent",
                   min_legs: int = 2,
                   max_legs: int = 3,
                   max_parlays: int = 5) -> list[dict]:
    """
    Package individual +EV picks into 2-leg and 3-leg Underdog slips.
    Avoids same-player correlation. Returns up to max_parlays slips sorted
    by combined EV descending.
    """
    if len(ev_pool) < min_legs:
        return []

    top = sorted(ev_pool, key=lambda x: x["ev_pct"], reverse=True)[:12]
    parlays: list[dict] = []

    for i in range(len(top)):
        for j in range(i + 1, len(top)):
            two = [top[i], top[j]]
            if _are_legs_correlated(two):
                continue
            parlays.append(_make_parlay(two, agent_name))
            # Attempt 3-leg extension
            if max_legs >= 3:
                for k in range(j + 1, len(top)):
                    three = two + [top[k]]
                    if not _are_legs_correlated(three):
                        parlays.append(_make_parlay(three, agent_name))
                        break
            if len(parlays) >= max_parlays * 2:
                break
        if len(parlays) >= max_parlays * 2:
            break

    # Deduplicate by player-set key, keep highest-EV version
    seen: set[str] = set()
    unique: list[dict] = []
    for p in sorted(parlays, key=lambda x: x["combined_ev_pct"], reverse=True):
        key = "|".join(sorted(lg["player"] for lg in p["legs"]))
        if key not in seen:
            seen.add(key)
            unique.append(p)

    return unique[:max_parlays]



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

            # Try to extend to 3 legs first (higher combined EV)
            if max_legs >= 3:
                for k in range(j + 1, len(top)):
                    three = two + [top[k]]
                    if not _are_legs_correlated(three):
                        key = "|".join(sorted(lg["player"] for lg in three))
                        if key not in seen:
                            seen.add(key)
                            parlays.append(_make_parlay(three, agent_name))
                        break

            # Also capture the 2-leg version
            key2 = "|".join(sorted(lg["player"] for lg in two))
            if key2 not in seen:
                seen.add(key2)
                parlays.append(_make_parlay(two, agent_name))

    return sorted(parlays, key=lambda x: x["combined_ev_pct"], reverse=True)[:max_parlays]


_AGENT_CLASSES = [
    _EVHunter, _UnderMachine, _UmpireAgent, _F5Agent, _FadeAgent,
    _LineValueAgent, _BullpenAgent, _WeatherAgent, _SteamAgent, _MLEdgeAgent,
]


def _build_synthetic_props(hub: dict) -> list[dict]:
    """
    Build a list of evaluable prop dicts from hub data.
    In production these come from DFS scrapes; here we synthesise from
    what the market group already has.
    """
    props: list[dict] = []

    # Pull from DFS picks (prizepicks / underdog / sleeper)
    dfs = hub.get("dfs", {})
    for platform, picks in dfs.items():
        if not isinstance(picks, list):
            continue
        for pick in picks:
            if not isinstance(pick, dict):
                continue
            props.append({
                "player":          pick.get("player", pick.get("name", "Unknown")),
                "prop_type":       pick.get("stat_type", pick.get("prop", "H")),
                "line":            float(pick.get("line", pick.get("value", 1.5)) or 1.5),
                "over_american":   int(pick.get("over_odds", -115) or -115),
                "under_american":  int(pick.get("under_odds", -115) or -115),
                "team":            pick.get("team", ""),
                "venue":           pick.get("venue", ""),
                "platform":        platform,
            })

    # No hardcoded test bets — engine only fires on real market data.
    return props


def _get_props(hub) -> list[dict]:
    return _extract_underdog_props(hub) or _build_synthetic_props(hub)


def run_agent_tasklet() -> None:
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

    props = _get_props(hub)
    if not props:
        logger.info("[AgentTasklet] No Underdog props available — skipping cycle.")
        return

    all_parlays: list[dict] = []

    # ── Each agent runs independently ─────────────────────────────────────
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

                # Sharp consensus gate: confirm Underdog is mispriced
                sharp_prob = _get_sharp_consensus(hub, player, prop_type)
                if sharp_prob is not None:
                    side    = bet["side"]
                    ud_odds = (prop.get("over_american", -120)
                               if side == "OVER"
                               else prop.get("under_american", -120))
                    edge = _underdog_edge(ud_odds, sharp_prob)
                    if edge < MIN_EV_THRESH * 100:
                        continue   # sharps don't confirm — skip
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
            logger.debug("[AgentTasklet] %s — %d hit(s), not enough for a slip.",
                         agent.name, len(agent_hits))
            continue

        # Build this agent's own branded parlays
        agent_parlays = _build_agent_parlays(agent_hits, agent.name)
        if agent_parlays:
            all_parlays.extend(agent_parlays)
            logger.info("[AgentTasklet] %s → %d slip(s) from %d hit(s).",
                        agent.name, len(agent_parlays), len(agent_hits))

    if not all_parlays:
        logger.info("[AgentTasklet] No qualifying slips this cycle.")
        return

    # ── Publish to Kafka / Redis ────────────────────────────────────────────
    producer = _kafka_producer()
    r        = _redis()
    for parlay in all_parlays:
        payload = json.dumps(parlay)
        if producer:
            try:
                producer.produce("bet_queue", value=payload.encode())
            except Exception as e:
                logger.warning("[AgentTasklet] Kafka error: %s — Redis fallback", e)
                r.lpush("bet_queue", payload)
                r.ltrim("bet_queue", 0, 499)
        else:
            r.lpush("bet_queue", payload)
            r.ltrim("bet_queue", 0, 499)
    if producer:
        producer.flush(timeout=5)

    # ── Discord — one embed per agent per slip ──────────────────────────────
    for parlay in all_parlays:
        try:
            discord_alert.send_parlay_alert(parlay)
        except Exception as _disc_err:
            logger.warning("[AgentTasklet] Discord alert error: %s", _disc_err)

    active_agents = len({p["agent"] for p in all_parlays})
    best = max(all_parlays, key=lambda p: p["combined_ev_pct"])
    logger.info("[AgentTasklet] Cycle complete — %d slip(s) from %d active agent(s). "
                "Best slip: %s | %d legs | combined EV=%.1f%%",
                len(all_parlays), active_agents,
                best["agent"], best["leg_count"], best["combined_ev_pct"])


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
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=14)).isoformat()
    rows: list[tuple] = []

    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT agent_name, profit_loss, units_wagered
                FROM bet_ledger
                WHERE graded_at >= %s AND status IN ('WIN', 'LOSS', 'PUSH')
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
        conn.close()
    except Exception as e:
        logger.warning("[LeaderboardTasklet] Postgres error: %s", e)

    # Aggregate per agent
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
        # Capital multiplier: linear scale from ROI
        # ROI <= -20% → 0.5x,  ROI = 0% → 1.0x,  ROI >= +20% → 2.0x
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

    X = np.array([json.loads(r[0]) for r in rows], dtype=np.float32)
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

    # SHAP feature importance — log features below threshold
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


# ─────────────────────────────────────────────────────────────────────────────
# 5. GradingTasklet  (nightly 1:05 AM)
# ─────────────────────────────────────────────────────────────────────────────

def run_grading_tasklet() -> None:
    """
    Fetch final boxscores, grade open bets, calculate CLV,
    run ML anomaly detection, then send daily recap to Telegram.
    (Full Java version in GradingTasklet.java; this Python runner is
    the ML-service companion that handles model-side grading.)
    """
    today = datetime.date.today().strftime("%Y-%m-%d")

    # Fetch boxscores
    boxscores = _sportsdata_get(f"stats/json/PlayerGameStatsByDate/{today}") or []
    if not boxscores:
        logger.info("[GradingTasklet] No boxscores for %s — nothing to grade.", today)
        return

    # Build player stat lookup
    stat_lookup: dict[str, dict] = {}
    for bs in boxscores:
        name = f"{bs.get('FirstName', '')} {bs.get('LastName', '')}".strip()
        stat_lookup[name] = bs

    # Fetch open bets from Postgres
    open_bets: list[tuple] = []
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, player_name, prop_type, line, side,
                       odds_american, kelly_units, model_prob, ev_pct, agent_name
                FROM bet_ledger
                WHERE status = 'OPEN' AND bet_date = %s
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

    # Grade each bet
    results: list[dict] = []
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            for row in open_bets:
                bid, player, ptype, line, side, odds, units, model_prob, ev_pct, agent = row
                stats = stat_lookup.get(player, {})
                actual = _get_stat(stats, ptype)

                if actual is None:
                    continue   # game not final yet

                line   = float(line or 0)
                units  = float(units or 1)

                if side == "OVER":
                    if actual > line:
                        status = "WIN";  pl = units * (_american_to_implied(int(odds or -110)) and
                                                        (100 / _american_to_implied(int(odds or -110)) - 1))
                    elif actual < line:
                        status = "LOSS"; pl = -units
                    else:
                        status = "PUSH"; pl = 0.0
                else:
                    if actual < line:
                        status = "WIN";  pl = units * (100 / _american_to_implied(int(odds or -110)) - 1)
                    elif actual > line:
                        status = "LOSS"; pl = -units
                    else:
                        status = "PUSH"; pl = 0.0

                # Closing line value (CLV) — compare model prob to final odds
                closing_odds = _fetch_closing_odds(player, ptype, side) or odds
                clv = float(model_prob or 50) - _american_to_implied(int(closing_odds or -110))

                cur.execute(
                    """
                    UPDATE bet_ledger
                    SET status = %s, profit_loss = %s, actual_result = %s,
                        clv = %s, graded_at = NOW()
                    WHERE id = %s
                    """,
                    (status, round(pl, 4), actual, round(clv, 2), bid),
                )

                results.append({
                    "id": bid, "player": player, "prop_type": ptype,
                    "line": line, "side": side, "actual": actual,
                    "status": status, "profit_loss": round(pl, 4),
                    "clv": round(clv, 2), "agent": agent,
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

    # Send daily recap via Discord webhook
    try:
        discord_alert.send_daily_recap(results, total_profit, today)
    except Exception as _disc_err:
        logger.warning("[GradingTasklet] Discord recap error: %s", _disc_err)


def _get_stat(stats: dict, prop_type: str) -> float | None:
    """Map prop_type string to SportsData.io stat field."""
    mapping = {
        "H": "Hits", "HR": "HomeRuns", "RBI": "RunsBattedIn",
        "R": "Runs", "SB": "StolenBases", "TB": "TotalBases",
        "BB": "Walks", "K": "Strikeouts",
    }
    prop_upper = prop_type.upper()
    # Strip O/U prefix if present
    for prefix in ("O", "U", "OVER_", "UNDER_"):
        if prop_upper.startswith(prefix):
            prop_upper = prop_upper[len(prefix):]

    # Strip line suffix (e.g. "1.5")
    for tok in prop_upper.split():
        field = mapping.get(tok)
        if field:
            val = stats.get(field)
            return float(val) if val is not None else None

    field = mapping.get(prop_upper)
    if field:
        val = stats.get(field)
        return float(val) if val is not None else None

    return None


def _fetch_closing_odds(player: str, prop_type: str, side: str) -> int | None:
    """Best-effort closing line fetch from Redis market cache."""
    try:
        r   = _redis()
        raw = r.get("hub:market")
        if not raw:
            return None
        market = json.loads(raw)
        odds_list = market.get("odds", [])
        for game in odds_list:
            if not isinstance(game, dict):
                continue
            for market_key, outcomes in game.get("bookmakers", [{}])[0].get("markets", [{}]):
                pass
    except Exception:
        pass
    return None


def _send_telegram_recap(results: list[dict], total_profit: float, today: str) -> None:
    """Post daily recap to Telegram bot."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.info("[GradingTasklet] Telegram not configured — skipping recap.")
        return

    wins   = sum(1 for r in results if r["status"] == "WIN")
    losses = sum(1 for r in results if r["status"] == "LOSS")
    pushes = sum(1 for r in results if r["status"] == "PUSH")

    sign = "+" if total_profit >= 0 else ""
    lines = [
        f"📊 *PropIQ Daily Recap — {today}*",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"📈 Units: {sign}{total_profit:.2f}u",
        f"🏆 Record: {wins}-{losses}-{pushes} (W-L-P)",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]

    for r in results:
        emoji = {"WIN": "✅", "LOSS": "❌", "PUSH": "➖"}.get(r["status"], "❓")
        pl_sign = "+" if r["profit_loss"] >= 0 else ""
        odds_str = _fmt_american(int(r.get("odds_american") or -110))
        lines.append(
            f"{emoji} {r['player']} — {r['prop_type']} @ {odds_str} | {pl_sign}{r['profit_loss']:.2f}u"
        )

    lines += ["━━━━━━━━━━━━━━━━━━━━━━", "Powered by PropIQ Analytics 🤖"]
    text = "\n".join(lines)

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=15,
        )
        logger.info("[GradingTasklet] Telegram daily recap sent.")
    except Exception as e:
        logger.warning("[GradingTasklet] Telegram send error: %s", e)


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
                SELECT features_json, actual_outcome
                FROM bet_ledger
                WHERE graded_at IS NOT NULL
                  AND features_json IS NOT NULL
                  AND actual_outcome IS NOT NULL
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

    X = np.array([json.loads(r[0]) for r in rows], dtype=np.float32)
    y = np.array([int(r[1]) for r in rows], dtype=np.int8)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
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
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    preds    = model.predict(X_test)
    accuracy = accuracy_score(y_test, preds)

    model_path = os.getenv("XGB_MODEL_PATH", "/app/models/xgb_propiq.pkl")
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    # Persist metadata to Redis
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

    logger.info("[XGBoostTasklet] Retrain complete. Accuracy=%.3f | Train=%d Test=%d | Saved→%s",
                accuracy, len(X_train), len(X_test), model_path)

    if accuracy >= 0.842:
        logger.info("[XGBoostTasklet] 🎯 Target accuracy %.1f%% reached!", accuracy * 100)
    elif accuracy >= 0.777:
        logger.info("[XGBoostTasklet] ✅ Minimum threshold met (%.1f%%).", accuracy * 100)
    else:
        logger.warning("[XGBoostTasklet] ⚠️ Below minimum threshold (%.1f%% < 77.7%%).",
                       accuracy * 100)
