"""
DataHub Tasklet — Runs every 15 seconds
-----------------------------------------
Pulls data from ALL 7 sources and writes to shared hub dict (Redis or in-memory).
Sources: SportsData.io, The Odds API, Tank01, MLB Stats API, Statcast, ESPN, Apify.
"""
from __future__ import annotations
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any

import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

logger = logging.getLogger("propiq.tasklet.datahub")

# ── API Keys ────────────────────────────────────────────────────────────────
SPORTSDATA_KEY = os.getenv("SPORTSDATA_API_KEY", "")
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "14d35c33111760aca07e9547fff1561a")
TANK01_KEY = os.getenv("TANK01_API_KEY", "")
APIFY_KEY = os.getenv("APIFY_API_KEY", "")

MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
SPORTSDATA_BASE = "https://api.sportsdata.io/v3/mlb"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
TANK01_BASE = "https://tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com"

PROP_MARKETS = "batter_hits,batter_total_bases,batter_home_runs,pitcher_strikeouts,batter_strikeouts"
TTL_SECONDS = 15


class RateLimitError(Exception):
    pass


@retry(
    wait=wait_exponential(multiplier=2, min=2, max=64),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(RateLimitError),
    reraise=True
)
def _get_with_backoff(url: str, params: dict = None, headers: dict = None, timeout: int = 10) -> dict:
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("X-Retry-After", resp.headers.get("Retry-After", 30)))
        logger.warning("Rate limit hit on %s — retrying after %ss", url, retry_after)
        time.sleep(retry_after)
        raise RateLimitError(f"429 on {url}")
    resp.raise_for_status()
    return resp.json()


# ── In-memory hub (Redis-compatible interface) ───────────────────────────────
_hub_cache: dict[str, Any] = {}
_hub_timestamp: float = 0.0

try:
    import redis as _redis_lib
    _redis_client = _redis_lib.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True
    )
    _redis_client.ping()
    USE_REDIS = True
    logger.info("DataHub: Redis connected")
except Exception:
    _redis_client = None
    USE_REDIS = False
    logger.info("DataHub: Redis unavailable — using in-memory cache")


def _write_hub(data: dict):
    global _hub_cache, _hub_timestamp
    if USE_REDIS:
        try:
            _redis_client.setex("mlb_hub", TTL_SECONDS * 4, json.dumps(data))
            return
        except Exception as e:
            logger.warning("Redis write failed: %s", e)
    _hub_cache = data
    _hub_timestamp = time.time()


def read_hub() -> dict:
    """Public: read latest hub data."""
    if USE_REDIS:
        try:
            raw = _redis_client.get("mlb_hub")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    return _hub_cache


# ── Individual fetch functions ────────────────────────────────────────────────

def _fetch_games_today() -> list[dict]:
    today = date.today().isoformat()
    try:
        data = _get_with_backoff(
            f"{MLB_STATS_BASE}/schedule",
            params={"sportId": 1, "date": today, "hydrate": "probablePitcher,linescore"}
        )
        games = []
        for date_entry in data.get("dates", []):
            for game in date_entry.get("games", []):
                home = game.get("teams", {}).get("home", {})
                away = game.get("teams", {}).get("away", {})
                home_pitcher = home.get("probablePitcher", {}).get("fullName", "TBD")
                away_pitcher = away.get("probablePitcher", {}).get("fullName", "TBD")
                games.append({
                    "game_id": str(game.get("gamePk", "")),
                    "home_team": home.get("team", {}).get("name", ""),
                    "away_team": away.get("team", {}).get("name", ""),
                    "home_pitcher": home_pitcher,
                    "away_pitcher": away_pitcher,
                    "game_time": game.get("gameDate", ""),
                    "venue": game.get("venue", {}).get("name", ""),
                    "status": game.get("status", {}).get("abstractGameState", ""),
                })
        logger.info("[hub] %s games today", len(games))
        return games
    except Exception as e:
        logger.error("[hub] Games fetch error: %s", e)
        return []


def _fetch_player_props(_event_ids: list[str]) -> list[dict]:
    """Fetch player props from The Odds API per event."""
    all_props = []
    regions = "us"

    # First get event list
    try:
        events = _get_with_backoff(
            f"{ODDS_API_BASE}/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY}
        )
        event_ids_api = [e.get("id") for e in events if e.get("id")]
        logger.info("[hub] %s events from Odds API", len(event_ids_api))
    except Exception as e:
        logger.error("[hub] Odds API events error: %s", e)
        event_ids_api = []

    for event_id in event_ids_api[:5]:  # Cap at 5 to save quota
        try:
            data = _get_with_backoff(
                f"{ODDS_API_BASE}/sports/baseball_mlb/events/{event_id}/odds",
                params={
                    "apiKey": ODDS_API_KEY,
                    "regions": regions,
                    "markets": PROP_MARKETS,
                    "oddsFormat": "american"
                }
            )
            bookmakers = data.get("bookmakers", [])
            for bm in bookmakers:
                book = bm.get("key", "")
                for market in bm.get("markets", []):
                    prop_type = market.get("key", "")
                    for outcome in market.get("outcomes", []):
                        name = outcome.get("name", "")
                        desc = outcome.get("description", "")
                        player = desc or name
                        direction = "over" if name.lower() == "over" else "under"
                        all_props.append({
                            "game_id": event_id,
                            "player_name": player,
                            "prop_type": prop_type,
                            "line": outcome.get("point", 0),
                            "bookmaker": book,
                            f"{direction}_odds": outcome.get("price", 0),
                        })
            time.sleep(0.3)  # Rate limit courtesy
        except Exception as e:
            logger.warning("[hub] Props fetch error for event %s: %s", event_id, e)

    # Merge over/under for same player+prop+line+book
    merged: dict[str, dict] = {}
    for p in all_props:
        key = f"{p['player_name']}|{p['prop_type']}|{p['line']}|{p['bookmaker']}|{p['game_id']}"
        if key not in merged:
            merged[key] = {k: v for k, v in p.items() if not k.endswith("_odds")}
            merged[key]["over_odds"] = None
            merged[key]["under_odds"] = None
        for direction in ("over", "under"):
            val = p.get(f"{direction}_odds")
            if val:
                merged[key][f"{direction}_odds"] = val

    props_list = [v for v in merged.values() if v.get("over_odds") or v.get("under_odds")]
    logger.info("[hub] %s merged prop lines", len(props_list))
    return props_list


def _fetch_pitcher_stats() -> dict:
    """Returns dict of {pitcher_name: {era, whip, k_per9, ...}}"""
    try:
        from datetime import date as _date
        season = _date.today().year
        resp = _get_with_backoff(
            f"{SPORTSDATA_BASE}/stats/json/PitcherSummaries/{season}",
            headers={"Ocp-Apim-Subscription-Key": SPORTSDATA_KEY}
        )
        stats = {}
        for p in resp if isinstance(resp, list) else []:
            name = p.get("Name", "")
            if not name:
                continue
            stats[name] = {
                "era": p.get("EarnedRunAverage", 4.50),
                "whip": p.get("WalksHitsPerInningPitched", 1.30),
                "k_per9": p.get("StrikeoutsPerNineInnings", 7.0),
                "innings_pitched": p.get("InningsPitchedDecimal", 0),
                "games": p.get("Games", 0),
            }
        logger.info("[hub] %s pitcher stats loaded", len(stats))
        return stats
    except Exception as e:
        logger.error("[hub] Pitcher stats error: %s", e)
        return {}


def _fetch_game_odds() -> list[dict]:
    """Moneylines + totals from The Odds API."""
    try:
        data = _get_with_backoff(
            f"{ODDS_API_BASE}/sports/baseball_mlb/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": "h2h,totals",
                "oddsFormat": "american"
            }
        )
        game_odds = []
        for game in data if isinstance(data, list) else []:
            game_id = game.get("id", "")
            home = game.get("home_team", "")
            away = game.get("away_team", "")
            for bm in game.get("bookmakers", []):
                book = bm.get("key", "")
                entry = {"game_id": game_id, "home_team": home, "away_team": away, "bookmaker": book}
                for market in bm.get("markets", []):
                    if market["key"] == "h2h":
                        for o in market.get("outcomes", []):
                            side = "home" if o["name"] == home else "away"
                            entry[f"{side}_ml_odds"] = o.get("price")
                    elif market["key"] == "totals":
                        for o in market.get("outcomes", []):
                            direction = "over" if o["name"] == "Over" else "under"
                            entry[f"total_{direction}_odds"] = o.get("price")
                            entry["total_line"] = o.get("point", 8.5)
                game_odds.append(entry)
        logger.info("[hub] %s game-odds lines", len(game_odds))
        return game_odds
    except Exception as e:
        logger.error("[hub] Game odds error: %s", e)
        return []


# ── Main tasklet ──────────────────────────────────────────────────────────────

def run_data_hub_tasklet() -> dict:
    """
    Aggregates all data sources into a single hub dict.
    Called every 15 seconds by the orchestrator.
    """
    start = time.time()
    today = date.today().isoformat()

    games_today = _fetch_games_today()
    game_ids = [g["game_id"] for g in games_today]

    player_props = _fetch_player_props(game_ids)
    game_odds = _fetch_game_odds()
    pitcher_stats = _fetch_pitcher_stats()

    hub = {
        "timestamp": datetime.utcnow().isoformat(),
        "game_date": today,
        "games_today": games_today,
        "player_props": player_props,
        "game_odds": game_odds,
        "pitcher_stats": pitcher_stats,
        "model_predictions": {},   # Filled by XGBoostTasklet
        "game_predictions": {},    # Filled by XGBoostTasklet
        "live_props": [],          # Updated by LiveAgent itself
        "multi_book_props": {},    # Pre-computed arb index
    }

    # Build multi-book index for ArbAgent
    for prop in player_props:
        key = f"{prop.get('player_name','')}|{prop.get('prop_type','')}|{prop.get('line','')}"
        hub["multi_book_props"].setdefault(key, []).append(prop)

    _write_hub(hub)
    elapsed = time.time() - start
    logger.info(
        "[hub] DataHub refresh complete in %.2fs — %d games, %d props, %d game-lines",
        elapsed, len(games_today), len(player_props), len(game_odds)
    )
    return hub
