"""
wpa_drama_layer.py
==================
Late-inning Win Probability Added (WPA) drama scoring.

Pulls yesterday's completed game WPA data from MLB Stats API
/api/v1.1/game/{gamePk}/winProbability and computes:
  - max |WPA swing| in innings 7+ (late-leverage signal)
  - game tag: walkoff | comeback | blowout | neutral
  - drama_score: 0.0–1.0 normalized

Feeds two downstream consumers:
  1. BVI layer — teams with high late-game WPA drama yesterday have
     higher bullpen volatility (relievers were used up in tight spots).
  2. CorrelatedParlayAgent — avoids pairing two starters from the same
     matchup when yesterday's drama was high (both bullpens taxed).

Public API
----------
get_team_drama_score(team_abbr, game_date=None) → float  (0.0–1.0)
get_game_drama(game_pk) → dict
prefetch_yesterday_drama() → dict[str, dict]   (team_abbr → drama dict)

All results cached in Redis with 20h TTL keyed by date.
Falls back to 0.0 (no adjustment) on any failure.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_MLB_SCHEDULE_URL  = "https://statsapi.mlb.com/api/v1/schedule"
_MLB_WINPROB_URL   = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/winProbability"
_MLB_HEADERS       = {"Accept": "application/json"}
_REQUEST_TIMEOUT   = 15

# Drama threshold configuration
_WALKOFF_THRESHOLD  = 0.40   # single WPA swing ≥ 40% late → walkoff tag
_COMEBACK_THRESHOLD = 0.30   # max swing 30–40% → comeback
_BLOWOUT_MAX_SWING  = 0.10   # max swing < 10% → blowout (never close)

# BVI adjustment per tag: added to impact_volatility component
DRAMA_BVI_ADJUSTMENTS: dict[str, float] = {
    "walkoff":  0.12,   # both bullpens near exhaustion
    "comeback": 0.08,
    "neutral":  0.0,
    "blowout":  -0.04,  # closer was never used
}


def _get_redis():
    try:
        import redis as _r
        url = os.environ.get("REDIS_URL") or os.environ.get("REDIS_PUBLIC_URL")
        if not url:
            return None
        return _r.from_url(url, decode_responses=True, socket_connect_timeout=3)
    except Exception:
        return None


def _redis_key(game_date: date) -> str:
    return f"wpa_drama_{game_date.isoformat()}"


def _get_finished_game_pks(game_date: date) -> list[tuple[int, str, str]]:
    """Return [(game_pk, home_abbr, away_abbr)] for finished games on game_date."""
    try:
        resp = requests.get(
            _MLB_SCHEDULE_URL,
            params={"sportId": 1, "date": game_date.isoformat(), "gameType": "R"},
            headers=_MLB_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return []
        games = []
        for date_block in resp.json().get("dates", []):
            for game in date_block.get("games", []):
                state = game.get("status", {}).get("codedGameState", "")
                if state not in ("F", "O"):
                    continue
                gk = int(game.get("gamePk", 0))
                home = game.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation", "")
                away = game.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation", "")
                if gk:
                    games.append((gk, home, away))
        return games
    except Exception as exc:
        logger.debug("[WPADrama] Schedule fetch failed: %s", exc)
        return []


def get_game_drama(game_pk: int) -> dict:
    """Fetch and analyse WPA data for a single game.

    Returns:
        {
            "game_pk": int,
            "max_wpa_late": float,   # max |WPA| swing in innings 7+
            "max_wpa_full": float,   # max |WPA| swing entire game
            "drama_score":  float,   # 0.0–1.0
            "tag":          str,     # walkoff | comeback | blowout | neutral
            "home_abbr":    str,
            "away_abbr":    str,
        }
    """
    try:
        resp = requests.get(
            _MLB_WINPROB_URL.format(game_pk=game_pk),
            headers=_MLB_HEADERS,
            timeout=_REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return {}

        events = resp.json()
        if not isinstance(events, list):
            return {}

        max_late = 0.0
        max_full = 0.0
        prev_home_wp = None

        for evt in events:
            inning  = evt.get("inning", 0)
            home_wp = evt.get("homeTeamWinProbability")
            if home_wp is None:
                home_wp = evt.get("homeTeamWinProbabilityAdded")
            if home_wp is None:
                continue

            home_wp = float(home_wp)

            # WPA is sometimes stored as the raw probability (0–100 scale)
            if home_wp > 1.0:
                home_wp /= 100.0

            if prev_home_wp is not None:
                swing = abs(home_wp - prev_home_wp)
                if swing > max_full:
                    max_full = swing
                if inning >= 7 and swing > max_late:
                    max_late = swing

            prev_home_wp = home_wp

        # Tag
        if max_late >= _WALKOFF_THRESHOLD:
            tag = "walkoff"
        elif max_late >= _COMEBACK_THRESHOLD:
            tag = "comeback"
        elif max_full < _BLOWOUT_MAX_SWING:
            tag = "blowout"
        else:
            tag = "neutral"

        # Drama score: normalise max_late to 0–1 (capped at 0.5 = score 1.0)
        drama_score = round(min(1.0, max_late / 0.50), 3)

        return {
            "game_pk":      game_pk,
            "max_wpa_late": round(max_late, 4),
            "max_wpa_full": round(max_full, 4),
            "drama_score":  drama_score,
            "tag":          tag,
        }

    except Exception as exc:
        logger.debug("[WPADrama] get_game_drama(%d) failed: %s", game_pk, exc)
        return {}


def prefetch_yesterday_drama(game_date: Optional[date] = None) -> dict[str, dict]:
    """Fetch WPA drama for all finished games on game_date (default: yesterday).

    Returns:
        dict keyed by team_abbr (both home and away get the same game drama).
        Cached in Redis for 20h.
    """
    if game_date is None:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        game_date = datetime.now(ZoneInfo("America/Los_Angeles")).date() - timedelta(days=1)

    rkey = _redis_key(game_date)
    r = _get_redis()
    if r:
        try:
            cached = r.get(rkey)
            if cached:
                return json.loads(cached)
        except Exception:
            pass

    game_list = _get_finished_game_pks(game_date)
    if not game_list:
        logger.info("[WPADrama] No finished games for %s", game_date)
        return {}

    result: dict[str, dict] = {}
    for gk, home_abbr, away_abbr in game_list:
        drama = get_game_drama(gk)
        if not drama:
            continue
        drama["home_abbr"] = home_abbr
        drama["away_abbr"] = away_abbr
        for abbr in (home_abbr, away_abbr):
            if abbr:
                result[abbr.upper()] = drama

    if r and result:
        try:
            r.setex(rkey, 72000, json.dumps(result))   # 20h TTL
        except Exception:
            pass

    logger.info("[WPADrama] Prefetched %d games → %d teams", len(game_list), len(result))
    return result


# In-process cache populated once at 8:15 AM prefetch
_DAY_CACHE: dict[str, dict] = {}


def get_team_drama_score(
    team_abbr: str,
    game_date: Optional[date] = None,
) -> float:
    """Return yesterday's drama score (0.0–1.0) for a team.

    Uses in-process cache populated by prefetch_yesterday_drama().
    Falls back to Redis on cold start.  Returns 0.0 on any failure.
    """
    global _DAY_CACHE
    if not _DAY_CACHE:
        try:
            _DAY_CACHE = prefetch_yesterday_drama(game_date)
        except Exception:
            return 0.0

    entry = _DAY_CACHE.get(team_abbr.upper(), {})
    return float(entry.get("drama_score", 0.0))


def get_team_bvi_adjustment(team_abbr: str, game_date: Optional[date] = None) -> float:
    """Return BVI impact_volatility additive adjustment based on yesterday's drama tag.

    Walkoff  → +0.12  (bullpen fully taxed)
    Comeback → +0.08
    Neutral  →  0.0
    Blowout  → -0.04  (closer never used)
    """
    global _DAY_CACHE
    if not _DAY_CACHE:
        try:
            _DAY_CACHE = prefetch_yesterday_drama(game_date)
        except Exception:
            return 0.0

    entry = _DAY_CACHE.get(team_abbr.upper(), {})
    tag   = entry.get("tag", "neutral")
    return DRAMA_BVI_ADJUSTMENTS.get(tag, 0.0)
