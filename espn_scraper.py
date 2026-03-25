"""
espn_scraper.py
===============
ESPN internal JSON API scraper for MLB game states and player box scores.

Uses site.api.espn.com — free, no API key required, same data source
as the public ESPN website.

Public API
----------
    get_game_states(date_str)       → list[dict]  game-level status
    get_all_player_stats(date_str)  → dict[str, dict]  player box scores

date_str formats accepted: 'YYYY-MM-DD' or 'YYYYMMDD'
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
_REQUEST_SLEEP = 0.8   # seconds between consecutive ESPN requests


# ---------------------------------------------------------------------------
# Game state fetcher
# ---------------------------------------------------------------------------

def get_game_states(date_str: str) -> list[dict]:
    """
    Fetch MLB game states for a given date.

    Returns a list of dicts:
        {game_id, name, status, home_score, away_score, home_team, away_team}

    status values: 'SCHEDULED' | 'IN_PROGRESS' | 'FINAL'
    """
    date_fmt = date_str.replace("-", "")
    try:
        resp = requests.get(
            f"{_ESPN_BASE}/scoreboard",
            params={"dates": date_fmt, "limit": 20},
            headers=_HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("[ESPN] scoreboard HTTP %d for %s", resp.status_code, date_fmt)
            return []

        data = resp.json()
        games: list[dict] = []

        for event in data.get("events", []):
            competition = (event.get("competitions") or [{}])[0]
            status_name = (
                competition
                .get("status", {})
                .get("type", {})
                .get("name", "")
            ).lower()

            # Map ESPN status → our status string
            if "final" in status_name:
                status = "FINAL"
            elif "progress" in status_name or "live" in status_name:
                status = "IN_PROGRESS"
            else:
                status = "SCHEDULED"

            competitors = competition.get("competitors", [])
            home = next((c for c in competitors if c.get("homeAway") == "home"), {})
            away = next((c for c in competitors if c.get("homeAway") == "away"), {})

            games.append({
                "game_id":   event.get("id", ""),
                "name":      event.get("name", ""),
                "status":    status,
                "home_score": int(home.get("score") or 0),
                "away_score": int(away.get("score") or 0),
                "home_team":  home.get("team", {}).get("abbreviation", ""),
                "away_team":  away.get("team", {}).get("abbreviation", ""),
            })

        logger.info("[ESPN] %d games found for %s", len(games), date_fmt)
        return games

    except Exception as exc:
        logger.warning("[ESPN] get_game_states failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Player stats parser helpers
# ---------------------------------------------------------------------------

# ESPN batter box score column order (standard game summary)
_BATTER_KEYS = [
    "at_bats", "runs", "hits", "rbi", "home_runs",
    "base_on_balls", "strikeouts", "batting_avg",
]

# ESPN pitcher box score column order
_PITCHER_KEYS = [
    "innings_pitched", "hits_allowed", "runs_allowed", "earned_runs",
    "base_on_balls", "strikeouts", "home_runs_allowed", "era",
]


def _parse_athlete_stats(athlete: dict, is_pitcher: bool) -> dict:
    """Parse an ESPN athlete entry from a box-score statistics group."""
    stats_raw = athlete.get("stats", [])
    keys      = _PITCHER_KEYS if is_pitcher else _BATTER_KEYS
    out: dict[str, float] = {}

    for i, key in enumerate(keys):
        if i < len(stats_raw):
            try:
                out[key] = float(stats_raw[i])
            except (ValueError, TypeError):
                out[key] = 0.0
        else:
            out[key] = 0.0

    if not is_pitcher:
        # Derived convenience fields expected by settlement_engine
        out["rbis"]    = out.get("rbi", 0.0)
        out["stolen_bases"] = 0.0           # not in standard ESPN box score
        # total_bases: approximate (1×singles + 2×doubles + 3×triples + 4×HR)
        # We only have hits and HRs directly, so use a rough estimate
        h  = out.get("hits", 0.0)
        hr = out.get("home_runs", 0.0)
        out["total_bases"]   = h + hr * 3   # conservative: treats non-HR hits as singles
        out["hits_runs_rbis"] = h + out.get("runs", 0.0) + out.get("rbi", 0.0)

    return out


# ---------------------------------------------------------------------------
# Main player stats fetcher
# ---------------------------------------------------------------------------

def get_all_player_stats(date_str: str) -> dict[str, dict]:
    """
    Fetch all player box-score stats from ESPN for a given date.

    date_str: 'YYYYMMDD' (preferred) or 'YYYY-MM-DD'

    Returns:
        dict keyed by lowercase player full name:
            {full_name, is_pitcher, hits, runs, rbi, home_runs, ...}

    Players not in a FINAL or IN_PROGRESS game are excluded.
    """
    date_fmt = date_str.replace("-", "")
    games = get_game_states(date_fmt)
    if not games:
        logger.warning("[ESPN] No games for %s — no player stats available", date_fmt)
        return {}

    all_stats: dict[str, dict] = {}

    for game in games:
        if game["status"] not in ("IN_PROGRESS", "FINAL"):
            continue

        game_id = game.get("game_id")
        if not game_id:
            continue

        try:
            resp = requests.get(
                f"{_ESPN_BASE}/summary",
                params={"event": game_id},
                headers=_HEADERS,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning(
                    "[ESPN] summary HTTP %d for game %s", resp.status_code, game_id
                )
                time.sleep(_REQUEST_SLEEP)
                continue

            data     = resp.json()
            box_data = data.get("boxscore", {})

            for team_data in box_data.get("players", []):
                for stats_group in team_data.get("statistics", []):
                    group_name = (stats_group.get("name") or "").lower()
                    is_pitcher = "pitch" in group_name

                    for athlete_entry in stats_group.get("athletes", []):
                        athlete_info = athlete_entry.get("athlete", {})
                        full_name    = (athlete_info.get("fullName") or "").strip()
                        if not full_name:
                            continue

                        parsed = _parse_athlete_stats(athlete_entry, is_pitcher)
                        all_stats[full_name.lower()] = {
                            "full_name":  full_name,
                            "is_pitcher": is_pitcher,
                            **parsed,
                        }

            time.sleep(_REQUEST_SLEEP)

        except Exception as exc:
            logger.warning("[ESPN] summary failed for game %s: %s", game_id, exc)
            time.sleep(_REQUEST_SLEEP)

    logger.info("[ESPN] Parsed box-score stats for %d players", len(all_stats))
    return all_stats
