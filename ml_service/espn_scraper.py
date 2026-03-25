"""
espn_scraper.py
===============
Fetches MLB game results and player box score stats from ESPN's
public JSON API (no HTML scraping, no auth required).

ESPN Internal API endpoints used:
  Scoreboard : https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=YYYYMMDD
  Box score  : https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event={game_id}

Rate limiting: 1.5-second sleep between box score fetches to stay friendly.
All responses are cached in /tmp/espn_cache/ for the session to avoid re-hits.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

_BASE_SCOREBOARD = (
    "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date}"
)
_BASE_SUMMARY = (
    "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event={game_id}"
)
_CACHE_DIR = Path("/tmp/espn_cache")
_SLEEP_BETWEEN_REQUESTS = 1.5  # seconds — keeps ESPN happy


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_json(url: str, cache_key: str) -> Optional[dict]:
    """Fetch JSON from URL, using a local cache to avoid duplicate requests."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = _CACHE_DIR / f"{cache_key}.json"

    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text())
        except Exception:
            pass  # Corrupt cache — re-fetch

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.espn.com/",
    }

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
        data = json.loads(raw)
        cache_file.write_text(json.dumps(data))
        return data
    except urllib.error.HTTPError as exc:
        logger.warning("ESPN HTTP %s for %s", exc.code, url)
    except Exception as exc:
        logger.warning("ESPN fetch error for %s: %s", url, exc)
    return None


# ---------------------------------------------------------------------------
# Public: Scoreboard
# ---------------------------------------------------------------------------

def get_game_states(date_str: str) -> list[dict]:
    """
    Return a list of today's MLB games with their current ESPN status.

    Each dict:
        game_id    (str)   ESPN event ID
        name       (str)   "Team A at Team B"
        status     (str)   "SCHEDULED" | "IN_PROGRESS" | "FINAL"
        away_score (int)
        home_score (int)
        start_time (str)   ISO timestamp

    date_str: "YYYY-MM-DD" format.
    """
    date_compact = date_str.replace("-", "")
    url = _BASE_SCOREBOARD.format(date=date_compact)
    data = _fetch_json(url, f"scoreboard_{date_compact}")
    if data is None:
        return []

    games: list[dict] = []
    for event in data.get("events", []):
        raw_status = event.get("status", {}).get("type", {}).get("name", "")
        if "IN_PROGRESS" in raw_status or "PROGRESS" in raw_status:
            phase = "IN_PROGRESS"
        elif "FINAL" in raw_status or "POST" in raw_status:
            phase = "FINAL"
        else:
            phase = "SCHEDULED"

        comps = event.get("competitions", [{}])[0]
        away_score = home_score = 0
        for competitor in comps.get("competitors", []):
            score = int(competitor.get("score", 0) or 0)
            if competitor.get("homeAway") == "home":
                home_score = score
            else:
                away_score = score

        games.append({
            "game_id":    event.get("id", ""),
            "name":       event.get("name", ""),
            "status":     phase,
            "away_score": away_score,
            "home_score": home_score,
            "start_time": event.get("date", ""),
        })

    logger.info("[ESPN] get_game_states: %d games for %s", len(games), date_str)
    return games


def get_game_ids(date_str: str) -> list[str]:
    """
    Return a list of ESPN game IDs for all MLB games on date_str (YYYYMMDD).
    Only includes completed games (status.type.completed == True).
    """
    url = _BASE_SCOREBOARD.format(date=date_str)
    data = _fetch_json(url, cache_key=f"scoreboard_{date_str}")
    if data is None:
        return []

    game_ids = []
    for event in data.get("events", []):
        status = event.get("status", {})
        if status.get("type", {}).get("completed", False):
            game_ids.append(event["id"])

    logger.info("ESPN: %d completed games found for %s", len(game_ids), date_str)
    return game_ids


# ---------------------------------------------------------------------------
# Public: Box score player stats
# ---------------------------------------------------------------------------

def get_player_stats(game_id: str, sleep: bool = True) -> dict[str, dict]:
    """
    Return a dict mapping player_name_lower → stats dict for a single game.

    Stats dict keys (all int or float):
        Batting : r, h, hr, rbi, bb, so, sb, tb, doubles, triples, ab
        Pitching: ip, h_allowed, r_allowed, er, bb_allowed, so_pitched

    Pitchers appear in the 'pitching' section; batters in 'batting'.
    A player who pitches AND bats will have both sets merged.

    Unknown / missing stats default to -1 (can't settle that leg).
    """
    if sleep:
        time.sleep(_SLEEP_BETWEEN_REQUESTS)

    url = _BASE_SUMMARY.format(game_id=game_id)
    data = _fetch_json(url, cache_key=f"summary_{game_id}")
    if data is None:
        return {}

    player_map: dict[str, dict] = {}

    def _upsert(name_key: str, updates: dict) -> None:
        if name_key not in player_map:
            player_map[name_key] = {}
        player_map[name_key].update(updates)

    boxscore = data.get("boxscore", {})

    for team_block in boxscore.get("players", []):
        for stat_group in team_block.get("statistics", []):
            group_name = stat_group.get("name", "").lower()
            keys = [k.lower() for k in stat_group.get("keys", [])]

            for athlete_entry in stat_group.get("athletes", []):
                display_name = (
                    athlete_entry.get("athlete", {})
                    .get("displayName", "")
                    .strip()
                    .lower()
                )
                if not display_name:
                    continue

                raw_stats = athlete_entry.get("stats", [])
                stat_dict = {
                    k: _parse_stat(v)
                    for k, v in zip(keys, raw_stats)
                }

                if group_name == "batting":
                    # ESPN batting keys: ab, r, h, rbi, hr, bb, so, avg, obp, slg, ops
                    # Sometimes includes: 2b, 3b, sb, cs, tb
                    batting = {
                        "r":       stat_dict.get("r",   -1),
                        "h":       stat_dict.get("h",   -1),
                        "hr":      stat_dict.get("hr",  -1),
                        "rbi":     stat_dict.get("rbi", -1),
                        "bb":      stat_dict.get("bb",  -1),
                        "so":      stat_dict.get("so",  -1),
                        "sb":      stat_dict.get("sb",  -1),
                        "tb":      stat_dict.get("tb",  -1),
                        "doubles": stat_dict.get("2b",  -1),
                        "triples": stat_dict.get("3b",  -1),
                        "ab":      stat_dict.get("ab",  -1),
                    }
                    # Compute TB if not directly available
                    if batting["tb"] == -1:
                        singles   = batting["h"]   - max(0, batting.get("doubles", 0) or 0) \
                                                   - max(0, batting.get("triples", 0) or 0) \
                                                   - max(0, batting.get("hr", 0)      or 0)
                        doubles   = batting.get("doubles", -1)
                        triples   = batting.get("triples", -1)
                        hrs       = batting.get("hr", -1)
                        if all(v >= 0 for v in [singles, doubles, triples, hrs]):
                            batting["tb"] = singles + 2 * doubles + 3 * triples + 4 * hrs
                    _upsert(display_name, batting)

                elif group_name == "pitching":
                    # ESPN pitching keys: ip, h, r, er, bb, so, hr, era, ...
                    pitching = {
                        "ip":         _parse_ip(stat_dict.get("ip", -1)),
                        "h_allowed":  stat_dict.get("h",  -1),
                        "r_allowed":  stat_dict.get("r",  -1),
                        "er":         stat_dict.get("er", -1),
                        "bb_allowed": stat_dict.get("bb", -1),
                        "so_pitched": stat_dict.get("so", -1),
                    }
                    _upsert(display_name, pitching)

    logger.debug("ESPN: parsed stats for %d players in game %s", len(player_map), game_id)
    return player_map


# ---------------------------------------------------------------------------
# Public: Full day stats aggregated across all games
# ---------------------------------------------------------------------------

def get_all_player_stats(date_str: str) -> dict[str, dict]:
    """
    Return a merged player_name_lower → stats dict for ALL completed games
    on date_str (YYYYMMDD).  Players who appear in multiple games
    (double-headers) have their numeric stats summed.
    """
    game_ids = get_game_ids(date_str)
    if not game_ids:
        logger.warning("ESPN: no completed games for %s", date_str)
        return {}

    all_stats: dict[str, dict] = {}
    for game_id in game_ids:
        game_stats = get_player_stats(game_id, sleep=True)
        for player, stats in game_stats.items():
            if player not in all_stats:
                all_stats[player] = dict(stats)
            else:
                # Sum numeric stats (skip -1 unknowns)
                for k, v in stats.items():
                    if isinstance(v, (int, float)) and v >= 0:
                        existing = all_stats[player].get(k, -1)
                        all_stats[player][k] = (existing if existing >= 0 else 0) + v

    logger.info(
        "ESPN: %d total players with stats across %d games on %s",
        len(all_stats), len(game_ids), date_str,
    )
    return all_stats


# ---------------------------------------------------------------------------
# Internal stat parsers
# ---------------------------------------------------------------------------

def _parse_stat(value) -> float:
    """Convert a raw stat string/number to float; return -1 on failure."""
    try:
        return float(str(value).replace("--", "-1").replace("-", "-1") if value in ("--", "") else value)
    except (ValueError, TypeError):
        return -1.0


def _parse_ip(value) -> float:
    """
    Parse innings pitched.  ESPN returns '6.0', '6.1', '6.2' where .1 = 1/3 IP
    and .2 = 2/3 IP.  Convert to decimal innings.
    """
    try:
        s = str(value)
        whole, frac = (s.split(".") + ["0"])[:2]
        return int(whole) + int(frac) / 3.0
    except Exception:
        return -1.0
