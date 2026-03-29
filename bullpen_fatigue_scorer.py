"""
bullpen_fatigue_scorer.py
=========================
PropIQ — Daily reliever workload tracker for BullpenAgent.

WHY THIS EXISTS
---------------
BullpenAgent evaluates earned_runs, runs, and hits props based on
opposing bullpen fatigue. Without real pitching-log data every prop
scores at neutral fatigue (2.0), making BullpenAgent blind to its
core signal.

HOW IT WORKS
------------
PitchingLogFetcher:
  - Hits MLB Stats API /schedule for the last N days to get gamePks
  - Hits /game/{gamePk}/boxscore for each game to get all pitchers
  - Identifies relievers as any pitcher at index > 0 in the pitching order
  - Caches to /tmp/propiq_bullpen_{date}.json so Railway re-runs hit disk

BullpenFatigueScorer:
  - Ingests list of pitching log dicts
  - Weights workload by recency: yesterday=1.0, 2d ago=0.50, 3d ago=0.25
  - score(team) -> float 0.0–5.0  (2.0 = neutral, 0.0 = rested, 5.0 = cooked)
  - get_fatigue_boost(team) -> prob_delta to add to implied_prob

INTEGRATION (live_dispatcher.py)
---------------------------------
  from bullpen_fatigue_scorer import build_bullpen_fatigue_scorer

  # In LiveDispatcher.run(), after schedule fetch:
  self._bullpen_scorer = build_bullpen_fatigue_scorer(games)

  # In _evaluate_props(), for runs/earned_runs/hits Over:
  boost = self._bullpen_scorer.get_fatigue_boost(opposing_team)
  prob = min(0.80, prob + boost)

SIGNAL STRENGTH
---------------
Each IP thrown by a reliever in the last 3 days contributes to their
team's fatigue score. The boost caps at +0.045 (4.5%) when a bullpen
is fully exhausted (score = 5.0). At neutral (2.0) boost = 0.0.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from typing import Any

import requests

logger = logging.getLogger("propiq.bullpen")

_MLBAPI_BASE = "https://statsapi.mlb.com/api/v1"
_HEADERS = {"User-Agent": "PropIQ/1.0", "Accept": "application/json"}

# Decay weights by days ago (1 = yesterday, 2 = 2 days ago, 3 = 3 days ago)
_DECAY: dict[int, float] = {1: 1.0, 2: 0.50, 3: 0.25}

# How many weighted IP-equivalent units = fully fatigued (score 5.0)
# Typical busy bullpen uses 4 relievers at 1 IP each = 4 weighted IP/day
# Over 3 days with decay: ~4 + 2 + 1 = 7 → maps to 5.0
_MAX_WEIGHTED_IP = 7.0

# Neutral score when no data — matches the previously hard-coded default
NEUTRAL_SCORE = 2.0


# ---------------------------------------------------------------------------
# Team name normalisation
# ---------------------------------------------------------------------------

_TEAM_ALIASES: dict[str, str] = {
    # Official full name → short key used everywhere in PropIQ
    "arizona diamondbacks":     "arizona diamondbacks",
    "atlanta braves":           "atlanta braves",
    "baltimore orioles":        "baltimore orioles",
    "boston red sox":           "boston red sox",
    "chicago cubs":             "chicago cubs",
    "chicago white sox":        "chicago white sox",
    "cincinnati reds":          "cincinnati reds",
    "cleveland guardians":      "cleveland guardians",
    "colorado rockies":         "colorado rockies",
    "detroit tigers":           "detroit tigers",
    "houston astros":           "houston astros",
    "kansas city royals":       "kansas city royals",
    "los angeles angels":       "los angeles angels",
    "los angeles dodgers":      "los angeles dodgers",
    "miami marlins":            "miami marlins",
    "milwaukee brewers":        "milwaukee brewers",
    "minnesota twins":          "minnesota twins",
    "new york mets":            "new york mets",
    "new york yankees":         "new york yankees",
    "oakland athletics":        "oakland athletics",
    "philadelphia phillies":    "philadelphia phillies",
    "pittsburgh pirates":       "pittsburgh pirates",
    "san diego padres":         "san diego padres",
    "san francisco giants":     "san francisco giants",
    "seattle mariners":         "seattle mariners",
    "st. louis cardinals":      "st. louis cardinals",
    "tampa bay rays":           "tampa bay rays",
    "texas rangers":            "texas rangers",
    "toronto blue jays":        "toronto blue jays",
    "washington nationals":     "washington nationals",
    # Common short-forms
    "diamondbacks": "arizona diamondbacks",
    "braves":       "atlanta braves",
    "orioles":      "baltimore orioles",
    "red sox":      "boston red sox",
    "cubs":         "chicago cubs",
    "white sox":    "chicago white sox",
    "reds":         "cincinnati reds",
    "guardians":    "cleveland guardians",
    "rockies":      "colorado rockies",
    "tigers":       "detroit tigers",
    "astros":       "houston astros",
    "royals":       "kansas city royals",
    "angels":       "los angeles angels",
    "dodgers":      "los angeles dodgers",
    "marlins":      "miami marlins",
    "brewers":      "milwaukee brewers",
    "twins":        "minnesota twins",
    "mets":         "new york mets",
    "yankees":      "new york yankees",
    "athletics":    "oakland athletics",
    "phillies":     "philadelphia phillies",
    "pirates":      "pittsburgh pirates",
    "padres":       "san diego padres",
    "giants":       "san francisco giants",
    "mariners":     "seattle mariners",
    "cardinals":    "st. louis cardinals",
    "rays":         "tampa bay rays",
    "rangers":      "texas rangers",
    "blue jays":    "toronto blue jays",
    "nationals":    "washington nationals",
}


def _norm_team(name: str) -> str:
    """Normalise a team name to a canonical lowercase key."""
    key = (name or "").strip().lower()
    return _TEAM_ALIASES.get(key, key)


def _ip_to_decimal(ip_str: str | float) -> float:
    """
    Convert MLB innings-pitched string to a decimal float.
    '5.2' means 5 full innings + 2 outs = 5.667 innings.
    '6.0' means 6 full innings.
    """
    try:
        val = float(ip_str or 0)
        full = int(val)
        outs = round((val - full) * 10)   # '0.2' -> 2 outs
        return full + outs / 3.0
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# PitchingLogFetcher
# ---------------------------------------------------------------------------

class PitchingLogFetcher:
    """
    Fetches reliever pitching logs for the last N days via MLB Stats API.

    Results are cached to /tmp/propiq_bullpen_{date}.json so multiple
    calls on the same day are free. Cache key includes lookback_days so
    different lookback windows don't collide.
    """

    CACHE_DIR = "/tmp"

    def __init__(self, lookback_days: int = 3) -> None:
        self.lookback_days = lookback_days

    def _cache_path(self, today: date) -> str:
        return os.path.join(
            self.CACHE_DIR,
            f"propiq_bullpen_{today.isoformat()}_d{self.lookback_days}.json",
        )

    def _load_cache(self, today: date) -> list[dict] | None:
        path = self._cache_path(today)
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                data = json.load(f)
            logger.info("[Bullpen] Cache hit: %d log entries from %s", len(data), path)
            return data
        except Exception:
            return None

    def _save_cache(self, today: date, logs: list[dict]) -> None:
        path = self._cache_path(today)
        try:
            with open(path, "w") as f:
                json.dump(logs, f)
        except Exception as exc:
            logger.debug("[Bullpen] Cache write failed: %s", exc)

    def _get_game_pks(self, d: date) -> list[int]:
        """Fetch game PKs for a specific date."""
        try:
            resp = requests.get(
                f"{_MLBAPI_BASE}/schedule",
                params={
                    "sportId": 1,
                    "date": d.isoformat(),
                    "gameType": "R",
                },
                headers=_HEADERS,
                timeout=12,
            )
            if resp.status_code != 200:
                return []
            pks: list[int] = []
            for date_block in resp.json().get("dates", []):
                for g in date_block.get("games", []):
                    pk = g.get("gamePk")
                    status = g.get("status", {}).get("abstractGameState", "")
                    # Only completed games have boxscore pitching data
                    if pk and status == "Final":
                        pks.append(int(pk))
            return pks
        except Exception as exc:
            logger.warning("[Bullpen] Schedule fetch failed for %s: %s", d, exc)
            return []

    def _get_boxscore_logs(self, game_pk: int, days_ago: int) -> list[dict]:
        """
        Fetch one game's boxscore and extract reliever pitching entries.

        Returns list of dicts:
            {team, player_id, player_name, ip_decimal, is_starter, days_ago}
        """
        try:
            resp = requests.get(
                f"{_MLBAPI_BASE}/game/{game_pk}/boxscore",
                headers=_HEADERS,
                timeout=12,
            )
            if resp.status_code != 200:
                return []

            data = resp.json()
            logs: list[dict] = []

            for side in ("home", "away"):
                team_info = data.get("teams", {}).get(side, {})
                team_name = team_info.get("team", {}).get("name", "")
                team_norm = _norm_team(team_name)

                # Pitcher order list (mlbam IDs in order of appearance)
                pitcher_ids: list[int] = team_info.get("pitchers", [])
                # Player detail map
                players: dict = team_info.get("players", {})

                for idx, pid in enumerate(pitcher_ids):
                    is_starter = (idx == 0)
                    player_key = f"ID{pid}"
                    player_data = players.get(player_key, {})
                    pit_stats = (
                        player_data.get("stats", {})
                        .get("pitching", {})
                    )
                    ip_str = pit_stats.get("inningsPitched", "0.0")
                    ip_dec = _ip_to_decimal(ip_str)

                    full_name = (
                        player_data.get("person", {}).get("fullName", "")
                        or f"player_{pid}"
                    )

                    logs.append({
                        "team":        team_norm,
                        "player_id":   int(pid),
                        "player_name": full_name,
                        "ip_decimal":  ip_dec,
                        "is_starter":  is_starter,
                        "days_ago":    days_ago,
                        "game_pk":     game_pk,
                    })

            return logs

        except Exception as exc:
            logger.debug("[Bullpen] Boxscore %d failed: %s", game_pk, exc)
            return []

    def fetch(self) -> list[dict]:
        """
        Fetch pitching logs for the last N days.

        Returns list of dicts with reliever entries only (is_starter=False).
        Cached per calendar day.
        """
        today = date.today()
        cached = self._load_cache(today)
        if cached is not None:
            return cached

        all_logs: list[dict] = []

        for days_ago in range(1, self.lookback_days + 1):
            d = today - timedelta(days=days_ago)
            pks = self._get_game_pks(d)
            logger.info(
                "[Bullpen] %s: %d completed games found", d.isoformat(), len(pks)
            )
            for pk in pks:
                entries = self._get_boxscore_logs(pk, days_ago)
                all_logs.extend(entries)

        logger.info("[Bullpen] Total pitching entries fetched: %d", len(all_logs))
        self._save_cache(today, all_logs)
        return all_logs


# ---------------------------------------------------------------------------
# BullpenFatigueScorer
# ---------------------------------------------------------------------------

class BullpenFatigueScorer:
    """
    Converts pitching logs into per-team fatigue scores.

    Score range: 0.0 (completely rested) → 5.0 (fully exhausted)
    Neutral:     2.0 (returned when no data available)

    Fatigue formula:
        For each reliever appearance:
            weighted_ip += ip_decimal * _DECAY[days_ago]
        team_score = (total_weighted_ip / _MAX_WEIGHTED_IP) * 5.0
        clamped to [0.0, 5.0]

    Probability boost:
        boost = (score - NEUTRAL_SCORE) * 0.015
        max +0.045 when score = 5.0
        min  0.0   (never penalise — BullpenAgent only bets Over)
    """

    def __init__(self, logs: list[dict]) -> None:
        # {team_norm: total_weighted_ip}
        self._team_weighted_ip: dict[str, float] = {}
        self._team_reliever_count: dict[str, int] = {}
        self._raw_logs = logs

        # Build per-team weighted workload from RELIEVERS ONLY (starters excluded)
        for entry in logs:
            if entry.get("is_starter", True):
                continue   # starters don't fatigue the bullpen
            ip  = float(entry.get("ip_decimal", 0.0))
            if ip <= 0.0:
                continue
            dago  = int(entry.get("days_ago", 1))
            decay = _DECAY.get(dago, 0.0)
            team  = entry.get("team", "")
            if not team:
                continue

            self._team_weighted_ip[team] = (
                self._team_weighted_ip.get(team, 0.0) + ip * decay
            )
            self._team_reliever_count[team] = (
                self._team_reliever_count.get(team, 0) + 1
            )

        n_teams = len(self._team_weighted_ip)
        logger.info(
            "[Bullpen] Scorer built: %d teams with reliever data (of %d raw entries)",
            n_teams, len(logs),
        )
        if n_teams:
            top3 = sorted(
                self._team_weighted_ip.items(), key=lambda x: -x[1]
            )[:3]
            logger.info(
                "[Bullpen] Most fatigued: %s",
                ", ".join(f"{t} ({round(v, 2)} wIP)" for t, v in top3),
            )

    def score(self, team: str) -> float:
        """
        Return fatigue score for a team (0.0=rested, 5.0=exhausted, 2.0=no data).
        """
        key = _norm_team(team)
        weighted_ip = self._team_weighted_ip.get(key)
        if weighted_ip is None:
            return NEUTRAL_SCORE
        raw_score = (weighted_ip / _MAX_WEIGHTED_IP) * 5.0
        return round(min(5.0, max(0.0, raw_score)), 3)

    def get_fatigue_boost(self, team: str) -> float:
        """
        Return probability boost to add to implied_prob for Over bets
        against a fatigued bullpen.

        Returns 0.0 when team is neutral or rested (never penalises).
        Max return: +0.045 (4.5%) when score = 5.0.
        """
        s = self.score(team)
        delta = s - NEUTRAL_SCORE
        if delta <= 0.0:
            return 0.0   # rested bullpen -> no boost
        return round(min(0.045, delta * 0.015), 4)

    def summary(self) -> dict[str, float]:
        """Return {team: score} for all teams with data."""
        return {
            team: self.score(team)
            for team in self._team_weighted_ip
        }


# ---------------------------------------------------------------------------
# Top-level builder — call this from live_dispatcher
# ---------------------------------------------------------------------------

def build_bullpen_fatigue_scorer(lookback_days: int = 3) -> BullpenFatigueScorer:
    """
    Fetch reliever logs for last N days and return a ready BullpenFatigueScorer.

    Falls back to an empty scorer (all teams return NEUTRAL_SCORE) on any error.
    Safe to call at dispatcher startup — cached after first call of the day.
    """
    try:
        fetcher = PitchingLogFetcher(lookback_days=lookback_days)
        logs = fetcher.fetch()
        return BullpenFatigueScorer(logs)
    except Exception as exc:
        logger.warning(
            "[Bullpen] build_bullpen_fatigue_scorer failed: %s -- neutral fallback",
            exc,
        )
        return BullpenFatigueScorer([])
