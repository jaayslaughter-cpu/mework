"""
PropIQ Analytics — MLBDataAggregator
======================================
Pulls player/team/game data from multiple sources:
  1. MLB Stats API (statsapi.mlb.com) — official, free, no key
  2. ESPN API (public endpoints) — scores, standings, injuries
  3. Baseball Reference (requests scrape) — historical stats
  4. pybaseball (Statcast) — advanced metrics

Drop this into: api/services/mlb_data.py
"""

import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any

import requests

logger = logging.getLogger(__name__)

MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb"
ESPN_CORE_BASE = "https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb"
HEADERS = {"User-Agent": "PropIQ/2.0 (analytics)"}


# ─────────────────────────────────────────────
# MLB Stats API
# ─────────────────────────────────────────────
class MLBStatsClient:
    """Official MLB Stats API — free, no auth required."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, path: str, params: Dict = None) -> Optional[Dict]:
        try:
            resp = self.session.get(f"{MLB_STATS_BASE}{path}", params=params or {}, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("MLB Stats API error %s: %s", path, e)
            return None

    def get_schedule(self, game_date: Optional[str] = None) -> List[Dict]:
        """Get games for a date (YYYY-MM-DD). Defaults to today."""
        date_str = game_date or str(date.today())
        data = self._get("/schedule", {"sportId": 1, "date": date_str, "hydrate": "team,venue,weather,probablePitcher"})
        if not data:
            return []

        games = []
        for date_entry in data.get("dates", []):
            for g in date_entry.get("games", []):
                games.append({
                    "game_pk": g["gamePk"],
                    "game_date": g.get("gameDate"),
                    "status": g.get("status", {}).get("detailedState"),
                    "home_team": g.get("teams", {}).get("home", {}).get("team", {}).get("name"),
                    "away_team": g.get("teams", {}).get("away", {}).get("team", {}).get("name"),
                    "home_team_id": g.get("teams", {}).get("home", {}).get("team", {}).get("id"),
                    "away_team_id": g.get("teams", {}).get("away", {}).get("team", {}).get("id"),
                    "venue": g.get("venue", {}).get("name"),
                    "home_pitcher": self._extract_pitcher(g, "home"),
                    "away_pitcher": self._extract_pitcher(g, "away"),
                    "weather": g.get("weather", {}),
                })
        return games

    def get_boxscore(self, game_pk: int) -> Optional[Dict]:
        """Get full boxscore for a completed game."""
        return self._get(f"/game/{game_pk}/boxscore")

    def get_player_stats(self, player_id: int, season: int = None, stat_type: str = "season") -> Optional[Dict]:
        """Get batting or pitching stats for a player."""
        season = season or datetime.now().year
        return self._get(f"/people/{player_id}/stats", {
            "stats": stat_type,
            "season": season,
            "sportId": 1,
        })

    def get_team_roster(self, team_id: int) -> List[Dict]:
        """Get active 26-man roster."""
        data = self._get(f"/teams/{team_id}/roster", {"rosterType": "active"})
        if not data:
            return []
        return [
            {
                "player_id": p["person"]["id"],
                "player_name": p["person"]["fullName"],
                "position": p.get("position", {}).get("abbreviation"),
                "jersey_number": p.get("jerseyNumber"),
                "status": p.get("status", {}).get("description"),
            }
            for p in data.get("roster", [])
        ]

    def get_player_season_log(self, player_id: int, game_type: str = "R") -> List[Dict]:
        """Get game-by-game batting log for a player."""
        data = self._get(f"/people/{player_id}/stats", {
            "stats": "gameLog",
            "season": datetime.now().year,
            "sportId": 1,
            "gameType": game_type,
        })
        if not data:
            return []
        splits = data.get("stats", [{}])[0].get("splits", [])
        return [
            {
                "date": s.get("date"),
                "opponent": s.get("opponent", {}).get("name"),
                "hits": s.get("stat", {}).get("hits", 0),
                "at_bats": s.get("stat", {}).get("atBats", 0),
                "home_runs": s.get("stat", {}).get("homeRuns", 0),
                "rbi": s.get("stat", {}).get("rbi", 0),
                "strikeouts": s.get("stat", {}).get("strikeOuts", 0),
                "walks": s.get("stat", {}).get("baseOnBalls", 0),
                "total_bases": s.get("stat", {}).get("totalBases", 0),
                "is_home": s.get("isHome", False),
            }
            for s in splits
        ]

    def get_game_results(self, game_pk: int) -> Optional[Dict]:
        """Get player-level results from a completed game (for backfilling outcomes)."""
        boxscore = self.get_boxscore(game_pk)
        if not boxscore:
            return None

        results = {"home": [], "away": []}
        for side in ["home", "away"]:
            players = boxscore.get("teams", {}).get(side, {}).get("players", {})
            for _, pdata in players.items():
                batting = pdata.get("stats", {}).get("batting", {})
                pitching = pdata.get("stats", {}).get("pitching", {})
                if batting or pitching:
                    results[side].append({
                        "player_id": pdata["person"]["id"],
                        "player_name": pdata["person"]["fullName"],
                        "batting": batting,
                        "pitching": pitching,
                    })
        return results

    def search_player(self, name: str) -> List[Dict]:
        """Search for a player by name."""
        data = self._get("/people/search", {"names": name, "sportId": 1})
        if not data:
            return []
        return [
            {"id": p["id"], "name": p["fullName"], "position": p.get("primaryPosition", {}).get("abbreviation")}
            for p in data.get("people", [])
        ]

    @staticmethod
    def _extract_pitcher(game: Dict, side: str) -> Optional[str]:
        try:
            pp = game["teams"][side].get("probablePitcher", {})
            return pp.get("fullName")
        except (KeyError, TypeError):
            return None


# ─────────────────────────────────────────────
# ESPN Client
# ─────────────────────────────────────────────
class ESPNClient:
    """ESPN public API — scores, standings, injuries, odds."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, path: str, params: Dict = None) -> Optional[Dict]:
        try:
            resp = self.session.get(f"{ESPN_BASE}{path}", params=params or {}, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("ESPN API error %s: %s", path, e)
            return None

    def get_scoreboard(self, game_date: Optional[str] = None) -> List[Dict]:
        """Get live/completed scores."""
        params = {}
        if game_date:
            params["dates"] = game_date.replace("-", "")
        data = self._get("/scoreboard", params)
        if not data:
            return []

        games = []
        for event in data.get("events", []):
            comp = event.get("competitions", [{}])[0]
            competitors = {
                c["homeAway"]: c
                for c in comp.get("competitors", [])
            }
            games.append({
                "espn_id": event["id"],
                "name": event.get("name"),
                "date": event.get("date"),
                "status": event.get("status", {}).get("type", {}).get("description"),
                "home_team": competitors.get("home", {}).get("team", {}).get("displayName"),
                "away_team": competitors.get("away", {}).get("team", {}).get("displayName"),
                "home_score": competitors.get("home", {}).get("score", "0"),
                "away_score": competitors.get("away", {}).get("score", "0"),
                "inning": event.get("status", {}).get("period"),
                "broadcasts": [b.get("names", []) for b in comp.get("broadcasts", [])],
            })
        return games

    def get_standings(self) -> List[Dict]:
        """Get AL/NL standings."""
        data = self._get("/standings")
        if not data:
            return []

        standings = []
        for group in data.get("children", []):
            for entry in group.get("standings", {}).get("entries", []):
                team = entry.get("team", {})
                stats = {s["name"]: s["value"] for s in entry.get("stats", [])}
                standings.append({
                    "team": team.get("displayName"),
                    "wins": stats.get("wins", 0),
                    "losses": stats.get("losses", 0),
                    "pct": stats.get("winPercent", 0),
                    "gb": stats.get("gamesBehind", 0),
                    "division": group.get("name"),
                    "run_diff": stats.get("differential", 0),
                })
        return standings

    def get_injuries(self) -> List[Dict]:
        """Get current MLB injury report."""
        data = self._get("/injuries")
        if not data:
            return []

        injuries = []
        for item in data.get("injuries", []):
            injuries.append({
                "player": item.get("athlete", {}).get("displayName"),
                "team": item.get("team", {}).get("displayName"),
                "status": item.get("status"),
                "date": item.get("date"),
                "type": item.get("type"),
                "detail": item.get("shortComment"),
            })
        return injuries

    def get_news(self, limit: int = 20) -> List[Dict]:
        """Get latest MLB news."""
        data = self._get("/news", {"limit": limit})
        if not data:
            return []
        return [
            {
                "headline": a.get("headline"),
                "description": a.get("description"),
                "published": a.get("published"),
                "url": a.get("links", {}).get("web", {}).get("href"),
            }
            for a in data.get("articles", [])
        ]

    def get_team_stats(self) -> List[Dict]:
        """Get team batting/pitching stats."""
        data = self._get("/teams")
        if not data:
            return []
        return [
            {
                "id": t.get("id"),
                "name": t.get("displayName"),
                "abbreviation": t.get("abbreviation"),
            }
            for t in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
        ]


# ─────────────────────────────────────────────
# Statcast via pybaseball (optional)
# ─────────────────────────────────────────────
class StatcastClient:
    """Statcast advanced metrics via pybaseball."""

    def __init__(self):
        self._available = self._check_pybaseball()

    @staticmethod
    def _check_pybaseball() -> bool:
        try:
            import pybaseball  # noqa
            return True
        except ImportError:
            logger.warning("pybaseball not installed. Statcast disabled. Run: pip install pybaseball")
            return False

    def get_batter_statcast(self, player_name: str, start_dt: str, end_dt: str) -> Optional[Any]:
        """Get Statcast data for a batter."""
        if not self._available:
            return None
        try:
            from pybaseball import statcast_batter, playerid_lookup
            parts = player_name.split()
            lkup = playerid_lookup(parts[-1], parts[0] if len(parts) > 1 else "")
            if lkup.empty:
                return None
            player_id = int(lkup.iloc[0]["key_mlbam"])
            return statcast_batter(start_dt, end_dt, player_id)
        except Exception as e:
            logger.error("Statcast batter error: %s", e)
            return None

    def get_pitcher_statcast(self, player_name: str, start_dt: str, end_dt: str) -> Optional[Any]:
        if not self._available:
            return None
        try:
            from pybaseball import statcast_pitcher, playerid_lookup
            parts = player_name.split()
            lkup = playerid_lookup(parts[-1], parts[0] if len(parts) > 1 else "")
            if lkup.empty:
                return None
            player_id = int(lkup.iloc[0]["key_mlbam"])
            return statcast_pitcher(start_dt, end_dt, player_id)
        except Exception as e:
            logger.error("Statcast pitcher error: %s", e)
            return None

    def get_sprint_speed(self, season: int = None) -> Optional[Any]:
        if not self._available:
            return None
        try:
            from pybaseball import statcast_sprint_speed
            return statcast_sprint_speed(season or datetime.now().year)
        except Exception as e:
            logger.error("Sprint speed error: %s", e)
            return None


# ─────────────────────────────────────────────
# Unified Aggregator
# ─────────────────────────────────────────────
class MLBDataAggregator:
    """
    Single entry point for all MLB data.
    Combines MLB Stats API + ESPN + Statcast into unified dicts.
    """

    def __init__(self):
        self.mlb = MLBStatsClient()
        self.espn = ESPNClient()
        self.statcast = StatcastClient()

    def get_todays_context(self) -> Dict:
        """
        Pull everything needed for today's prop model:
        games, probable pitchers, injuries, standings.
        """
        today = str(date.today())
        return {
            "date": today,
            "games": self.mlb.get_schedule(today),
            "injuries": self.espn.get_injuries(),
            "scoreboard": self.espn.get_scoreboard(today),
            "news": self.espn.get_news(limit=10),
        }

    def build_player_features(self, player_name: str, game_date: Optional[str] = None) -> Dict:
        """
        Build feature dict for a player for model input.
        Uses last-7 and last-30 day rolling stats.
        """
        game_date = game_date or str(date.today())

        # Find player ID
        results = self.mlb.search_player(player_name)
        if not results:
            return {"player": player_name, "error": "Player not found"}

        player_id = results[0]["id"]

        # Game log
        log = self.mlb.get_player_season_log(player_id)
        if not log:
            return {"player": player_name, "player_id": player_id}

        import pandas as pd
        df = pd.DataFrame(log)
        if df.empty:
            return {"player": player_name, "player_id": player_id}

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date", ascending=False)

        # Recent form
        last_7 = df.head(7)
        last_30 = df.head(30)

        def safe_mean(series):
            return round(float(series.mean()), 3) if not series.empty else 0.0

        return {
            "player": player_name,
            "player_id": player_id,
            "position": results[0].get("position"),
            # 7-day averages
            "hits_avg_7": safe_mean(last_7["hits"]),
            "hr_avg_7": safe_mean(last_7["home_runs"]),
            "k_avg_7": safe_mean(last_7["strikeouts"]),
            "tb_avg_7": safe_mean(last_7["total_bases"]),
            "rbi_avg_7": safe_mean(last_7["rbi"]),
            # 30-day averages
            "hits_avg_30": safe_mean(last_30["hits"]),
            "hr_avg_30": safe_mean(last_30["home_runs"]),
            "k_avg_30": safe_mean(last_30["strikeouts"]),
            "tb_avg_30": safe_mean(last_30["total_bases"]),
            "rbi_avg_30": safe_mean(last_30["rbi"]),
            # Trend flag
            "on_hot_streak": safe_mean(last_7["hits"]) > safe_mean(last_30["hits"]) * 1.2,
            "games_in_log": len(df),
        }

    def get_pitcher_context(self, pitcher_name: str) -> Dict:
        """Get key context for an opposing pitcher."""
        results = self.mlb.search_player(pitcher_name)
        if not results:
            return {"pitcher": pitcher_name, "error": "Not found"}

        player_id = results[0]["id"]
        log = self.mlb.get_player_season_log(player_id)
        if not log:
            return {"pitcher": pitcher_name, "player_id": player_id}

        # For pitchers the stats come back differently — use season totals
        stats_data = self.mlb.get_player_stats(player_id, stat_type="season")
        stats = {}
        if stats_data:
            splits = stats_data.get("stats", [{}])[0].get("splits", [{}])
            if splits:
                stats = splits[0].get("stat", {})

        return {
            "pitcher": pitcher_name,
            "player_id": player_id,
            "era": stats.get("era", "N/A"),
            "whip": stats.get("whip", "N/A"),
            "k9": stats.get("strikeoutsPer9Inn", "N/A"),
            "bb9": stats.get("walksPer9Inn", "N/A"),
            "avg_against": stats.get("avg", "N/A"),
            "games_started": stats.get("gamesStarted", 0),
        }

    def backfill_prop_results(self, game_pk: int) -> List[Dict]:
        """
        Get actual stat lines from a completed game.
        Use to update PropModel with actual_result.
        """
        results = self.mlb.get_game_results(game_pk)
        if not results:
            return []

        rows = []
        for side in ["home", "away"]:
            for player in results[side]:
                batting = player["batting"]
                pitching = player["pitching"]
                rows.append({
                    "player_name": player["player_name"],
                    "player_id": player["player_id"],
                    "hits": batting.get("hits", 0),
                    "home_runs": batting.get("homeRuns", 0),
                    "rbi": batting.get("rbi", 0),
                    "total_bases": batting.get("totalBases", 0),
                    "strikeouts_batter": batting.get("strikeOuts", 0),
                    "walks": batting.get("baseOnBalls", 0),
                    "pitcher_ks": pitching.get("strikeOuts", 0),
                    "pitcher_er": pitching.get("earnedRuns", 0),
                    "pitcher_hits": pitching.get("hits", 0),
                    "pitcher_walks": pitching.get("baseOnBalls", 0),
                    "outs_recorded": pitching.get("outs", 0),
                })
        return rows


# ─────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────
_aggregator: Optional[MLBDataAggregator] = None


def get_mlb_data() -> MLBDataAggregator:
    global _aggregator
    if _aggregator is None:
        _aggregator = MLBDataAggregator()
    return _aggregator
