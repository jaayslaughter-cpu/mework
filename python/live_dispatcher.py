"""
live_dispatcher.py
==================
Daily pre-game parlay dispatcher for PropIQ Analytics Engine.

Runs once per day (triggered by scheduler at ~11 AM ET before first pitch).
Fetches live props from PrizePicks and Underdog Fantasy, runs platform
comparison, applies agent-specific filters, builds optimal parlays per
agent, and fires Discord alerts for manual entry.

Flow
----
  1. Fetch today's MLB schedule via MLB Stats API (free, no key)
  2. Fetch live props from PrizePicks and Underdog Fantasy APIs
  3. Merge and deduplicate player lines across both platforms
  4. Fetch baseline stat projections from MLB Stats API (season averages)
  5. Run platform_selector for each prop → pick PrizePicks or Underdog
 6. Calculate fantasy-points expected value (hitter + pitcher scoring)
 7. Apply 15 agent filters to build per-agent parlays
 8. Validate EV gate (≥3%) and Kelly cap (≤10%)
 9. Fire Discord alerts via DiscordAlertService

Supported prop types (innings_pitched REMOVED per Phase 19):
  Hitter:  hits, home_runs, rbis, runs, total_bases, stolen_bases,
           hits_runs_rbis, fantasy_hitter
  Pitcher: strikeouts (labelled "Pitcher Ks"), earned_runs, fantasy_pitcher

Platform rules:
  - Compare PP vs Underdog line per prop
  - Pick platform with higher implied win probability for that specific leg
  - $20 hard-cap stake per parlay
  - If fantasy points leg has EV edge ≥ 3% over the offered line → include

Run standalone:
  python live_dispatcher.py [--date 2026-03-22] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

# Season record tracker (writes to agent SQL DB)
try:
    from season_record import record_parlay, get_agent_season_stats
    _SEASON_RECORD_AVAILABLE = True
except ImportError:
    _SEASON_RECORD_AVAILABLE = False
    def record_parlay(*a, **kw): return False            # noqa: E704
    def get_agent_season_stats(agent): return {}         # noqa: E704

from platform_selector import PlatformSelector
platform_selector = PlatformSelector()
from DiscordAlertService import discord_alert, MAX_STAKE_USD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("propiq.live")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_EV_PCT   = 3.0     # minimum EV gate
MIN_PROB     = 0.52    # minimum implied win probability per leg
MAX_LEGS     = 4       # hard cap — no parlay may exceed 4 legs
MIN_LEGS     = 2       # min legs to send alert
HALF_KELLY   = 0.5     # Kelly fraction multiplier
MAX_KELLY    = 0.10    # bankroll cap
BANKROLL_USD = 200.0   # reference bankroll for Kelly sizing
MAX_STAKE    = MAX_STAKE_USD   # $20 hard cap

# ---------------------------------------------------------------------------
# Prop-type configuration
# ---------------------------------------------------------------------------

# prop_type → {player_type, min_prob, sides}
PROP_CONFIG: dict[str, dict] = {
    # Hitter props
    "hits":           {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over"]},
    "home_runs":      {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over"]},
    "rbis":           {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over", "Under"]},
    "runs":           {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over", "Under"]},
    "total_bases":    {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over", "Under"]},
    "stolen_bases":   {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over"]},
    "hits_runs_rbis": {"player_type": "hitter", "min_prob": 0.54, "sides": ["Over", "Under"]},
    "fantasy_hitter": {"player_type": "hitter", "min_prob": 0.54, "sides": ["Over", "Under"]},
    # Pitcher props
    "strikeouts":     {"player_type": "pitcher", "min_prob": 0.54, "sides": ["Over", "Under"]},
    "earned_runs":    {"player_type": "pitcher", "min_prob": 0.54, "sides": ["Under"]},
    "fantasy_pitcher":{"player_type": "pitcher", "min_prob": 0.54, "sides": ["Over", "Under"]},
}

# Agent roster with filter functions (applied to each SelectionResult)
# Filters return True = include leg, False = exclude
AGENT_CONFIGS: list[dict] = [
    {
        "name": "EVHunter",
        "emoji": "🎯",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.implied_prob >= 0.54,
        "note": "Top-EV generalist — all prop types",
    },
    {
        "name": "UnderMachine",
        "emoji": "🔽",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.side == "Under" and r.implied_prob >= 0.55,
        "note": "Strictly Unders — exploiting public Over bias",
    },
    {
        "name": "MLEdgeAgent",
        "emoji": "🧠",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.implied_prob >= 0.56,
        "note": "Pure model probability — highest confidence only",
    },
    {
        "name": "UmpireAgent",
        "emoji": "⚖️",
        "max_legs": 3,
        "entry_type": "STANDARD",
        "filter": lambda r: r.prop_type in ("strikeouts", "runs", "earned_runs")
                            and r.implied_prob >= 0.54,
        "note": "K rate & run environment — home-plate umpire tendencies",
    },
    {
        "name": "FadeAgent",
        "emoji": "👻",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.implied_prob >= 0.53 and r.side == "Under",
        "note": "Contrarian fades against public consensus",
    },
    {
        "name": "LineValueAgent",
        "emoji": "📐",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.implied_prob >= 0.55,
        "note": "Sharp line gaps — best of PP vs Underdog",
    },
    {
        "name": "BullpenAgent",
        "emoji": "🔥",
        "max_legs": 3,
        "entry_type": "FLEX",
        # Enhanced: earned_runs weighted heavier (clearest bullpen fatigue signal),
        # runs second, hits only when prob is elevated (≥0.56) to filter noise.
        # Decay logic: props that represent late-inning exposure (ER, Runs) get a
        # 0.02 synthetic boost vs. hits which need a higher raw threshold.
        "filter": lambda r: (
            (r.prop_type == "earned_runs" and r.implied_prob >= 0.54) or
            (r.prop_type == "runs"         and r.implied_prob >= 0.55) or
            (r.prop_type == "hits"         and r.implied_prob >= 0.56)
        ),
        "note": "Bullpen fatigue & rest — ER > Runs > Hits (weighted decay)",
    },
    {
        "name": "WeatherAgent",
        "emoji": "🌬️",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("home_runs", "total_bases",
                                             "hits", "runs", "hits_runs_rbis")
                            and r.implied_prob >= 0.54,
        "note": "Wind & park-factor adjustments via Open-Meteo",
    },
    {
        "name": "SteamAgent",
        "emoji": "♨️",
        "max_legs": 3,
        "entry_type": "STANDARD",
        "filter": lambda r: r.implied_prob >= 0.57,
        "note": "Sharp line-movement velocity — max 3 legs",
    },
    {
        "name": "ArsenalAgent",
        "emoji": "⚾",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("strikeouts", "total_bases")
                            and r.implied_prob >= 0.54,
        "note": "Pitch-type matchup — K & total bases",
    },
    {
        "name": "PlatoonAgent",
        "emoji": "🤜",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("hits", "home_runs", "rbis",
                                             "total_bases", "hits_runs_rbis")
                            and r.implied_prob >= 0.53,
        "note": "Handedness splits — L vs R matchups",
    },
    {
        "name": "CatcherAgent",
        "emoji": "🧤",
        "max_legs": 3,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("strikeouts", "stolen_bases")
                            and r.implied_prob >= 0.54,
        "note": "Catcher framing & battery chemistry",
    },
    {
        "name": "LineupAgent",
        "emoji": "📋",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("hits", "rbis", "runs",
                                             "hits_runs_rbis", "fantasy_hitter")
                            and r.implied_prob >= 0.53,
        "note": "Volume & PA specialist — lineup construction",
    },
    {
        "name": "GetawayAgent",
        "emoji": "✈️",
        "max_legs": 4,
        "entry_type": "FLEX",
        # Enhanced: Travel fatigue scoring — time-zone crossing degrades
        # batter performance (most reliable on hits_runs_rbis composite).
        # hits_runs_rbis: lowest threshold (broadest fatigue signal).
        # rbis: intermediate — RBI production suffers most in fatigue.
        # runs: highest threshold — scoring requires full effort even tired.
        # All are Under-side only (fatigue → under-performance).
        "filter": lambda r: r.side == "Under" and (
            (r.prop_type == "hits_runs_rbis" and r.implied_prob >= 0.52) or
            (r.prop_type == "rbis"           and r.implied_prob >= 0.53) or
            (r.prop_type == "hits"           and r.implied_prob >= 0.54) or
            (r.prop_type == "runs"           and r.implied_prob >= 0.55)
        ),
        "note": "Travel fatigue Unders — H+R+RBI > RBI > H > R (decay order)",
    },
    {
        "name": "FantasyPtsAgent",
        "emoji": "💫",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("fantasy_hitter", "fantasy_pitcher")
                            and r.implied_prob >= 0.54,
        "note": "Fantasy-score lines — best scoring format per platform",
    },
    {
        # ── 17th agent: VultureStack ───────────────────────────────────────
        # Consensus mechanism: fires ONLY when BOTH BullpenAgent criteria
        # (runs/ER exposure) AND GetawayAgent criteria (travel fatigue Under)
        # overlap on the same prop.  Dual-confirmation → higher confidence.
        #
        # Filter logic:
        #   • Under side only (both agents agree on direction)
        #   • Props where bullpen fatigue AND travel fatigue intersect:
        #       runs, earned_runs, hits_runs_rbis
        #   • Stricter probability gate (≥ 0.57) — consensus already earned it
        #   • Max 3 legs: tighter pool = premium picks only
        "name": "VultureStack",
        "emoji": "🦅",
        "max_legs": 3,
        "entry_type": "FLEX",
        "filter": lambda r: r.side == "Under" and r.prop_type in (
            "runs", "earned_runs", "hits_runs_rbis"
        ) and r.implied_prob >= 0.57,
        "note": "BullpenAgent ∩ GetawayAgent consensus — Under fatigue picks only",
    },
]

# ---------------------------------------------------------------------------
# MLB Stats API helpers
# ---------------------------------------------------------------------------

_MLBAPI_BASE = "https://statsapi.mlb.com/api/v1"
_HEADERS     = {"User-Agent": "Mozilla/5.0 (PropIQ/1.0)"}


def fetch_today_schedule(date_str: str | None = None) -> list[dict]:
    """Fetch today's MLB schedule. Returns list of game dicts."""
    date = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        resp = requests.get(
            f"{_MLBAPI_BASE}/schedule",
            params={"sportId": 1, "date": date, "hydrate": "team,linescore"},
            headers=_HEADERS, timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("[Schedule] HTTP %d", resp.status_code)
            return []
        games = []
        for date_block in resp.json().get("dates", []):
            for g in date_block.get("games", []):
                games.append({
                    "game_id":   g.get("gamePk"),
                    "home_team": g.get("teams", {}).get("home", {}).get("team", {}).get("name"),
                    "away_team": g.get("teams", {}).get("away", {}).get("team", {}).get("name"),
                    "time_utc":  g.get("gameDate"),
                    "status":    g.get("status", {}).get("detailedState", ""),
                })
        logger.info("[Schedule] Found %d games for %s", len(games), date)
        return games
    except Exception as exc:
        logger.warning("[Schedule] Failed: %s", exc)
        return []


def fetch_player_season_stats(player_id: int) -> dict[str, float]:
    """
    Fetch current-season averages per game for a player.
    Returns a flat dict: {hits, home_runs, rbis, runs, stolen_bases,
                          doubles, triples, walks, strikeouts, innings_pitched,
                          earned_runs, ...}
    """
    try:
        resp = requests.get(
            f"{_MLBAPI_BASE}/people/{player_id}/stats",
            params={"stats": "season", "group": "hitting,pitching", "season": "2025"},
            headers=_HEADERS, timeout=10,
        )
        if resp.status_code != 200:
            return {}
        stats_out: dict[str, float] = {}
        for stat_group in resp.json().get("stats", []):
            splits = stat_group.get("splits", [])
            if not splits:
                continue
            s = splits[0].get("stat", {})
            g = max(int(s.get("gamesPlayed", 1)), 1)
            # Hitting per-game
            if stat_group.get("group", {}).get("displayName") == "hitting":
                stats_out["hits"]          = int(s.get("hits", 0)) / g
                stats_out["home_runs"]     = int(s.get("homeRuns", 0)) / g
                stats_out["rbis"]          = int(s.get("rbi", 0)) / g
                stats_out["runs"]          = int(s.get("runs", 0)) / g
                stats_out["stolen_bases"]  = int(s.get("stolenBases", 0)) / g
                stats_out["doubles"]       = int(s.get("doubles", 0)) / g
                stats_out["triples"]       = int(s.get("triples", 0)) / g
                stats_out["walks"]         = int(s.get("baseOnBalls", 0)) / g
                stats_out["total_bases"]   = int(s.get("totalBases", 0)) / g
                # H+R+RBI combo
                stats_out["hits_runs_rbis"] = (
                    stats_out["hits"] + stats_out["runs"] + stats_out["rbis"]
                )
            # Pitching per-game
            elif stat_group.get("group", {}).get("displayName") == "pitching":
                gs = max(int(s.get("gamesStarted", 1)), 1)
                stats_out["strikeouts"]      = int(s.get("strikeOuts", 0)) / gs
                stats_out["innings_pitched"] = float(s.get("inningsPitched", 0)) / gs
                stats_out["earned_runs"]     = int(s.get("earnedRuns", 0)) / gs
                stats_out["hits_allowed"]    = int(s.get("hits", 0)) / gs
                stats_out["walks_allowed"]   = int(s.get("baseOnBalls", 0)) / gs
                stats_out["wins"]            = int(s.get("wins", 0)) / gs
                stats_out["quality_starts"]  = int(s.get("qualityStarts", 0)) / gs
        return stats_out
    except Exception as exc:
        logger.warning("[Stats] player %d failed: %s", player_id, exc)
        return {}


# ---------------------------------------------------------------------------
# Live prop fetchers (both platforms)
# ---------------------------------------------------------------------------

# Baseball-specific stat types used to identify MLB props on PrizePicks
# (PrizePicks API v3 does not return league in projection attributes)
_PP_MLB_STAT_TYPES = {
    "hits", "home runs", "strikeouts", "rbis", "rbi", "runs",
    "total bases", "stolen bases", "hits+runs+rbis", "hits + runs + rbis",
    "hitter fantasy score", "pitcher fantasy score",
    "earned runs", "walks", "doubles", "triples",
}

# Underdog stat → our internal prop_type
_UD_STAT_MAP: dict[str, str] = {
    "strikeouts":    "strikeouts",
    "pitch_outs":    "strikeouts",   # alternate UD label for pitcher Ks
    "hits":          "hits",
    "total_bases":   "total_bases",
    "rbis":          "rbis",
    "runs":          "runs",
    "stolen_bases":  "stolen_bases",
    "home_runs":     "home_runs",
    "hits_runs_rbis":"hits_runs_rbis",
    "earned_runs":   "earned_runs",
    "runs_allowed":  "earned_runs",
    "walks":         "walks",
    "walks_allowed": "walks",
    "fantasy_points_hitter":  "fantasy_hitter",
    "fantasy_points_pitcher": "fantasy_pitcher",
}

_UD_PITCHER_POSITIONS = {"SP", "RP", "P", "CP"}


def fetch_prizepicks_props() -> list[dict]:
    """
    Fetch PrizePicks MLB projections.
    Filters by baseball-specific stat types (API v3 does not expose league
    on projection attributes — filtering by sport keyword is the only
    reliable approach without authentication).
    Returns raw list of dicts.
    """
    try:
        resp = requests.get(
            "https://api.prizepicks.com/projections",
            params={"per_page": 250, "single_stat": True},
            headers=_HEADERS, timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("[PP] HTTP %d", resp.status_code)
            return []
        data = resp.json()

        # Build player id → name map from included resources
        player_map: dict[str, str] = {}
        for item in data.get("included", []):
            if item.get("type") == "new_player":
                pid  = item["id"]
                name = item.get("attributes", {}).get("display_name", "")
                if name:
                    player_map[pid] = name

        props = []
        for proj in data.get("data", []):
            attrs    = proj.get("attributes", {})
            stat_raw = str(attrs.get("stat_type", "") or "")

            # Filter to baseball stat types
            if stat_raw.lower() not in _PP_MLB_STAT_TYPES:
                continue

            # Skip innings pitched (removed in Phase 19)
            if "inning" in stat_raw.lower():
                continue

            line_val = attrs.get("line_score")
            if line_val is None:
                continue

            pid   = (proj.get("relationships", {})
                        .get("new_player", {})
                        .get("data", {})
                        .get("id", ""))
            pname = player_map.get(pid, "")
            if not pname:
                continue

            props.append({
                "source":      "prizepicks",
                "player_name": pname,
                "stat_type":   stat_raw,
                "line":        float(line_val),
            })

        logger.info("[PP] Fetched %d MLB props", len(props))
        return props
    except Exception as exc:
        logger.warning("[PP] Fetch failed: %s", exc)
        return []


def fetch_underdog_props() -> list[dict]:
    """
    Fetch Underdog Fantasy MLB over/under lines.

    Correct join chain (confirmed from Phase 18 UnderdogLinesFetcher):
      over_under_lines[n]["over_under"]["appearance_stat"]["stat"]
                                       ["appearance_stat"]["appearance_id"]
        → appearances_map[appearance_id]["player_id"]
        → players_map[player_id]["sport_id"] == "MLB"

    Filters out innings (removed Phase 19) and inactive lines.
    Returns raw list of dicts.
    """
    try:
        resp = requests.get(
            "https://api.underdogfantasy.com/v1/over_under_lines",
            headers=_HEADERS, timeout=20,
        )
        if resp.status_code != 200:
            logger.warning("[UD] HTTP %d", resp.status_code)
            return []
        data = resp.json()

        # Build lookup maps
        players_map: dict[str, dict] = {
            p["id"]: p for p in data.get("players", [])
        }
        appearances_map: dict[str, dict] = {
            a["id"]: a for a in data.get("appearances", [])
        }

        props: list[dict] = []
        seen: set[str] = set()

        for line in data.get("over_under_lines", []):
            # Active only
            if line.get("status") != "active":
                continue

            # Deduplicate on stable_id
            stable_id = line.get("stable_id", line.get("id", ""))
            if stable_id in seen:
                continue

            # Navigate embedded over_under → appearance_stat
            ou        = line.get("over_under") or {}
            app_stat  = ou.get("appearance_stat") or {}
            stat_ud   = app_stat.get("stat", "")
            app_id    = app_stat.get("appearance_id", "")

            if not stat_ud or not app_id:
                continue

            # Skip innings
            if "inning" in stat_ud.lower():
                continue

            # Resolve player
            appearance = appearances_map.get(app_id, {})
            player_id  = appearance.get("player_id", "")
            player     = players_map.get(player_id, {})

            # Must be MLB
            if player.get("sport_id") != "MLB":
                continue

            pname = f"{player.get('first_name','')} {player.get('last_name','')}".strip()
            if not pname:
                continue

            line_val   = float(line.get("stat_value") or 0)
            position   = player.get("position_name", "")
            entry_type = "FLEX"   # Underdog default; STANDARD for limited markets

            # Check options for entry type indicator
            opts = line.get("options", [])
            higher_opt = next((o for o in opts if o.get("choice") == "higher"), {})
            if not higher_opt.get("payout_multiplier"):
                entry_type = "STANDARD"

            seen.add(stable_id)
            props.append({
                "source":      "underdog",
                "player_name": pname,
                "stat_type":   stat_ud,
                "line":        line_val,
                "entry_type":  entry_type,
                "position":    position,
            })

        logger.info("[UD] Fetched %d MLB lines", len(props))
        return props
    except Exception as exc:
        logger.warning("[UD] Fetch failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Prop normalisation
# ---------------------------------------------------------------------------

# raw stat_type string → PROP_CONFIG key
_STAT_TYPE_MAP: dict[str, str] = {
    # Strikeouts / pitcher ks
    "strikeouts":           "strikeouts",
    "pitcher strikeouts":   "strikeouts",
    "ks":                   "strikeouts",
    # Hits
    "hits":                 "hits",
    # Home runs
    "home runs":            "home_runs",
    "home_runs":            "home_runs",
    # RBIs
    "rbis":                 "rbis",
    "rbi":                  "rbis",
    # Runs
    "runs":                 "runs",
    # Total bases
    "total bases":          "total_bases",
    "total_bases":          "total_bases",
    # Stolen bases
    "stolen bases":         "stolen_bases",
    "stolen_bases":         "stolen_bases",
    # Combo
    "hits+runs+rbis":       "hits_runs_rbis",
    "hits + runs + rbis":   "hits_runs_rbis",
    # Fantasy
    "hitter fantasy score": "fantasy_hitter",
    "pitcher fantasy score":"fantasy_pitcher",
    "fantasy_points_hitter":"fantasy_hitter",
    "fantasy_points_pitcher":"fantasy_pitcher",
    # Earned runs
    "earned runs":          "earned_runs",
    "earned_runs":          "earned_runs",
    # Walks (pitcher)
    "walks":                "walks",
}


def normalise_stat(raw: str) -> str | None:
    """Return PROP_CONFIG key for a raw stat_type string, or None if unknown."""
    key = raw.strip().lower().replace("-", " ")
    return _STAT_TYPE_MAP.get(key)


# ---------------------------------------------------------------------------
# EV & Kelly math
# ---------------------------------------------------------------------------

def american_to_decimal(odds: float) -> float:
    if odds >= 0:
        return 1 + odds / 100
    return 1 - 100 / odds


def implied_prob_from_odds(odds: float) -> float:
    dec = american_to_decimal(odds)
    return 1.0 / dec


def calc_ev(true_prob: float, odds: float = -110.0) -> float:
    """Return EV percentage given true probability and American odds."""
    dec     = american_to_decimal(odds)
    implied = implied_prob_from_odds(odds)
    no_vig  = true_prob
    ev_pct  = (no_vig * (dec - 1) - (1 - no_vig)) * 100
    return ev_pct


def kelly_fraction(prob: float, odds: float = -110.0) -> float:
    """Half-Kelly capped at MAX_KELLY."""
    dec = american_to_decimal(odds)
    b   = dec - 1.0
    q   = 1.0 - prob
    if b <= 0:
        return 0.0
    k = (b * prob - q) / b
    return max(0.0, min(k * HALF_KELLY, MAX_KELLY))


# ---------------------------------------------------------------------------
# Parlay builder
# ---------------------------------------------------------------------------

@dataclass
class PropLeg:
    """A fully evaluated parlay leg ready for Discord."""
    player_name:   str
    prop_type:     str
    side:          str
    line:          float
    platform:      str
    implied_prob:  float
    entry_type:    str   = ""
    fantasy_pts:   float = 0.0
    ev_pct:        float = 0.0


def build_parlay(
    legs: list[PropLeg],
    agent: dict,
    excluded_keys: set[tuple] | None = None,
) -> dict | None:
    """
    From a list of candidate legs, apply agent filter and build a parlay dict
    ready for DiscordAlertService.send_parlay_alert().

    excluded_keys: set of (player_name, prop_type, side) tuples already claimed
                   by a higher-priority agent — these legs are off-limits.
    """
    if excluded_keys is None:
        excluded_keys = set()

    filtered = [
        l for l in legs
        if agent["filter"](
            type("SR", (), {
                "side": l.side,
                "prop_type": l.prop_type,
                "implied_prob": l.implied_prob,
                "fantasy_pts_edge": l.fantasy_pts,
            })()
        )
        # Hard exclusion: leg already used by a previous agent
        and (l.player_name, l.prop_type, l.side) not in excluded_keys
    ]

    # Sort by implied prob desc, cap at min(agent max_legs, global MAX_LEGS)
    filtered.sort(key=lambda x: -x.implied_prob)
    cap = min(agent["max_legs"], MAX_LEGS)
    selected = filtered[:cap]

    if len(selected) < MIN_LEGS:
        return None

    # Overall parlay EV (average of per-leg EVs)
    ev_pct    = sum(l.ev_pct for l in selected) / len(selected)

    # --- Confidence score (1–10) ---
    # Three components:
    #   1. prob_score  : avg win prob scaled 0→7 over the range 50%→80%
    #   2. ev_bonus    : EV% scaled 0→2 (caps at 15% EV)
    #   3. legs_penalty: -0.3 per leg above 3 (more legs = more variance)
    avg_prob     = sum(l.implied_prob for l in selected) / len(selected)
    prob_score   = (avg_prob - 0.50) / 0.30 * 7.0
    ev_bonus     = min(ev_pct / 15.0 * 2.0, 2.0)
    legs_penalty = max(0, len(selected) - 3) * 0.3
    conf         = round(min(10.0, max(1.0, prob_score + ev_bonus - legs_penalty)), 1)

    # Dominant entry type
    etypes     = [l.entry_type for l in selected if l.entry_type]
    entry_type = max(set(etypes), key=etypes.count) if etypes else agent.get("entry_type", "FLEX")

    return {
        "agent_name":  agent["name"],
        "agent_emoji": agent["emoji"],
        "entry_type":  entry_type,
        "ev_pct":      round(ev_pct, 2),
        "confidence":  conf,
        "notes":       agent.get("note", ""),
        "legs": [
            {
                "player_name":  l.player_name,
                "prop_type":    l.prop_type,
                "side":         l.side,
                "line":         l.line,
                "platform":     l.platform,
                "implied_prob": round(l.implied_prob, 4),
                "entry_type":   l.entry_type,
                "fantasy_pts":  round(l.fantasy_pts, 2),
            }
            for l in selected
        ],
    }


# ---------------------------------------------------------------------------
# Main dispatcher
# ---------------------------------------------------------------------------

class LiveDispatcher:
    """Orchestrates daily prop fetching, analysis, and Discord dispatch."""

    def __init__(self, dry_run: bool = False) -> None:
        self.dry_run   = dry_run
        self.selector  = platform_selector

    def run(self, date_str: str | None = None) -> None:
        date = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        logger.info("=" * 60)
        logger.info("PropIQ LiveDispatcher — %s", date)
        logger.info("=" * 60)

        # 1. Schedule check
        games = fetch_today_schedule(date)
        if not games:
            logger.warning("No MLB games found for %s — no alerts sent.", date)
            return
        logger.info("%d games scheduled", len(games))

        # 2. Fetch live props from both platforms
        pp_props = fetch_prizepicks_props()
        ud_props = fetch_underdog_props()
        all_raw  = pp_props + ud_props

        if not all_raw:
            logger.warning("No props fetched from either platform — aborting.")
            return

        # 3. Build evaluated leg pool
        leg_pool: list[PropLeg] = self._evaluate_props(all_raw)
        logger.info("Leg pool: %d evaluated legs (min prob %.0f%%)",
                    len(leg_pool), MIN_PROB * 100)

        if not leg_pool:
            logger.warning("No legs passed EV/prob gates — no alerts today.")
            return

        # 4. Per-agent parlay building + Discord dispatch
        # global_used: tracks (player_name, prop_type, side) already claimed.
        # Once a leg is in a sent parlay it cannot appear in any subsequent one.
        global_used: set[tuple] = set()
        sent = 0
        for agent in AGENT_CONFIGS:
            parlay = build_parlay(leg_pool, agent, excluded_keys=global_used)
            if not parlay:
                logger.info("[%s] No qualifying parlay today.", agent["name"])
                continue
            ev   = parlay.get("ev_pct", 0)
            conf = parlay.get("confidence", 0)

            if ev < MIN_EV_PCT:
                logger.info("[%s] EV %.1f%% below gate — skipped.", agent["name"], ev)
                continue
            if conf < 7.0:
                logger.info(
                    "[%s] Confidence %.1f/10 below 7.0 gate — skipped.",
                    agent["name"], conf,
                )
                continue

            n = len(parlay["legs"])
            logger.info(
                "[%s] %d-leg parlay EV=%.1f%% conf=%.1f/10 → SEND",
                agent["name"], n, ev, conf,
            )

            # Claim these legs — no subsequent agent may reuse them
            for leg in parlay["legs"]:
                global_used.add((leg["player_name"], leg["prop_type"], leg["side"]))

            if not self.dry_run:
                # Attach live season record for this agent before sending
                parlay["season_stats"] = get_agent_season_stats(agent["name"])
                discord_alert.send_parlay_alert(parlay)
                time.sleep(1.5)   # rate-limit Discord webhook (25 req/s global)
                # Record parlay in DB as PENDING (resolved manually or via future hook)
                record_parlay(
                    date=date,
                    agent=agent["name"],
                    num_legs=len(parlay["legs"]),
                    confidence=conf,
                    ev_pct=ev,
                )
            else:
                logger.info("[DRY-RUN] Would send: %s",
                            json.dumps(parlay, indent=2)[:400])
            sent += 1

        logger.info("Dispatch complete — %d parlays sent for %s", sent, date)

    # ── private ───────────────────────────────────────────────────────────────

    def _evaluate_props(self, raw_props: list[dict]) -> list[PropLeg]:
        """
        Normalise raw props, compare platforms, apply EV gate.

        Algorithm:
        1. Group props by (player_name_lower, prop_type) across both platforms.
        2. For each group: pick the platform with the more favourable line:
               Over → lower line is better  (easier to clear)
               Under → higher line is better (more room to stay under)
        3. Estimate implied win probability using MLB historical base rates
           for each stat+line combination (logistic function around base rate).
        4. Apply min_prob and EV gates.

        This avoids re-fetching APIs (platform_selector runs its own cache
        independently — we work directly from already-fetched raw_props).
        """
        # ── MLB historical base-rate probabilities ─────────────────────────
        # P(player hits Over X) based on MLB 2022-2025 season averages.
        # Derived from known per-game stat distributions.
        # Format: {prop_type: [(line_threshold, over_prob), ...]}
        # Interpolated linearly between thresholds.
        _BASE_RATES: dict[str, list[tuple[float, float]]] = {
            # Hitter: H ≥ X
            "hits":           [(0.5, 0.67), (1.5, 0.40), (2.5, 0.19), (3.5, 0.08)],
            # HR ≥ X
            "home_runs":      [(0.5, 0.22), (1.5, 0.04)],
            # RBI ≥ X
            "rbis":           [(0.5, 0.42), (1.5, 0.18), (2.5, 0.07)],
            # R ≥ X
            "runs":           [(0.5, 0.55), (1.5, 0.23), (2.5, 0.09)],
            # TB ≥ X
            "total_bases":    [(0.5, 0.70), (1.5, 0.49), (2.5, 0.28), (3.5, 0.14)],
            # SB ≥ X
            "stolen_bases":   [(0.5, 0.14), (1.5, 0.03)],
            # H+R+RBI ≥ X
            "hits_runs_rbis": [(0.5, 0.82), (1.5, 0.64), (2.5, 0.44), (3.5, 0.27), (4.5, 0.15)],
            # Pitcher K ≥ X
            "strikeouts":     [(3.5, 0.74), (4.5, 0.62), (5.5, 0.51), (6.5, 0.40), (7.5, 0.29), (8.5, 0.19)],
            # ER ≤ X (Under is typically the bet)
            "earned_runs":    [(0.5, 0.42), (1.5, 0.59), (2.5, 0.72), (3.5, 0.82)],
            # Fantasy hitter score
            "fantasy_hitter": [(15.0, 0.58), (20.0, 0.45), (25.0, 0.33), (30.0, 0.22)],
            # Fantasy pitcher score
            "fantasy_pitcher":[(30.0, 0.58), (35.0, 0.47), (40.0, 0.36), (45.0, 0.27)],
            # Walks (pitcher)
            "walks":          [(0.5, 0.68), (1.5, 0.42), (2.5, 0.22)],
        }

        # ── Per-game line range validation ────────────────────────────────
        # Lines outside these ranges are season-long or special markets.
        # We ONLY bet per-game props. These are realistic MLB per-game ranges.
        _GAME_LINE_RANGES: dict[str, tuple[float, float]] = {
            "hits":           (0.5, 4.5),
            "home_runs":      (0.5, 2.5),
            "rbis":           (0.5, 4.5),
            "runs":           (0.5, 3.5),
            "total_bases":    (0.5, 5.5),
            "stolen_bases":   (0.5, 2.5),
            "hits_runs_rbis": (0.5, 8.5),
            "strikeouts":     (1.5, 12.5),
            "earned_runs":    (0.5, 6.5),
            "walks":          (0.5, 5.5),
            "fantasy_hitter": (5.0, 60.0),
            "fantasy_pitcher":(15.0, 70.0),
        }

        def is_game_prop(prop_type: str, line: float) -> bool:
            """Return True only if line is within realistic per-game range."""
            rng = _GAME_LINE_RANGES.get(prop_type)
            if rng is None:
                return True
            return rng[0] <= line <= rng[1]

        def base_prob(prop_type: str, line: float, side: str) -> float:
            """Interpolate MLB base-rate probability for a given prop + line."""
            rates = _BASE_RATES.get(prop_type, [])
            if not rates:
                return 0.50
            xs = [r[0] for r in rates]
            ys = [r[1] for r in rates]
            # Clamp
            if line <= xs[0]:
                p_over = ys[0]
            elif line >= xs[-1]:
                p_over = ys[-1]
            else:
                # Linear interpolation
                for i in range(len(xs) - 1):
                    if xs[i] <= line <= xs[i + 1]:
                        t = (line - xs[i]) / (xs[i + 1] - xs[i])
                        p_over = ys[i] + t * (ys[i + 1] - ys[i])
                        break
                else:
                    p_over = 0.50
            return p_over if side == "Over" else (1.0 - p_over)

        from collections import defaultdict
        groups: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)

        for raw in raw_props:
            pname    = raw.get("player_name", "")
            raw_stat = raw.get("stat_type", "")
            line_val = float(raw.get("line") or 0)
            source   = raw.get("source", "")
            etype    = raw.get("entry_type", "FLEX")
            position = raw.get("position", "")

            prop_type = normalise_stat(raw_stat)
            if not prop_type or prop_type not in PROP_CONFIG:
                continue
            if line_val <= 0:
                continue

            key     = (pname.lower().strip(), prop_type)
            platform = "prizepicks" if source == "prizepicks" else "underdog"

            existing = groups[key].get(platform)
            if not existing:
                groups[key][platform] = {
                    "line": line_val, "entry_type": etype,
                    "player_name": pname, "position": position,
                }

        # ── Evaluate each group ────────────────────────────────────────────
        legs: list[PropLeg] = []
        seen: set[tuple[str, str, str]] = set()

        for (player_lower, prop_type), platforms in groups.items():
            cfg = PROP_CONFIG[prop_type]

            for side in cfg["sides"]:
                leg_key = (player_lower, prop_type, side)
                if leg_key in seen:
                    continue

                # Pick best platform per side
                pp_entry = platforms.get("prizepicks")
                ud_entry = platforms.get("underdog")

                # For Over: prefer lower line; for Under: prefer higher line
                def line_score(entry: dict | None, s: str) -> float:
                    if entry is None:
                        return float("inf") if s == "Over" else float("-inf")
                    return entry["line"] if s == "Over" else -entry["line"]

                if pp_entry and ud_entry:
                    # Compare — pick platform with better (more favorable) line
                    if line_score(pp_entry, side) <= line_score(ud_entry, side):
                        chosen_entry = pp_entry
                        chosen_platform = "PrizePicks"
                    else:
                        chosen_entry = ud_entry
                        chosen_platform = "Underdog"
                elif pp_entry:
                    chosen_entry = pp_entry
                    chosen_platform = "PrizePicks"
                elif ud_entry:
                    chosen_entry = ud_entry
                    chosen_platform = "Underdog"
                else:
                    continue

                line_val   = chosen_entry["line"]
                entry_type = chosen_entry.get("entry_type", "FLEX")
                pname      = chosen_entry.get("player_name",
                             player_lower.title())

                # Block season-long props (e.g., RBI Under 76.5 is a season total)
                if not is_game_prop(prop_type, line_val):
                    continue

                # Calculate implied probability from MLB base rates
                prob = base_prob(prop_type, line_val, side)

                # Platform edge bonus: if both platforms available and we chose
                # the better line, add a small bonus (0.5–2.5%) for line advantage
                if pp_entry and ud_entry:
                    pp_line = pp_entry["line"]
                    ud_line = ud_entry["line"]
                    line_diff = abs(pp_line - ud_line)
                    # Each 0.5 unit of line difference ≈ +0.5% edge
                    platform_bonus = min(0.03, line_diff * 0.01)
                    prob = min(0.80, prob + platform_bonus)

                # Gate checks
                if prob < cfg["min_prob"]:
                    continue

                ev = calc_ev(prob)
                if ev < MIN_EV_PCT:
                    continue

                seen.add(leg_key)
                legs.append(PropLeg(
                    player_name=pname,
                    prop_type=prop_type,
                    side=side,
                    line=line_val,
                    platform=chosen_platform,
                    implied_prob=round(prob, 4),
                    entry_type=entry_type,
                    fantasy_pts=0.0,
                    ev_pct=round(ev, 2),
                ))

        # Sort by implied prob desc
        legs.sort(key=lambda l: -l.implied_prob)
        logger.info("Leg pool built: %d legs across %d platform comparisons",
                    len(legs), len(groups))
        return legs


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropIQ Live Dispatcher")
    parser.add_argument("--date",    type=str, help="Date override YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log parlays without sending Discord alerts")
    args = parser.parse_args()

    discord_alert.send_startup_ping()
    dispatcher = LiveDispatcher(dry_run=args.dry_run)
    dispatcher.run(date_str=args.date)
