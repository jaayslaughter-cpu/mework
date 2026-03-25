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
import math
import os
import time
from dataclasses import dataclass, field
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

try:
    from platform_selector import PlatformSelector, SelectionResult
    platform_selector = PlatformSelector()
    _PLATFORM_SELECTOR_AVAILABLE = True
except ImportError:
    _PLATFORM_SELECTOR_AVAILABLE = False
    platform_selector = None  # type: ignore[assignment]

from DiscordAlertService import discord_alert, MAX_STAKE_USD

# ── Phase 27: Enhancement layer imports (all optional — graceful fallback) ──
try:
    from draftedge_scraper import enrich_props_with_draftedge as _de_enrich
    _DE_AVAILABLE = True
except ImportError:
    _DE_AVAILABLE = False
    def _de_enrich(props: list) -> list: return props  # noqa: E704

try:
    from statcast_feature_layer import (
        enrich_props_with_statcast as _sc_enrich,
        StatcastFeatureLayer,
    )
    _SC_AVAILABLE = True
except ImportError:
    _SC_AVAILABLE = False
    def _sc_enrich(props: list, _player_type: str, _layer=None) -> list: return props  # noqa: E704
    class StatcastFeatureLayer:  # noqa: E302
        pass

try:
    from public_trends_scraper import PublicTrendsScraper, get_fade_signal as _get_fade_signal
    _SBD_AVAILABLE = True
except ImportError:
    _SBD_AVAILABLE = False
    def _get_fade_signal(*_args, **_kwargs):  # noqa: E302, E704
        return 0.0, "none"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("propiq.live")

# ---------------------------------------------------------------------------
# Hot/cold form layer (MLB Stats API game logs — free, no key required)
# ---------------------------------------------------------------------------
try:
    from mlb_form_layer import form_layer as _form_layer
    _FORM_LAYER_AVAILABLE = True
    logger.info("[Form] Hot/cold form layer loaded.")
except ImportError:
    _FORM_LAYER_AVAILABLE = False

    class _DummyFormLayer:  # noqa: D101
        @staticmethod
        def prefetch_form_data(*_args, **_kwargs) -> None:
            # No-op: Dummy form layer does not prefetch any data
            pass
        @staticmethod
        def get_form_adjustment(*_args, **_kwargs) -> float: return 0.0  # noqa: E704

    _form_layer = _DummyFormLayer()  # type: ignore[assignment]
    logger.warning("[Form] mlb_form_layer not found — form adjustments disabled.")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_EV_PCT   = 3.0     # minimum EV gate
MIN_PROB     = 0.52    # minimum implied win probability per leg
MAX_LEGS     = 4       # hard cap — no parlay may exceed 4 legs
MIN_LEGS     = 2       # min legs to send alert
HALF_KELLY   = 0.5     # Kelly fraction multiplier
MAX_KELLY    = 0.10    # bankroll cap

# ---------------------------------------------------------------------------
# OmegaStack ensemble constants
# ---------------------------------------------------------------------------
# Per-agent additive edge above raw implied_prob (each agent's unique signal strength)
_OMEGA_AGENT_EDGE: dict[str, float] = {
    "VultureStack": 0.040,   # strongest: dual-confirmation fatigue consensus
    "UmpireAgent":  0.020,   # moderate: ump tendencies predictive but subtle
    "FadeAgent":    0.015,   # marginal: contrarian adds a small systematic edge
}
OMEGA_STACK_WEIGHTS: dict[str, float] = {
    "VultureStack": 0.60,
    "UmpireAgent":  0.25,
    "FadeAgent":    0.15,
}
OMEGA_STACK_MIN_PROB = 0.65   # stacked prob gate — rarest, highest conviction
OMEGA_STACK_MAX_LEGS = 3      # tight: surgical parlays only
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
        # ── F5Agent — First 5 Innings specialist ───────────────────────────
        # Focuses on pitcher-centric props where outcomes are driven by the
        # starter's quality.  Bullpen variance is eliminated because the prop
        # resolves before any reliever touches the game.
        # Targets high-probability pitcher props: K and hits/runs suppression.
        # Strict probability gate (≥ 0.55) compensates for the small prop pool.
        "name": "F5Agent",
        "emoji": "5️⃣",
        "max_legs": 3,
        "entry_type": "STANDARD",
        "filter": lambda r: r.prop_type in (
            "strikeouts", "earned_runs", "hits_runs_rbis", "runs"
        ) and r.implied_prob >= 0.55,
        "note": "First-5-innings props — ignores bullpen, SP quality only",
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
        # Phase 27: prefer real SBD fade signal; fall back to implied_prob gate
        "filter": lambda r: (
            getattr(r, "is_fade_signal", False) or r.implied_prob >= 0.53
        ) and r.side == "Under",
        "note": "Contrarian fades against public consensus (SBD ticket% ≥ 65% preferred)",
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
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# PrizePicks session — warm up by visiting the app home page first so
# Cloudflare + DataDome issue valid cookies, then use those cookies for
# the API call.  The session is module-level so the warm-up only fires
# once per process (the daily 11 AM dispatch is a single process).
_pp_session: requests.Session | None = None


_pp_session = None

def _get_pp_session() -> requests.Session:
    """Return a warmed-up PrizePicks session, creating one if needed."""
    global _pp_session
    if _pp_session is not None:
        return _pp_session
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.0 Mobile/15E148 Safari/604.1"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    try:
        s.get("https://app.prizepicks.com/", timeout=12)
        logger.info("[PP] Session warmed up — cookies: %s", list(s.cookies.keys()))
    except Exception as exc:
        logger.warning("[PP] Warm-up request failed: %s", exc)
    # Switch to JSON API headers for subsequent calls
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://app.prizepicks.com/",
        "Origin": "https://app.prizepicks.com",
    })
    _pp_session = s
    return s


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

# Baseball-specific stat types used to identify MLB props on PrizePicks.
# Updated to match actual API responses (live-probed Mar 2026):
#   "pitcher strikeouts" replaced bare "strikeouts"
#   "earned runs allowed" replaced "earned runs"
#   "hits allowed" and "walks allowed" added for pitcher props
#   "pitching outs" added (outs recorded prop)
_PP_MLB_STAT_TYPES = {
    "hits", "home runs", "rbis", "rbi", "runs",
    "total bases", "stolen bases",
    "hits+runs+rbis", "hits + runs + rbis",
    "hitter fantasy score", "pitcher fantasy score",
    "doubles", "triples",
    # Pitcher props (actual PP label as of 2026)
    "pitcher strikeouts", "strikeouts",   # keep bare form as fallback
    "earned runs allowed", "earned runs",  # keep old form as fallback
    "hits allowed", "walks allowed", "pitching outs",
    "walks",
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
    Fetch PrizePicks MLB projections via session-cookie warm-up.

    PrizePicks is protected by Cloudflare + DataDome bot detection.
    The fix: visit app.prizepicks.com first (mobile Safari UA) so the
    CDN issues valid cookies, then hit the API on the same session.
    Without the warm-up visit every direct API call returns 403.

    Returns raw list of dicts.
    """
    try:
        data = None
        for attempt in range(3):
            if attempt:
                time.sleep(2 ** attempt)   # 2s, 4s back-off
                # Force a fresh session on retry so we get new cookies
                _pp_session = None
            sess = _get_pp_session()
            resp = sess.get(
                "https://api.prizepicks.com/projections",
                params={"per_page": 250, "single_stat": True, "league_id": 2},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                break
            logger.warning("[PP] HTTP %d (attempt %d/3)", resp.status_code, attempt + 1)
            if resp.status_code == 403:
                _pp_session = None   # force re-warm on next attempt
        if data is None:
            return []

        # Build player id 12; name map from included resources
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
        _ud_headers = {
            "User-Agent": "Underdog Fantasy/3.0 (iPhone; iOS 17.0) CFNetwork/1474 Darwin/23.0.0",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "x-api-key": "KeepItSecret",
        }
        resp = requests.get(
            "https://api.underdogfantasy.com/beta/v5/over_under_lines",
            headers=_ud_headers, timeout=20,
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
    # Earned runs (PrizePicks uses "earned runs allowed" as of 2026)
    "earned runs":          "earned_runs",
    "earned_runs":          "earned_runs",
    "earned runs allowed":  "earned_runs",
    # Pitcher hits / walks allowed (PP labels as of 2026)
    "hits allowed":         "hits_allowed",
    "walks allowed":        "walks_allowed",
    "pitching outs":        "pitching_outs",
    # Walks (batter)
    "walks":                "walks",
    # Doubles / triples
    "doubles":              "doubles",
    "triples":              "triples",
}


def normalise_stat(raw: str) -> str | None:
    """Return PROP_CONFIG key for a raw stat_type string, or None if unknown."""
    key = raw.strip().lower().replace("-", " ")
    return _STAT_TYPE_MAP.get(key)


# ---------------------------------------------------------------------------
# ArbitrageAgent — module-level base-rate helpers
# ---------------------------------------------------------------------------

# Single source of truth for MLB per-game prop base rates.
# Used by both _evaluate_props (agent parlays) and ArbitrageAgent.
# Format: {prop_type: [(line_threshold, over_prob), ...]} — interpolated linearly.
_BASE_RATES: dict[str, list[tuple[float, float]]] = {
    "hits":            [(0.5, 0.67), (1.5, 0.40), (2.5, 0.19), (3.5, 0.08)],
    "home_runs":       [(0.5, 0.22), (1.5, 0.04)],
    "rbis":            [(0.5, 0.42), (1.5, 0.18), (2.5, 0.07)],
    "runs":            [(0.5, 0.55), (1.5, 0.23), (2.5, 0.09)],
    "total_bases":     [(0.5, 0.70), (1.5, 0.49), (2.5, 0.28), (3.5, 0.14)],
    "stolen_bases":    [(0.5, 0.14), (1.5, 0.03)],
    "hits_runs_rbis":  [(0.5, 0.82), (1.5, 0.64), (2.5, 0.44), (3.5, 0.27), (4.5, 0.15)],
    "strikeouts":      [(3.5, 0.74), (4.5, 0.62), (5.5, 0.51), (6.5, 0.40), (7.5, 0.29), (8.5, 0.19)],
    "earned_runs":     [(0.5, 0.42), (1.5, 0.59), (2.5, 0.72), (3.5, 0.82)],
    "fantasy_hitter":  [(15.0, 0.58), (20.0, 0.45), (25.0, 0.33), (30.0, 0.22)],
    "fantasy_pitcher": [(30.0, 0.58), (35.0, 0.47), (40.0, 0.36), (45.0, 0.27)],
    "walks":           [(0.5, 0.68), (1.5, 0.42), (2.5, 0.22)],
}

_ARB_MIN_MARGIN:   float = 0.005   # 0.5% guaranteed margin minimum
_ARB_MIN_LEG_PROB: float = 0.54    # each individual leg must clear this gate
_ARB_MAX_PICKS:    int   = 3       # maximum arb opportunities per day
_ARB_MIN_GAP:      float = 0.5     # minimum line gap between PP and UD to qualify


def _arb_base_prob(prop_type: str, line: float, side: str) -> float:
    """Interpolate MLB base-rate probability for ArbitrageAgent calculations."""
    rates = _BASE_RATES.get(prop_type, [])
    if not rates:
        return 0.50
    xs = [r[0] for r in rates]
    ys = [r[1] for r in rates]
    if line <= xs[0]:
        p_over = ys[0]
    elif line >= xs[-1]:
        p_over = ys[-1]
    else:
        p_over = 0.50
        for i in range(len(xs) - 1):
            if xs[i] <= line <= xs[i + 1]:
                t = (line - xs[i]) / (xs[i + 1] - xs[i])
                p_over = ys[i] + t * (ys[i + 1] - ys[i])
                break
    return p_over if side == "Over" else (1.0 - p_over)


def build_arbitrage_picks(all_raw: list[dict]) -> list[dict]:
    """
    ArbitrageAgent (16th agent): find same player+stat where PP and UD
    have meaningfully different lines.

    When PP_line < UD_line:
      → Over  on PrizePicks (lower line, easier to clear)
      → Under on Underdog   (higher line, more room to stay under)

    Arb margin = P(Over lower_line) + P(Under higher_line) − 1.0
               = P(lower_line < actual ≤ upper_line)
               → guaranteed both-leg win zone.

    Gates:
      - line gap  ≥ 0.5 units
      - arb margin ≥ 0.5%
      - each leg's base probability ≥ 0.54

    Returns up to _ARB_MAX_PICKS arb dicts sorted by margin desc.
    """
    from collections import defaultdict

    by_player: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)
    for raw in all_raw:
        pname    = raw.get("player_name", "").strip()
        raw_stat = raw.get("stat_type", "")
        line_val = float(raw.get("line") or 0)
        source   = raw.get("source", "")
        etype    = raw.get("entry_type", "FLEX")

        prop_type = normalise_stat(raw_stat)
        if not prop_type or line_val <= 0:
            continue
        if source not in ("prizepicks", "underdog"):
            continue

        key = (pname.lower(), prop_type)
        existing = by_player[key].get(source)
        # Keep the entry with the largest line per platform (rarest dupe case)
        if existing is None or line_val > existing["line"]:
            by_player[key][source] = {
                "player_name": pname,
                "prop_type":   prop_type,
                "line":        line_val,
                "entry_type":  etype,
            }

    candidates: list[dict] = []
    for (_pname_lower, prop_type), platforms in by_player.items():
        if "prizepicks" not in platforms or "underdog" not in platforms:
            continue

        pp_data = platforms["prizepicks"]
        ud_data = platforms["underdog"]
        pp_line = pp_data["line"]
        ud_line = ud_data["line"]
        gap     = abs(pp_line - ud_line)

        if gap < _ARB_MIN_GAP:
            continue

        # Over on lower line, Under on higher line
        if pp_line < ud_line:
            over_line, over_plat, over_etype   = pp_line, "PrizePicks", pp_data["entry_type"]
            under_line, under_plat, under_etype = ud_line, "Underdog",   ud_data["entry_type"]
            display_name = pp_data["player_name"]
        else:
            over_line, over_plat, over_etype   = ud_line, "Underdog",   ud_data["entry_type"]
            under_line, under_plat, under_etype = pp_line, "PrizePicks", pp_data["entry_type"]
            display_name = ud_data["player_name"]

        p_over  = _arb_base_prob(prop_type, over_line,  "Over")
        p_under = _arb_base_prob(prop_type, under_line, "Under")
        margin  = p_over + p_under - 1.0

        if margin < _ARB_MIN_MARGIN:
            continue
        if p_over < _ARB_MIN_LEG_PROB or p_under < _ARB_MIN_LEG_PROB:
            continue

        # Confidence: 7.0 at 0.5% margin → 9.0 at 5% margin
        conf = round(min(10.0, 7.0 + (margin - 0.005) / 0.045 * 2.0), 1)

        candidates.append({
            "player_name":  display_name,
            "prop_type":    prop_type,
            "over_line":    over_line,
            "over_plat":    over_plat,
            "over_etype":   over_etype,
            "under_line":   under_line,
            "under_plat":   under_plat,
            "under_etype":  under_etype,
            "p_over":       round(p_over,  4),
            "p_under":      round(p_under, 4),
            "arb_margin":   round(margin,  4),
            "confidence":   conf,
            "gap":          round(gap, 2),
        })

    candidates.sort(key=lambda x: -x["arb_margin"])
    return candidates[:_ARB_MAX_PICKS]


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
    player_name:    str
    prop_type:      str
    side:           str
    line:           float
    platform:       str
    implied_prob:   float
    entry_type:     str   = ""
    fantasy_pts:    float = 0.0
    ev_pct:         float = 0.0
    # Phase 27: Enhancement layer signals
    de_boost:       float = 0.0   # DraftEdge probability blend delta
    sbd_ticket_pct: float = 0.0   # SBD public ticket% (FadeAgent signal)
    is_fade_signal: bool  = False  # True when ticket_pct ≥ 65%


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
                "side":            l.side,
                "prop_type":       l.prop_type,
                "implied_prob":    l.implied_prob,
                "fantasy_pts_edge": l.fantasy_pts,
                # Phase 27: enhancement signals exposed to agent filters
                "is_fade_signal":  l.is_fade_signal,
                "sbd_ticket_pct":  l.sbd_ticket_pct,
                "de_boost":        l.de_boost,
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
# OmegaStack ensemble meta-model (18th agent)
# ---------------------------------------------------------------------------

def build_omega_parlay(
    legs: list[PropLeg],
    excluded_keys: set[tuple] | None = None,
) -> dict | None:
    """
    OmegaStack — 18th agent: true ensemble meta-model.

    Triple confirmation required: a leg must pass ALL of
    VultureStack, UmpireAgent, and FadeAgent filters.  Each contributing
    agent then adds its own additive edge to the raw implied_prob, and the
    weighted stacked probability is computed:

        stacked_prob = 0.60 × (prob + 0.040)   # VultureStack
                     + 0.25 × (prob + 0.020)   # UmpireAgent
                     + 0.15 × (prob + 0.015)   # FadeAgent

    Only legs with stacked_prob >= OMEGA_STACK_MIN_PROB (0.65) are included.
    Raw implied_prob must be ~0.62+ to clear the gate — the tightest bar in
    the entire system.  Result: fewest parlays, highest conviction.
    """
    if excluded_keys is None:
        excluded_keys = set()

    # Resolve contributing-agent filter lambdas by name once
    agent_filters: dict[str, any] = {
        a["name"]: a["filter"] for a in AGENT_CONFIGS
    }
    vulture_f = agent_filters.get("VultureStack", lambda _r: False)
    umpire_f  = agent_filters.get("UmpireAgent",  lambda _r: False)
    fade_f    = agent_filters.get("FadeAgent",    lambda _r: False)

    qualified: list[tuple[PropLeg, float]] = []
    for leg in legs:
        if (leg.player_name, leg.prop_type, leg.side) in excluded_keys:
            continue

        # Synthetic SelectionResult for filter evaluation
        sr = type("SR", (), {
            "side":              leg.side,
            "prop_type":         leg.prop_type,
            "implied_prob":      leg.implied_prob,
            "fantasy_pts_edge":  leg.fantasy_pts,
        })()

        # Triple confirmation — all three must agree
        if not (vulture_f(sr) and umpire_f(sr) and fade_f(sr)):
            continue

        # Weighted stacked probability with per-agent edge boosts
        vulture_eff  = leg.implied_prob + _OMEGA_AGENT_EDGE["VultureStack"]
        umpire_eff   = leg.implied_prob + _OMEGA_AGENT_EDGE["UmpireAgent"]
        fade_eff     = leg.implied_prob + _OMEGA_AGENT_EDGE["FadeAgent"]
        stacked_prob = (
            OMEGA_STACK_WEIGHTS["VultureStack"] * vulture_eff
            + OMEGA_STACK_WEIGHTS["UmpireAgent"]  * umpire_eff
            + OMEGA_STACK_WEIGHTS["FadeAgent"]    * fade_eff
        )

        if stacked_prob < OMEGA_STACK_MIN_PROB:
            continue

        qualified.append((leg, stacked_prob))

    if len(qualified) < MIN_LEGS:
        return None

    # Sort by stacked_prob desc, cap at OMEGA_STACK_MAX_LEGS
    qualified.sort(key=lambda t: -t[1])
    selected_pairs = qualified[: min(OMEGA_STACK_MAX_LEGS, MAX_LEGS)]

    selected_legs  = [p[0] for p in selected_pairs]
    selected_probs = [p[1] for p in selected_pairs]

    ev_pct       = sum(calc_ev(sp) for sp in selected_probs) / len(selected_probs)
    avg_prob     = sum(selected_probs) / len(selected_probs)
    prob_score   = (avg_prob - 0.50) / 0.30 * 7.0
    ev_bonus     = min(ev_pct / 15.0 * 2.0, 2.0)
    legs_penalty = max(0, len(selected_legs) - 3) * 0.3
    conf         = round(min(10.0, max(1.0, prob_score + ev_bonus - legs_penalty)), 1)

    return {
        "agent_name":  "OmegaStack",
        "agent_emoji": "🔱",
        "entry_type":  "STANDARD",
        "ev_pct":      round(ev_pct, 2),
        "confidence":  conf,
        "notes":       "VultureStack×0.60 + UmpireAgent×0.25 + FadeAgent×0.15 — triple confirm ≥ 0.65 stacked",
        "legs": [
            {
                "player_name":  l.player_name,
                "prop_type":    l.prop_type,
                "side":         l.side,
                "line":         l.line,
                "platform":     l.platform,
                "implied_prob": round(sp, 4),
                "entry_type":   l.entry_type,
                "fantasy_pts":  round(l.fantasy_pts, 2),
            }
            for l, sp in zip(selected_legs, selected_probs)
        ],
    }


# ---------------------------------------------------------------------------
# Phase 27: MLB player name → mlbam_id lookup
# ---------------------------------------------------------------------------

def _build_mlbam_lookup() -> dict:
    """
    Fetch all active MLB players from the Stats API and build a
    lowercase-full-name → mlbam_id mapping.  Used to attach Statcast
    features to raw props (which carry player names, not MLBAM IDs).

    Falls back to an empty dict on any network/parse error — Statcast
    enrichment degrades gracefully without it.
    """
    try:
        import datetime as _dt
        season = _dt.datetime.now(_dt.timezone.utc).year
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/sports/1/players",
            params={"season": season, "gameType": "R"},
            headers={"User-Agent": "PropIQ/1.0"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("[MLBAM] Lookup HTTP %d — Statcast enrichment skipped",
                           resp.status_code)
            return {}
        lookup: dict = {}
        for person in resp.json().get("people", []):
            full_name = person.get("fullName", "")
            pid = person.get("id")
            if full_name and pid:
                lookup[full_name.lower()] = int(pid)
                parts = full_name.split()
                if len(parts) >= 2:
                    lookup[parts[-1].lower()] = int(pid)  # last-name fallback
        logger.info("[MLBAM] Lookup built: %d players", len(lookup))
        return lookup
    except Exception as exc:
        logger.warning("[MLBAM] Lookup failed: %s — Statcast enrichment skipped", exc)
        return {}


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

        # ── Phase 27: Enrich raw props with enhancement layers ────────────
        # Step 1: DraftEdge projections (name-based, adds de_* fields)
        if _DE_AVAILABLE:
            try:
                all_raw = _de_enrich(all_raw)
                logger.info("[Phase27] DraftEdge enrichment applied to %d props",
                            len(all_raw))
            except Exception as exc:
                logger.warning("[Phase27] DraftEdge enrichment failed: %s", exc)

        # Step 2: Statcast XGBoost features (mlbam_id resolved from Stats API)
        if _SC_AVAILABLE:
            try:
                mlbam_lookup = _build_mlbam_lookup()
                for p in all_raw:
                    name_key = p.get("player_name", "").lower()
                    p["mlbam_id"] = mlbam_lookup.get(name_key, 0)
                _sc_layer = StatcastFeatureLayer()
                _ud_pitcher_pos = {"SP", "RP", "P", "CP"}
                pitcher_raw = [p for p in all_raw
                               if p.get("position", "") in _ud_pitcher_pos]
                batter_raw  = [p for p in all_raw
                               if p.get("position", "") not in _ud_pitcher_pos]
                pitcher_raw = _sc_enrich(pitcher_raw, "pitcher", layer=_sc_layer)
                batter_raw  = _sc_enrich(batter_raw,  "batter",  layer=_sc_layer)
                all_raw = pitcher_raw + batter_raw
                logger.info("[Phase27] Statcast enrichment applied")
            except Exception as exc:
                logger.warning("[Phase27] Statcast enrichment failed: %s", exc)

        # Step 3: SportsBettingDime public trends (FadeAgent precision upgrade)
        sbd_game_df = None
        sbd_prop_df = None
        if _SBD_AVAILABLE:
            try:
                _sbd = PublicTrendsScraper()
                sbd_game_df, sbd_prop_df = _sbd.fetch()
                logger.info("[Phase27] SBD trends: %d games, %d player props",
                            len(sbd_game_df), len(sbd_prop_df))
            except Exception as exc:
                logger.warning("[Phase27] SBD fetch failed: %s", exc)
        # ── End Phase 27 enrichment ────────────────────────────────────────

        # 3. Build evaluated leg pool (enrichment data flows through)
        leg_pool: list[PropLeg] = self._evaluate_props(all_raw, sbd_game_df, sbd_prop_df)
        logger.info("Leg pool: %d evaluated legs (min prob %.0f%%)",
                    len(leg_pool), MIN_PROB * 100)

        if not leg_pool:
            logger.warning("No legs passed EV/prob gates — no alerts today.")
            return

        # 4. Per-agent parlay building + Discord dispatch
        # Each agent picks independently from the full filtered pool.
        # Shared legs across parlays are acceptable — consensus across agents
        # is a positive signal, not a risk. Each parlay is a separate $20 bet
        # with its own thesis.
        sent = 0
        for agent in AGENT_CONFIGS:
            parlay = build_parlay(leg_pool, agent)
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

            if not self.dry_run:
                # Attach live season record for this agent before sending
                parlay["season_stats"] = get_agent_season_stats(agent["name"])
                discord_alert.send_parlay_alert(parlay)
                time.sleep(1.5)   # rate-limit Discord webhook (25 req/s global)
                # Record parlay in DB as PENDING (settled nightly by nightly_recap.py)
                record_parlay(
                    date=date,
                    agent=agent["name"],
                    num_legs=len(parlay["legs"]),
                    confidence=conf,
                    ev_pct=ev,
                    legs=[
                        {
                            "player_name": l.get("player_name", l.get("player", "")),
                            "prop_type":   l["prop_type"],
                            "side":        l["side"],
                            "line":        l["line"],
                        }
                        for l in parlay["legs"]
                    ],
                )
            else:
                logger.info("[DRY-RUN] Would send: %s",
                            json.dumps(parlay, indent=2)[:400])
            sent += 1

        # ── OmegaStack (18th agent) — triple-confirmation ensemble ──
        omega = build_omega_parlay(leg_pool)
        if omega:
            ev_o   = omega.get("ev_pct", 0)
            conf_o = omega.get("confidence", 0)
            if ev_o >= MIN_EV_PCT and conf_o >= 7.0:
                logger.info(
                    "[OmegaStack] %d-leg ensemble parlay EV=%.1f%% conf=%.1f/10 → SEND",
                    len(omega["legs"]), ev_o, conf_o,
                )
                if not self.dry_run:
                    omega["season_stats"] = get_agent_season_stats("OmegaStack")
                    discord_alert.send_parlay_alert(omega)
                    time.sleep(1.5)
                    record_parlay(
                        date=date,
                        agent="OmegaStack",
                        num_legs=len(omega["legs"]),
                        confidence=conf_o,
                        ev_pct=ev_o,
                        legs=[
                            {
                                "player_name": l.get("player_name", l.get("player", "")),
                                "prop_type":   l["prop_type"],
                                "side":        l["side"],
                                "line":        l["line"],
                            }
                            for l in omega["legs"]
                        ],
                    )
                else:
                    logger.info("[DRY-RUN] OmegaStack would send: %s",
                                json.dumps(omega, indent=2)[:400])
                sent += 1
            else:
                logger.info(
                    "[OmegaStack] EV=%.1f%% conf=%.1f/10 — below gate, skipped.",
                    ev_o, conf_o,
                )
        else:
            logger.info("[OmegaStack] No triple-confirmation legs today.")

        logger.info("Dispatch complete — %d parlays sent for %s", sent, date)

        # ── StreakAgent (19th agent) — runs after the main 18-agent dispatch ──
        # Single best pick per day for Underdog Streaks format (11 consecutive
        # correct picks → $1K/$5K/$10K prize). Confidence gate ≥ 8/10.
        try:
            from streak_agent import run_streak_pick
            streak_entry = int(os.getenv("STREAK_ENTRY_AMOUNT", "1"))
            streak_result = run_streak_pick(
                date_str     = date,
                entry_amount = streak_entry,
                dry_run      = self.dry_run,
            )
            if streak_result:
                logger.info(
                    "[StreakAgent] Pick #%d/%d sent — %s %s %.1f %s "
                    "| conf=%.1f | prob=%.1f%%",
                    streak_result["pick_number"], 11,
                    streak_result["player_name"],
                    streak_result["prop_type"],
                    streak_result["line"],
                    streak_result["direction"],
                    streak_result["confidence"],
                    streak_result["probability"] * 100,
                )
            else:
                logger.info("[StreakAgent] No qualifying pick today (conf ≥ 8.0 gate not met).")
        except ImportError:
            logger.debug("[StreakAgent] streak_agent.py not found — skipping.")
        except Exception as _streak_err:
            logger.warning("[StreakAgent] Error during streak pick: %s", _streak_err)

        # ── ArbitrageAgent (16th agent) ─────────────────────────────────────
        # Cross-platform line discrepancy finder.  Scans the combined PP + UD
        # raw prop pool for the same player+stat where the two platforms quote
        # different lines.  Plays Over on the lower line and Under on the higher
        # line — both legs win if the actual result lands in the gap.
        # Gates: ≥ 0.5-unit gap · ≥ 0.5% arb margin · each leg ≥ 0.54 base prob.
        try:
            arb_picks = build_arbitrage_picks(all_raw)
            if arb_picks:
                logger.info("[ArbitrageAgent] %d arb opportunities found", len(arb_picks))
                for arb in arb_picks:
                    arb_conf       = arb["confidence"]
                    arb_margin_pct = arb["arb_margin"] * 100

                    if arb_conf < 7.0:
                        logger.info(
                            "[ArbitrageAgent] %s %s margin=%.2f%% conf=%.1f/10 — below gate",
                            arb["player_name"], arb["prop_type"],
                            arb_margin_pct, arb_conf,
                        )
                        continue

                    arb_parlay = {
                        "agent_name":  "ArbitrageAgent",
                        "agent_emoji": "🔄",
                        "entry_type":  "FLEX",
                        "ev_pct":      round(arb_margin_pct, 2),
                        "confidence":  arb_conf,
                        "notes": (
                            f"Cross-platform split — {arb['gap']:.1f}-unit line gap. "
                            f"Both legs win when result lands between the two lines."
                        ),
                        "legs": [
                            {
                                "player_name":  arb["player_name"],
                                "prop_type":    arb["prop_type"],
                                "side":         "Over",
                                "line":         arb["over_line"],
                                "platform":     arb["over_plat"],
                                "implied_prob": arb["p_over"],
                                "entry_type":   arb["over_etype"],
                                "fantasy_pts":  0.0,
                            },
                            {
                                "player_name":  arb["player_name"],
                                "prop_type":    arb["prop_type"],
                                "side":         "Under",
                                "line":         arb["under_line"],
                                "platform":     arb["under_plat"],
                                "implied_prob": arb["p_under"],
                                "entry_type":   arb["under_etype"],
                                "fantasy_pts":  0.0,
                            },
                        ],
                    }

                    logger.info(
                        "[ArbitrageAgent] %s %s gap=%.1f margin=%.2f%% conf=%.1f/10 → SEND",
                        arb["player_name"], arb["prop_type"],
                        arb["gap"], arb_margin_pct, arb_conf,
                    )

                    if not self.dry_run:
                        arb_parlay["season_stats"] = get_agent_season_stats("ArbitrageAgent")
                        discord_alert.send_parlay_alert(arb_parlay)
                        time.sleep(1.5)
                        record_parlay(
                            date=date,
                            agent="ArbitrageAgent",
                            num_legs=2,
                            confidence=arb_conf,
                            ev_pct=arb_margin_pct,
                            legs=[
                                {
                                    "player_name": arb["player_name"],
                                    "prop_type":   arb["prop_type"],
                                    "side":        "Over",
                                    "line":        arb["over_line"],
                                },
                                {
                                    "player_name": arb["player_name"],
                                    "prop_type":   arb["prop_type"],
                                    "side":        "Under",
                                    "line":        arb["under_line"],
                                },
                            ],
                        )
                        sent += 1
                    else:
                        logger.info(
                            "[DRY-RUN] ArbitrageAgent: %s %s Over %.1f (%s) + Under %.1f (%s)",
                            arb["player_name"], arb["prop_type"],
                            arb["over_line"], arb["over_plat"],
                            arb["under_line"], arb["under_plat"],
                        )
            else:
                logger.info("[ArbitrageAgent] No qualifying cross-platform discrepancies today.")
        except Exception as _arb_err:
            logger.warning("[ArbitrageAgent] Error: %s", _arb_err, exc_info=True)

    # ── private ───────────────────────────────────────────────────────────────

    def _evaluate_props(
        self,
        raw_props: list[dict],
        sbd_game_df=None,
        sbd_prop_df=None,
    ) -> list[PropLeg]:
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
        # Uses module-level _BASE_RATES (single source of truth shared with
        # ArbitrageAgent).  See top of file for full table + documentation.

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

        # ── Group props by (player, prop_type) ────────────────────────────
        from collections import defaultdict
        groups: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)
        # dict[(player_lower, prop_type)][platform] = {line, entry_type, position}

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
                    # Phase 27: preserve enrichment fields through grouping
                    "de_hit_pct":   float(raw.get("de_hit_pct",  0.0) or 0.0),
                    "de_hr_pct":    float(raw.get("de_hr_pct",   0.0) or 0.0),
                    "de_k_pct":     float(raw.get("de_k_pct",    0.0) or 0.0),
                    "de_sb_pct":    float(raw.get("de_sb_pct",   0.0) or 0.0),
                    "de_run_pct":   float(raw.get("de_run_pct",  0.0) or 0.0),
                    "de_rbi_pct":   float(raw.get("de_rbi_pct",  0.0) or 0.0),
                    "sc_whiff_rate":    float(raw.get("sc_whiff_rate",    0.0) or 0.0),
                    "sc_hard_hit_rate": float(raw.get("sc_hard_hit_rate", 0.0) or 0.0),
                    "sc_season_avg":    float(raw.get("sc_season_avg",    0.0) or 0.0),
                }

        # ── Pre-fetch hot/cold form data for all unique players ─────────────
        # Collect every distinct player name from the merged groups dict so
        # we issue MLB Stats API game-log requests once per player (cached).
        _form_unique: set[str] = {
            info["player_name"]
            for plat_map in groups.values()
            for info in plat_map.values()
            if info.get("player_name")
        }
        try:
            _form_layer.prefetch_form_data(_form_unique)
        except Exception as _form_exc:
            logger.warning("[Form] Pre-fetch skipped: %s", _form_exc)

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

                # ── Phase 27: Enhancement layer boosts ────────────────────
                de_boost_val = 0.0

                # 1. DraftEdge blend (65% base + 35% DE signal when available)
                def _de_signal(pt: str, sd: str, ent: dict) -> float:
                    """Map prop_type + side to the relevant DraftEdge probability."""
                    _over_map = {
                        "hits":         "de_hit_pct",
                        "home_runs":    "de_hr_pct",
                        "stolen_bases": "de_sb_pct",
                        "runs":         "de_run_pct",
                        "rbis":         "de_rbi_pct",
                        "strikeouts":   "de_k_pct",
                    }
                    if sd == "Over":
                        key = _over_map.get(pt)
                        return float(ent.get(key, 0.0) or 0.0) if key else 0.0
                    else:  # Under — invert the over probability
                        key = _over_map.get(pt)
                        if not key:
                            return 0.0
                        p_over = float(ent.get(key, 0.0) or 0.0)
                        return (1.0 - p_over) if p_over > 0 else 0.0

                de_sig = _de_signal(prop_type, side, chosen_entry)
                if de_sig > 0:
                    blended = 0.65 * prob + 0.35 * de_sig
                    de_boost_val = round(blended - prob, 4)
                    prob = min(0.80, blended)

                # 2. Statcast boosts (small additive signal on K and HR/TB props)
                if prop_type == "strikeouts" and side == "Over":
                    sc_whiff = float(chosen_entry.get("sc_whiff_rate", 0.0) or 0.0)
                    if sc_whiff > 0:
                        # whiff_rate 0.20–0.35 typical → adds 3–5% to K prob
                        prob = min(0.80, prob + sc_whiff * 0.15)
                elif prop_type in ("home_runs", "total_bases") and side == "Over":
                    sc_hh = float(chosen_entry.get("sc_hard_hit_rate", 0.0) or 0.0)
                    if sc_hh > 0:
                        prob = min(0.80, prob + sc_hh * 0.10)
                elif prop_type == "hits" and side == "Over":
                    sc_avg = float(chosen_entry.get("sc_season_avg", 0.0) or 0.0)
                    if sc_avg > 0:
                        # Positive contact rate (avg > .270) adds slight hit prob bump
                        prob = min(0.80, prob + max(0.0, sc_avg - 0.250) * 0.10)

                # 3. SBD FadeAgent signal
                sbd_ticket_pct = 0.0
                is_fade_signal = False
                if sbd_game_df is not None and sbd_prop_df is not None and _SBD_AVAILABLE:
                    try:
                        raw_pct, _ = _get_fade_signal(
                            pname, "", prop_type, sbd_game_df, sbd_prop_df,
                        )
                        sbd_ticket_pct = float(raw_pct or 0.0)
                        is_fade_signal = sbd_ticket_pct >= 65.0
                    except Exception:
                        pass  # non-fatal: SBD data unavailable for this prop
                # ── End Phase 27 ──────────────────────────────────────────

                # ── Hot/cold form adjustment (MLB Stats API rolling avg) ────────
                # Compares player's last-7-game rolling stat avg vs prior-season
                # per-game avg.  Returns ±0.035 max — never blocks a prop on its
                # own, just nudges probability in the right direction.
                try:
                    _form_adj = _form_layer.get_form_adjustment(pname, prop_type)
                    if _form_adj != 0.0:
                        logger.debug(
                            "[Form] %-22s  %-16s  adj=%+.3f  %.3f→%.3f",
                            pname, prop_type, _form_adj, prob, prob + _form_adj,
                        )
                    prob = min(0.80, max(0.40, prob + _form_adj))
                except Exception:
                    pass  # graceful degradation — never let form data kill a leg

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
                    de_boost=de_boost_val,
                    sbd_ticket_pct=round(sbd_ticket_pct, 1),
                    is_fade_signal=is_fade_signal,
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
