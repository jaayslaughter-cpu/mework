"""
action_network_layer.py
=======================
Fetches MLB public betting data from Action Network.

Public endpoints (no auth required)
-------------------------------------
  fetch_mlb_game_sentiment()      → game-level ticket%/money% + RLM signal
                                     Gaps filled vs v1:
                                       ✅ moneyline ticket%/money% per team
                                       ✅ spread (run-line) ticket%/money%
                                       ✅ num_bets volume signal per game
                                       ✅ pitcher game stats (k9/era/whip/kbb/gofo)
                                     All from the same scoreboard endpoint, zero extra cost.

  fetch_mlb_pitcher_game_stats()  → same-day pitcher stats keyed by player_id
                                     Injects into game_prediction_layer as recency override.

PRO-gated endpoints (require ACTION_NETWORK_COOKIE env var — Bearer JWT)
-------------------------------------------------------------------------
  fetch_mlb_prop_projections()    → player-level ticket%/money% per prop market
                                     Unlocks _SharpFadeAgent Path 1 (true player-level signal).
                                     Reads ACTION_NETWORK_COOKIE as Bearer JWT from Railway env.

  fetch_live_projections()        → live MLB prop projections from REST v2 endpoint
                                     api.actionnetwork.com/web/v2/leagues/1/projections/available
                                     Same Bearer token. Returns [] if no live games.

Usage
-----
    from action_network_layer import (
        fetch_mlb_game_sentiment,
        fetch_mlb_pitcher_game_stats,
        fetch_mlb_prop_projections,
        fetch_live_projections,
    )

    sentiment      = fetch_mlb_game_sentiment()
    pitcher_stats  = fetch_mlb_pitcher_game_stats()
    props          = fetch_mlb_prop_projections()   # empty list if no token
    live_projs     = fetch_live_projections()        # empty list if no live games
"""

from __future__ import annotations

import logging
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_PT = ZoneInfo("America/Los_Angeles")

# ── Startup check — warn once if ACTION_NETWORK_COOKIE is not configured ─────────
# SharpFadeAgent will run in Path 2 (game-level RLM) without it, but PRO data
# is active for this account. Missing token = wasted PRO subscription.
# FIX: Railway → select SERVICE (not project) → Variables → ACTION_NETWORK_COOKIE
_jwt_startup = os.getenv("ACTION_NETWORK_COOKIE", "").strip()
if not _jwt_startup:
    logging.getLogger(__name__).warning(
        "[ActionNetwork] STARTUP: ACTION_NETWORK_COOKIE env var not set. "        "SharpFadeAgent PRO path disabled. "        "Set this variable at the Railway SERVICE level (not project level)."
    )
del _jwt_startup

# Books included: Bovada(15), Caesars(30), BetMGM(79), FanDuel(2988),
#                  Bet365(75), PointsBet(123), WynnBet(71), DraftKings(68), BetRivers(69)
_BOOK_IDS      = "15,30,79,2988,75,123,71,68,69"
_PRIMARY_BOOK  = "68"   # DraftKings — most liquid, most reliable ticket data
_FALLBACK_BOOKS = ["15", "69", "79", "2988", "30"]

# ── Scoreboard public betting endpoint ────────────────────────────────────────
_SCOREBOARD_URL = (
    "https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb"
    "?bookIds={books}&date={date}&periods=event"
)

# ── Prop projections endpoint (PRO-gated — Bearer JWT required) ───────────────
# build_id changes when AN deploys; must match the live value.
# If fetches return 404, capture a fresh HAR from actionnetwork.com/mlb/prop-projections
# and update _BUILD_ID from the _next/data/{build_id}/... URL.
_BUILD_ID = "xCV3Npj3q37WLxXkJelCQ"
_PROP_PROJ_URL = (
    "https://www.actionnetwork.com/_next/data/{build_id}/mlb/prop-projections.json"
    "?league=mlb"
)

# ── Live projections REST endpoint (PRO-gated — Bearer JWT required) ──────────
# Returns live MLB prop projections; league 1 = MLB.
# stateCode=CA required by AN backend (matches user's PRO session state).
_LIVE_PROJ_URL = (
    "https://api.actionnetwork.com/web/v2/leagues/1/projections/available"
    "?isLive=true&limit=50&stateCode=CA"
)

# ── AN market types → PropIQ prop_type canonical names ───────────────────────
# Full set confirmed from HAR marketDropdownItems (14 markets total).
_MARKET_TO_PROP_TYPE: dict[str, str] = {
    "core_bet_type_33_hr":              "home_runs",
    "core_bet_type_36_hits":            "hits",
    "core_bet_type_34_rbi":             "rbis",
    "core_bet_type_32_singles":         "singles",
    "core_bet_type_431_hits_runs_rbis": "hits_runs_rbis",
    "core_bet_type_35_doubles":         "doubles",
    "core_bet_type_75_triples":         "triples",
    "core_bet_type_710_hitter_walks":   "walks",
    "core_bet_type_709_hitter_strikeouts": "hitter_strikeouts",
    "core_bet_type_37_strikeouts":      "strikeouts",      # pitcher Ks
    "core_bet_type_42_pitching_outs":   "pitching_outs",
    "core_bet_type_72_hits_allowed":    "hits_allowed",
    "core_bet_type_76_walks":           "pitcher_walks",
}

# ── Base headers — public scoreboard, no auth ─────────────────────────────────
_PUBLIC_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.actionnetwork.com/mlb/public-betting",
    "Origin": "https://www.actionnetwork.com",
}

# ── Headers for PRO-gated _next/data endpoint ─────────────────────────────────
# Bearer token injected at call-time from ACTION_NETWORK_COOKIE env var.
_NEXT_HEADERS = {
    "User-Agent": _PUBLIC_HEADERS["User-Agent"],
    "Accept": "application/json",
    "Referer": "https://www.actionnetwork.com/mlb/prop-projections",
    "x-nextjs-data": "1",
    "sec-ch-ua-mobile": "?0",
}

# ── Headers for PRO REST API (api.actionnetwork.com) ─────────────────────────
_PRO_REST_HEADERS = {
    "User-Agent": _PUBLIC_HEADERS["User-Agent"],
    "Accept": "application/json",
    "Origin": "https://www.actionnetwork.com",
    "Referer": "https://www.actionnetwork.com/pro-dashboard",
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}

# ── Per-day in-process cache ─────────────────────────────────────────────────
_SENTIMENT_CACHE: dict       = {}
_SENTIMENT_CACHE_DATE: str   = ""

_PITCHER_CACHE: dict         = {}
_PITCHER_CACHE_DATE: str     = ""

_PROP_PROJ_CACHE: list       = []
_PROP_PROJ_CACHE_DATE: str   = ""

_LIVE_PROJ_CACHE: list       = []
_LIVE_PROJ_CACHE_DATE: str   = ""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_pt() -> str:
    return datetime.now(_PT).strftime("%Y%m%d")


def _best_book_data(markets: dict) -> dict:
    """Return the first non-empty event-level market block for any known book."""
    for bid in [_PRIMARY_BOOK] + _FALLBACK_BOOKS:
        bd = markets.get(bid, {}).get("event", {})
        if bd:
            return bd
    return {}


def _parse_total(book_data: dict) -> dict:
    """Ticket%/money% for the game total (Over/Under)."""
    out = {
        "over_ticket_pct":  50,
        "over_money_pct":   50,
        "under_ticket_pct": 50,
        "under_money_pct":  50,
        "game_total": None,
    }
    for entry in book_data.get("total", []):
        if not isinstance(entry, dict):
            continue
        side  = entry.get("side", "")
        pct_t = entry.get("bet_info", {}).get("tickets", {}).get("percent", 0) or 0
        pct_m = entry.get("bet_info", {}).get("money",   {}).get("percent", 0) or 0
        if pct_t == 0 and pct_m == 0:
            continue
        if side == "over":
            out["over_ticket_pct"] = pct_t
            out["over_money_pct"]  = pct_m
            out["game_total"]      = entry.get("value")
        elif side == "under":
            out["under_ticket_pct"] = pct_t
            out["under_money_pct"]  = pct_m
            if out["game_total"] is None:
                out["game_total"] = entry.get("value")
    return out


def _parse_moneyline(book_data: dict, home_team_id: int, away_team_id: int) -> dict:
    """
    Ticket%/money% for the moneyline market, split by home/away.

    Returns:
        {
          "home_ml_ticket_pct": int,
          "home_ml_money_pct":  int,
          "away_ml_ticket_pct": int,
          "away_ml_money_pct":  int,
          "home_ml_odds":       int,   # American odds, e.g. -150
          "away_ml_odds":       int,
        }
    """
    out = {
        "home_ml_ticket_pct": 50, "home_ml_money_pct": 50,
        "away_ml_ticket_pct": 50, "away_ml_money_pct": 50,
        "home_ml_odds": None,     "away_ml_odds": None,
    }
    for entry in book_data.get("moneyline", []):
        if not isinstance(entry, dict):
            continue
        team_id = entry.get("team_id")
        pct_t   = entry.get("bet_info", {}).get("tickets", {}).get("percent") or 0
        pct_m   = entry.get("bet_info", {}).get("money",   {}).get("percent") or 0
        odds    = entry.get("odds")
        if team_id == home_team_id:
            if pct_t:
                out["home_ml_ticket_pct"] = pct_t
            if pct_m:
                out["home_ml_money_pct"]  = pct_m
            if odds:
                out["home_ml_odds"] = odds
        elif team_id == away_team_id:
            if pct_t:
                out["away_ml_ticket_pct"] = pct_t
            if pct_m:
                out["away_ml_money_pct"]  = pct_m
            if odds:
                out["away_ml_odds"] = odds
    return out


def _parse_spread(book_data: dict, home_team_id: int, away_team_id: int) -> dict:
    """
    Ticket%/money% for the run-line (spread) market.

    Returns:
        {
          "home_rl_ticket_pct": int,
          "home_rl_money_pct":  int,
          "away_rl_ticket_pct": int,
          "away_rl_money_pct":  int,
          "home_rl_value":      float,  # e.g. -1.5
          "away_rl_value":      float,  # e.g. +1.5
        }
    """
    out = {
        "home_rl_ticket_pct": 50, "home_rl_money_pct": 50,
        "away_rl_ticket_pct": 50, "away_rl_money_pct": 50,
        "home_rl_value": None,    "away_rl_value": None,
    }
    for entry in book_data.get("spread", []):
        if not isinstance(entry, dict):
            continue
        team_id = entry.get("team_id")
        pct_t   = entry.get("bet_info", {}).get("tickets", {}).get("percent") or 0
        pct_m   = entry.get("bet_info", {}).get("money",   {}).get("percent") or 0
        value   = entry.get("value")
        if team_id == home_team_id:
            if pct_t:
                out["home_rl_ticket_pct"] = pct_t
            if pct_m:
                out["home_rl_money_pct"]  = pct_m
            if value is not None:
                out["home_rl_value"] = value
        elif team_id == away_team_id:
            if pct_t:
                out["away_rl_ticket_pct"] = pct_t
            if pct_m:
                out["away_rl_money_pct"]  = pct_m
            if value is not None:
                out["away_rl_value"] = value
    return out


def _parse_pitcher_stats(player_stats_block: dict) -> dict:
    """
    Extract per-player_id pitching stats from the game's player_stats block.

    player_stats_block is {"home": [...], "away": [...]} where each entry is:
        {"player_id": int, "pitching": {"k9": float, "era": str, "whip": float, ...}}

    Returns:
        {player_id (int): {"k9": float, "era": float, "whip": float, "kbb": float,
                           "gofo": float, "oba": float, "ip": float, "side": "home"|"away"}}
    """
    result: dict[int, dict] = {}
    for side in ("home", "away"):
        for entry in player_stats_block.get(side, []):
            if not isinstance(entry, dict):
                continue
            pid = entry.get("player_id")
            pit = entry.get("pitching")
            if not pid or not pit:
                continue
            # ip = ip_1 full innings + ip_2/3 partial outs
            ip_1 = float(pit.get("ip_1", 0) or 0)
            ip_2 = float(pit.get("ip_2", 0) or 0)
            ip_full = ip_1 + (ip_2 / 3.0)
            # era stored as string in the API
            try:
                era_val = float(pit.get("era", 4.5) or 4.5)
            except (TypeError, ValueError):
                era_val = 4.5
            result[pid] = {
                "k9":    float(pit.get("k9",   0.0) or 0.0),
                "era":   era_val,
                "whip":  float(pit.get("whip", 1.3) or 1.3),
                "kbb":   float(pit.get("kbb",  2.0) or 2.0),
                "gofo":  float(pit.get("gofo", 1.0) or 1.0),
                "oba":   float(pit.get("oba",  0.25) or 0.25),
                "ip":    round(ip_full, 2),
                "side":  side,
            }
    return result


def _rlm_signal(over_ticket: int, over_money: int, threshold: int = 15) -> tuple[bool, str | None]:
    """
    Reverse Line Movement: large divergence between ticket% and money%
    indicates sharp action on the side the public is NOT on.

    Returns (signal_fired: bool, sharp_direction: "over"|"under"|None)
    """
    divergence = over_ticket - over_money  # positive → public on Over, sharp on Under
    if abs(divergence) >= threshold:
        return True, "under" if divergence > 0 else "over"
    return False, None


# ── Public API — game sentiment ───────────────────────────────────────────────

def fetch_mlb_game_sentiment(date_str: str | None = None) -> dict:
    """
    Return a dict keyed by team full_name.lower() with full public-betting context.

    Schema per team entry:
        over_ticket_pct     int     % of bets placed on Over
        over_money_pct      int     % of dollar volume on Over
        under_ticket_pct    int
        under_money_pct     int
        rlm_signal          bool    |ticket% - money%| >= 15pp on total
        rlm_direction       str|None  "over" | "under"

        home_ml_ticket_pct  int     % of ML tickets on home team
        home_ml_money_pct   int
        away_ml_ticket_pct  int
        away_ml_money_pct   int
        home_ml_odds        int|None  American odds
        away_ml_odds        int|None

        home_rl_ticket_pct  int     % of run-line tickets on home (-1.5)
        home_rl_money_pct   int
        away_rl_ticket_pct  int
        away_rl_money_pct   int
        home_rl_value       float|None  e.g. -1.5
        away_rl_value       float|None

        ml_rlm_signal       bool    RLM on moneyline (sharp on dog or favorite)
        ml_rlm_direction    str|None  "home" | "away"

        num_bets            int     total public bets tracked for this game
        game_total          float|None

        opposing_team       str     full_name.lower() of opponent
        is_home             bool

    RLM thresholds:
        Total  : 15pp divergence
        ML     : 15pp divergence (home_ticket% vs home_money%)
    """
    global _SENTIMENT_CACHE, _SENTIMENT_CACHE_DATE

    date_str = date_str or _today_pt()
    if _SENTIMENT_CACHE_DATE == date_str and _SENTIMENT_CACHE:
        return _SENTIMENT_CACHE

    url = _SCOREBOARD_URL.format(books=_BOOK_IDS, date=date_str)
    try:
        resp = requests.get(url, headers=_PUBLIC_HEADERS, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[ActionNetwork] scoreboard fetch failed: %s", exc)
        return {}

    result: dict = {}
    games = data.get("games", [])

    for game in games:
        teams = game.get("teams", [])
        if len(teams) < 2:
            continue

        home_id = game.get("home_team_id")
        away_id = game.get("away_team_id")
        team_map = {t["id"]: t for t in teams}

        home_team = team_map.get(home_id, {})
        away_team = team_map.get(away_id, {})
        home_name = home_team.get("full_name", "").lower()
        away_name = away_team.get("full_name", "").lower()

        if not home_name or not away_name:
            continue

        markets   = game.get("markets", {})
        book_data = _best_book_data(markets)
        num_bets  = int(game.get("num_bets", 0) or 0)

        if not book_data:
            continue

        # ── Total (Over/Under) ────────────────────────────────────────────────
        totals                  = _parse_total(book_data)
        total_rlm, total_dir    = _rlm_signal(
            totals["over_ticket_pct"], totals["over_money_pct"]
        )

        # ── Moneyline ─────────────────────────────────────────────────────────
        ml = _parse_moneyline(book_data, home_id, away_id)
        # ML RLM: home_ticket% vs home_money% — sharp on away if home is getting
        # the tickets but money is flowing to away
        ml_divergence           = ml["home_ml_ticket_pct"] - ml["home_ml_money_pct"]
        ml_rlm                  = abs(ml_divergence) >= 15
        ml_rlm_dir: str | None  = None
        if ml_rlm:
            # public on home → sharp on away, and vice versa
            ml_rlm_dir = "away" if ml_divergence > 0 else "home"

        # ── Spread (run-line) ─────────────────────────────────────────────────
        rl = _parse_spread(book_data, home_id, away_id)

        # ── Build shared sentiment block ──────────────────────────────────────
        base = {
            # Totals
            "over_ticket_pct":  totals["over_ticket_pct"],
            "over_money_pct":   totals["over_money_pct"],
            "under_ticket_pct": totals["under_ticket_pct"],
            "under_money_pct":  totals["under_money_pct"],
            "rlm_signal":       total_rlm,
            "rlm_direction":    total_dir,
            "game_total":       totals["game_total"],
            # Moneyline
            "home_ml_ticket_pct": ml["home_ml_ticket_pct"],
            "home_ml_money_pct":  ml["home_ml_money_pct"],
            "away_ml_ticket_pct": ml["away_ml_ticket_pct"],
            "away_ml_money_pct":  ml["away_ml_money_pct"],
            "home_ml_odds":       ml["home_ml_odds"],
            "away_ml_odds":       ml["away_ml_odds"],
            "ml_rlm_signal":      ml_rlm,
            "ml_rlm_direction":   ml_rlm_dir,
            # Run-line
            "home_rl_ticket_pct": rl["home_rl_ticket_pct"],
            "home_rl_money_pct":  rl["home_rl_money_pct"],
            "away_rl_ticket_pct": rl["away_rl_ticket_pct"],
            "away_rl_money_pct":  rl["away_rl_money_pct"],
            "home_rl_value":      rl["home_rl_value"],
            "away_rl_value":      rl["away_rl_value"],
            # Volume
            "num_bets": num_bets,
        }

        result[home_name] = {**base, "opposing_team": away_name, "is_home": True}
        result[away_name] = {**base, "opposing_team": home_name, "is_home": False}

    _SENTIMENT_CACHE      = result
    _SENTIMENT_CACHE_DATE = date_str

    rlm_count    = sum(1 for v in result.values() if v["rlm_signal"]) // 2
    ml_rlm_count = sum(1 for v in result.values() if v["ml_rlm_signal"]) // 2
    logger.info(
        "[ActionNetwork] %d games | %d teams | %d total RLM | %d ML RLM | "
        "%d avg bets/game",
        len(games), len(result), rlm_count, ml_rlm_count,
        int(sum(v["num_bets"] for v in result.values()) / max(len(result), 1)),
    )
    return result


# ── Public API — pitcher game stats ──────────────────────────────────────────

def fetch_mlb_pitcher_game_stats(date_str: str | None = None) -> dict:
    """
    Return same-day pitcher stats for all starters/relievers who have appeared.

    Keyed by player_id (int).  Useful as a recency override in game_prediction_layer
    on top of Steamer/FanGraphs season projections.

    Schema per entry:
        k9    float   strikeouts per 9 innings (current game pace)
        era   float   earned run average (current game)
        whip  float
        kbb   float   K/BB ratio
        gofo  float   ground-out / fly-out ratio
        oba   float   opponent batting average
        ip    float   innings pitched (decimal, e.g. 5.333 = 5⅓)
        side  str     "home" | "away"

    NOTE: Stats reflect only the current game, not the season.
    Small sample until late innings — weight accordingly (weight = min(ip/5, 1.0)).

    Returns {} on fetch failure.
    """
    global _PITCHER_CACHE, _PITCHER_CACHE_DATE

    date_str = date_str or _today_pt()
    if _PITCHER_CACHE_DATE == date_str and _PITCHER_CACHE:
        return _PITCHER_CACHE

    # Reuse the scoreboard response rather than making a second request
    url = _SCOREBOARD_URL.format(books=_BOOK_IDS, date=date_str)
    try:
        resp = requests.get(url, headers=_PUBLIC_HEADERS, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[ActionNetwork] pitcher stats fetch failed: %s", exc)
        return {}

    result: dict[int, dict] = {}
    for game in data.get("games", []):
        ps_block = game.get("player_stats")
        if not isinstance(ps_block, dict):
            continue
        game_stats = _parse_pitcher_stats(ps_block)
        result.update(game_stats)

    _PITCHER_CACHE      = result
    _PITCHER_CACHE_DATE = date_str

    logger.info(
        "[ActionNetwork] Pitcher game stats: %d pitchers across today's slate",
        len(result),
    )
    return result


# ── PRO-gated API — player-level prop projections ────────────────────────────

def fetch_mlb_prop_projections(date_str: str | None = None) -> list[dict]:
    """
    Fetch player-level ticket%/money% from Action Network's prop projections page.

    Fully public endpoint — no cookie or API key required (confirmed from HAR).
    ACTION_NETWORK_COOKIE env var is optional; injected if set as a future-proof
    measure in case AN restricts access to PRO accounts.
    Returns empty list if request fails or props not yet posted pre-game.

    Schema per returned entry (mirrors sharp_report format used by _SharpFadeAgent):
        player          str     player full name
        player_id       int     Action Network player ID
        prop_type       str     PropIQ canonical (e.g. "strikeouts", "hits")
        an_market_key   str     raw AN market key (e.g. "core_bet_type_37_strikeouts")
        line            float   prop line value
        over_ticket_pct int
        over_money_pct  int
        under_ticket_pct int
        under_money_pct  int
        ticket_pct      int     alias → over_ticket_pct  (for _SharpFadeAgent compat)
        money_pct       int     alias → over_money_pct
        rlm_signal      bool
        rlm_direction   str|None  "over" | "under"
        num_bets        int     game-level volume (proxy for prop volume)
        source          str     "action_network_pro"

    Returns [] if Bearer token not set (graceful degradation — _SharpFadeAgent falls
    through to game-level Path 2 automatically).
    """
    global _PROP_PROJ_CACHE, _PROP_PROJ_CACHE_DATE

    date_str = date_str or _today_pt()
    if _PROP_PROJ_CACHE_DATE == date_str and _PROP_PROJ_CACHE:
        return _PROP_PROJ_CACHE

    token = os.getenv("ACTION_NETWORK_COOKIE", "").strip()
    if not token:
        logger.warning(
            "[ActionNetwork] ACTION_NETWORK_COOKIE not set — "
            "PRO prop projections unavailable. SharpFadeAgent falls back to game-level RLM. "
            "FIX: Railway → select your SERVICE (not the project) → Variables → "
            "add ACTION_NETWORK_COOKIE with the Bearer JWT value."
        )
        return []

    # Fetch build_id dynamically from AN homepage to survive deploys
    build_id = _BUILD_ID
    try:
        _home = requests.get(
            "https://www.actionnetwork.com/mlb/prop-projections",
            headers=_PUBLIC_HEADERS,
            timeout=10,
        )
        import re as _re
        _m = _re.search(r'"buildId"\s*:\s*"([^"]+)"', _home.text)
        if _m:
            build_id = _m.group(1)
    except Exception:
        pass  # fall back to cached _BUILD_ID

    url = _PROP_PROJ_URL.format(build_id=build_id)
    headers = {
        **_NEXT_HEADERS,
        "Authorization": f"Bearer {token}",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[ActionNetwork] prop projections fetch failed: %s", exc)
        return []

    # ── Navigate to playerProps ───────────────────────────────────────────────
    try:
        proj_resp = (
            data["pageProps"]["initialProjectionsConfig"]
                ["projectionsResponse"]["response"]
        )
    except (KeyError, TypeError) as exc:
        logger.warning("[ActionNetwork] prop projections unexpected schema: %s", exc)
        return []

    player_props = proj_resp.get("playerProps", [])
    players_meta = proj_resp.get("players", [])

    if not player_props:
        logger.info(
            "[ActionNetwork] prop projections returned 0 player props. "
            "Props not yet posted — check back closer to game time."
        )
        return []

    # ── Build player_id → name lookup ────────────────────────────────────────
    _id_to_name: dict[int, str] = {}
    for p in players_meta:
        if isinstance(p, dict) and p.get("id"):
            fname = p.get("first_name", "")
            lname = p.get("last_name", "")
            _id_to_name[p["id"]] = f"{fname} {lname}".strip()

    # ── Parse each playerProp entry ───────────────────────────────────────────
    result: list[dict] = []
    for pp in player_props:
        if not isinstance(pp, dict):
            continue

        market_key = pp.get("type", pp.get("market_type", ""))
        prop_type  = _MARKET_TO_PROP_TYPE.get(market_key, market_key)
        if not prop_type:
            continue

        player_id  = pp.get("player_id") or pp.get("id")
        player_name = _id_to_name.get(player_id, pp.get("player_name", ""))
        line        = pp.get("value") or pp.get("line")

        # ── Extract ticket%/money% ────────────────────────────────────────────
        bet_info   = pp.get("bet_info", {})
        over_t     = int(bet_info.get("over_tickets",  {}).get("percent", 50) or 50)
        over_m     = int(bet_info.get("over_money",    {}).get("percent", 50) or 50)
        under_t    = int(bet_info.get("under_tickets", {}).get("percent", 50) or 50)
        under_m    = int(bet_info.get("under_money",   {}).get("percent", 50) or 50)

        # Fallback: some AN responses use flat ticket_pct / money_pct keys
        if over_t == 50 and under_t == 50:
            over_t  = int(pp.get("over_ticket_pct",  pp.get("ticket_pct",  50)) or 50)
            over_m  = int(pp.get("over_money_pct",   pp.get("money_pct",   50)) or 50)
            under_t = 100 - over_t
            under_m = 100 - over_m

        rlm, rlm_dir = _rlm_signal(over_t, over_m)

        result.append({
            "player":           player_name,
            "player_id":        player_id,
            "prop_type":        prop_type,
            "an_market_key":    market_key,
            "line":             line,
            "over_ticket_pct":  over_t,
            "over_money_pct":   over_m,
            "under_ticket_pct": under_t,
            "under_money_pct":  under_m,
            # _SharpFadeAgent compatibility aliases
            "ticket_pct":       over_t,
            "money_pct":        over_m,
            "rlm_signal":       rlm,
            "rlm_direction":    rlm_dir,
            "source":           "action_network_pro",
        })

    _PROP_PROJ_CACHE      = result
    _PROP_PROJ_CACHE_DATE = date_str

    rlm_count = sum(1 for r in result if r["rlm_signal"])
    logger.info(
        "[ActionNetwork] PRO prop projections: %d player props | %d with RLM signal",
        len(result), rlm_count,
    )
    return result


# ── PRO REST API — live projections ──────────────────────────────────────────

def fetch_live_projections() -> list[dict]:
    """
    Fetch live MLB prop projections from Action Network PRO REST API.

    Endpoint: api.actionnetwork.com/web/v2/leagues/1/projections/available
    Requires ACTION_NETWORK_COOKIE env var (same Bearer JWT as prop projections).

    Returns [] if no live games, token not set, or fetch fails.
    Cached per PT calendar day.

    Schema per entry (raw AN response — fields vary):
        id              int
        player_id       int
        league_id       int     (1 = MLB)
        market_id       int
        value           float   projected value
        is_live         bool
        stateCode       str
    """
    global _LIVE_PROJ_CACHE, _LIVE_PROJ_CACHE_DATE

    date_str = _today_pt()
    if _LIVE_PROJ_CACHE_DATE == date_str and _LIVE_PROJ_CACHE:
        return _LIVE_PROJ_CACHE

    token = os.getenv("ACTION_NETWORK_COOKIE", "").strip()
    if not token:
        logger.debug("[ActionNetwork] fetch_live_projections: no Bearer token set.")
        return []

    headers = {
        **_PRO_REST_HEADERS,
        "Authorization": f"Bearer {token}",
    }

    try:
        resp = requests.get(_LIVE_PROJ_URL, headers=headers, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[ActionNetwork] live projections fetch failed: %s", exc)
        return []

    # AN v2 wraps data in {"data": [...], "meta": {...}} or returns flat list
    if isinstance(data, dict):
        projections = data.get("data", data.get("projections", []))
    elif isinstance(data, list):
        projections = data
    else:
        projections = []

    _LIVE_PROJ_CACHE      = projections
    _LIVE_PROJ_CACHE_DATE = date_str

    logger.info(
        "[ActionNetwork] Live projections: %d entries",
        len(projections),
    )
    return projections


# ── Convenience: combined sharp_report list for DataHub ──────────────────────

def build_sharp_report() -> list[dict]:
    """
    Build the sharp_report list consumed by _SharpFadeAgent Path 1.

    Requires ACTION_NETWORK_COOKIE env var (Bearer JWT) for PRO endpoint.
    Returns [] if token not set or props not yet posted pre-game.

    The game-level RLM signal lives in an_game_sentiment and is handled by
    _SharpFadeAgent Path 2 directly — it does not need to be in sharp_report.

    This function is the single call-site for DataHub Group 3.
    """
    return fetch_mlb_prop_projections()
