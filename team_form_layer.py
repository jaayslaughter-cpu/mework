"""
team_form_layer.py
==================
Last-15-game W/L record + runs-per-game averages for MLB teams.

Adapted from claude-baseball-dashboard/src/fetch/team_form.py.
Uses MLB Stats API schedule endpoint — Railway-safe, no external API key.

Usage
-----
    from team_form_layer import get_team_form, warm_team_form_cache

    # Warm the cache at the start of each DataHub cycle:
    warm_team_form_cache(team_ids=[147, 111, 119, ...])

    # Per-prop lookup (MLBAM team ID or abbreviation):
    form = get_team_form(team_id=147)  # NYY
    # → {
    #     "wins": 9, "losses": 6, "games": 15, "streak": "W3",
    #     "season_rpg": 4.82, "season_rapg": 4.11,
    #     "l15_rpg": 5.33, "l15_rapg": 3.87,
    #     "hot_offense": True,   # l15_rpg > season_rpg
    #     "hot_defense": True,   # l15_rapg < season_rapg
    #   }

hot_offense = True when L15 RPG > season RPG (team is running hot).
hot_defense = True when L15 RAPG < season RAPG (team is suppressing runs).
Both flags are directly useful in WeatherAgent and CorrelatedParlayAgent.
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-process cache — refreshed once per PT calendar day
# ---------------------------------------------------------------------------

_FORM_CACHE: dict[int, dict] = {}   # team_id (int) → form dict
_CACHE_DATE: str = ""               # YYYY-MM-DD of last warm

# Abbreviation → MLBAM team ID (covers all 30 teams)
ABBREV_TO_ID: dict[str, int] = {
    "ARI": 109, "ATL": 144, "BAL": 110, "BOS": 111, "CHC": 112,
    "CWS": 145, "CIN": 113, "CLE": 114, "COL": 115, "DET": 116,
    "HOU": 117, "KC":  118, "LAA": 108, "LAD": 119, "MIA": 146,
    "MIL": 158, "MIN": 142, "NYM": 121, "NYY": 147, "OAK": 133,
    "PHI": 143, "PIT": 134, "SD":  135, "SF":  137, "SEA": 136,
    "STL": 138, "TB":  139, "TEX": 140, "TOR": 141, "WSH": 120,
    # Alternates
    "CHW": 145, "CHN": 112, "SDP": 135, "SFG": 137, "SLN": 138,
    "KCR": 118, "LAN": 119, "ANA": 108,
}

ID_TO_ABBREV: dict[int, str] = {v: k for k, v in ABBREV_TO_ID.items() if len(k) <= 3}


# ---------------------------------------------------------------------------
# MLB Stats API helper
# ---------------------------------------------------------------------------

def _statsapi_schedule(team_id: int, start_date: str, end_date: str) -> list[dict]:
    """Call statsapi.schedule for one team. Returns list of game dicts."""
    try:
        import statsapi
        return statsapi.schedule(
            team=team_id,
            start_date=start_date,
            end_date=end_date,
        ) or []
    except Exception as exc:
        logger.warning("[TeamForm] statsapi.schedule(%s) failed: %s", team_id, exc)
        return []


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _runs(game: dict, team_id: int) -> tuple[int, int]:
    """Return (runs_scored, runs_allowed) for team_id in a completed game."""
    if game.get("home_id") == team_id:
        return (game.get("home_score") or 0), (game.get("away_score") or 0)
    return (game.get("away_score") or 0), (game.get("home_score") or 0)


def _team_won(game: dict, team_id: int) -> bool:
    scored, allowed = _runs(game, team_id)
    return scored > allowed


def _compute_form(team_id: int, as_of_date: str) -> dict:
    """
    Fetch and compute W/L + run averages for a team.
    Looks from Jan 1 of the current year through as_of_date − 1 day.
    """
    end_dt   = date.fromisoformat(as_of_date) - timedelta(days=1)
    start_dt = date(end_dt.year, 1, 1)   # full current season

    raw = _statsapi_schedule(
        team_id,
        start_date=start_dt.strftime("%m/%d/%Y"),
        end_date=end_dt.strftime("%m/%d/%Y"),
    )

    final = [g for g in raw if g.get("status") == "Final"]
    if not final:
        return _empty()

    # Season-long averages
    season_rs = [_runs(g, team_id)[0] for g in final]
    season_ra = [_runs(g, team_id)[1] for g in final]
    season_rpg  = round(sum(season_rs) / len(season_rs), 2)
    season_rapg = round(sum(season_ra) / len(season_ra), 2)

    # Last 15 games
    l15 = final[-15:]
    results = ["W" if _team_won(g, team_id) else "L" for g in l15]
    wins   = results.count("W")
    losses = results.count("L")

    l15_rs  = [_runs(g, team_id)[0] for g in l15]
    l15_ra  = [_runs(g, team_id)[1] for g in l15]
    l15_rpg  = round(sum(l15_rs) / len(l15_rs), 2)
    l15_rapg = round(sum(l15_ra) / len(l15_ra), 2)

    # Current streak
    streak_char = results[-1]
    streak_len  = 0
    for r in reversed(results):
        if r == streak_char:
            streak_len += 1
        else:
            break

    return {
        "wins":         wins,
        "losses":       losses,
        "games":        len(l15),
        "streak":       f"{streak_char}{streak_len}",
        "season_rpg":   season_rpg,
        "season_rapg":  season_rapg,
        "l15_rpg":      l15_rpg,
        "l15_rapg":     l15_rapg,
        # Derived flags — directly usable by agents
        "hot_offense":  l15_rpg > season_rpg,   # running hot offensively
        "hot_defense":  l15_rapg < season_rapg, # suppressing runs vs season avg
        "season_games": len(final),
    }


def _empty() -> dict:
    return {
        "wins": None, "losses": None, "games": 0, "streak": "",
        "season_rpg": None, "season_rapg": None,
        "l15_rpg": None,    "l15_rapg": None,
        "hot_offense": False, "hot_defense": False,
        "season_games": 0,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def warm_team_form_cache(
    team_ids: list[int] | None = None,
    as_of_date: str | None = None,
    sleep_ms: float = 0.15,
) -> None:
    """
    Pre-populate the in-process form cache for all teams playing today.

    Call this once from the DataHub cycle after the game schedule is loaded.
    Subsequent get_team_form() calls return instantly from cache.

    Args:
        team_ids    : list of MLBAM team IDs to warm (default: all 30 teams)
        as_of_date  : YYYY-MM-DD for "as of" calculation (default: today)
        sleep_ms    : polite delay between statsapi calls (seconds)
    """
    global _FORM_CACHE, _CACHE_DATE

    today = date.today().isoformat()
    as_of = as_of_date or today

    # Already warmed today
    if _CACHE_DATE == today and _FORM_CACHE:
        return

    ids_to_warm = team_ids or list(ID_TO_ABBREV.keys())
    warmed = 0

    for tid in ids_to_warm:
        try:
            _FORM_CACHE[tid] = _compute_form(tid, as_of)
            warmed += 1
        except Exception as exc:
            logger.debug("[TeamForm] team_id=%s failed: %s", tid, exc)
            _FORM_CACHE[tid] = _empty()
        time.sleep(sleep_ms)

    _CACHE_DATE = today
    logger.info(
        "[TeamForm] Cache warmed: %d teams  "
        "(hot offense: %d | hot defense: %d)",
        warmed,
        sum(1 for f in _FORM_CACHE.values() if f.get("hot_offense")),
        sum(1 for f in _FORM_CACHE.values() if f.get("hot_defense")),
    )


def get_team_form(
    team_id: int | None = None,
    abbrev: str | None = None,
) -> dict:
    """
    Return L15 form dict for a team.

    Accepts either MLBAM team_id (int) or abbreviation string.
    Fetches live if not in cache (single-team call — slower).
    Returns _empty() if data unavailable.
    """
    # Resolve team_id from abbreviation
    if team_id is None and abbrev:
        abbrev_upper = (abbrev or "").upper()
        team_id = ABBREV_TO_ID.get(abbrev_upper)

    if not team_id:
        return _empty()

    # Cache hit
    if team_id in _FORM_CACHE:
        return _FORM_CACHE[team_id]

    # Single-team live fetch (slower — prefer warm_team_form_cache() first)
    today = date.today().isoformat()
    try:
        form = _compute_form(team_id, today)
        _FORM_CACHE[team_id] = form
        return form
    except Exception as exc:
        logger.warning("[TeamForm] get_team_form(%s) live fetch failed: %s", team_id, exc)
        return _empty()


def get_team_form_by_abbrev(abbrev: str) -> dict:
    """Convenience alias — get form by team abbreviation string (e.g. 'NYY')."""
    return get_team_form(abbrev=abbrev)
