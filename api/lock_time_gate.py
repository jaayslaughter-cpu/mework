"""
lock_time_gate.py — Phase 91 Step 3
====================================
Prevents lookahead bias by classifying every prop's game status before analysis.

Rules
-----
- PRE_GAME  (>5 min to first pitch): safe — full enrichment and evaluation allowed
- LOCKED    (≤5 min to first pitch): warn only — allow pass-through but log
- LIVE      (game in progress):      BLOCK — no new bets on active games
- FINAL     (game complete):         BLOCK — no grading-loop contamination
- UNKNOWN   (no time data yet):      allow with warning (fail open, not closed)

Why this matters
----------------
Without this gate, a prop for a game that started at 1:05pm could still be
evaluated at 2:30pm using in-game or post-game context (batter already 2-for-3,
bullpen already used) — lookahead bias masquerading as live analysis.

The gate is the *only* place game status is checked.  All other code simply
reads prop["lookahead_safe"] and prop["game_state"] that this module stamps.
"""

from __future__ import annotations

import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
LOCK_MINUTES: int = 5        # minutes before first pitch where we stop accepting new bets
WARN_MINUTES: int = 30       # minutes before first pitch where we emit a warning

# Game state constants
PRE_GAME = "pre_game"
LOCKED   = "locked"
LIVE     = "live"
FINAL    = "final"
UNKNOWN  = "unknown"

# Abstract game states from MLB Stats API
_MLB_LIVE_STATES  = {"Live", "InProgress"}
_MLB_FINAL_STATES = {"Final", "Completed", "F/OT", "Game Over", "Postponed"}
_MLB_PRE_STATES   = {"Preview", "Scheduled", "Warmup", "Pre-Game"}


# ── Game time data structure ──────────────────────────────────────────────────
# context["game_times"] is a dict shaped like:
# {
#   "New York Yankees": {
#       "game_time_utc": "2026-03-29T18:05:00Z",   # ISO 8601 UTC
#       "abstract_state": "Preview",                # from MLB API
#       "opponent": "Boston Red Sox",
#       "venue": "Yankee Stadium",
#   },
#   ...
# }


# ── Core classification ────────────────────────────────────────────────────────
def classify_game(team: str, game_times: dict) -> str:
    """
    Return one of: PRE_GAME | LOCKED | LIVE | FINAL | UNKNOWN
    Uses abstract_state from MLB API first; falls back to time math.
    """
    if not team or not game_times:
        return UNKNOWN

    entry = _find_entry(team, game_times)
    if entry is None:
        return UNKNOWN

    abstract_state = entry.get("abstract_state", "")

    # MLB API status is authoritative
    if abstract_state in _MLB_FINAL_STATES:
        return FINAL
    if abstract_state in _MLB_LIVE_STATES:
        return LIVE

    # For Preview/Scheduled states, use time math
    mins = minutes_to_first_pitch(team, game_times)
    if mins is None:
        return UNKNOWN
    if mins <= 0:
        # API says Preview but time math says started — trust time math
        return LIVE
    if mins <= LOCK_MINUTES:
        return LOCKED
    return PRE_GAME


def should_skip_prop(prop: dict, game_times: dict) -> tuple[bool, str]:
    """
    Returns (skip: bool, reason: str).

    skip=True  → drop this prop from the pipeline entirely
    skip=False → process normally (caller may still see LOCKED warning)
    """
    team  = prop.get("team", "") or prop.get("opponent", "")
    state = classify_game(team, game_times)

    if state == FINAL:
        return True, f"game_final:{team}"
    if state == LIVE:
        return True, f"game_live:{team}"
    if state == LOCKED:
        mins = minutes_to_first_pitch(team, game_times) or 0
        logger.warning(
            "[LockGate] LOCKED — %s | team=%s | %d min to first pitch — allowing pass-through",
            prop.get("player", "?"), team, mins,
        )
        return False, f"game_locked:{team}:{mins}min"
    if state == UNKNOWN:
        logger.debug("[LockGate] UNKNOWN game time for team=%s — allowing pass-through", team)
        return False, "unknown_game_time"

    return False, "pre_game"


def stamp_prop(prop: dict, game_times: dict) -> dict:
    """
    Attach game_time_utc, game_state, lookahead_safe to the prop dict in-place.
    Called by prop_enrichment_layer after enrichment.
    """
    team  = prop.get("team", "") or prop.get("opponent", "")
    entry = _find_entry(team, game_times) or {}
    state = classify_game(team, game_times)
    mins  = minutes_to_first_pitch(team, game_times)

    prop["game_time_utc"]    = entry.get("game_time_utc", "")
    prop["game_state"]       = state
    prop["minutes_to_pitch"] = mins
    prop["lookahead_safe"]   = state in (PRE_GAME, LOCKED, UNKNOWN)

    return prop


# ── Time math helpers ──────────────────────────────────────────────────────────
def minutes_to_first_pitch(team: str, game_times: dict) -> Optional[int]:
    """
    Returns minutes until first pitch (negative if game has started).
    Returns None if no timestamp available.
    """
    entry = _find_entry(team, game_times)
    if entry is None:
        return None

    raw_time = entry.get("game_time_utc", "")
    if not raw_time:
        return None

    try:
        # Parse ISO 8601 UTC timestamp
        game_dt = _parse_utc(raw_time)
        now_utc = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        delta   = game_dt - now_utc
        return int(delta.total_seconds() / 60)
    except Exception:
        return None


def is_pre_game(team: str, game_times: dict) -> bool:
    return classify_game(team, game_times) in (PRE_GAME, UNKNOWN)


def is_locked(team: str, game_times: dict) -> bool:
    return classify_game(team, game_times) == LOCKED


def is_live(team: str, game_times: dict) -> bool:
    return classify_game(team, game_times) == LIVE


def is_final(team: str, game_times: dict) -> bool:
    return classify_game(team, game_times) == FINAL


# ── Game-times fetch (called from DataHub) ────────────────────────────────────
def fetch_game_times_today() -> dict[str, dict]:
    """
    Fetch first-pitch times and abstract game states for all MLB games today.
    Returns dict keyed by team name (both home and away).

    Called once per DataHub context refresh (TTL 10 min).
    Free — MLB Stats API, no key required.
    """
    import datetime as _dt
    import requests

    today = _dt.date.today().strftime("%Y-%m-%d")
    result: dict[str, dict] = {}

    try:
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": today, "hydrate": "team,venue,game(content(summary))"},
            timeout=15,
        )
        resp.raise_for_status()

        for date_block in resp.json().get("dates", []):
            for game in date_block.get("games", []):
                game_time_raw   = game.get("gameDate", "")           # e.g. "2026-03-29T18:05:00Z"
                abstract_state  = (game.get("status") or {}).get("abstractGameState", "Preview")
                home_team       = (game.get("teams", {}).get("home", {})
                                       .get("team", {}).get("name", ""))
                away_team       = (game.get("teams", {}).get("away", {})
                                       .get("team", {}).get("name", ""))
                venue           = (game.get("venue") or {}).get("name", "")

                for team, opponent in ((home_team, away_team), (away_team, home_team)):
                    if team:
                        # Convert UTC game time to PT string "HH:MM" for easy comparison
                        _game_time_pt_str = ""
                        try:
                            import datetime as _dt2  # noqa: PLC0415
                            from zoneinfo import ZoneInfo as _ZI  # noqa: PLC0415
                            _gdt = _dt2.datetime.fromisoformat(game_time_raw.replace("Z", "+00:00"))
                            _gdt_pt = _gdt.astimezone(_ZI("America/Los_Angeles"))
                            _game_time_pt_str = _gdt_pt.strftime("%H:%M")
                        except Exception:
                            pass
                        result[team] = {
                            "game_time_utc":  game_time_raw,
                            "game_time_pt":   _game_time_pt_str,  # "HH:MM" in PT, e.g. "10:05"
                            "abstract_state": abstract_state,
                            "opponent":       opponent,
                            "venue":          venue,
                        }

        logger.info("[LockGate] Game times fetched: %d teams", len(result))

    except Exception as exc:
        logger.warning("[LockGate] fetch_game_times_today failed: %s", exc)

    return result


# ── Stat-line contamination check (for backtest / grading) ───────────────────
def data_is_contaminated(prop: dict, enrichment_ts: str, game_times: dict) -> bool:
    """
    Returns True if enrichment data was collected AFTER the game's first pitch.
    Used in backtest to invalidate training rows with lookahead data.

    enrichment_ts: ISO 8601 UTC string of when enrich_props() ran
    """
    team  = prop.get("team", "") or prop.get("opponent", "")
    entry = _find_entry(team, game_times)
    if entry is None or not entry.get("game_time_utc"):
        return False  # can't tell — assume clean

    try:
        game_dt = _parse_utc(entry["game_time_utc"])
        enrich_dt = _parse_utc(enrichment_ts)
        return enrich_dt >= game_dt  # enrichment happened after first pitch
    except Exception:
        return False


# ── Internal helpers ──────────────────────────────────────────────────────────
def _find_entry(team: str, game_times: dict) -> Optional[dict]:
    """Fuzzy team name lookup — handles abbreviations and partial matches."""
    if not team:
        return None

    team_lower = team.lower().strip()

    # Exact match first
    if team in game_times:
        return game_times[team]

    # Case-insensitive match
    for k, v in game_times.items():
        if k.lower() == team_lower:
            return v

    # Partial match (last word of team name, e.g. "Yankees" matches "New York Yankees")
    team_words = set(team_lower.split())
    for k, v in game_times.items():
        k_words = set(k.lower().split())
        if team_words & k_words:  # any word overlap
            return v

    return None


def _parse_utc(ts: str) -> datetime.datetime:
    """Parse ISO 8601 UTC timestamp to aware datetime."""
    ts = ts.rstrip("Z")
    if "T" in ts:
        dt = datetime.datetime.fromisoformat(ts)
    else:
        dt = datetime.datetime.fromisoformat(ts)
    return dt.replace(tzinfo=datetime.timezone.utc)
