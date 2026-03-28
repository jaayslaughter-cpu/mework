"""
sportsbook_reference_layer.py
==============================
Layer 7 — Sharp Sportsbook Reference (The Odds API)

Fetches live MLB player prop lines from DraftKings, FanDuel, BetMGM and
strips the vig to produce fair-value implied probabilities. Used as a sharp
market reference signal to validate DFS prop edges and power LineValueAgent.

Key:  673bf195062e60e666399be40f763545 (override via ODDS_API_KEY env var)
Quota: ~16 requests per day (1 event list + ~15 per-game prop calls)

Markets pulled:
    pitcher_strikeouts, batter_hits, batter_total_bases,
    batter_home_runs, batter_rbis, batter_stolen_bases, batter_runs_scored

Output: enrich_props_with_sportsbook(props) adds to each prop dict:
    sb_implied_prob       – vig-stripped sharp market probability (Over side)
    sb_line               – sportsbook consensus line (0.0 if not found)
    sb_line_gap           – prop["line"] - sb_line (negative = DFS line is
                            more favorable for Over; positive = DFS harder)
    sb_bookmakers         – list of bookmakers contributing to consensus
    sb_implied_prob_over  – vig-stripped Over probability
    sb_implied_prob_under – vig-stripped Under probability
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("propiq.sb_ref")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# 3-key fallback chain: primary (14d35c33) -> backup1 (673bf195) -> backup2 (e4e30098)
ODDS_API_KEY = (
    os.getenv("ODDS_API_KEY")
    or os.getenv("ODDS_API_KEY_PRIMARY", "14d35c33111760aca07e9547fff1561a")
)
_ODDS_KEY_FALLBACKS = [
    os.getenv("ODDS_API_KEY_BACKUP1", "673bf195062e60e666399be40f763545"),
    os.getenv("ODDS_API_KEY_BACKUP2", "e4e30098807a9eece674d85e30471f03"),
]
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SPORT         = "baseball_mlb"

# The Odds API market keys → our internal prop_type
_MARKET_MAP: dict[str, str] = {
    "pitcher_strikeouts":  "strikeouts",
    "batter_hits":         "hits",
    "batter_total_bases":  "total_bases",
    "batter_home_runs":    "home_runs",
    "batter_rbis":         "rbis",
    "batter_stolen_bases": "stolen_bases",
    "batter_runs_scored":  "runs",
}

_MARKETS_PARAM = ",".join(_MARKET_MAP.keys())

# Preferred bookmakers — sharpest first (sharp books set market)
_PREFERRED_BOOKS = [
    "draftkings", "fanduel", "betmgm", "williamhill_us",
    "pointsbetus", "betrivers", "unibet_us", "bovada",
]

# Daily disk cache to avoid burning quota on multiple runs
_CACHE_DIR = "/tmp"

_REQUEST_HEADERS = {
    "User-Agent": "PropIQ/1.0",
    "Accept":     "application/json",
}

# Jitter between per-game API calls (seconds)
_CALL_JITTER = 0.25


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _cache_path(date: str) -> str:
    return os.path.join(_CACHE_DIR, f"sb_ref_{date}.json")


def _load_cache(date: str) -> dict | None:
    path = _cache_path(date)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        logger.info("[SB_REF] Cache hit — %d reference entries", len(data))
        return data
    except Exception as exc:
        logger.warning("[SB_REF] Cache load failed: %s", exc)
        return None


def _save_cache(date: str, data: dict) -> None:
    try:
        with open(_cache_path(date), "w") as f:
            json.dump(data, f)
        logger.info("[SB_REF] Cache saved — %d entries", len(data))
    except Exception as exc:
        logger.warning("[SB_REF] Cache save failed: %s", exc)


def _strip_vig(over_dec: float, under_dec: float) -> tuple[float, float]:
    """
    Two-way vig removal using decimal odds.

    Removes the bookmaker's margin so both sides sum to 1.0.
    Returns (fair_over_prob, fair_under_prob).
    """
    if over_dec <= 1.0 or under_dec <= 1.0:
        return 0.5, 0.5
    implied_over  = 1.0 / over_dec
    implied_under = 1.0 / under_dec
    overround     = implied_over + implied_under
    if overround <= 0:
        return 0.5, 0.5
    return (
        round(implied_over  / overround, 5),
        round(implied_under / overround, 5),
    )


def _normalize_name(name: str) -> str:
    """Lowercase and strip punctuation for fuzzy name matching."""
    return (
        name.lower()
        .strip()
        .replace(".", "")
        .replace("'", "")
        .replace("-", " ")
        .replace("  ", " ")
    )


def _log_quota(response: requests.Response) -> None:
    """Log remaining API quota from response headers."""
    remaining = response.headers.get("x-requests-remaining", "?")
    used      = response.headers.get("x-requests-used", "?")
    logger.info(
        "[SB_REF] Quota — used: %s | remaining: %s",
        used, remaining,
    )


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def _fetch_events(date: str) -> list[dict]:
    """
    Fetch today's MLB game event IDs from The Odds API.
    Consumes 1 API request.
    """
    try:
        resp = requests.get(
            f"{ODDS_API_BASE}/sports/{SPORT}/events",
            params={
                "apiKey":     ODDS_API_KEY,
                "dateFormat": "iso",
            },
            headers=_REQUEST_HEADERS,
            timeout=15,
        )
        _log_quota(resp)
        # Fallback key rotation on auth failure
        if resp.status_code in (401, 403):
            for fallback_key in _ODDS_KEY_FALLBACKS:
                resp = requests.get(
                    f"{ODDS_API_BASE}/sports/{SPORT}/events",
                    params={"apiKey": fallback_key, "dateFormat": "iso"},
                    headers=_REQUEST_HEADERS,
                    timeout=15,
                )
                if resp.status_code == 200:
                    break
        if resp.status_code != 200:
            logger.warning(
                "[SB_REF] Events fetch HTTP %d: %s",
                resp.status_code, resp.text[:200],
            )
            return []
        events = resp.json()
        # Filter to today's date (commence_time starts with YYYY-MM-DD)
        todays = [
            e for e in events
            if e.get("commence_time", "").startswith(date)
        ]
        logger.info(
            "[SB_REF] %d events today (%s) from %d total",
            len(todays), date, len(events),
        )
        return todays
    except Exception as exc:
        logger.warning("[SB_REF] Events fetch failed: %s", exc)
        return []


def _fetch_event_odds(event_id: str) -> list[dict]:
    """
    Fetch player prop odds for a single game event.
    Consumes 1 API request. Returns list of bookmaker dicts.
    """
    try:
        resp = requests.get(
            f"{ODDS_API_BASE}/sports/{SPORT}/events/{event_id}/odds",
            params={
                "apiKey":     ODDS_API_KEY,
                "regions":    "us",
                "markets":    _MARKETS_PARAM,
                "oddsFormat": "decimal",
            },
            headers=_REQUEST_HEADERS,
            timeout=15,
        )
        if resp.status_code == 200:
            logger.debug("[SB_REF] Event %s — %d bookmakers", event_id,
                         len(resp.json().get("bookmakers", [])))
            return resp.json().get("bookmakers", [])
        elif resp.status_code == 422:
            # No player props available for this game (early-morning, not yet posted)
            logger.debug("[SB_REF] Event %s — no player props yet", event_id)
            return []
        else:
            logger.warning(
                "[SB_REF] Event %s HTTP %d: %s",
                event_id, resp.status_code, resp.text[:150],
            )
            return []
    except Exception as exc:
        logger.warning("[SB_REF] Event %s fetch failed: %s", event_id, exc)
        return []


# ---------------------------------------------------------------------------
# Core: build the sportsbook reference lookup
# ---------------------------------------------------------------------------

def build_sportsbook_reference(date: str | None = None) -> dict[tuple, dict]:
    """
    Fetch today's MLB player props from The Odds API.

    Returns a lookup dict keyed by (player_name_lower, prop_type, side):
        {
          "sb_implied_prob": float,   # vig-stripped fair probability
          "sb_line":         float,   # consensus line (most common across books)
          "bookmakers":      list,    # bookmakers that contributed
        }

    Caches to /tmp/sb_ref_{date}.json — safe to call multiple times per day.
    Returns empty dict on any error (Layer 7 is always additive/optional).
    """
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Return from disk cache if available (avoids burning quota on re-runs)
    cached = _load_cache(date)
    if cached is not None:
        return {
            tuple(json.loads(k)): v
            for k, v in cached.items()
        }

    # ── Step 1: Get today's event IDs ────────────────────────────────────
    events = _fetch_events(date)
    if not events:
        logger.warning("[SB_REF] No events found — Layer 7 unavailable today")
        return {}

    # ── Step 2: Fetch player props per game ──────────────────────────────
    # raw_entries: {(player_norm, prop_type, side, line)} → [(fair_prob, bm_title)]
    raw_entries: dict[tuple, list[tuple[float, str]]] = defaultdict(list)

    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue

        bookmakers = _fetch_event_odds(event_id)
        time.sleep(_CALL_JITTER)

        for bm in bookmakers:
            bm_key   = bm.get("key", "")
            bm_title = bm.get("title", bm_key)

            for market in bm.get("markets", []):
                market_key = market.get("key", "")
                prop_type  = _MARKET_MAP.get(market_key)
                if not prop_type:
                    continue

                # Group outcomes by player + point to get Over/Under pairs
                by_player: dict[tuple[str, float], dict[str, float]] = {}

                for outcome in market.get("outcomes", []):
                    side   = outcome.get("name", "")       # "Over" / "Under"
                    desc   = outcome.get("description", "")  # player name
                    point  = float(outcome.get("point") or 0)
                    price  = float(outcome.get("price") or 0)

                    if not desc or side not in ("Over", "Under"):
                        continue
                    if point <= 0 or price <= 1.0:
                        continue

                    player_norm = _normalize_name(desc)
                    key         = (player_norm, point)
                    if key not in by_player:
                        by_player[key] = {}
                    by_player[key][side] = price

                # Strip vig and record both sides
                for (player_norm, point), sides in by_player.items():
                    over_dec  = sides.get("Over",  0.0)
                    under_dec = sides.get("Under", 0.0)
                    if not over_dec or not under_dec:
                        continue

                    fair_over, fair_under = _strip_vig(over_dec, under_dec)
                    raw_entries[(player_norm, prop_type, "Over",  point)].append(
                        (fair_over,  bm_title)
                    )
                    raw_entries[(player_norm, prop_type, "Under", point)].append(
                        (fair_under, bm_title)
                    )

    if not raw_entries:
        logger.warning("[SB_REF] No prop outcomes parsed — check market availability")
        return {}

    # ── Step 3: Aggregate across bookmakers ──────────────────────────────
    # Group by (player, prop_type, side) and pick the most-covered line
    grouped: dict[tuple[str, str, str], dict[float, list]] = {}

    for (player_norm, prop_type, side, line), entries in raw_entries.items():
        key = (player_norm, prop_type, side)
        if key not in grouped:
            grouped[key] = {}
        if line not in grouped[key]:
            grouped[key][line] = []
        grouped[key][line].extend(entries)

    reference: dict[tuple, dict] = {}

    for (player_norm, prop_type, side), lines_data in grouped.items():
        # Pick line with the most bookmaker coverage
        best_line  = max(lines_data.keys(), key=lambda l: len(lines_data[l]))
        entries    = lines_data[best_line]
        avg_prob   = sum(p for p, _ in entries) / len(entries)
        books      = list({bm for _, bm in entries})

        reference[(player_norm, prop_type, side)] = {
            "sb_implied_prob": round(avg_prob, 4),
            "sb_line":         best_line,
            "bookmakers":      books,
        }

    logger.info(
        "[SB_REF] Reference built — %d player/prop/side combos from %d events",
        len(reference), len(events),
    )

    # ── Cache to disk ─────────────────────────────────────────────────────
    _save_cache(
        date,
        {json.dumps(list(k)): v for k, v in reference.items()},
    )

    return reference


# ---------------------------------------------------------------------------
# Public interface: enrich raw props list (called from live_dispatcher.py)
# ---------------------------------------------------------------------------

_RAW_STAT_TO_PROP: dict[str, str] = {
    "strikeouts":           "strikeouts",
    "pitcher strikeouts":   "strikeouts",
    "hits":                 "hits",
    "home runs":            "home_runs",
    "home_runs":            "home_runs",
    "rbis":                 "rbis",
    "rbi":                  "rbis",
    "total bases":          "total_bases",
    "total_bases":          "total_bases",
    "stolen bases":         "stolen_bases",
    "stolen_bases":         "stolen_bases",
    "runs":                 "runs",
    "hits+runs+rbis":       "hits_runs_rbis",
    "hits + runs + rbis":   "hits_runs_rbis",
    "earned runs":          "earned_runs",
    "earned runs allowed":  "earned_runs",
    "earned_runs":          "earned_runs",
}


def enrich_props_with_sportsbook(
    props: list[dict],
    date: str | None = None,
) -> list[dict]:
    """
    Add sportsbook reference fields to each prop dict in-place.

    Fields added per prop:
        sb_implied_prob       – vig-stripped sportsbook probability (Over side)
        sb_implied_prob_over  – explicit Over probability
        sb_implied_prob_under – explicit Under probability
        sb_line               – sportsbook consensus line
        sb_line_gap           – prop["line"] - sb_line
                                Negative = DFS line is lower (favorable for Over)
                                Positive = DFS line is higher (favorable for Under)
        sb_bookmakers         – list of contributing bookmakers

    Matching uses normalized player name. Falls back to last-name if no full
    match found. Unmatched props get 0.0 defaults (Layer 7 is always additive).
    """
    if not props:
        return props

    date      = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    reference = build_sportsbook_reference(date)

    if not reference:
        logger.warning("[SB_REF] Empty reference — props pass through unchanged")
        # Set defaults on all props so downstream code can safely read these fields
        for prop in props:
            prop.setdefault("sb_implied_prob",       0.0)
            prop.setdefault("sb_implied_prob_over",  0.0)
            prop.setdefault("sb_implied_prob_under", 0.0)
            prop.setdefault("sb_line",               0.0)
            prop.setdefault("sb_line_gap",           0.0)
            prop.setdefault("sb_bookmakers",          [])
        return props

    # Build a last-name index for fallback matching
    # {last_name → set of (player_norm, prop_type, side)}
    _last_name_idx: dict[str, set] = defaultdict(set)
    for (player_norm, prop_type, side) in reference:
        parts = player_norm.split()
        if parts:
            _last_name_idx[parts[-1]].add((player_norm, prop_type, side))

    matched = 0
    for prop in props:
        # Initialize defaults
        prop["sb_implied_prob"]       = 0.0
        prop["sb_implied_prob_over"]  = 0.0
        prop["sb_implied_prob_under"] = 0.0
        prop["sb_line"]               = 0.0
        prop["sb_line_gap"]           = 0.0
        prop["sb_bookmakers"]         = []

        raw_stat  = prop.get("stat_type", "")
        player    = prop.get("player_name", "")
        dfs_line  = float(prop.get("line") or 0)
        prop_type = _RAW_STAT_TO_PROP.get(raw_stat.strip().lower())

        if not prop_type or not player or dfs_line <= 0:
            continue

        player_norm = _normalize_name(player)

        # Try both sides and attach results
        side_probs: dict[str, dict] = {}
        for side in ("Over", "Under"):
            ref_key = (player_norm, prop_type, side)
            ref     = reference.get(ref_key)

            # Last-name fallback
            if not ref:
                parts     = player_norm.split()
                last_name = parts[-1] if parts else ""
                candidates = _last_name_idx.get(last_name, set())
                for cand in candidates:
                    if cand[1] == prop_type and cand[2] == side:
                        ref = reference.get(cand)
                        break

            if ref:
                side_probs[side] = ref

        if not side_probs:
            continue

        matched += 1

        # Over fields (primary — most props are bet Over-side)
        if "Over" in side_probs:
            over_ref = side_probs["Over"]
            sb_prob  = over_ref["sb_implied_prob"]
            sb_line  = over_ref["sb_line"]
            prop["sb_implied_prob"]      = sb_prob
            prop["sb_implied_prob_over"] = sb_prob
            prop["sb_line"]              = sb_line
            prop["sb_line_gap"]          = round(dfs_line - sb_line, 2)
            prop["sb_bookmakers"]        = over_ref.get("bookmakers", [])

        # Under probability (used by UnderMachine / FadeAgent cross-reference)
        if "Under" in side_probs:
            prop["sb_implied_prob_under"] = side_probs["Under"]["sb_implied_prob"]

    pct = round(100 * matched / len(props), 1) if props else 0
    logger.info(
        "[SB_REF] Enriched %d/%d props (%.1f%% match rate)",
        matched, len(props), pct,
    )
    return props
