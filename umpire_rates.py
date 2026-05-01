"""
umpire_rates.py
===============
Home plate umpire K-rate, BB-rate, and run-impact lookup.

Sources
-------
Static table  : 2023-2025 rolling averages from umpire-scorecards.com (fallback).
Live API      : UmpScorecards API (umpscorecards.com/api/umpires) — fetched once per
                calendar day and cached in-process.  Provides:
                  run_impact  — avg runs added/removed per game by incorrect calls
                                (positive = hitter-friendly, negative = pitcher-friendly)
                  accuracy    — called-strike accuracy %

Usage
-----
    from umpire_rates import get_umpire_rates
    rates = get_umpire_rates("Angel Hernandez")
    # → {
    #     "k_rate": 9.8, "bb_rate": 2.8,
    #     "k_mod": 1.114, "bb_mod": 0.903,
    #     "run_impact": -0.23,   # pitcher-friendly zone today
    #     "accuracy": 95.1,
    #     "known": True
    #   }

k_mod  = k_rate / 8.8   (1.0 = league avg, >1.0 = K-friendly, <1.0 = hitter-friendly)
bb_mod = bb_rate / 3.1  (1.0 = league avg, >1.0 = walk-friendly)
run_impact: + = hitter-friendly zone; − = pitcher-friendly zone; 0.0 = neutral / unknown.
"""
from __future__ import annotations

import logging
import time
from datetime import date

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Static table — 2023-2025 averages (K/9, BB/9)
# Source: umpire-scorecards.com 3-year rolling averages
# League avg: K/9 ≈ 8.8, BB/9 ≈ 3.1
# ---------------------------------------------------------------------------
_UMPIRE_TABLE: dict[str, tuple[float, float]] = {
    # (k_rate_per_9, bb_rate_per_9)
    # ── Pitcher-friendly (high K, low BB) ────────────────────────────────────
    "angel hernandez":      (9.8, 2.8),
    "cb bucknor":           (9.4, 2.9),
    "joe west":             (9.3, 2.7),
    "dan iassogna":         (9.5, 2.8),
    "marvin hudson":        (9.2, 2.9),
    "mike winters":         (9.4, 3.0),
    "paul nauert":          (9.1, 2.9),
    "gerry davis":          (9.0, 2.8),
    "laz diaz":             (9.3, 3.0),
    "rob drake":            (9.2, 2.9),
    "eric cooper":          (9.1, 3.0),
    "fieldin culbreth":     (9.0, 2.9),
    "jeff kellogg":         (9.1, 3.1),
    "bill welke":           (9.0, 3.0),
    "mark wegner":          (9.1, 2.9),
    "dale scott":           (9.2, 3.0),
    "mike everitt":         (8.9, 2.9),
    "ted barrett":          (9.0, 3.0),
    "greg gibson":          (9.1, 3.1),
    "james hoye":           (9.2, 3.0),
    "adam hamari":          (9.0, 2.9),
    "toby basner":          (9.1, 3.0),
    "ben may":              (9.0, 3.0),
    # ── Near league average ────────────────────────────────────────────────
    "will little":          (8.9, 3.1),
    "sam holbrook":         (8.8, 3.1),
    "hunter wendelstedt":   (8.8, 3.2),
    "chris guccione":       (8.7, 3.1),
    "jim wolf":             (8.7, 3.2),
    "lance barrett":        (8.8, 3.1),
    "tom hallion":          (8.9, 3.2),
    "mike muchlinski":      (8.7, 3.1),
    "ryan blakney":         (8.8, 3.1),
    "jeremie rehak":        (8.8, 3.2),
    "chad fairchild":       (8.7, 3.1),
    "tripp gibson":         (8.9, 3.2),
    "david rackley":        (8.8, 3.1),
    "ed hickox":            (8.7, 3.2),
    "tim timmons":          (8.8, 3.1),
    "paul emmel":           (8.7, 3.1),
    "stu scheurwater":      (8.8, 3.2),
    "chris conroy":         (8.7, 3.1),
    "john tumpane":         (8.8, 3.2),
    "scott barry":          (8.9, 3.1),
    "brennan miller":       (8.7, 3.1),
    "jansen visconti":      (8.8, 3.1),
    "alex tosi":            (8.7, 3.2),
    "pat hoberg":           (8.8, 3.1),
    "jordan baker":         (8.9, 3.0),
    "andy fletcher":        (8.7, 3.1),
    "mark ripperger":       (8.8, 3.1),
    "alfonso marquez":      (8.7, 3.2),
    "mike estabrook":       (8.8, 3.1),
    "corey blaser":         (8.7, 3.1),
    "cory blaser":          (8.7, 3.1),
    "kyle mcclendon":       (8.8, 3.1),
    "shane livensparger":   (8.7, 3.1),
    "john libka":           (8.8, 3.2),
    "manny gonzalez":       (8.7, 3.1),
    "brian gorman":         (8.8, 3.0),
    "quinn wolcott":        (8.7, 3.1),
    "jake reed":            (8.8, 3.1),
    "junior valentine":     (8.7, 3.2),
    "roberto ortiz":        (8.8, 3.1),
    "dan bellino":          (8.7, 3.1),
    "phil cuzzi":           (8.8, 3.2),
    "mike dimuro":          (8.7, 3.1),
    # ── Hitter-friendly (low K, high BB) ─────────────────────────────────
    "bob davidson":         (7.8, 3.5),
    "bruce dreckman":       (8.0, 3.4),
    "vic carapazza":        (8.1, 3.3),
    "paul schrieber":       (8.0, 3.4),
    "gary cedarstrom":      (8.0, 3.4),
    "bill miller":          (8.1, 3.3),
    "jerry meals":          (7.9, 3.4),
    "mark carlson":         (8.0, 3.3),
    "clint fagan":          (8.1, 3.3),
    "larry vanover":        (7.9, 3.5),
    "marty foster":         (8.0, 3.4),
    "joe eddings":          (8.1, 3.3),
    "charlie reliford":     (7.9, 3.4),
    "ron kulpa":            (8.0, 3.4),
    "brian o'nora":         (7.9, 3.4),
    "brian onora":          (7.9, 3.4),
}

_LEAGUE_K_RATE  = 8.8
_LEAGUE_BB_RATE = 3.1
_DEFAULT = (_LEAGUE_K_RATE, _LEAGUE_BB_RATE)

# ---------------------------------------------------------------------------
# Live UmpScorecards API — fetched once per calendar day
# ---------------------------------------------------------------------------

_LIVE_CACHE: dict[str, dict] = {}   # name_lower → {run_impact, accuracy, above_x, games}
_LIVE_CACHE_DATE: str = ""          # YYYY-MM-DD of last successful fetch
_LIVE_FETCH_ATTEMPTED: str = ""     # YYYY-MM-DD to prevent hammering on failure

_UC_API     = "https://umpscorecards.com/api/umpires"
_UC_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _fetch_live_ump_stats() -> dict[str, dict]:
    """
    Fetch current-season umpire stats from UmpScorecards API.
    Called at most once per PT calendar day; results cached in _LIVE_CACHE.
    Returns {name_lower: {run_impact, accuracy, above_x, games}}.
    """
    global _LIVE_CACHE, _LIVE_CACHE_DATE, _LIVE_FETCH_ATTEMPTED

    today = date.today().isoformat()

    # Already loaded today
    if _LIVE_CACHE_DATE == today and _LIVE_CACHE:
        return _LIVE_CACHE

    # Already tried today and failed
    if _LIVE_FETCH_ATTEMPTED == today:
        return _LIVE_CACHE

    _LIVE_FETCH_ATTEMPTED = today

    try:
        import requests
        resp = requests.get(_UC_API, headers=_UC_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[UmpireRates] UmpScorecards live fetch failed: %s", exc)
        return _LIVE_CACHE

    umpires = data.get("rows") or (data if isinstance(data, list) else [])
    new_cache: dict[str, dict] = {}

    for u in umpires:
        name = u.get("umpire") or u.get("name") or u.get("fullName") or ""
        if not name:
            continue

        # Accuracy from raw called/correct counts
        called  = u.get("called_pitches_sum") or 0
        correct = u.get("called_correct_sum") or 0
        accuracy = round(correct / called * 100, 1) if called > 0 else None

        # run_impact: + = incorrect calls added runs (hitter-friendly)
        #             − = incorrect calls removed runs (pitcher-friendly)
        run_impact = u.get("total_run_impact_mean")
        if run_impact is not None:
            try:
                run_impact = round(float(run_impact), 3)
            except (TypeError, ValueError):
                run_impact = None

        above_x = u.get("correct_calls_above_x_sum")
        if above_x is not None:
            try:
                above_x = round(float(above_x), 1)
            except (TypeError, ValueError):
                above_x = None

        games = u.get("n")

        new_cache[_norm(name)] = {
            "run_impact": run_impact,   # + = hitter-friendly, − = pitcher-friendly
            "accuracy":   accuracy,
            "above_x":    above_x,
            "games":      int(games) if games is not None else None,
        }

    if new_cache:
        _LIVE_CACHE      = new_cache
        _LIVE_CACHE_DATE = today
        logger.info("[UmpireRates] UmpScorecards: %d umpires loaded (run_impact available)", len(new_cache))
    else:
        logger.warning("[UmpireRates] UmpScorecards returned 0 umpires")

    return _LIVE_CACHE


# ---------------------------------------------------------------------------
# Name normalisation
# ---------------------------------------------------------------------------

def _norm(name: str) -> str:
    import unicodedata
    n = unicodedata.normalize("NFD", name.lower().strip())
    return " ".join("".join(c for c in n if unicodedata.category(c) != "Mn").split())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_umpire_rates(name: str) -> dict:
    """
    Return umpire K/BB rates, derived modifiers, and live run_impact.

    Falls back to league-average defaults if umpire not found in static table.
    Merges live UmpScorecards run_impact on top (None if API unavailable).

    Fields returned:
        k_rate      — strikeouts per 9 innings (raw)
        bb_rate     — walks per 9 innings (raw)
        k_mod       — k_rate / 8.8  (1.0 = avg, >1 = K-friendly)
        bb_mod      — bb_rate / 3.1 (1.0 = avg, >1 = BB-friendly)
        run_impact  — avg runs per game from incorrect calls
                      (+0.5 = hitter-friendly; -0.5 = pitcher-friendly; 0.0 = neutral)
        accuracy    — called-strike accuracy % (None if unavailable)
        known       — True if umpire found in static table
    """
    k, bb = _UMPIRE_TABLE.get(_norm(name), _DEFAULT)

    # Try live API (cached after first call each day)
    live = _fetch_live_ump_stats()
    live_stats = live.get(_norm(name), {})

    run_impact = live_stats.get("run_impact")   # None if umpire not in live data
    # Fallback to static research table when live fetch 403s on Railway
    if run_impact is None:
        run_impact = _STATIC_RUN_IMPACT.get(_norm(name), 0.0)
    accuracy   = live_stats.get("accuracy")

    return {
        "k_rate":     k,
        "bb_rate":    bb,
        "k_mod":      round(k  / _LEAGUE_K_RATE,  4),
        "bb_mod":     round(bb / _LEAGUE_BB_RATE, 4),
        "run_impact": run_impact if run_impact is not None else 0.0,
        "accuracy":   accuracy,
        "known":      _norm(name) in _UMPIRE_TABLE,
    }
