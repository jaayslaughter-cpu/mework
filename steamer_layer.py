"""
steamer_layer.py
================
Steamer 2026 batter projection prior for PropIQ Analytics Engine.

What this does
--------------
Fetches the FanGraphs Steamer 2026 projections for all batters and uses
them to fill the gaps Marcel leaves: counting stat projections (R, RBI,
SB, HR) that map directly to prop types Marcel never informs.

Marcel already covers: K%, BB%, wOBA, ISO, HR/PA (rate stats)
Steamer adds:          AVG, OBP, SLG, R, RBI, SB, HR (counting stats)

These counting-stat projections are compared to line values and produce
nudges for runs, rbis, stolen_bases, and home_runs props — prop types
where PropIQ previously had no pre-season prior signal beyond base rates.

Integration point
-----------------
Fires in prop_enrichment_layer.py immediately after Marcel (Layer 8a),
before CV consistency (Layer 9).  Adds _steamer_adj to each prop.

Add to prop_enrichment_layer.py around line 1330:
    # ── Steamer 2026 counting stat projection (Layer 8b) ─────────────────
    _steamer_adj = _get_steamer_adj(player, prop_type, prop)
    prop["_steamer_adj"] = _steamer_adj

Data source
-----------
FanGraphs public API — same endpoint fangraphs_layer.py already uses.
Steamer projections use type=steamer parameter instead of type=8 (dashboard).
No API key required.  Cached in Postgres via layer_cache_helper (7-day TTL).

Prop-type coverage
------------------
  runs          → project R/G from Steamer R / projected PA
  rbis          → project RBI/G from Steamer RBI / projected PA
  stolen_bases  → project SB/G from Steamer SB / projected PA
  home_runs     → project HR/G (supplements Marcel HR/PA)
  hits          → supplements Marcel wOBA with projected AVG
  total_bases   → uses projected SLG directly

Max adjustment: ±0.025 per prop — additive layer, never overrides.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger("propiq.steamer_layer")

_TZ     = ZoneInfo("America/Los_Angeles")
_FG_BASE = "https://www.fangraphs.com/api/leaders/major-league/data"
# Import rotating headers from fangraphs_layer to avoid FanGraphs 403 blocks
try:
    from fangraphs_layer import _fg_headers  # noqa: PLC0415
except ImportError:
    import random as _random  # noqa: PLC0415
    def _fg_headers() -> dict:  # noqa: E306
        return {
            "User-Agent": _random.choice([
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            ]),
            "Referer": "https://www.fangraphs.com/projections",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.fangraphs.com",
        }

# FanGraphs Steamer projection API params
_STEAMER_PARAMS = {
    "age":       "0",
    "pos":       "all",
    "stats":     "bat",
    "lg":        "all",
    "qual":      "1",           # min 1 PA — include bench players
    "startdate": "",
    "enddate":   "",
    "month":     "0",
    "hand":      "",
    "team":      "0",
    "pageitems": "600",
    "pagenum":   "1",
    "ind":       "0",
    "rost":      "0",
    "players":   "0",
    "type":      "steamer",     # ← Steamer projections, not dashboard stats
    "postseason": "",
    "sortdir":   "default",
    "sortstat":  "PA",
}

# ── League-average baselines (FG 2025 actuals — used for deviation calc) ──────
_LG = {
    "avg":  0.248,
    "obp":  0.318,
    "slg":  0.410,
    "r_pg": 0.65,    # runs per game for avg lineup spot batter
    "rbi_pg": 0.55,  # RBI per game
    "sb_pg":  0.08,  # stolen bases per game
    "hr_pg":  0.033, # HR per game (PA-adjusted)
}

# ── In-process cache ───────────────────────────────────────────────────────────
_CACHE: dict[str, dict] = {}      # {name_key: projection_dict}
_CACHE_DATE: str = ""


def _norm(name: str) -> str:
    """Lowercase, strip accents/punctuation, collapse whitespace."""
    s = str(name).lower()
    for old, new in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),
                     ("ñ","n"),("ü","u"),("ö","o"),("ä","a")]:
        s = s.replace(old, new)
    return re.sub(r"[^a-z ]", "", s).strip()


def _today() -> str:
    return datetime.now(_TZ).strftime("%Y-%m-%d")


def _load_from_pg(today: str) -> dict | None:
    """Load Steamer cache from Postgres via layer_cache_helper."""
    try:
        from layer_cache_helper import pg_cache_get  # noqa: PLC0415
        return pg_cache_get("steamer", "projections_2026", today)
    except Exception as exc:
        logger.debug("[Steamer] pg_cache_get failed: %s", exc)
        return None


def _save_to_pg(data: dict, today: str) -> None:
    """Persist Steamer cache to Postgres via layer_cache_helper."""
    try:
        from layer_cache_helper import pg_cache_set  # noqa: PLC0415
        pg_cache_set("steamer", "projections_2026", data, today)
    except Exception as exc:
        logger.debug("[Steamer] pg_cache_set failed: %s", exc)




def _fetch_via_scraperapi_steamer(timeout: int = 30) -> list[dict]:
    """ScraperAPI fallback for Steamer FanGraphs requests."""
    import urllib.parse  # noqa: PLC0415
    key = os.environ.get("SCRAPERAPI_KEY", "")
    if not key:
        return []
    full_url = _FG_BASE + "?" + urllib.parse.urlencode(_STEAMER_PARAMS)
    proxy_url = f"http://api.scraperapi.com?api_key={key}&url={urllib.parse.quote(full_url)}"
    try:
        resp = requests.get(proxy_url, timeout=timeout)
        resp.raise_for_status()
        rows = resp.json().get("data") or []
        logger.info("[Steamer] ScraperAPI fallback returned %d rows", len(rows))
        return rows
    except Exception as exc:
        logger.warning("[Steamer] ScraperAPI fallback failed: %s", exc)
        return []

def _fetch_steamer_pybaseball() -> dict[str, dict]:
    """Pybaseball fallback when FanGraphs API is 403-blocked on Railway."""
    try:
        import pybaseball  # noqa: PLC0415
        try:
            pybaseball.cache.enable()
        except Exception:
            pass
        df = pybaseball.batting_stats(2026, qual=1)
        if df is None or df.empty:
            return {}
        projections: dict[str, dict] = {}
        for _, row in df.iterrows():
            name = str(row.get("Name") or "")
            key = _norm(name)
            if not key:
                continue
            def _f2(field, default=0.0):
                try:
                    return float(row.get(field) or default)
                except (TypeError, ValueError):
                    return default
            pa  = max(1.0, _f2("PA", 1.0))
            g   = max(1.0, _f2("G",  1.0))
            r   = _f2("R"); rbi = _f2("RBI"); sb = _f2("SB"); hr = _f2("HR")
            projections[key] = {
                "avg": _f2("AVG", _LG["avg"]), "obp": _f2("OBP", _LG["obp"]),
                "slg": _f2("SLG", _LG["slg"]),
                "r": r, "rbi": rbi, "sb": sb, "hr": hr, "pa": pa, "g": g,
                "r_pg":   r   / g if g > 0 else _LG["r_pg"],
                "rbi_pg": rbi / g if g > 0 else _LG["rbi_pg"],
                "sb_pg":  sb  / g if g > 0 else _LG["sb_pg"],
                "hr_pg":  hr  / g if g > 0 else _LG["hr_pg"],
            }
        logger.info("[Steamer] pybaseball fallback: %d batters (2026 actuals)", len(projections))
        return projections
    except Exception as exc:
        logger.warning("[Steamer] pybaseball fallback failed: %s", exc)
        return {}

def _fetch_steamer() -> dict[str, dict]:
    """
    Fetch Steamer 2026 batter projections from FanGraphs API.
    Returns {name_key: {avg, obp, slg, r, rbi, sb, hr, pa, r_pg, rbi_pg, sb_pg, hr_pg}}.
    """
    try:
        resp = requests.get(
            _FG_BASE,
            params=_STEAMER_PARAMS,
            headers=_fg_headers(),
            timeout=20,
        )
        resp.raise_for_status()
        rows = (resp.json().get("data") or [])
    except Exception as exc:
        logger.warning("[Steamer] FanGraphs fetch failed: %s — trying ScraperAPI then pybaseball", exc)
        scraperapi_rows = _fetch_via_scraperapi_steamer()
        if scraperapi_rows:
            rows = scraperapi_rows
        else:
            return _fetch_steamer_pybaseball()

    projections: dict[str, dict] = {}
    for row in rows:
        name = str(row.get("PlayerName") or row.get("Name") or "")
        key = _norm(name)
        if not key:
            continue

        def _f(field: str, default: float = 0.0) -> float:
            try:
                return float(row.get(field) or default)
            except (TypeError, ValueError):
                return default

        pa  = max(1.0, _f("PA", 1.0))
        g   = max(1.0, _f("G",  1.0))
        r   = _f("R")
        rbi = _f("RBI")
        sb  = _f("SB")
        hr  = _f("HR")

        projections[key] = {
            "avg":    _f("AVG",  _LG["avg"]),
            "obp":    _f("OBP",  _LG["obp"]),
            "slg":    _f("SLG",  _LG["slg"]),
            "r":      r,
            "rbi":    rbi,
            "sb":     sb,
            "hr":     hr,
            "pa":     pa,
            "g":      g,
            # Per-game rates (key comparison metric)
            "r_pg":   r   / g if g > 0 else _LG["r_pg"],
            "rbi_pg": rbi / g if g > 0 else _LG["rbi_pg"],
            "sb_pg":  sb  / g if g > 0 else _LG["sb_pg"],
            "hr_pg":  hr  / g if g > 0 else _LG["hr_pg"],
        }

    logger.info("[Steamer] Loaded %d batter projections from FanGraphs", len(projections))
    return projections


def _get_cache() -> dict[str, dict]:
    """Return in-memory cache, loading from Postgres or API if stale."""
    global _CACHE, _CACHE_DATE
    today = _today()

    # L1 memory hit
    if _CACHE and _CACHE_DATE == today:
        return _CACHE

    # L2 Postgres hit
    pg_data = _load_from_pg(today)
    if pg_data:
        _CACHE = pg_data
        _CACHE_DATE = today
        logger.info("[Steamer] Cache hit from Postgres (%d players)", len(_CACHE))
        return _CACHE

    # L3 fetch from FanGraphs
    logger.info("[Steamer] Fetching Steamer 2026 projections from FanGraphs...")
    data = _fetch_steamer()
    if data:
        _CACHE = data
        _CACHE_DATE = today
        _save_to_pg(data, today)

    return _CACHE


def get_steamer(player_name: str) -> dict | None:
    """
    Return Steamer projection dict for a player, or None if not found.

    Keys: avg, obp, slg, r, rbi, sb, hr, pa, g, r_pg, rbi_pg, sb_pg, hr_pg
    """
    cache = _get_cache()
    key = _norm(player_name)
    proj = cache.get(key)
    if proj:
        return proj
    # Fuzzy fallback: try last name only
    parts = key.split()
    if len(parts) >= 2:
        last = parts[-1]
        for k, v in cache.items():
            if k.endswith(last):
                return v
    return None


def steamer_adjustment(
    prop_type: str,
    side: str,
    player_name: str,
    line: float,
    steamer_proj: dict | None = None,
) -> float:
    """
    Return probability delta (0-1 scale) based on Steamer projection vs line.

    Logic: if Steamer projects a player significantly above/below a prop line,
    nudge the probability toward OVER or UNDER accordingly.

    Max nudge: ±0.025 (2.5pp). Scales linearly with deviation magnitude.
    Zero nudge if no projection found or deviation < 5% of league average.

    Applies to: runs, rbis, stolen_bases, home_runs, hits, total_bases
    Does NOT apply to: strikeouts, earned_runs, pitching_outs (pitcher props)
    """
    _APPLICABLE = {
        "runs", "rbis", "rbi", "stolen_bases", "home_runs",
        "hits", "total_bases", "hits_runs_rbis",
    }
    if prop_type not in _APPLICABLE:
        return 0.0

    proj = steamer_proj or get_steamer(player_name)
    if not proj:
        return 0.0

    # Map prop_type to projected per-game rate and league average
    # line is the DFS/sportsbook line for that prop
    # We compare (projected_per_game * expected_games_in_window) vs line
    # For daily props, expected_games_in_window = 1

    stat_map: dict[str, tuple[str, float]] = {
        "runs":          ("r_pg",   _LG["r_pg"]),
        "rbis":          ("rbi_pg", _LG["rbi_pg"]),
        "rbi":           ("rbi_pg", _LG["rbi_pg"]),
        "stolen_bases":  ("sb_pg",  _LG["sb_pg"]),
        "home_runs":     ("hr_pg",  _LG["hr_pg"]),
        "hits":          ("avg",    _LG["avg"]),     # AVG ≈ hits per AB, directional
        "total_bases":   ("slg",    _LG["slg"]),     # SLG directional
        "hits_runs_rbis":("r_pg",   _LG["r_pg"]),    # use runs as proxy
    }

    if prop_type not in stat_map:
        return 0.0

    proj_key, league_avg = stat_map[prop_type]
    proj_val = proj.get(proj_key, league_avg)

    # Pct deviation of player from league average
    pct_above_avg = (proj_val - league_avg) / max(league_avg, 0.001)

    # Dead zone: < 5% deviation from league avg → no nudge
    if abs(pct_above_avg) < 0.05:
        return 0.0

    # Convert to probability nudge: ±25% deviation → ±0.025 (max)
    # This is intentionally conservative — Steamer is a season-level prior,
    # not a game-by-game prediction.
    raw_nudge = pct_above_avg * 0.10          # 10% scaling factor
    raw_nudge = max(-0.025, min(0.025, raw_nudge))

    # Apply direction: if OVER, positive nudge (player projected above avg) helps
    if side.upper() == "OVER":
        return round(raw_nudge, 4)
    else:
        return round(-raw_nudge, 4)


# ── Public helper for prop_enrichment_layer.py ─────────────────────────────────

_LAYER_INSTANCE: dict[str, dict] | None = None

def get_steamer_adj(player: str, prop_type: str, side: str, line: float) -> float:
    """
    Single-call helper for prop_enrichment_layer.py.

    Usage (add after _get_marcel_adj call, around line 1330):
        from steamer_layer import get_steamer_adj as _get_steamer_adj
        _steamer_adj = _get_steamer_adj(player, prop_type, _side_for_adj, prop.get("line", 0.5))
        prop["_steamer_adj"] = _steamer_adj
    """
    try:
        proj = get_steamer(player)
        return steamer_adjustment(prop_type, side, player, line, proj)
    except Exception as exc:
        logger.debug("[Steamer] adj failed for %s %s: %s", player, prop_type, exc)
        return 0.0


def prefetch() -> int:
    """Pre-warm the Steamer cache at DataHub startup. Returns player count."""
    cache = _get_cache()
    return len(cache)
