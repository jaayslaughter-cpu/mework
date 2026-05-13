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
Steamer600 projections use type=steamer600 parameter (rest-of-season, in-season).
type=steamer returns 404 after Opening Day — Steamer600 is the live in-season equivalent.
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
    "type":      "steamer600",  # Steamer600 = in-season ROS projection (type=steamer is pre-season only, 404 after Opening Day)
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
_FETCH_ATTEMPTED_DATE: str = ""   # tracks date of last fetch attempt (success OR fail)


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
        import traceback as _tb
        logger.warning(
            "[Steamer] pybaseball fallback failed (%s: %s)\n%s",
            type(exc).__name__,
            exc,
            _tb.format_exc(limit=3),
        )
        return {}

def _scraperapi_get(url: str, params: dict, headers: dict, timeout: int = 30) -> "requests.Response":
    """
    GET with automatic ScraperAPI fallback on 403/429.
    If SCRAPERAPI_KEY env var is set and the direct call is blocked, retries
    via ScraperAPI residential proxy. Free tier: 1,000 calls/month.
    """
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    if resp.status_code in (403, 429, 407):
        scraper_key = os.getenv("SCRAPERAPI_KEY", "")
        if scraper_key:
            proxy = f"http://scraperapi:{scraper_key}@proxy-server.scraperapi.com:8001"
            proxies = {"http": proxy, "https": proxy}
            logger.info("[Steamer] Direct fetch %d — retrying via ScraperAPI proxy", resp.status_code)
            resp = requests.get(
                url, params=params, headers=headers,
                timeout=60, proxies=proxies, verify=False,
            )
        else:
            logger.warning(
                "[Steamer] Got %d from FanGraphs and SCRAPERAPI_KEY is not set. "
                "Add SCRAPERAPI_KEY to Railway env vars to bypass the IP block.",
                resp.status_code,
            )
    return resp


def _fetch_steamer() -> dict[str, dict]:
    """
    Fetch Steamer 2026 batter projections from FanGraphs API.
    Returns {name_key: {avg, obp, slg, r, rbi, sb, hr, pa, r_pg, rbi_pg, sb_pg, hr_pg}}.
    Tier 1: direct FanGraphs API
    Tier 2: ScraperAPI residential proxy (if SCRAPERAPI_KEY set and direct is 403/429)
    Tier 3: pybaseball fallback (2026 actuals)
    """
    try:
        resp = _scraperapi_get(
            _FG_BASE,
            params=_STEAMER_PARAMS,
            headers=_fg_headers(),
            timeout=20,
        )
        resp.raise_for_status()
        rows = (resp.json().get("data") or [])
    except Exception as exc:
        # Log the full exception detail (including HTTP status code) so we can
        # diagnose whether this is a 403 IP block, rate limit, timeout, etc.
        import traceback as _tb
        logger.warning(
            "[Steamer] FanGraphs fetch failed (%s: %s) — falling back to pybaseball\n%s",
            type(exc).__name__,
            exc,
            _tb.format_exc(limit=3),
        )
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


def _get_cache(hub: dict | None = None) -> dict[str, dict]:
    """Return in-memory cache, loading from Postgres or API if stale.

    Fetch-attempt guard: if all three fetch paths fail today, we set
    _FETCH_ATTEMPTED_DATE so subsequent calls in the same day return the
    empty cache immediately instead of hammering FanGraphs every 15 s.
    """
    global _CACHE, _CACHE_DATE, _FETCH_ATTEMPTED_DATE
    today = _today()

    # L1 memory hit (cache populated and still fresh)
    if _CACHE and _CACHE_DATE == today:
        return _CACHE

    # L2 Postgres hit (cache empty in memory but written to DB earlier today)
    # Require >= 100 players — rejects stale/partial DraftEdge-seeded rows
    pg_data = _load_from_pg(today)
    if pg_data and len(pg_data) >= 100:
        _CACHE = pg_data
        _CACHE_DATE = today
        logger.info("[Steamer] Cache hit from Postgres (%d players)", len(_CACHE))
        return _CACHE
    elif pg_data:
        logger.warning(
            "[Steamer] Postgres cache only has %d players (< 100 threshold) — "
            "discarding and falling through to live fetch / Tier 5 static CSV.",
            len(pg_data),
        )

    # Guard: if we already attempted a live fetch today and got nothing, don't
    # retry until tomorrow -- avoids hammering FanGraphs every 15 s.
    if _FETCH_ATTEMPTED_DATE == today:
        return _CACHE

    # L3 fetch from FanGraphs (once per day)
    logger.info("[Steamer] Fetching Steamer 2026 projections from FanGraphs...")
    _FETCH_ATTEMPTED_DATE = today
    data = _fetch_steamer()
    if data:
        _CACHE = data
        _CACHE_DATE = today
        _save_to_pg(data, today)
        logger.info("[Steamer] Projections cached: %d players", len(data))
    elif not data:
        # Tier 4: DraftEdge projections already fetched by DataHub — no extra API call needed
        logger.warning(
            "[Steamer] Tiers 1-3 (FanGraphs/ScraperAPI/pybaseball) all failed for %s. "
            "Trying DraftEdge projections as Tier 4 fallback...",
            today,
        )
        de_data = _fetch_steamer_draftedge(hub)
        # Require >= 50 players from DraftEdge — when hub=None at startup,
        # the live DraftEdge API only returns ~32 players and blocks the
        # 5,663-player static CSV (Tier 5). Treat thin results as a miss.
        if de_data and len(de_data) >= 50:
            _CACHE = de_data
            _CACHE_DATE = today
            logger.info(
                "[Steamer] Tier 4 DraftEdge active: %d players. "
                "Probabilities are today-specific but less precise than FanGraphs. "
                "Set SCRAPERAPI_KEY to restore full accuracy.",
                len(de_data),
            )
        else:
            if de_data:
                logger.warning(
                    "[Steamer] Tier 4 DraftEdge only returned %d players "
                    "(< 50 threshold) — falling through to Tier 5 static CSV.",
                    len(de_data),
                )
            # Tier 5: static CSV bundled in data/fg/steamer_ros_2026.csv
            logger.info("[Steamer] Tier 4 failed — trying bundled static CSV (Tier 5)...")
            static_data = _fetch_steamer_static_csv()
            if static_data:
                _CACHE = static_data
                _CACHE_DATE = today
                # Persist to Postgres so next restart hits L2 cache (instant load)
                _save_to_pg(static_data, today)
                logger.info(
                    "[Steamer] Tier 5 static CSV active: %d players. "
                    "Data from FanGraphs ATC+Steamer RoS export.",
                    len(static_data) // 2,
                )
            else:
                # Tier 5b: Baseball Savant xStats (2026 actuals)
                logger.info("[Steamer] Tier 5 failed — trying Savant xStats (Tier 5b)...")
                savant_data = _fetch_steamer_savant()
                if savant_data:
                    _CACHE = savant_data
                    _CACHE_DATE = today
                else:
                    # Tier 5c: BBRef static CSV / live scrape
                    logger.info("[Steamer] Tier 5b failed — trying BBRef stats (Tier 5c)...")
                    bbref_data = _fetch_steamer_bbref_static()
                    if bbref_data:
                        _CACHE = bbref_data
                        _CACHE_DATE = today
                    else:
                        logger.warning(
                            "[Steamer] All tiers failed (1=FG 2=ScraperAPI 3=pybaseball "
                            "4=DraftEdge 5=StaticCSV 5b=Savant 5c=BBRef). "
                            "Model using league-average priors.",
                        )

    return _CACHE


def get_steamer(player_name: str) -> dict | None:
    """
    Return Steamer projection dict for a player, or None if not found.

    Keys: avg, obp, slg, r, rbi, sb, hr, pa, g, r_pg, rbi_pg, sb_pg, hr_pg

    Source priority:
      1. mlb_stats_layer — MLB Stats API season-to-date actuals (always works on Railway).
         r_pg/rbi_pg/sb_pg/hr_pg are derived from season totals / games played.
         Same player, same season — no FanGraphs dependency.
      2. FanGraphs Steamer cache — used if mlb_stats_layer returns no data yet
         (e.g. very early season before enough PA accumulate).
    """
    # ── Primary: MLB Stats API via mlb_stats_layer ───────────────────────
    try:
        from mlb_stats_layer import get_batter as _mlb_get_batter  # noqa: PLC0415
        mlb = _mlb_get_batter(player_name)
        if mlb and mlb.get("r_pg") is not None:
            # Map mlb_stats_layer output to steamer schema
            return {
                "avg":    mlb.get("avg",   _LG["avg"]),
                "obp":    mlb.get("obp",   _LG["obp"]),
                "slg":    mlb.get("slg",   _LG["slg"]),
                "r":      mlb.get("r_total",   0),
                "rbi":    mlb.get("rbi_total",  0),
                "sb":     mlb.get("sb_total",   0),
                "hr":     mlb.get("hr_total",   0),
                "r_pg":   mlb.get("r_pg",   _LG["r_pg"]),
                "rbi_pg": mlb.get("rbi_pg", _LG["rbi_pg"]),
                "sb_pg":  mlb.get("sb_pg",  _LG["sb_pg"]),
                "hr_pg":  mlb.get("hr_pg",  _LG["hr_pg"]),
                "_source": "mlb_stats_api",
            }
    except Exception:
        pass  # fall through to FanGraphs cache

    # ── Fallback: FanGraphs Steamer cache ────────────────────────────────
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


def _fetch_steamer_draftedge(hub: dict | None = None) -> dict[str, dict]:
    """
    Tier 4 fallback: use DraftEdge projections already fetched in the DataHub.
    DraftEdge provides hit_pct, hr_pct, run_pct, rbi_pct as per-game probabilities.
    These are today-specific DFS projections — better than league-average priors.
    Fires when FanGraphs is 403-blocked, ScraperAPI quota exhausted, and pybaseball fails.
    """
    try:
        # Try to get DraftEdge data from the hub if passed, otherwise import from module
        # ── Get raw DraftEdge data ─────────────────────────────────────────────
        if hub is None:
            try:
                from draftedge_scraper import fetch_all_projections as _de_fetch  # noqa: PLC0415
                raw_de = _de_fetch()  # returns {"batters": DataFrame, "pitchers": DataFrame}
            except Exception:
                return {}
        else:
            # hub["dfs"]["prop_projections"] is list[dict] with keys:
            #   player_name, prop_type, projected_prob, source="draftedge"
            # This is already processed by _fetch_draftedge_projections() in tasklets.py
            raw_de = hub.get("dfs", {}).get("prop_projections")

        if not raw_de:
            return {}

        projections: dict[str, dict] = {}

        # ── Handle list[dict] from hub (the normal runtime path) ──────────────
        if isinstance(raw_de, list):
            # Group by player_name — accumulate per-prop-type projected_prob
            player_props: dict[str, dict] = {}
            for row in raw_de:
                if not isinstance(row, dict):
                    continue
                name = str(row.get("player_name") or row.get("player") or "")
                key = _norm(name)
                if not key:
                    continue
                prop_type = str(row.get("prop_type", ""))
                prob = float(row.get("projected_prob") or 0.0)
                if key not in player_props:
                    player_props[key] = {}
                player_props[key][prop_type] = prob

            for key, pmap in player_props.items():
                hit_pct = pmap.get("hits",          0.0)
                hr_pct  = pmap.get("home_runs",     0.0)
                run_pct = pmap.get("runs",          0.0)
                rbi_pct = pmap.get("rbis",          0.0)
                sb_pct  = pmap.get("stolen_bases",  0.0)
                if hit_pct == 0.0 and hr_pct == 0.0:
                    continue
                projections[key] = {
                    "avg":    round(hit_pct, 4),
                    "obp":    round(min(hit_pct * 1.15, 0.500), 4),
                    "slg":    round(hit_pct + hr_pct * 3.0, 4),
                    "r":      round(run_pct * 162, 2),
                    "rbi":    round(rbi_pct * 162, 2),
                    "hr":     round(hr_pct  * 162, 2),
                    "sb":     round(sb_pct  * 162, 2),
                    "pa":     4.2,
                    "g":      1.0,
                    "r_pg":   round(run_pct, 4),
                    "rbi_pg": round(rbi_pct, 4),
                    "hr_pg":  round(hr_pct,  4),
                    "sb_pg":  round(sb_pct,  4),
                    "_source": "draftedge_hub",
                }

        # ── Handle {"batters": DataFrame, "pitchers": DataFrame} from draftedge_scraper ──
        elif isinstance(raw_de, dict):
            import pandas as _pd  # noqa: PLC0415
            batters = raw_de.get("batters")
            if batters is not None and hasattr(batters, "iterrows") and not batters.empty:
                for _, row in batters.iterrows():
                    name = str(row.get("player_name", "") or "").strip()
                    key = _norm(name)
                    if not key:
                        continue
                    hit_pct = float(row.get("hit_pct") or 0.0)
                    hr_pct  = float(row.get("hr_pct")  or 0.0)
                    run_pct = float(row.get("run_pct") or row.get("runs_pct") or 0.0)
                    rbi_pct = float(row.get("rbi_pct") or 0.0)
                    sb_pct  = float(row.get("sb_pct")  or 0.0)
                    if hit_pct == 0.0 and hr_pct == 0.0:
                        continue
                    projections[key] = {
                        "avg":    round(hit_pct, 4),
                        "obp":    round(min(hit_pct * 1.15, 0.500), 4),
                        "slg":    round(hit_pct + hr_pct * 3.0, 4),
                        "r":      round(run_pct * 162, 2),
                        "rbi":    round(rbi_pct * 162, 2),
                        "hr":     round(hr_pct  * 162, 2),
                        "sb":     round(sb_pct  * 162, 2),
                        "pa":     4.2,
                        "g":      1.0,
                        "r_pg":   round(run_pct, 4),
                        "rbi_pg": round(rbi_pct, 4),
                        "hr_pg":  round(hr_pct,  4),
                        "sb_pg":  round(sb_pct,  4),
                        "_source": "draftedge_scraper",
                    }

        logger.info(
            "[Steamer] Tier 4 DraftEdge fallback: %d batters with projections", len(projections)
        )
        return projections

    except Exception as exc:
        logger.warning("[Steamer] Tier 4 DraftEdge fallback failed: %s", exc)
        return {}


def _fetch_steamer_static_csv() -> dict[str, dict]:
    """
    Tier 5 static fallback: load steamer_ros_2026.csv from data/fg/.
    Built from the FanGraphs percentile projections export (ATC RoS primary,
    Steamer RoS secondary, Steamer full-season tertiary).
    Never fails — returns {} only if file is missing.
    """
    import os as _os  # noqa: PLC0415
    csv_path = _os.path.join(
        _os.path.dirname(_os.path.abspath(__file__)),
        "data", "fg", "steamer_ros_2026.csv"
    )
    if not _os.path.exists(csv_path):
        logger.debug("[Steamer] Static CSV not found at %s", csv_path)
        return {}
    try:
        import csv as _csv  # noqa: PLC0415
        projections: dict[str, dict] = {}
        with open(csv_path, encoding="utf-8-sig") as f:
            for row in _csv.DictReader(f):
                key = (row.get("name_key") or _norm(row.get("name", ""))).strip()
                mlbam = row.get("mlbam_id", "").strip()
                if not key:
                    continue
                proj = {
                    "avg":    float(row.get("avg")    or _LG["avg"]),
                    "obp":    float(row.get("obp")    or _LG["obp"]),
                    "slg":    float(row.get("slg")    or _LG["slg"]),
                    "r":      float(row.get("r")      or 0),
                    "rbi":    float(row.get("rbi")    or 0),
                    "sb":     float(row.get("sb")     or 0),
                    "hr":     float(row.get("hr")     or 0),
                    "pa":     float(row.get("pa")     or 4.2),
                    "g":      float(row.get("g")      or 1.0),
                    "r_pg":   float(row.get("r_pg")   or _LG["r_pg"]),
                    "rbi_pg": float(row.get("rbi_pg") or _LG["rbi_pg"]),
                    "sb_pg":  float(row.get("sb_pg")  or _LG["sb_pg"]),
                    "hr_pg":  float(row.get("hr_pg")  or _LG["hr_pg"]),
                    "_source": "static_csv",
                }
                projections[key] = proj
                if mlbam:
                    projections[f"mlbam:{mlbam}"] = proj
        logger.info("[Steamer] Static CSV loaded: %d players", len(projections) // 2)
        return projections
    except Exception as exc:
        logger.warning("[Steamer] Static CSV load failed: %s", exc)
        return {}


def _fetch_steamer_savant() -> dict[str, dict]:
    """
    Tier 5b: Baseball Savant expected statistics CSV (2026 actuals).
    Endpoint: baseballsavant.mlb.com/expected_statistics?type=batter&year=2026&min=1&csv=true
    Provides: ba, slg, woba per batter + MLBAM player_id for exact key lookup.
    OBP is estimated from wOBA (woba*0.90 + 0.030) — close enough for a fallback tier.
    Per-game counting rates (r_pg/rbi_pg/hr_pg/sb_pg) default to league average because
    the expected-stats endpoint doesn't carry R/RBI/HR/SB season totals.
    Savant blocks no DC IPs — no proxy needed, but ScraperAPI is tried on 403.
    """
    import csv as _csv, io as _io  # noqa: PLC0415

    SAVANT_URL = (
        "https://baseballsavant.mlb.com/expected_statistics"
        "?type=batter&year=2026&position=&team=&min=1&csv=true"
    )
    _SAVANT_HDRS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv,text/plain,*/*",
        "Referer": "https://baseballsavant.mlb.com/",
    }
    try:
        resp = requests.get(SAVANT_URL, headers=_SAVANT_HDRS, timeout=30)
        if resp.status_code in (403, 429):
            scraper_key = os.getenv("SCRAPERAPI_KEY", "")
            if scraper_key:
                proxy = f"http://scraperapi:{scraper_key}@proxy-server.scraperapi.com:8001"
                resp = requests.get(
                    SAVANT_URL, headers=_SAVANT_HDRS, timeout=60,
                    proxies={"https": proxy}, verify=False,
                )
        resp.raise_for_status()

        # Strip BOM; column 0 is the combined "last_name, first_name" field
        text = resp.text.lstrip("\ufeff")
        reader = _csv.DictReader(_io.StringIO(text))

        projections: dict[str, dict] = {}
        for row in reader:
            name_raw = row.get("last_name, first_name", "").strip()
            mlbam    = row.get("player_id", "").strip()
            if not name_raw:
                continue

            # "Wood, James" → "James Wood"
            if "," in name_raw:
                last, first = [x.strip() for x in name_raw.split(",", 1)]
                full_name = f"{first} {last}".strip()
            else:
                full_name = name_raw

            key = _norm(full_name)
            if not key:
                continue

            def _sv(col: str, default: float = 0.0) -> float:
                try:
                    return float(row.get(col) or default)
                except (TypeError, ValueError):
                    return default

            ba   = _sv("ba",   _LG["avg"])
            slg  = _sv("slg",  _LG["slg"])
            woba = _sv("woba", 0.315)
            # wOBA × 0.90 + 0.030 ≈ OBP (avg: 0.315*0.90+0.030=0.314 vs actual 0.318)
            obp  = round(min(woba * 0.90 + 0.030, 0.550), 3)

            proj = {
                "avg":    round(ba,  4),
                "obp":    obp,
                "slg":    round(slg, 4),
                "r":      0,  "rbi": 0,  "sb": 0,  "hr": 0,
                "pa":     float(row.get("pa") or 4.2),
                "g":      1.0,
                # Counting rates unavailable from this endpoint — use league averages
                "r_pg":   _LG["r_pg"],
                "rbi_pg": _LG["rbi_pg"],
                "sb_pg":  _LG["sb_pg"],
                "hr_pg":  _LG["hr_pg"],
                "_source": "savant_xstats",
            }
            projections[key] = proj
            # MLBAM lookup alias for player_id_resolver hits
            if mlbam:
                projections[f"mlbam:{mlbam}"] = proj

        logger.info("[Steamer] Tier 5b Savant xStats: %d batters (2026 actuals)", len(projections) // 2 or len(projections))
        return projections

    except Exception as exc:
        import traceback as _tb  # noqa: PLC0415
        logger.warning(
            "[Steamer] Tier 5b Savant failed (%s: %s)\n%s",
            type(exc).__name__, exc, _tb.format_exc(limit=3),
        )
        return {}


def _fetch_steamer_bbref_static() -> dict[str, dict]:
    """
    Tier 5c: Baseball Reference 2026 batting stats from repo CSV.
    Primary: data/bbref/bbref_batting_2026_v2.csv  (300+ players, actual 2026 season).
    Fallback: live HTML scrape via ScraperAPI if CSV missing from deploy.

    CSV columns: Season,Name,Tm,G,PA,AB,H,1B,2B,3B,HR,R,RBI,BB,IBB,SO,HBP,SF,SH,GDP,SB,CS,AVG
    OBP computed exactly: (H+BB+HBP) / (AB+BB+HBP+SF)
    SLG computed exactly: (1B + 2*2B + 3*3B + 4*HR) / AB
    Per-game rates: actual R/G, RBI/G, HR/G, SB/G from season totals.

    Advantage over Savant 5b: full per-game counting rates (r_pg, rbi_pg, hr_pg, sb_pg).
    Advantage over Savant 5b: OBP/SLG are exact, not estimated.
    Limitation: only ~300 players (qualified starters); bench players fall to league avg.
    """
    import os as _os, csv as _csv  # noqa: PLC0415

    csv_path = _os.path.join(
        _os.path.dirname(_os.path.abspath(__file__)),
        "data", "bbref", "bbref_batting_2026_v2.csv",
    )

    def _parse_bbref_csv(path: str) -> dict[str, dict]:
        projections: dict[str, dict] = {}
        try:
            with open(path, encoding="utf-8-sig") as f:
                for row in _csv.DictReader(f):
                    name = (row.get("Name") or "").strip()
                    key  = _norm(name)
                    # Skip header rows (BBRef repeats headers mid-table) and blanks
                    if not key or key == "name":
                        continue

                    def _i(col: str, default: int = 0) -> int:
                        try:
                            return int(row.get(col) or default)
                        except (TypeError, ValueError):
                            return default

                    def _f(col: str, default: float = 0.0) -> float:
                        try:
                            return float(row.get(col) or default)
                        except (TypeError, ValueError):
                            return default

                    g  = max(1, _i("G",   1))
                    ab = max(1, _i("AB",  1))
                    h  = _i("H"); bb = _i("BB"); hbp = _i("HBP"); sf = _i("SF")
                    b1 = _i("1B"); b2 = _i("2B"); b3 = _i("3B"); hr = _i("HR")
                    r  = _i("R");  rbi = _i("RBI"); sb = _i("SB")

                    obp = (h + bb + hbp) / max(1, ab + bb + hbp + sf)
                    tb  = b1 + 2 * b2 + 3 * b3 + 4 * hr
                    slg = tb / ab

                    projections[key] = {
                        "avg":    round(_f("AVG", _LG["avg"]), 4),
                        "obp":    round(obp, 4),
                        "slg":    round(slg, 4),
                        "r":      float(r),
                        "rbi":    float(rbi),
                        "sb":     float(sb),
                        "hr":     float(hr),
                        "pa":     float(_i("PA") or 4.2),
                        "g":      float(g),
                        "r_pg":   round(r   / g, 4),
                        "rbi_pg": round(rbi / g, 4),
                        "hr_pg":  round(hr  / g, 4),
                        "sb_pg":  round(sb  / g, 4),
                        "_source": "bbref_csv",
                    }
        except Exception as exc:
            logger.warning("[Steamer] Tier 5c BBRef CSV parse error: %s", exc)
        return projections

    # ── Primary: static CSV bundled in repo ───────────────────────────────
    if _os.path.exists(csv_path):
        data = _parse_bbref_csv(csv_path)
        if data:
            logger.info("[Steamer] Tier 5c BBRef static CSV: %d batters (2026 actuals)", len(data))
            return data

    # ── Fallback: live ScraperAPI scrape of BBRef 2026 standard batting ──
    scraper_key = os.getenv("SCRAPERAPI_KEY", "")
    if not scraper_key:
        logger.warning("[Steamer] Tier 5c BBRef: static CSV missing and SCRAPERAPI_KEY not set — skipping live scrape")
        return {}

    BBREF_URL = "https://www.baseball-reference.com/leagues/majors/2026-standard-batting.shtml"
    scrape_url = f"https://api.scraperapi.com/?api_key={scraper_key}&url={BBREF_URL}&render=false"
    try:
        import pandas as _pd  # noqa: PLC0415
        tables = _pd.read_html(scrape_url, attrs={"id": "players_standard_batting"}, flavor="lxml")
        if not tables:
            raise ValueError("No table found")
        df = tables[0]
        df.columns = [str(c) for c in df.columns]
        # Drop duplicate header rows (BBRef repeats "Name" rows)
        df = df[df["Name"] != "Name"].dropna(subset=["Name"])
        projections: dict[str, dict] = {}
        for _, row in df.iterrows():
            key = _norm(str(row.get("Name", "")))
            if not key:
                continue
            def _fi(col: str, default: float = 0.0) -> float:
                try:
                    return float(row.get(col) or default)
                except (TypeError, ValueError):
                    return default
            g  = max(1.0, _fi("G", 1))
            ab = max(1.0, _fi("AB", 1))
            h  = _fi("H"); bb = _fi("BB"); hbp = _fi("HBP", 0.0); sf = _fi("SF", 0.0)
            b1 = _fi("1B", 0.0); b2 = _fi("2B"); b3 = _fi("3B"); hr = _fi("HR")
            r  = _fi("R"); rbi = _fi("RBI"); sb = _fi("SB", 0.0)
            obp = (h + bb + hbp) / max(1, ab + bb + hbp + sf)
            tb  = b1 + 2 * b2 + 3 * b3 + 4 * hr
            slg = tb / ab
            projections[key] = {
                "avg":    round(_fi("BA", _LG["avg"]), 4),
                "obp":    round(obp, 4),
                "slg":    round(slg, 4),
                "r":      r, "rbi": rbi, "sb": sb, "hr": hr,
                "pa":     _fi("PA", 4.2), "g": g,
                "r_pg":   round(r   / g, 4),
                "rbi_pg": round(rbi / g, 4),
                "hr_pg":  round(hr  / g, 4),
                "sb_pg":  round(sb  / g, 4),
                "_source": "bbref_live",
            }
        logger.info("[Steamer] Tier 5c BBRef live scrape: %d batters", len(projections))
        return projections
    except Exception as exc:
        import traceback as _tb  # noqa: PLC0415
        logger.warning("[Steamer] Tier 5c BBRef live scrape failed: %s\n%s", exc, _tb.format_exc(limit=3))
        return {}


def prefetch(hub: dict | None = None) -> int:
    """Pre-warm the Steamer cache at DataHub startup. Returns player count."""
    cache = _get_cache(hub=hub)
    return len(cache)
