"""
fangraphs_layer.py
------------------
Phase 40 - FanGraphs season statistics via direct HTTP API.

Replaces pybaseball with direct calls to FanGraphs internal JSON API.
No library dependencies. No CSV export issues. Consistent daily updates.

API: https://www.fangraphs.com/api/leaders/major-league/data
Cached to /tmp/propiq_fg_cache_{year}.json daily.

Provides per-agent signal enhancement for all 18 agents:

  Pitchers
  --------
  csw_pct    : Called Strikes + Whiffs % (C+SwStr% in FanGraphs — best single K predictor)
  swstr_pct  : Swinging Strike %
  k_bb_pct   : K% minus BB% (true command metric)
  xfip       : Expected FIP - strips HR variance (true skill ERA)
  siera      : Skill-Interactive ERA (sequence-adjusted skill metric)
  fip        : Fielding Independent Pitching
  hr_fb_pct  : Home run per fly ball rate
  lob_pct    : Left-on-base strand rate (regression flag)
  babip      : Pitcher BABIP (luck normaliser)

  Batters
  -------
  wrc_plus   : Park/league-adjusted hitting value
  woba       : Weighted on-base average
  iso        : Isolated power (SLG - AVG)
  babip      : Batter BABIP (luck/regression flag)
  o_swing    : O-Swing% (chase rate)
  z_contact  : Z-Contact% (contact rate in zone)
  hr_fb_pct  : HR per fly ball rate
  k_pct      : Strikeout rate
  bb_pct     : Walk rate
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Inline helpers
# ---------------------------------------------------------------------------

def _normalise_name(s: str) -> str:
    """Lowercase, strip non-alpha characters, collapse whitespace."""
    return re.sub(r"[^a-z ]", "", s.lower()).strip()


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert val to float; return default on failure."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_FG_API_BASE = "https://www.fangraphs.com/api/leaders/major-league/data"
# Rotating User-Agent pool — FanGraphs 403-blocks repeated identical UAs on Railway
_FG_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]


def _fg_headers() -> dict:
    """Return a randomised header set to avoid FanGraphs 403 blocks on Railway."""
    import random  # noqa: PLC0415
    return {
        "User-Agent":      random.choice(_FG_UA_POOL),
        "Referer":         "https://www.fangraphs.com/leaders/major-league",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin":          "https://www.fangraphs.com",
        "Connection":      "keep-alive",
        "Sec-Fetch-Site":  "same-origin",
        "Sec-Fetch-Mode":  "cors",
    }


# Backward-compat alias for any call sites that reference _FG_HEADERS directly
_FG_HEADERS = _fg_headers()

_BATTING_PARAMS = {
    "age": "0",
    "pos": "all",
    "stats": "bat",
    "lg": "all",
    "qual": "20",
    "startdate": "",
    "enddate": "",
    "month": "0",
    "hand": "",
    "team": "0",
    "pageitems": "500",
    "pagenum": "1",
    "ind": "0",
    "rost": "0",
    "players": "0",
    "type": "8",
    "postseason": "",
    "sortdir": "default",
    "sortstat": "WAR",
}

_PITCHING_PARAMS = {
    **_BATTING_PARAMS,
    "stats": "pit",
    "qual": "10",
}

# Daily cache path template
_CACHE_PATH_TMPL = "/tmp/propiq_fg_cache_{year}.json"


# ---------------------------------------------------------------------------
# Postgres cache helpers (survives Railway container restarts)
# ---------------------------------------------------------------------------

def _pg_load_cache(season: int) -> tuple[dict, dict]:
    """Load FanGraphs cache from Postgres.  Returns (batters, pitchers) or ({}, {})."""
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        return {}, {}
    try:
        import psycopg2  # type: ignore
        conn = psycopg2.connect(db_url, connect_timeout=5)
        cur = conn.cursor()
        cur.execute(
            "SELECT data_type, data FROM fg_cache WHERE season = %s",
            (season,),
        )
        rows = cur.fetchall()
        conn.close()
        batters: dict = {}
        pitchers: dict = {}
        for data_type, blob in rows:
            if data_type == "batters":
                batters = blob if isinstance(blob, dict) else json.loads(blob)
            elif data_type == "pitchers":
                pitchers = blob if isinstance(blob, dict) else json.loads(blob)
        if batters or pitchers:
            logger.info(
                "[FG] Postgres cache hit — %d batters  %d pitchers (season=%d)",
                len(batters), len(pitchers), season,
            )
        return batters, pitchers
    except Exception as exc:
        logger.warning("[FG] Postgres cache load failed: %s", exc)
        return {}, {}


def _pg_save_cache(season: int, batters: dict, pitchers: dict) -> None:
    """Upsert FanGraphs cache into Postgres fg_cache table."""
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        return
    try:
        import psycopg2          # type: ignore
        import psycopg2.extras   # type: ignore
        conn = psycopg2.connect(db_url, connect_timeout=5)
        cur = conn.cursor()
        for data_type, payload in (("batters", batters), ("pitchers", pitchers)):
            cur.execute(
                """
                INSERT INTO fg_cache (season, data_type, data, cached_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (season, data_type) DO UPDATE
                    SET data = EXCLUDED.data,
                        cached_at = EXCLUDED.cached_at
                """,
                (season, data_type, psycopg2.extras.Json(payload)),
            )
        conn.commit()
        conn.close()
        logger.info("[FG] Postgres cache updated (season=%d)", season)
    except Exception as exc:
        logger.warning("[FG] Postgres cache save failed: %s", exc)


# ─── Module-level caches ──────────────────────────────────────────────────────
_BATTER_CACHE: dict[str, dict[str, float]] = {}
_PITCHER_CACHE: dict[str, dict[str, float]] = {}
_loaded: bool = False
_data_year: int = 0

# ─── League-average baselines (2025 season) ──────────────────────────────────
# FIX: Updated to 2025 MLB actuals (FanGraphs leaderboards)
# Used as fallback when FanGraphs API is unavailable or player not found
LEAGUE_DEFAULTS: dict[str, dict[str, float]] = {
    "pitcher": {
        "csw_pct":   0.275,   # FG 2025: ~27.5% (was 0.280)
        "swstr_pct": 0.110,   # FG 2025: ~11.0% (unchanged)
        "k_bb_pct":  0.130,   # FG 2025: ~13.0% (unchanged)
        "xfip":      4.06,    # FG 2025 (was 4.15)  
        "siera":     4.06,    # FG 2025 (was 4.15)  
        "fip":       4.06,    # FG 2025 (was 4.15)  
        "hr_fb_pct": 0.119,   # FG 2025: 11.9% (confirmed VSiN Feb 2026): ~11.8% (was 0.120)
        "lob_pct":   0.720,   # unchanged
        "babip":     0.288,   # FG 2025: .289 (confirmed VSiN Feb 2026): ~0.298 (was 0.300)
    },
    "batter": {
        "wrc_plus":    100.0,  # by definition
        "woba":        0.308,  # FG 2025 (was 0.312)
        "iso":         0.156,  # FG 2025: elevated power (was 0.158)
        "babip":       0.288,  # FG 2025: .289 (confirmed VSiN Feb 2026)
        "o_swing":     0.316,  # FG 2025 (was 0.318)
        "z_contact":   0.848,  # FG 2025: ~84.8% (was 0.850)
        "hr_fb_pct":   0.119,  # FG 2025: 11.9% (confirmed VSiN Feb 2026)
        "k_pct":       0.223,  # FG 2025: 22.2% (confirmed VSiN Feb 2026)
        "bb_pct":      0.087,  # FG 2025: 8.4% (confirmed VSiN Feb 2026)
        "slg":         0.410,  # FG 2025 (was 0.411)
        "xbh_per_game": 0.50,  # extra base hits per game — #1 feature for TB (45% importance)
    },
}


# ---------------------------------------------------------------------------
# Internal fetch helpers
# ---------------------------------------------------------------------------


def _fetch_via_scraperapi(url: str, params: dict, timeout: int = 30) -> list[dict]:
    """Route FanGraphs request through ScraperAPI when direct access is 403-blocked on Railway."""
    import urllib.parse  # noqa: PLC0415
    key = os.environ.get("SCRAPERAPI_KEY2", "")
    if not key:
        return []
    full_url = url + "?" + urllib.parse.urlencode(params)
    proxy_url = f"http://api.scraperapi.com?api_key={key}&url={urllib.parse.quote(full_url)}"
    try:
        resp = requests.get(proxy_url, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("data") or []
        logger.info("[FG] ScraperAPI fallback returned %d rows", len(rows))
        return rows
    except Exception as exc:
        logger.warning("[FG] ScraperAPI fallback failed: %s", exc)
        return []

def _fetch_season(stats: str, season: int) -> list[dict]:
    """Fetch one season of batter or pitcher data from FanGraphs API.
    Returns list of player dicts, or empty list on failure.
    """
    params = dict(_BATTING_PARAMS if stats == "bat" else _PITCHING_PARAMS)
    params["season"] = str(season)
    params["season1"] = str(season)
    try:
        resp = requests.get(
            _FG_API_BASE,
            params=params,
            headers=_fg_headers(),
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data") or []
    except Exception as exc:
        logger.warning("[FG] API fetch failed (stats=%s, season=%d): %s — trying ScraperAPI", stats, season, exc)
        scraperapi_rows = _fetch_via_scraperapi(_FG_API_BASE, params)
        if scraperapi_rows:
            return scraperapi_rows
        return []


def _parse_batters(rows: list[dict]) -> dict[str, dict[str, float]]:
    """Parse batter rows into normalised-name -> stats dict."""
    bd = LEAGUE_DEFAULTS["batter"]
    cache: dict[str, dict[str, float]] = {}
    for row in rows:
        name = str(row.get("PlayerName") or row.get("Name") or "")
        key = _normalise_name(name)
        if not key:
            continue
        # Compute xbh_per_game = (2B + 3B + HR) / G — #1 feature for TB props (45% importance)
        _hr  = _safe_float(row.get("HR"),  0.0)
        _2b  = _safe_float(row.get("2B"),  0.0)
        _3b  = _safe_float(row.get("3B"),  0.0)
        _g   = max(1.0, _safe_float(row.get("G"), 1.0))
        _xbh_pg = (_hr + _2b + _3b) / _g

        cache[key] = {
            "wrc_plus":     _safe_float(row.get("wRC+"),       bd["wrc_plus"]),
            "woba":         _safe_float(row.get("wOBA"),       bd["woba"]),
            "iso":          _safe_float(row.get("ISO"),        bd["iso"]),
            "babip":        _safe_float(row.get("BABIP"),      bd["babip"]),
            "o_swing":      _safe_float(row.get("O-Swing%"),   bd["o_swing"]),
            "z_contact":    _safe_float(row.get("Z-Contact%"), bd["z_contact"]),
            "hr_fb_pct":    _safe_float(row.get("HR/FB"),      bd["hr_fb_pct"]),
            "k_pct":        _safe_float(row.get("K%"),         bd["k_pct"]),
            "bb_pct":       _safe_float(row.get("BB%"),        bd["bb_pct"]),
            "slg":          _safe_float(row.get("SLG"),        bd["slg"]),
            "xbh_per_game": _xbh_pg if _xbh_pg > 0 else bd["xbh_per_game"],
        }
    return cache


def _parse_pitchers(rows: list[dict]) -> dict[str, dict[str, float]]:
    """Parse pitcher rows into normalised-name -> stats dict."""
    pd_ = LEAGUE_DEFAULTS["pitcher"]
    cache: dict[str, dict[str, float]] = {}
    for row in rows:
        name = str(row.get("PlayerName") or row.get("Name") or "")
        key = _normalise_name(name)
        if not key:
            continue
        cache[key] = {
            # C+SwStr% is FanGraphs' column name for CSW%
            "csw_pct":   _safe_float(row.get("C+SwStr%"),  pd_["csw_pct"]),
            "swstr_pct": _safe_float(row.get("SwStr%"),    pd_["swstr_pct"]),
            "k_bb_pct":  _safe_float(row.get("K-BB%"),     pd_["k_bb_pct"]),
            "xfip":      _safe_float(row.get("xFIP"),      pd_["xfip"]),
            "siera":     _safe_float(row.get("SIERA"),     pd_["siera"]),
            "fip":       _safe_float(row.get("FIP"),       pd_["fip"]),
            "hr_fb_pct": _safe_float(row.get("HR/FB"),     pd_["hr_fb_pct"]),
            "lob_pct":   _safe_float(row.get("LOB%"),      pd_["lob_pct"]),
            "babip":     _safe_float(row.get("BABIP"),     pd_["babip"]),
        }
    return cache


def _load() -> None:
    """Fetch or load from daily cache. Sets _loaded = True on completion."""
    global _BATTER_CACHE, _PITCHER_CACHE, _loaded, _data_year

    season = date.today().year
    cache_path = _CACHE_PATH_TMPL.format(year=season)

    # ── Try disk cache first ─────────────────────────────────────────────────
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as fh:
                data = json.load(fh)
            _BATTER_CACHE  = data.get("batters", {})
            _PITCHER_CACHE = data.get("pitchers", {})
            _data_year     = data.get("season", season)
            logger.info(
                "[FG] Disk cache hit — %d batters  %d pitchers (season=%d)",
                len(_BATTER_CACHE), len(_PITCHER_CACHE), _data_year,
            )
            _loaded = True
            return
        except Exception as exc:
            logger.warning("[FG] Disk cache read failed (%s) — checking Postgres", exc)

    # ── Disk miss: try Postgres cache (survives Railway restarts) ─────────────
    pg_batters, pg_pitchers = _pg_load_cache(season)
    if pg_batters or pg_pitchers:
        _BATTER_CACHE  = pg_batters
        _PITCHER_CACHE = pg_pitchers
        _data_year     = season
        _loaded        = True
        try:
            with open(cache_path, "w") as fh:
                json.dump(
                    {"batters": _BATTER_CACHE, "pitchers": _PITCHER_CACHE, "season": season},
                    fh,
                )
        except Exception:
            pass
        return

    # ── Live fetch — prefer current year data, blend with prior if sample small ─
    # If 2026 has <100 players (early season), merge with 2025 for stability.
    for yr in (season, season - 1):
        logger.info("[FG] Fetching season %d from FanGraphs API...", yr)

        bat_rows = _fetch_season("bat", yr)
        pit_rows = _fetch_season("pit", yr)

        if not bat_rows and not pit_rows:
            logger.warning("[FG] No data returned for season %d — trying %d", yr, yr - 1)
            time.sleep(0.5)
            continue

        _BATTER_CACHE  = _parse_batters(bat_rows)
        _PITCHER_CACHE = _parse_pitchers(pit_rows)
        _data_year = yr

        logger.info(
            "[FG] Loaded %d batters  %d pitchers from season %d",
            len(_BATTER_CACHE), len(_PITCHER_CACHE), yr,
        )

        # ── Stability-weighted season blend ──────────────────────────────────────
        # Early season: ERA→ mostly 2025; K%→ mostly 2026 (stabilizes fast).
        # Always blends — even in August 2026 has more data than 2025 for rate stats.
        if yr == season:
            try:
                from season_blender import get_blender as _get_blender  # noqa: PLC0415
                _blender = _get_blender()
                prior_bat = _parse_batters(_fetch_season("bat", yr - 1))
                prior_pit = _parse_pitchers(_fetch_season("pit", yr - 1))

                # Stat-by-stat weighted blend for every player in 2026
                merged_bat: dict[str, dict] = {}
                for name, stats_2026 in _BATTER_CACHE.items():
                    stats_2025 = prior_bat.get(name, {})
                    merged_bat[name] = _blender.blend_batter(stats_2026, stats_2025) if stats_2025 else stats_2026
                # For players only in 2025 (not yet played in 2026), include them
                for name, stats_2025 in prior_bat.items():
                    if name not in merged_bat:
                        merged_bat[name] = stats_2025

                merged_pit: dict[str, dict] = {}
                for name, stats_2026 in _PITCHER_CACHE.items():
                    stats_2025 = prior_pit.get(name, {})
                    merged_pit[name] = _blender.blend_pitcher(stats_2026, stats_2025) if stats_2025 else stats_2026
                for name, stats_2025 in prior_pit.items():
                    if name not in merged_pit:
                        merged_pit[name] = stats_2025

                _BATTER_CACHE  = merged_bat
                _PITCHER_CACHE = merged_pit
                _blender.log_weights()
                logger.info(
                    "[FG] Stability-blended: %d batters, %d pitchers (%d+%d players)",
                    len(_BATTER_CACHE), len(_PITCHER_CACHE),
                    len(_BATTER_CACHE), len(prior_bat),
                )
            except Exception as _blend_err:
                # Fallback: binary merge (old behavior) if blender fails
                logger.warning("[FG] Blend failed (%s) — using binary merge", _blend_err)
                prior_bat = _parse_batters(_fetch_season("bat", yr - 1))
                prior_pit = _parse_pitchers(_fetch_season("pit", yr - 1))
                _BATTER_CACHE  = {**prior_bat, **_BATTER_CACHE}
                _PITCHER_CACHE = {**prior_pit, **_PITCHER_CACHE}

        # ── Persist cache: disk (fast) + Postgres (durable across restarts) ──
        try:
            with open(cache_path, "w") as fh:
                json.dump(
                    {
                        "batters":  _BATTER_CACHE,
                        "pitchers": _PITCHER_CACHE,
                        "season":   yr,
                    },
                    fh,
                )
            logger.info("[FG] /tmp cached: %s", cache_path)
        except Exception as exc:
            logger.warning("[FG] /tmp cache write failed: %s", exc)
        _pg_save_cache(yr, _BATTER_CACHE, _PITCHER_CACHE)

        break
    else:
        # ── FanGraphs API blocked (403) — fall back to pybaseball ────────────
        # FanGraphs blocks direct HTTP scraping from server IPs. pybaseball
        # caches results to /tmp after first fetch, so this only hits the
        # network once per deployment and is fast on subsequent startups.
        logger.warning("[FG] FanGraphs direct API blocked — trying pybaseball fallback")
        _pyb_loaded = False
        try:
            import pybaseball as _pyb  # noqa: PLC0415
            _pyb.cache.enable()

            for _yr in (season, season - 1):
                try:
                    _bat_df = _pyb.batting_stats(_yr, qual=20)
                    _pit_df = _pyb.pitching_stats(_yr, qual=10)
                except Exception as _pyb_err:
                    logger.warning("[FG] pybaseball %d failed: %s — trying %d", _yr, _pyb_err, _yr - 1)
                    continue

                if _bat_df is None or _pit_df is None:
                    continue
                if _bat_df.empty and _pit_df.empty:
                    continue

                # Map pybaseball column names to our schema
                # pybaseball returns the same FanGraphs columns, just via a different route
                _bat_rows = _bat_df.to_dict(orient="records") if not _bat_df.empty else []
                _pit_rows = _pit_df.to_dict(orient="records") if not _pit_df.empty else []

                # pybaseball uses 'Name' column (same as FanGraphs API)
                _BATTER_CACHE  = _parse_batters(_bat_rows)
                _PITCHER_CACHE = _parse_pitchers(_pit_rows)
                _data_year = _yr

                logger.info(
                    "[FG] pybaseball fallback OK — %d batters  %d pitchers (season=%d)",
                    len(_BATTER_CACHE), len(_PITCHER_CACHE), _yr,
                )

                # Blend prior season if we got current season data
                if _yr == season and not _bat_df.empty:
                    try:
                        _prior_bat_df = _pyb.batting_stats(_yr - 1, qual=20)
                        _prior_pit_df = _pyb.pitching_stats(_yr - 1, qual=10)
                        prior_bat = _parse_batters(_prior_bat_df.to_dict(orient="records")) if _prior_bat_df is not None else {}
                        prior_pit = _parse_pitchers(_prior_pit_df.to_dict(orient="records")) if _prior_pit_df is not None else {}
                        _BATTER_CACHE  = {**prior_bat, **_BATTER_CACHE}
                        _PITCHER_CACHE = {**prior_pit, **_PITCHER_CACHE}
                    except Exception:
                        pass  # blend is best-effort

                # Cache to disk + Postgres so future restarts skip pybaseball entirely
                try:
                    with open(cache_path, "w") as _cfh:
                        json.dump(
                            {"batters": _BATTER_CACHE, "pitchers": _PITCHER_CACHE, "season": _yr},
                            _cfh,
                        )
                except Exception:
                    pass
                _pg_save_cache(_yr, _BATTER_CACHE, _PITCHER_CACHE)
                _pyb_loaded = True
                break

        except ImportError:
            logger.warning("[FG] pybaseball not installed — pip install pybaseball to enable fallback")
        except Exception as _pyb_outer:
            logger.warning("[FG] pybaseball fallback failed: %s", _pyb_outer)

        if not _pyb_loaded:
            logger.error(
                "[FG] All data sources failed (FanGraphs 403, pybaseball unavailable). "
                "All agents will use league-average features (k_rate=0.22, era=4.06). "
                "Add pybaseball to requirements.txt to enable fallback."
            )

    _loaded = True


# ---------------------------------------------------------------------------
# Park Factors (FanGraphs 2025, 5-year weighted)
# Source: fangraphs.com/guts.aspx?type=pf — scraped 2025-03-31
# Scale: 100 = league average.  Already halved for use with full-season stats.
# Keys: hr, so, basic, 1b, 2b
# ---------------------------------------------------------------------------

_PARK_FACTORS_2025: dict[str, dict[str, int]] = {
    "angels":        {"hr": 105, "so": 102, "basic": 101, "1b": 100, "2b": 97},
    "orioles":       {"hr": 99,  "so": 99,  "basic": 99,  "1b": 103, "2b": 96},
    "red sox":       {"hr": 98,  "so": 98,  "basic": 104, "1b": 104, "2b": 109},
    "white sox":     {"hr": 105, "so": 99,  "basic": 100, "1b": 100, "2b": 96},
    "guardians":     {"hr": 98,  "so": 101, "basic": 99,  "1b": 100, "2b": 100},
    "tigers":        {"hr": 96,  "so": 98,  "basic": 100, "1b": 100, "2b": 100},
    "royals":        {"hr": 95,  "so": 97,  "basic": 103, "1b": 103, "2b": 108},
    "twins":         {"hr": 99,  "so": 100, "basic": 101, "1b": 101, "2b": 105},
    "yankees":       {"hr": 104, "so": 100, "basic": 99,  "1b": 97,  "2b": 95},
    "athletics":     {"hr": 103, "so": 101, "basic": 103, "1b": 102, "2b": 107},
    "mariners":      {"hr": 96,  "so": 104, "basic": 94,  "1b": 95,  "2b": 93},
    "rays":          {"hr": 104, "so": 100, "basic": 101, "1b": 103, "2b": 96},
    "rangers":       {"hr": 102, "so": 101, "basic": 99,  "1b": 98,  "2b": 100},
    "blue jays":     {"hr": 103, "so": 100, "basic": 99,  "1b": 98,  "2b": 102},
    "diamondbacks":  {"hr": 91,  "so": 99,  "basic": 101, "1b": 103, "2b": 105},
    "braves":        {"hr": 99,  "so": 102, "basic": 100, "1b": 101, "2b": 99},
    "cubs":          {"hr": 98,  "so": 101, "basic": 98,  "1b": 100, "2b": 94},
    "reds":          {"hr": 114, "so": 101, "basic": 105, "1b": 101, "2b": 101},
    "rockies":       {"hr": 107, "so": 96,  "basic": 113, "1b": 108, "2b": 111},
    "marlins":       {"hr": 97,  "so": 100, "basic": 101, "1b": 102, "2b": 101},
    "astros":        {"hr": 102, "so": 102, "basic": 99,  "1b": 99,  "2b": 100},
    "dodgers":       {"hr": 110, "so": 100, "basic": 99,  "1b": 96,  "2b": 98},
    "brewers":       {"hr": 104, "so": 104, "basic": 99,  "1b": 96,  "2b": 97},
    "nationals":     {"hr": 100, "so": 98,  "basic": 100, "1b": 100, "2b": 99},
    "mets":          {"hr": 99,  "so": 101, "basic": 96,  "1b": 97,  "2b": 94},
    "phillies":      {"hr": 105, "so": 101, "basic": 101, "1b": 99,  "2b": 97},
    "pirates":       {"hr": 93,  "so": 97,  "basic": 102, "1b": 103, "2b": 105},
    "cardinals":     {"hr": 94,  "so": 97,  "basic": 98,  "1b": 101, "2b": 99},
    "padres":        {"hr": 101, "so": 102, "basic": 96,  "1b": 97,  "2b": 95},
    "giants":        {"hr": 91,  "so": 98,  "basic": 97,  "1b": 102, "2b": 102},
    # hr=overall, hr_vs_l=vs LHB, hr_vs_r=vs RHB, so=strikeouts, basic=runs, 1b, 2b
    "angels":        {"hr": 105, "hr_vs_l": 103, "hr_vs_r": 107, "so": 102, "basic": 101, "1b": 100, "2b": 97},
    "orioles":       {"hr": 99,  "hr_vs_l": 101, "hr_vs_r":  97, "so":  99, "basic":  99, "1b": 103, "2b": 96},
    "red sox":       {"hr": 98,  "hr_vs_l":  96, "hr_vs_r": 100, "so":  98, "basic": 104, "1b": 104, "2b": 109},
    "white sox":     {"hr": 105, "hr_vs_l": 106, "hr_vs_r": 104, "so":  99, "basic": 100, "1b": 100, "2b": 96},
    "guardians":     {"hr": 98,  "hr_vs_l":  96, "hr_vs_r": 101, "so": 101, "basic":  99, "1b": 100, "2b": 100},
    "tigers":        {"hr": 96,  "hr_vs_l":  94, "hr_vs_r":  97, "so":  98, "basic": 100, "1b": 100, "2b": 100},
    "royals":        {"hr": 95,  "hr_vs_l":  93, "hr_vs_r":  97, "so":  97, "basic": 103, "1b": 103, "2b": 108},
    "twins":         {"hr": 99,  "hr_vs_l":  98, "hr_vs_r": 100, "so": 100, "basic": 101, "1b": 101, "2b": 105},
    # Yankee Stadium: iconic short RF porch → big LHB advantage
    "yankees":       {"hr": 104, "hr_vs_l": 118, "hr_vs_r":  92, "so": 100, "basic":  99, "1b":  97, "2b": 95},
    "athletics":     {"hr": 103, "hr_vs_l": 104, "hr_vs_r": 102, "so": 101, "basic": 103, "1b": 102, "2b": 107},
    # T-Mobile: marine air, deep gaps → LHB disadvantage
    "mariners":      {"hr": 96,  "hr_vs_l":  93, "hr_vs_r":  98, "so": 104, "basic":  94, "1b":  95, "2b": 93},
    "rays":          {"hr": 104, "hr_vs_l": 105, "hr_vs_r": 103, "so": 100, "basic": 101, "1b": 103, "2b": 96},
    "rangers":       {"hr": 102, "hr_vs_l": 103, "hr_vs_r": 101, "so": 101, "basic":  99, "1b":  98, "2b": 100},
    "blue jays":     {"hr": 103, "hr_vs_l": 105, "hr_vs_r": 101, "so": 100, "basic":  99, "1b":  98, "2b": 102},
    # Chase Field: AZ heat but pitcher-friendly dimensions — both hands suppressed
    "diamondbacks":  {"hr": 91,  "hr_vs_l":  90, "hr_vs_r":  92, "so":  99, "basic": 101, "1b": 103, "2b": 105},
    "braves":        {"hr": 99,  "hr_vs_l": 100, "hr_vs_r":  98, "so": 102, "basic": 100, "1b": 101, "2b": 99},
    "cubs":          {"hr": 98,  "hr_vs_l":  99, "hr_vs_r":  97, "so": 101, "basic":  98, "1b": 100, "2b": 94},
    # GABP: known extreme HR park, both hands benefit
    "reds":          {"hr": 114, "hr_vs_l": 117, "hr_vs_r": 111, "so": 101, "basic": 105, "1b": 101, "2b": 101},
    # Coors: altitude lifts all; thin-air effect slightly larger for pull hitters
    "rockies":       {"hr": 107, "hr_vs_l": 106, "hr_vs_r": 108, "so":  96, "basic": 113, "1b": 108, "2b": 111},
    "marlins":       {"hr": 97,  "hr_vs_l":  96, "hr_vs_r":  98, "so": 100, "basic": 101, "1b": 102, "2b": 101},
    # Minute Maid: Crawford Boxes in LF → RHB pull to LF is easier
    "astros":        {"hr": 102, "hr_vs_l":  98, "hr_vs_r": 106, "so": 102, "basic":  99, "1b":  99, "2b": 100},
    # Dodger Stadium: power alleys, LHB slight edge
    "dodgers":       {"hr": 110, "hr_vs_l": 113, "hr_vs_r": 107, "so": 100, "basic":  99, "1b":  96, "2b": 98},
    "brewers":       {"hr": 104, "hr_vs_l": 106, "hr_vs_r": 102, "so": 104, "basic":  99, "1b":  96, "2b": 97},
    "nationals":     {"hr": 100, "hr_vs_l": 101, "hr_vs_r":  99, "so":  98, "basic": 100, "1b": 100, "2b": 99},
    # Citi Field: pitcher-friendly, deep CF/LC; RHB pulls to short RF
    "mets":          {"hr": 99,  "hr_vs_l":  96, "hr_vs_r": 102, "so": 101, "basic":  96, "1b":  97, "2b": 94},
    "phillies":      {"hr": 105, "hr_vs_l": 107, "hr_vs_r": 103, "so": 101, "basic": 101, "1b":  99, "2b": 97},
    # PNC Park: deep RF → LHBs disadvantaged
    "pirates":       {"hr": 93,  "hr_vs_l":  90, "hr_vs_r":  96, "so":  97, "basic": 102, "1b": 103, "2b": 105},
    "cardinals":     {"hr": 94,  "hr_vs_l":  92, "hr_vs_r":  95, "so":  97, "basic":  98, "1b": 101, "2b": 99},
    # Petco: pitcher-friendly, LHB especially hurt by deep LCF/CF
    "padres":        {"hr": 101, "hr_vs_l":  98, "hr_vs_r": 103, "so": 102, "basic":  96, "1b":  97, "2b": 95},
    # Oracle Park: McCovey Cove, strong marine wind in from CF/LCF — LHB hit into wind
    "giants":        {"hr": 91,  "hr_vs_l":  82, "hr_vs_r":  98, "so":  98, "basic":  97, "1b": 102, "2b": 102},
}

# Full name → canonical key (covers full names, city+name combos, abbreviations)
_TEAM_PF_ALIASES: dict[str, str] = {
    # Angels
    "angels": "angels", "los angeles angels": "angels", "la angels": "angels",
    "anaheim angels": "angels", "california angels": "angels",
    # Orioles
    "orioles": "orioles", "baltimore orioles": "orioles",
    # Red Sox
    "red sox": "red sox", "boston red sox": "red sox",
    # White Sox
    "white sox": "white sox", "chicago white sox": "white sox",
    # Guardians
    "guardians": "guardians", "cleveland guardians": "guardians",
    # Tigers
    "tigers": "tigers", "detroit tigers": "tigers",
    # Royals
    "royals": "royals", "kansas city royals": "royals",
    # Twins
    "twins": "twins", "minnesota twins": "twins",
    # Yankees
    "yankees": "yankees", "new york yankees": "yankees",
    # Athletics
    "athletics": "athletics", "oakland athletics": "athletics",
    "a's": "athletics", "as": "athletics", "las vegas athletics": "athletics",
    # Mariners
    "mariners": "mariners", "seattle mariners": "mariners",
    # Rays
    "rays": "rays", "tampa bay rays": "rays",
    # Rangers
    "rangers": "rangers", "texas rangers": "rangers",
    # Blue Jays
    "blue jays": "blue jays", "toronto blue jays": "blue jays", "bluejays": "blue jays",
    # Diamondbacks
    "diamondbacks": "diamondbacks", "arizona diamondbacks": "diamondbacks",
    "d-backs": "diamondbacks", "dbacks": "diamondbacks",
    # Braves
    "braves": "braves", "atlanta braves": "braves",
    # Cubs
    "cubs": "cubs", "chicago cubs": "cubs",
    # Reds
    "reds": "reds", "cincinnati reds": "reds",
    # Rockies
    "rockies": "rockies", "colorado rockies": "rockies",
    # Marlins
    "marlins": "marlins", "miami marlins": "marlins",
    # Astros
    "astros": "astros", "houston astros": "astros",
    # Dodgers
    "dodgers": "dodgers", "los angeles dodgers": "dodgers", "la dodgers": "dodgers",
    # Brewers
    "brewers": "brewers", "milwaukee brewers": "brewers",
    # Nationals
    "nationals": "nationals", "washington nationals": "nationals",
    # Mets
    "mets": "mets", "new york mets": "mets",
    # Phillies
    "phillies": "phillies", "philadelphia phillies": "phillies",
    # Pirates
    "pirates": "pirates", "pittsburgh pirates": "pirates",
    # Cardinals
    "cardinals": "cardinals", "st. louis cardinals": "cardinals",
    "st louis cardinals": "cardinals", "saint louis cardinals": "cardinals",
    # Padres
    "padres": "padres", "san diego padres": "padres",
    # Giants
    "giants": "giants", "san francisco giants": "giants", "sf giants": "giants",
}

_PF_CAP = 0.025   # hard cap: park nudge never exceeds ±2.5 percentage points


def _resolve_team(team: str) -> str:
    """Normalise a team name to its park factors key.  Returns '' if not found."""
    key = re.sub(r"[^a-z ']", "", team.lower()).strip()
    return _TEAM_PF_ALIASES.get(key, "")


def get_park_factors(team: str) -> dict[str, int]:
    """Return the park factor dict for a team.  Empty dict if unknown."""
    resolved = _resolve_team(team)
    return _PARK_FACTORS_2025.get(resolved, {})


def park_factor_adjustment(
    prop_type: str,
    direction: str,   # "Over" or "Under"
    home_team: str,
    batter_hand: str = "", # "L", "R", or "" (unknown → use overall hr)
) -> float:
    """
    Probability nudge from park factors for the given prop type and direction.

    Returns float in [-0.025, +0.025].
    0.0 returned when home_team is unknown or prop type is unaffected.

    Prop-type routing:
      home_runs          → HR factor  (weight 0.20)
      total_bases        → blended HR/2B/1B factors (weight 0.15)
      strikeouts (SP)    → SO factor  (weight 0.10)
      hits/singles/2B    → 1B/2B/basic blend (weight 0.12)
      earned_runs/runs   → basic factor (weight 0.10)
      rbis               → basic factor (weight 0.10)
    """
    pf = get_park_factors(home_team)
    if not pf:
        return 0.0

    is_over = direction.lower() == "over"
    flip    = 1.0 if is_over else -1.0

    # Convert factor (100-scale) to fractional deviation from neutral
    hr_dev    = (pf.get("hr",    100) - 100) / 100.0
    # Use platoon split when batter handedness is known
    _hand = (batter_hand or "").upper().strip()
    if _hand == "L" and "hr_vs_l" in pf:
        hr_dev = (pf["hr_vs_l"] - 100) / 100.0
    elif _hand == "R" and "hr_vs_r" in pf:
        hr_dev = (pf["hr_vs_r"] - 100) / 100.0
    else:
        hr_dev = (pf.get("hr", 100) - 100) / 100.0
    so_dev    = (pf.get("so",    100) - 100) / 100.0
    basic_dev = (pf.get("basic", 100) - 100) / 100.0
    b1_dev    = (pf.get("1b",    100) - 100) / 100.0
    b2_dev    = (pf.get("2b",    100) - 100) / 100.0

    adj = 0.0
    pt  = prop_type.lower()

    if pt in ("home_runs",):
        adj = flip * hr_dev * 0.20

    elif pt in ("total_bases",):
        tb_dev = hr_dev * 0.40 + b2_dev * 0.35 + b1_dev * 0.25
        adj = flip * tb_dev * 0.15

    elif pt in ("strikeouts", "pitcher_strikeouts"):
        adj = flip * so_dev * 0.10

    elif pt in ("hits", "singles", "doubles"):
        hit_dev = b1_dev * 0.50 + b2_dev * 0.30 + basic_dev * 0.20
        adj = flip * hit_dev * 0.12

    elif pt in ("earned_runs", "earned_runs_allowed"):
        # Hitter-friendly park → more earned runs → positive for pitcher ER Over
        adj = flip * basic_dev * 0.10

    elif pt in ("rbis", "rbi", "runs"):
        adj = flip * basic_dev * 0.10

    return max(-_PF_CAP, min(_PF_CAP, adj))


# ─── Public getters ───────────────────────────────────────────────────────────

def get_batter(name: str) -> dict[str, float]:
    """Return FanGraphs batting stats for name. Empty dict if not found."""
    try:
        from mlb_stats_layer import get_batter as _mlb_get_batter  # noqa: PLC0415
        result = _mlb_get_batter(name)
        if result:
            return result
    except Exception:
        pass
    global _loaded
    if not _loaded:
        _load()
    return _BATTER_CACHE.get(_normalise_name(name), {})


def get_pitcher(name: str) -> dict[str, float]:
    """
    Return pitcher stats for name. Empty dict if not found (agents use LEAGUE_DEFAULTS).

    Delegation chain:
      1. mlb_stats_layer (statsapi.mlb.com — works on Railway, self-updating)
      2. Local FanGraphs cache (populated only if FanGraphs API was reachable)
      3. Empty dict → agent falls back to LEAGUE_DEFAULTS
    """
    # ── Primary: mlb_stats_layer (Railway-safe, auto-refreshed daily) ─────
    try:
        from mlb_stats_layer import get_pitcher as _mlb_get_pitcher  # noqa: PLC0415
        result = _mlb_get_pitcher(name)
        if result:
            return result
    except Exception:
        pass  # fall through to FanGraphs cache

    # ── Fallback: FanGraphs cache (populated when API accessible) ─────────
    global _loaded
    if not _loaded:
        _load()
    return _PITCHER_CACHE.get(_normalise_name(name), {})


# ─── Probability adjustment engine ───────────────────────────────────────────
_FG_CAP = 0.030

_PROP_GROUPS: dict[str, list[str]] = {
    "k_props":     ["strikeouts", "pitcher_strikeouts"],
    "er_props":    ["earned_runs", "earned_runs_allowed"],
    "hits_allow":  ["hits_allowed", "pitcher_hits", "walks_allowed"],
    "hit_props":   ["hits", "singles", "doubles"],
    "power_props": ["home_runs", "total_bases"],
    "rbi_run":     ["rbis", "runs", "rbi"],
    "batter_k":    ["batter_strikeouts"],
    "sb_props":    ["stolen_bases"],
}


def _in_group(prop_type: str, group: str) -> bool:
    return prop_type in _PROP_GROUPS.get(group, [])


def fangraphs_adjustment(
    prop_type: str,
    direction: str,       # "Over" or "Under"
    player_type: str,     # "pitcher" or "batter"
    fg: dict[str, float],
) -> float:
    """
    Compute a probability nudge from FanGraphs season stats.

    Returns float in [-0.030, +0.030]. Returns 0.0 if fg is empty.
    """
    if not fg:
        return 0.0

    adj = 0.0
    ld_p = LEAGUE_DEFAULTS["pitcher"]
    ld_b = LEAGUE_DEFAULTS["batter"]

    is_over = direction.lower() == "over"
    flip    = -1.0 if not is_over else 1.0

    if player_type == "pitcher":
        if _in_group(prop_type, "k_props"):
            csw   = fg.get("csw_pct",   ld_p["csw_pct"])
            swstr = fg.get("swstr_pct", ld_p["swstr_pct"])
            k_bb  = fg.get("k_bb_pct",  ld_p["k_bb_pct"])
            csw_adj   = (csw   - 0.280) / 0.040 * 0.014
            swstr_adj = (swstr - 0.108) / 0.030 * 0.008
            k_bb_adj  = (k_bb  - 0.130) / 0.050 * 0.006
            adj += flip * (csw_adj + swstr_adj + k_bb_adj)

        elif _in_group(prop_type, "er_props"):
            xfip  = fg.get("xfip",  ld_p["xfip"])
            siera = fg.get("siera", ld_p["siera"])
            xfip_adj  = (4.06 - xfip)  / 0.70 * 0.015
            siera_adj = (4.06 - siera) / 0.70 * 0.008
            adj += flip * (xfip_adj + siera_adj)

        elif _in_group(prop_type, "hits_allow"):
            swstr = fg.get("swstr_pct", ld_p["swstr_pct"])
            babip = fg.get("babip",     ld_p["babip"])
            swstr_adj = (swstr - 0.108) / 0.030 * 0.012
            babip_adj = (0.288 - babip) / 0.030 * 0.008
            adj += flip * swstr_adj
            adj -= babip_adj

    else:  # batter
        if _in_group(prop_type, "hit_props"):
            wrc  = fg.get("wrc_plus", ld_b["wrc_plus"])
            woba = fg.get("woba",     ld_b["woba"])
            wrc_adj  = (wrc  - 100.0) / 30.0 * 0.015
            woba_adj = (woba - 0.308) / 0.060 * 0.010  # FG 2025: center 0.308
            adj += flip * (wrc_adj + woba_adj)

        elif _in_group(prop_type, "power_props"):
            iso   = fg.get("iso",      ld_b["iso"])
            hr_fb = fg.get("hr_fb_pct", ld_b["hr_fb_pct"])
            wrc   = fg.get("wrc_plus", ld_b["wrc_plus"])
            iso_adj   = (iso   - 0.156) / 0.070 * 0.014  # FG 2025: center 0.156
            hr_fb_adj = (hr_fb - 0.119) / 0.050 * 0.010  # FG 2025: center 0.119
            wrc_adj   = (wrc   - 100.0) / 30.0  * 0.006
            adj += flip * (iso_adj + hr_fb_adj + wrc_adj)

        elif _in_group(prop_type, "rbi_run"):
            wrc  = fg.get("wrc_plus", ld_b["wrc_plus"])
            woba = fg.get("woba",     ld_b["woba"])
            adj += flip * (
                (wrc - 100.0) / 30.0 * 0.012
                + (woba - 0.308) / 0.060 * 0.010  # FG 2025: center 0.308
            )

        elif _in_group(prop_type, "batter_k"):
            o_swing = fg.get("o_swing", ld_b["o_swing"])
            k_pct   = fg.get("k_pct",   ld_b["k_pct"])
            o_adj = (o_swing - 0.316) / 0.100 * 0.015  # FG 2025: center 0.316
            k_adj = (k_pct   - 0.223) / 0.050 * 0.012  # FG 2025: center 0.223
            adj += flip * (o_adj + k_adj)

        elif _in_group(prop_type, "sb_props"):
            bb_pct = fg.get("bb_pct", ld_b["bb_pct"])
            adj += flip * (bb_pct - 0.087) / 0.030 * 0.010  # FG 2025: center 0.087

    return max(-_FG_CAP, min(_FG_CAP, adj))
