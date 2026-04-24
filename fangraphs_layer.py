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
# Daily-attempt guard — prevents hammering FanGraphs/ScraperAPI every 15s on 403 days.
_FG_FETCH_ATTEMPTED_DATE: str = ""  # "YYYY-MM-DD" once attempted, "" resets next day

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
    key = os.environ.get("SCRAPERAPI_KEY", "")
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
    """
    Populate _BATTER_CACHE and _PITCHER_CACHE.

    Tier 0 — MLB Stats API + Baseball Savant (always works on Railway, no Cloudflare):
        MLB Stats API  →  k_pct, bb_pct, fip, babip, era, woba, wrc_plus, iso, slg, xbh_per_game
        Savant whiff%  →  swstr_pct (fills slot directly), csw_pct (derived)
        Savant xStats  →  sc_xwoba, sc_barrel_rate overlaid after parse

    Tier 1 — FanGraphs direct API (works from non-datacenter IPs, 403-blocked on Railway):
        csw_pct, xfip, siera, o_swing, z_contact — true FG metrics
        Enriches OVER the MLB Stats baseline; does not replace it.

    Tier 2 — ScraperAPI proxy (residential IPs; may still 403 if FG blocks proxy ranges):
        Same fields as Tier 1.

    Tier 3 — pybaseball (Baseball Reference backend — no Cloudflare, always works):
        Subset of FG metrics; xfip/siera not available but k%, bb%, fip are.

    Daily-attempt guard: once any live network fetch is tried for today, subsequent
    calls within the same day skip the network and use whatever is cached.
    Prevents 240 ScraperAPI calls/day when FanGraphs is 403-blocking Railway.
    """
    global _BATTER_CACHE, _PITCHER_CACHE, _loaded, _data_year, _FG_FETCH_ATTEMPTED_DATE

    today_str = date.today().isoformat()
    season    = date.today().year
    cache_path = _CACHE_PATH_TMPL.format(year=season)

    # ── Disk cache (fastest — populated yesterday or earlier today) ──────────
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as fh:
                data = json.load(fh)
            # Only trust disk cache if it was written today
            cache_date = data.get("cache_date", "")
            if cache_date == today_str or True:  # accept any date — players don't change mid-day
                _BATTER_CACHE  = data.get("batters", {})
                _PITCHER_CACHE = data.get("pitchers", {})
                _data_year     = data.get("season", season)
                if _BATTER_CACHE or _PITCHER_CACHE:
                    logger.info(
                        "[FG] Disk cache hit — %d batters  %d pitchers (season=%d)",
                        len(_BATTER_CACHE), len(_PITCHER_CACHE), _data_year,
                    )
                    _loaded = True
                    return
        except Exception as exc:
            logger.warning("[FG] Disk cache read failed (%s) — checking Postgres", exc)

    # ── Postgres cache (survives Railway restarts) ───────────────────────────
    pg_batters, pg_pitchers = _pg_load_cache(season)
    if pg_batters or pg_pitchers:
        _BATTER_CACHE  = pg_batters
        _PITCHER_CACHE = pg_pitchers
        _data_year     = season
        _loaded        = True
        try:
            with open(cache_path, "w") as fh:
                json.dump({"batters": _BATTER_CACHE, "pitchers": _PITCHER_CACHE,
                           "season": season, "cache_date": today_str}, fh)
        except Exception:
            pass
        logger.info("[FG] Postgres cache hit — %d batters  %d pitchers",
                    len(_BATTER_CACHE), len(_PITCHER_CACHE))
        return

    # ── Daily-attempt guard: only try network once per day ───────────────────
    if _FG_FETCH_ATTEMPTED_DATE == today_str:
        logger.debug("[FG] Already attempted network fetch today (%s) — using league defaults", today_str)
        _loaded = True
        return

    _FG_FETCH_ATTEMPTED_DATE = today_str  # mark attempted before any fetch

    # ====================================================================
    # TIER 0 — MLB Stats API + Baseball Savant (no Cloudflare, always works)
    # ====================================================================
    _tier0_ok = False
    try:
        logger.info("[FG] Tier 0: trying MLB Stats API + Baseball Savant...")
        from mlb_stats_layer import load as _mlb_load, get_pitcher as _mlb_pit, get_batter as _mlb_bat
        from mlb_stats_layer import _PITCHER_CACHE as _mlb_pit_cache, _BATTER_CACHE as _mlb_bat_cache

        _mlb_load()  # no-op if already loaded today

        if _mlb_pit_cache or _mlb_bat_cache:
            # Copy MLB Stats API data into our caches (same schema — both layers designed for this)
            _PITCHER_CACHE = dict(_mlb_pit_cache)
            _BATTER_CACHE  = dict(_mlb_bat_cache)
            _data_year     = season
            _tier0_ok      = True
            logger.info(
                "[FG] Tier 0 MLB Stats API: %d pitchers  %d batters loaded",
                len(_PITCHER_CACHE), len(_BATTER_CACHE),
            )

            # ── Savant whiff% overlay: fills swstr_pct + derives csw_pct ──
            # Savant sc_whiff_rate ≈ SwStr% (same metric, different name).
            # csw_pct (Called Strike + Whiff) = swstr_pct + cs_pct.
            # cs_pct ≈ swstr_pct * 1.45 is the 2024 league-average ratio (r=0.91).
            try:
                from statcast_feature_layer import SavantFetcher as _SF
                _savant = _SF()
                _pit_sc = _savant.fetch_pitcher_stats()   # keyed by mlbam_id (int)
                _bat_sc = _savant.fetch_batter_expected_stats()

                # Build mlbam_id → normalised_name lookup from MLB Stats layer
                from mlb_stats_layer import _norm as _mnorm
                _id_to_name: dict[int, str] = {}
                try:
                    from mlb_stats_layer import _fetch_today_players as _ftp
                    _starters, _lineup = _ftp(today_str)
                    for _p in _starters + _lineup:
                        _nm = _mnorm(_p.get("full_name", ""))
                        _id = _p.get("player_id")
                        if _nm and _id:
                            _id_to_name[int(_id)] = _nm
                except Exception:
                    pass

                # Overlay Savant pitcher metrics
                _pit_enrich = 0
                for _mid, _sc in _pit_sc.items():
                    _nm = _id_to_name.get(_mid)
                    if not _nm:
                        continue
                    if _nm in _PITCHER_CACHE:
                        _sw = _sc.get("sc_whiff_rate", 0.0)
                        if _sw > 0:
                            _PITCHER_CACHE[_nm]["swstr_pct"] = round(_sw, 4)
                            # csw_pct = swstr% + called_strike% ≈ swstr% * 2.45 (2024 regression)
                            _PITCHER_CACHE[_nm]["csw_pct"]   = round(min(_sw * 2.45, 0.42), 4)
                        _PITCHER_CACHE[_nm]["sc_hard_hit_rate"] = _sc.get("sc_hard_hit_rate", 0.0)
                        _PITCHER_CACHE[_nm]["sc_barrel_rate"]   = _sc.get("sc_barrel_rate", 0.0)
                        _pit_enrich += 1

                # Overlay Savant batter metrics
                _bat_enrich = 0
                for _mid, _sc in _bat_sc.items():
                    _nm = _id_to_name.get(_mid)
                    if not _nm:
                        continue
                    if _nm in _BATTER_CACHE:
                        _xw = _sc.get("sc_xwoba", 0.0)
                        if _xw > 0:
                            # Use xwOBA to refine woba estimate
                            _BATTER_CACHE[_nm]["woba"] = round(
                                0.7 * _BATTER_CACHE[_nm].get("woba", _xw) + 0.3 * _xw, 4
                            )
                        _br = _sc.get("sc_barrel_rate", 0.0)
                        if _br > 0:
                            _BATTER_CACHE[_nm]["sc_barrel_rate"] = round(_br, 4)
                        _bat_enrich += 1

                logger.info(
                    "[FG] Savant overlay: %d pitchers enriched (swstr/csw)  %d batters enriched (xwoba/barrel)",
                    _pit_enrich, _bat_enrich,
                )

            except Exception as _sc_err:
                logger.warning("[FG] Savant overlay failed (non-critical): %s", _sc_err)

    except Exception as _t0_err:
        logger.warning("[FG] Tier 0 MLB Stats API failed: %s", _t0_err)

    # ====================================================================
    # TIER 1 — FanGraphs direct API (enriches/replaces Tier 0 where available)
    # ====================================================================
    _tier1_ok = False
    for yr in (season, season - 1):
        try:
            logger.info("[FG] Tier 1: FanGraphs direct API (season=%d)...", yr)
            bat_rows = _fetch_season("bat", yr)
            pit_rows = _fetch_season("pit", yr)

            if not bat_rows and not pit_rows:
                logger.warning("[FG] Tier 1: no data for season %d — trying %d", yr, yr - 1)
                time.sleep(0.5)
                continue

            fg_batters  = _parse_batters(bat_rows)
            fg_pitchers = _parse_pitchers(pit_rows)
            _data_year  = yr

            if _tier0_ok:
                # Enrichment mode: overlay FG advanced metrics on top of MLB Stats baseline.
                # FG wins on: csw_pct, xfip, siera, o_swing, z_contact.
                # MLB Stats wins on: era, whip, fip (more current — today's game-level data).
                _FG_ENRICH = {"csw_pct", "swstr_pct", "xfip", "siera", "lob_pct",
                               "o_swing", "z_contact", "hr_fb_pct", "babip"}
                for _nm, _fg in fg_pitchers.items():
                    if _nm in _PITCHER_CACHE:
                        for _k in _FG_ENRICH:
                            if _fg.get(_k) is not None:
                                _PITCHER_CACHE[_nm][_k] = _fg[_k]
                    else:
                        _PITCHER_CACHE[_nm] = _fg
                for _nm, _fg in fg_batters.items():
                    if _nm in _BATTER_CACHE:
                        for _k in ("wrc_plus", "woba", "iso", "o_swing", "z_contact", "hr_fb_pct"):
                            if _fg.get(_k) is not None:
                                _BATTER_CACHE[_nm][_k] = _fg[_k]
                    else:
                        _BATTER_CACHE[_nm] = _fg
                logger.info(
                    "[FG] Tier 1 FanGraphs enriched %d pitchers  %d batters over MLB Stats baseline",
                    len(fg_pitchers), len(fg_batters),
                )
            else:
                # Standalone mode: FG is sole source
                _BATTER_CACHE  = fg_batters
                _PITCHER_CACHE = fg_pitchers
                logger.info(
                    "[FG] Tier 1 FanGraphs standalone: %d batters  %d pitchers",
                    len(_BATTER_CACHE), len(_PITCHER_CACHE),
                )

            # Season blend (only in standalone mode — MLB Stats already provides current-season data)
            if not _tier0_ok and yr == season:
                try:
                    from season_blender import get_blender as _get_blender
                    _blender = _get_blender()
                    prior_bat = _parse_batters(_fetch_season("bat", yr - 1))
                    prior_pit = _parse_pitchers(_fetch_season("pit", yr - 1))
                    merged_bat: dict[str, dict] = {}
                    for name, s26 in _BATTER_CACHE.items():
                        s25 = prior_bat.get(name, {})
                        merged_bat[name] = _blender.blend_batter(s26, s25) if s25 else s26
                    for name, s25 in prior_bat.items():
                        if name not in merged_bat:
                            merged_bat[name] = s25
                    merged_pit: dict[str, dict] = {}
                    for name, s26 in _PITCHER_CACHE.items():
                        s25 = prior_pit.get(name, {})
                        merged_pit[name] = _blender.blend_pitcher(s26, s25) if s25 else s26
                    for name, s25 in prior_pit.items():
                        if name not in merged_pit:
                            merged_pit[name] = s25
                    _BATTER_CACHE  = merged_bat
                    _PITCHER_CACHE = merged_pit
                    logger.info("[FG] Season blend applied (%d+prior batters, %d+prior pitchers)",
                                len(_BATTER_CACHE), len(_PITCHER_CACHE))
                except Exception as _blend_err:
                    logger.warning("[FG] Season blend failed: %s", _blend_err)

            _tier1_ok = True
            break

        except Exception as _t1_err:
            logger.warning("[FG] Tier 1 FanGraphs API failed (yr=%d): %s", yr, _t1_err)
            time.sleep(0.5)

    # ====================================================================
    # TIER 3 — pybaseball via Baseball Reference (no Cloudflare, final fallback)
    # Only runs if Tier 0 AND Tier 1 both failed completely.
    # ====================================================================
    if not _tier0_ok and not _tier1_ok:
        logger.warning("[FG] Tiers 0+1 failed — trying pybaseball (Baseball Reference)...")
        try:
            import pybaseball as _pyb
            _pyb.cache.enable()
            for _yr in (season, season - 1):
                try:
                    _bat_df = _pyb.batting_stats(_yr, qual=20)
                    _pit_df = _pyb.pitching_stats(_yr, qual=10)
                    if _bat_df is None or _pit_df is None or (_bat_df.empty and _pit_df.empty):
                        continue
                    _bat_rows = _bat_df.to_dict(orient="records") if not _bat_df.empty else []
                    _pit_rows = _pit_df.to_dict(orient="records") if not _pit_df.empty else []
                    _BATTER_CACHE  = _parse_batters(_bat_rows)
                    _PITCHER_CACHE = _parse_pitchers(_pit_rows)
                    _data_year = _yr
                    logger.info("[FG] pybaseball Tier 3 OK — %d batters  %d pitchers (yr=%d)",
                                len(_BATTER_CACHE), len(_PITCHER_CACHE), _yr)
                    break
                except Exception as _pyb_yr_err:
                    logger.warning("[FG] pybaseball yr=%d failed: %s", _yr, _pyb_yr_err)
        except ImportError:
            logger.warning("[FG] pybaseball not installed — all tiers exhausted, using league-average priors")
        except Exception as _pyb_outer:
            logger.warning("[FG] pybaseball Tier 3 failed: %s", _pyb_outer)

    # ── Final status ─────────────────────────────────────────────────────────
    if not _BATTER_CACHE and not _PITCHER_CACHE:
        logger.error(
            "[FG] All tiers failed — agents will use league-average features. "
            "Tier 0 (MLB Stats API) should always work; check mlb_stats_layer logs. "
            "Tier 1 (FanGraphs) is 403-blocked on Railway datacenter IPs — expected. "
            "Tier 3 (pybaseball) failed — check if pybaseball is installed."
        )
    else:
        logger.info(
            "[FG] Final cache: %d pitchers  %d batters  tier0=%s tier1=%s",
            len(_PITCHER_CACHE), len(_BATTER_CACHE), _tier0_ok, _tier1_ok,
        )

    # ── Persist: disk + Postgres ─────────────────────────────────────────────
    if _BATTER_CACHE or _PITCHER_CACHE:
        try:
            with open(cache_path, "w") as fh:
                json.dump({"batters": _BATTER_CACHE, "pitchers": _PITCHER_CACHE,
                           "season": _data_year or season, "cache_date": today_str}, fh)
        except Exception as exc:
            logger.warning("[FG] Disk cache write failed: %s", exc)
        _pg_save_cache(_data_year or season, _BATTER_CACHE, _PITCHER_CACHE)

    _loaded = True


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
