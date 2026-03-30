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
# Postgres cache helpers (Phase 100 — survives Railway container restarts)
# ---------------------------------------------------------------------------

def _pg_load_cache(season: int) -> tuple[dict, dict] | None:
    """Try loading batter/pitcher cache from Postgres fg_cache table.
    Returns (batter_dict, pitcher_dict) or None if unavailable.
    """
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return None
    try:
        import psycopg2
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT data_type, data FROM fg_cache WHERE season = %s AND data_type IN ('batting','pitching')",
            (season,)
        )
        rows = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        conn.close()
        if "batting" in rows and "pitching" in rows:
            logger.info("[FG] Postgres cache hit — season=%d", season)
            return rows["batting"], rows["pitching"]
        return None
    except Exception as exc:
        logger.debug("[FG] Postgres cache read skipped: %s", exc)
        return None


def _pg_write_cache(season: int, batters: dict, pitchers: dict) -> None:
    """Upsert batter/pitcher cache to Postgres fg_cache table."""
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        for data_type, data in (("batting", batters), ("pitching", pitchers)):
            cur.execute(
                """
                INSERT INTO fg_cache (season, data_type, data, cached_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (season, data_type) DO UPDATE
                    SET data = EXCLUDED.data, cached_at = NOW()
                """,
                (season, data_type, psycopg2.extras.Json(data))
            )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("[FG] Postgres cache written — season=%d", season)
    except Exception as exc:
        logger.warning("[FG] Postgres cache write failed: %s", exc)

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
_FG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.fangraphs.com/leaders/major-league",
    "Accept": "application/json",
}

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
LEAGUE_DEFAULTS: dict[str, dict[str, float]] = {
    "pitcher": {
        "csw_pct":   0.280,
        "swstr_pct": 0.108,
        "k_bb_pct":  0.130,
        "xfip":      4.20,
        "siera":     4.20,
        "fip":       4.20,
        "hr_fb_pct": 0.120,
        "lob_pct":   0.720,
        "babip":     0.300,
    },
    "batter": {
        "wrc_plus":  100.0,
        "woba":      0.320,
        "iso":       0.150,
        "babip":     0.300,
        "o_swing":   0.310,
        "z_contact": 0.850,
        "hr_fb_pct": 0.120,
        "k_pct":     0.230,
        "bb_pct":    0.085,
    },
}


# ---------------------------------------------------------------------------
# Internal fetch helpers
# ---------------------------------------------------------------------------

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
            headers=_FG_HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data") or []
    except Exception as exc:
        logger.warning("[FG] API fetch failed (stats=%s, season=%d): %s", stats, season, exc)
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
        cache[key] = {
            "wrc_plus":  _safe_float(row.get("wRC+"),       bd["wrc_plus"]),
            "woba":      _safe_float(row.get("wOBA"),       bd["woba"]),
            "iso":       _safe_float(row.get("ISO"),        bd["iso"]),
            "babip":     _safe_float(row.get("BABIP"),      bd["babip"]),
            "o_swing":   _safe_float(row.get("O-Swing%"),   bd["o_swing"]),
            "z_contact": _safe_float(row.get("Z-Contact%"), bd["z_contact"]),
            "hr_fb_pct": _safe_float(row.get("HR/FB"),      bd["hr_fb_pct"]),
            "k_pct":     _safe_float(row.get("K%"),         bd["k_pct"]),
            "bb_pct":    _safe_float(row.get("BB%"),        bd["bb_pct"]),
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

    # ── Disk miss: try Postgres cache (survives Railway container restarts) ──
    pg_result = _pg_load_cache(season)
    if pg_result:
        _BATTER_CACHE, _PITCHER_CACHE = pg_result
        _data_year = season
        # Restore disk cache from Postgres for next access
        try:
            with open(cache_path, "w") as fh:
                json.dump({"batters": _BATTER_CACHE, "pitchers": _PITCHER_CACHE, "season": season}, fh)
            logger.info("[FG] Disk cache restored from Postgres")
        except Exception as exc:
            logger.debug("[FG] Disk restore failed (non-critical): %s", exc)
        _loaded = True
        return

    # ── Postgres cache (survives Railway restarts, checked after /tmp miss) ────
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
    # min=10 PA threshold means 2026 data appears from Opening Day.
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

        # ── Blend with prior year when current season sample is small ──────────
        # Early season (April/May): 2026 has <150 players vs 2025's ~700+
        # Merge: prefer 2026 values when available, fill gaps with 2025
        if yr == season and len(_BATTER_CACHE) < 150:
            logger.info(
                "[FG] Season %d small sample (%d batters) — blending with %d",
                yr, len(_BATTER_CACHE), yr - 1,
            )
            prior_bat = _parse_batters(_fetch_season("bat", yr - 1))
            prior_pit = _parse_pitchers(_fetch_season("pit", yr - 1))
            # Merge: current season overrides prior (current is more recent)
            merged_bat = {**prior_bat, **_BATTER_CACHE}
            merged_pit = {**prior_pit, **_PITCHER_CACHE}
            _BATTER_CACHE  = merged_bat
            _PITCHER_CACHE = merged_pit
            logger.info(
                "[FG] Blended cache: %d batters, %d pitchers (current=%d + prior=%d)",
                len(_BATTER_CACHE), len(_PITCHER_CACHE),
                len(_BATTER_CACHE), len(prior_bat),
            )

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
        logger.error("[FG] Failed to fetch any FanGraphs data — layer disabled")

    _loaded = True


# ─── Public getters ───────────────────────────────────────────────────────────

def get_batter(name: str) -> dict[str, float]:
    """Return FanGraphs batting stats for name. Empty dict if not found."""
    global _loaded
    if not _loaded:
        _load()
    return _BATTER_CACHE.get(_normalise_name(name), {})


def get_pitcher(name: str) -> dict[str, float]:
    """Return FanGraphs pitching stats for name. Empty dict if not found."""
    global _loaded
    if not _loaded:
        _load()
    return _PITCHER_CACHE.get(_normalise_name(name), {})


# ─── Probability adjustment engine ───────────────────────────────────────────

# Hard cap: no single FanGraphs nudge exceeds +/-0.030
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
            xfip_adj  = (4.20 - xfip)  / 0.70 * 0.015
            siera_adj = (4.20 - siera) / 0.70 * 0.008
            adj += flip * (xfip_adj + siera_adj)

        elif _in_group(prop_type, "hits_allow"):
            swstr = fg.get("swstr_pct", ld_p["swstr_pct"])
            babip = fg.get("babip",     ld_p["babip"])
            swstr_adj = (swstr - 0.108) / 0.030 * 0.012
            babip_adj = (0.300 - babip) / 0.030 * 0.008
            adj += flip * swstr_adj
            adj -= babip_adj

    else:  # batter
        if _in_group(prop_type, "hit_props"):
            wrc  = fg.get("wrc_plus", ld_b["wrc_plus"])
            woba = fg.get("woba",     ld_b["woba"])
            wrc_adj  = (wrc  - 100.0) / 30.0 * 0.015
            woba_adj = (woba - 0.320) / 0.060 * 0.010
            adj += flip * (wrc_adj + woba_adj)

        elif _in_group(prop_type, "power_props"):
            iso   = fg.get("iso",      ld_b["iso"])
            hr_fb = fg.get("hr_fb_pct", ld_b["hr_fb_pct"])
            wrc   = fg.get("wrc_plus", ld_b["wrc_plus"])
            iso_adj   = (iso   - 0.150) / 0.070 * 0.014
            hr_fb_adj = (hr_fb - 0.120) / 0.050 * 0.010
            wrc_adj   = (wrc   - 100.0) / 30.0  * 0.006
            adj += flip * (iso_adj + hr_fb_adj + wrc_adj)

        elif _in_group(prop_type, "rbi_run"):
            wrc  = fg.get("wrc_plus", ld_b["wrc_plus"])
            woba = fg.get("woba",     ld_b["woba"])
            adj += flip * (
                (wrc - 100.0) / 30.0 * 0.012
                + (woba - 0.320) / 0.060 * 0.010
            )

        elif _in_group(prop_type, "batter_k"):
            o_swing = fg.get("o_swing", ld_b["o_swing"])
            k_pct   = fg.get("k_pct",   ld_b["k_pct"])
            o_adj = (o_swing - 0.310) / 0.100 * 0.015
            k_adj = (k_pct   - 0.230) / 0.050 * 0.012
            adj += flip * (o_adj + k_adj)

        elif _in_group(prop_type, "sb_props"):
            bb_pct = fg.get("bb_pct", ld_b["bb_pct"])
            adj += flip * (bb_pct - 0.085) / 0.030 * 0.010

    return max(-_FG_CAP, min(_FG_CAP, adj))
