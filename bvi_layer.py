"""
bvi_layer.py — Bullpen Volatility Index
Adapted from mlb-bullpen-volatility-index v1.

Three-component BVI (weights: 40/40/20):
  impact_volatility    (40%) — pressure-weighted entry state × outcome std
  inherited_instability (40%) — Bayesian-stabilized IR scored-rate std
  fatigue_volatility    (20%) — CV of daily bullpen pitch totals

Entry pressure weights (Tango-calibrated):
  Inning:     ≤6→0.8, 7→1.0, 8→1.2, 9→1.4, extras→1.6
  Score diff: |≤1|→1.4, |2|→1.2, |3|→1.0, |≥4|→0.8
  Runners on: 0→1.0, 1→1.15, 2→1.3, 3→1.45
  Outs:       0→1.2, 1→1.0, 2→0.9

Data source: MLB Stats API live feeds (same endpoint as mlb_stats_layer).
Cache: layer_cache Postgres table (24h TTL) + in-process 1h memory cache.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import date, datetime, timedelta
from statistics import mean, stdev
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# ── Component weights ─────────────────────────────────────────────────────────
_IMPACT_W     = 0.40
_INHERITED_W  = 0.40
_FATIGUE_W    = 0.20
_IR_STABILIZE = 30.0   # Bayesian prior: regress toward mean until 30 IR observed

# ── Cache TTLs ────────────────────────────────────────────────────────────────
_CACHE_TTL_PG  = 86_400   # 24h Postgres
_CACHE_TTL_MEM = 3_600    # 1h in-process

# ── MLB Stats API ─────────────────────────────────────────────────────────────
_MLB_BASE = "https://statsapi.mlb.com/api/v1"
_MLB_V11  = "https://statsapi.mlb.com/api/v1.1"

# ── PT timezone ──────────────────────────────────────────────────────────────
_PT = ZoneInfo("America/Los_Angeles")

# ── In-process memory cache ──────────────────────────────────────────────────
_MEMORY_CACHE: dict[str, tuple[float, dict]] = {}   # key → (timestamp, data)


# =============================================================================
# ENTRY PRESSURE WEIGHTS
# =============================================================================

def _inning_weight(inning: int) -> float:
    if inning <= 6: return 0.8
    if inning == 7: return 1.0
    if inning == 8: return 1.2
    if inning == 9: return 1.4
    return 1.6   # extras


def _score_weight(score_diff: int) -> float:
    ad = abs(score_diff)
    if ad <= 1: return 1.4
    if ad == 2: return 1.2
    if ad == 3: return 1.0
    return 0.8


def _runners_weight(runners_on: int) -> float:
    return {0: 1.0, 1: 1.15, 2: 1.3, 3: 1.45}.get(runners_on, 1.0)


def _outs_weight(outs: int) -> float:
    return {0: 1.2, 1: 1.0, 2: 0.9}.get(outs, 1.0)


def entry_pressure(inning: int, runners_on: int, score_diff: int, outs: int) -> float:
    """Composite entry-state pressure weight for a relief appearance."""
    return (
        _inning_weight(inning)
        * _score_weight(score_diff)
        * _runners_weight(runners_on)
        * _outs_weight(outs)
    )


# =============================================================================
# MLB LIVE FEED EXTRACTION
# =============================================================================

def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v)) if v is not None else default
    except (TypeError, ValueError):
        return default


def _parse_box_relief(feed: dict) -> dict[tuple[int, int], dict]:
    """
    Parse boxscore to extract relief pitcher stats.
    Returns: (team_id, pitcher_id) → {name, runs, ir, irs, pitches}
    Excludes starters (gamesStarted > 0).
    """
    out: dict[tuple[int, int], dict] = {}
    box_teams = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    for side in ("home", "away"):
        team_blob = box_teams.get(side, {})
        tid = team_blob.get("team", {}).get("id")
        if not isinstance(tid, int):
            continue
        for _, player in team_blob.get("players", {}).items():
            person = player.get("person", {})
            pid = person.get("id")
            if not isinstance(pid, int):
                continue
            pit = player.get("stats", {}).get("pitching", {})
            if not pit:
                continue
            if _safe_int(pit.get("gamesStarted")) > 0:
                continue   # skip starters
            out[(tid, pid)] = {
                "name":    person.get("fullName", "Unknown"),
                "runs":    float(pit.get("runs", 0) or 0),
                "ir":      _safe_int(pit.get("inheritedRunners")),
                "irs":     _safe_int(pit.get("inheritedRunnersScored")),
                "pitches": _safe_int(
                    pit.get("numberOfPitches") or pit.get("pitchesThrown")
                ),
            }
    return out


def _parse_entry_states(feed: dict) -> dict[tuple[int, int], dict]:
    """
    Walk allPlays to find the first PA each reliever faced.
    Returns: (team_id, pitcher_id) → {inning, outs, runners_on, score_diff}
    score_diff = defensive_team_score − opponent_score (positive = leading)
    """
    gteams = feed.get("gameData", {}).get("teams", {})
    home_id = gteams.get("home", {}).get("id")
    away_id = gteams.get("away", {}).get("id")
    if not isinstance(home_id, int) or not isinstance(away_id, int):
        return {}

    entries: dict[tuple[int, int], dict] = {}
    for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
        pid = play.get("matchup", {}).get("pitcher", {}).get("id")
        if not isinstance(pid, int):
            continue
        is_top  = bool(play.get("about", {}).get("isTopInning", False))
        def_tid = home_id if is_top else away_id
        key     = (def_tid, pid)
        if key in entries:
            continue   # only record first appearance

        inning     = _safe_int(play.get("about", {}).get("inning"), 1)
        outs       = min(2, max(0, _safe_int(play.get("count", {}).get("outs"))))
        runners_on = len({
            r.get("movement", {}).get("start")
            for r in play.get("runners", [])
            if r.get("movement", {}).get("start") in {"1B", "2B", "3B"}
        })

        # Reconstruct score BEFORE this play (subtract runs scored on play)
        h_after = _safe_int(play.get("about", {}).get("homeScore"))
        a_after = _safe_int(play.get("about", {}).get("awayScore"))
        scored  = sum(
            1 for r in play.get("runners", [])
            if r.get("movement", {}).get("end") == "score"
        )
        if is_top:
            a_before, h_before = a_after - scored, h_after
        else:
            h_before, a_before = h_after - scored, a_after

        # score_diff: positive = defensive team is winning
        score_diff = (h_before - a_before) if def_tid == home_id else (a_before - h_before)

        entries[key] = {
            "inning":     inning,
            "outs":       outs,
            "runners_on": min(3, runners_on),
            "score_diff": score_diff,
        }
    return entries


def _extract_appearances(feed: dict, game_date: str) -> list[dict]:
    """
    Extract one record per relief appearance from a completed game live feed.
    Default entry state (inning=6, outs=1, runners=0, score=0) used when
    the pitcher never appeared in allPlays (e.g. entered mid-inning after feed cut).
    """
    box  = _parse_box_relief(feed)
    ents = _parse_entry_states(feed)

    gteams   = feed.get("gameData", {}).get("teams", {})
    team_meta: dict[int, tuple[str, str]] = {}
    for side in ("home", "away"):
        t = gteams.get(side, {})
        tid = t.get("id")
        if isinstance(tid, int):
            team_meta[tid] = (t.get("abbreviation", "UNK"), t.get("name", "Unknown"))

    out = []
    for (tid, pid), stats in box.items():
        state  = ents.get((tid, pid), {"inning": 6, "outs": 1, "runners_on": 0, "score_diff": 0})
        abbrev, tname = team_meta.get(tid, ("UNK", "Unknown"))
        out.append({
            "team_id":    tid,
            "team_abbrev": abbrev,
            "team_name":  tname,
            "pitcher_id": pid,
            "game_date":  game_date,
            "inning":     state["inning"],
            "outs":       state["outs"],
            "runners_on": state["runners_on"],
            "score_diff": state["score_diff"],
            "runs":       stats["runs"],
            "ir":         stats["ir"],
            "irs":        stats["irs"],
            "pitches":    stats["pitches"],
        })
    return out


# =============================================================================
# BVI METRIC COMPUTATION
# =============================================================================

def _std(values: list[float]) -> float:
    return float(stdev(values)) if len(values) >= 2 else 0.0


def _weighted_std(values: list[float], weights: list[float]) -> float:
    if len(values) < 2 or len(values) != len(weights):
        return 0.0
    tw = sum(weights)
    if tw <= 0:
        return 0.0
    wm  = sum(v * w for v, w in zip(values, weights)) / tw
    var = sum(w * (v - wm) ** 2 for v, w in zip(values, weights)) / tw
    return math.sqrt(var)


def _cv(values: list[float]) -> float:
    """Coefficient of variation (σ/μ). Scale-invariant measure of spread."""
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    return _std(values) / avg if avg > 0 else 0.0


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    pos = (len(xs) - 1) * q
    lo  = int(pos)
    hi  = math.ceil(pos)
    if lo == hi:
        return xs[lo]
    return xs[lo] * (1 - (pos - lo)) + xs[hi] * (pos - lo)


def _robust_minmax(values: list[float]) -> list[float]:
    """Clip to [p5, p95] then rescale to [0, 100]. Robust to outliers."""
    if not values:
        return []
    p5, p95 = _quantile(values, 0.05), _quantile(values, 0.95)
    if p95 <= p5:
        return [50.0] * len(values)
    span = p95 - p5
    return [
        round(min(100.0, max(0.0, (min(max(v, p5), p95) - p5) / span * 100.0)), 2)
        for v in values
    ]


def _compute_raw_metrics(appearances: list[dict]) -> dict:
    """
    Compute the three raw BVI components from a list of relief appearances.
    All components are non-negative floats; larger = more volatile.
    """
    impacts:      list[float] = []
    ir_rates:     list[float] = []
    ir_weights:   list[float] = []
    daily_pitches: dict[str, int] = {}

    for ap in appearances:
        pressure = entry_pressure(
            ap["inning"], ap["runners_on"], ap["score_diff"], ap["outs"]
        )
        # impact = pressure × total runs yielded (own + inherited)
        impacts.append(pressure * (ap["runs"] + ap["irs"]))

        # inherited runner handling
        if ap["ir"] > 0:
            ir_rates.append(ap["irs"] / ap["ir"])
            ir_weights.append(float(ap["ir"]))

        # daily pitch accumulation
        d = ap.get("game_date", "")
        if d:
            daily_pitches[d] = daily_pitches.get(d, 0) + ap["pitches"]

    # Component 1: std of pressure-weighted outcomes
    impact_vol = _std(impacts)

    # Component 2: Bayesian-stabilised weighted std of IR scored rates
    ir_std     = _weighted_std(ir_rates, ir_weights)
    total_ir   = sum(ir_weights)
    stab       = math.sqrt(total_ir / (total_ir + _IR_STABILIZE)) if total_ir > 0 else 0.0
    inh_instab = ir_std * stab

    # Component 3: CV of daily team pitch totals
    fatigue_vol = _cv([float(v) for v in daily_pitches.values()])

    return {
        "impact_volatility":     impact_vol,
        "inherited_instability": inh_instab,
        "fatigue_volatility":    fatigue_vol,
        "relief_appearances":    len(appearances),
        "season_days":           len(daily_pitches),
    }


def _finalize_bvi(raw: dict[str, dict]) -> dict[str, dict]:
    """
    Normalize three components via robust min-max (p5–p95) to [0,100],
    then compute weighted composite BVI score.
    Higher BVI = more volatile bullpen.
    """
    abbrevs   = list(raw.keys())
    impacts   = [raw[a]["impact_volatility"]     for a in abbrevs]
    inherited = [raw[a]["inherited_instability"] for a in abbrevs]
    fatigue   = [raw[a]["fatigue_volatility"]    for a in abbrevs]

    i_norm = _robust_minmax(impacts)
    h_norm = _robust_minmax(inherited)
    f_norm = _robust_minmax(fatigue)

    result: dict[str, dict] = {}
    for idx, abbrev in enumerate(abbrevs):
        m   = raw[abbrev]
        in_ = i_norm[idx]
        hn  = h_norm[idx]
        fn  = f_norm[idx]
        bvi = round(_IMPACT_W * in_ + _INHERITED_W * hn + _FATIGUE_W * fn, 2)
        result[abbrev] = {
            **m,
            "impact_norm":    round(in_, 2),
            "inherited_norm": round(hn, 2),
            "fatigue_norm":   round(fn, 2),
            "bvi":            bvi,
        }
    return result


# =============================================================================
# MLB STATS API HELPERS
# =============================================================================

def _fetch_live_feed(game_pk: int) -> dict:
    """Fetch a completed game live feed from MLB Stats API v1.1."""
    try:
        import requests
        r = requests.get(f"{_MLB_V11}/game/{game_pk}/feed/live", timeout=20)
        if r.status_code == 200:
            return r.json()
    except Exception as exc:
        logger.debug("[BVI] live feed game_pk=%d failed: %s", game_pk, exc)
    return {}


def _get_completed_game_pks(lookback_days: int = 10) -> list[int]:
    """Return completed regular-season gamePks for the last N calendar days (PT)."""
    try:
        import requests
        today = datetime.now(_PT).date()
        start = today - timedelta(days=lookback_days)
        r = requests.get(
            f"{_MLB_BASE}/schedule",
            params={
                "sportId": 1, "gameTypes": "R",
                "startDate": start.isoformat(),
                "endDate":   today.isoformat(),
                "hydrate":   "team",
            },
            timeout=20,
        )
        if r.status_code != 200:
            return []
        pks: set[int] = set()
        for date_row in r.json().get("dates", []):
            for game in date_row.get("games", []):
                st = game.get("status", {})
                final = (
                    st.get("abstractGameState") == "Final"
                    or st.get("codedGameState") == "F"
                    or "Final" in (st.get("detailedState") or "")
                )
                if final and game.get("gameType") == "R":
                    pk = game.get("gamePk")
                    if isinstance(pk, int):
                        pks.add(pk)
        return sorted(pks)
    except Exception as exc:
        logger.warning("[BVI] schedule fetch failed: %s", exc)
        return []


# =============================================================================
# CACHE HELPERS
# =============================================================================

def _pg_get(key: str) -> dict | None:
    try:
        from layer_cache_helper import pg_cache_get
        raw = pg_cache_get(key)
        if raw:
            return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        pass
    return None


def _pg_set(key: str, value: dict, ttl: int = _CACHE_TTL_PG) -> None:
    try:
        from layer_cache_helper import pg_cache_set
        pg_cache_set(key, json.dumps(value), ttl_seconds=ttl)
    except Exception:
        pass


# =============================================================================
# PUBLIC API
# =============================================================================

def get_bvi_map(lookback_days: int = 10, force_refresh: bool = False) -> dict[str, dict]:
    """
    Return BVI scores for all teams seen in recent completed games.

    Keys:   team abbreviation (e.g. "NYY", "BOS", "LAD").
    Values: dict with keys:
        bvi                  — composite 0–100 (higher = more volatile)
        impact_norm          — 0–100 normalized impact component
        inherited_norm       — 0–100 normalized inherited-runner component
        fatigue_norm         — 0–100 normalized fatigue component
        impact_volatility    — raw std of pressure-weighted outcomes
        inherited_instability— raw Bayesian-stabilised IR scored-rate std
        fatigue_volatility   — raw CV of daily pitch totals
        relief_appearances   — total relief appearances in window
        season_days          — unique game dates in window

    Cache layers:
        L1 — in-process dict (1h TTL)
        L2 — Postgres layer_cache (24h TTL)
        L3 — live MLB Stats API fetch (10-day lookback × ~15 games)
    """
    today     = datetime.now(_PT).strftime("%Y-%m-%d")
    cache_key = f"bvi_map_{today}"

    # L1: memory
    if not force_refresh and cache_key in _MEMORY_CACHE:
        ts, data = _MEMORY_CACHE[cache_key]
        if time.time() - ts < _CACHE_TTL_MEM:
            return data

    # L2: Postgres
    if not force_refresh:
        cached = _pg_get(cache_key)
        if cached:
            _MEMORY_CACHE[cache_key] = (time.time(), cached)
            return cached

    # L3: live fetch
    logger.info("[BVI] Building BVI map — lookback=%d days", lookback_days)
    game_pks = _get_completed_game_pks(lookback_days)
    if not game_pks:
        logger.warning("[BVI] No completed games found — returning empty map")
        return {}

    team_apps: dict[str, list[dict]] = {}
    success, total = 0, len(game_pks)

    for gpk in game_pks:
        try:
            feed = _fetch_live_feed(gpk)
            if not feed:
                continue
            gdate = (
                feed.get("gameData", {}).get("datetime", {}).get("officialDate")
                or today
            )
            for ap in _extract_appearances(feed, gdate):
                team_apps.setdefault(ap["team_abbrev"], []).append(ap)
            success += 1
            time.sleep(0.06)   # gentle rate limit
        except Exception as exc:
            logger.debug("[BVI] game_pk=%d error: %s", gpk, exc)

    logger.info("[BVI] Processed %d/%d games → %d teams", success, total, len(team_apps))
    if not team_apps:
        return {}

    raw  = {abbrev: _compute_raw_metrics(apps) for abbrev, apps in team_apps.items()}
    bvi_map = _finalize_bvi(raw)

    avg_bvi = sum(v["bvi"] for v in bvi_map.values()) / max(1, len(bvi_map))
    logger.info("[BVI] Cached %d teams — avg BVI=%.1f", len(bvi_map), avg_bvi)

    _pg_set(cache_key, bvi_map)
    _MEMORY_CACHE[cache_key] = (time.time(), bvi_map)
    return bvi_map


def get_team_bvi(team_abbrev: str, bvi_map: dict | None = None) -> float | None:
    """
    Return BVI score (0–100) for a single team abbreviation.

    High BVI (>60) → volatile bullpen → favor OVER on opposing hitter stats.
    Low  BVI (<35) → stable bullpen  → UNDER signal on opposing hitter stats.
    Returns None if the team has no data for the current window.
    """
    if bvi_map is None:
        bvi_map = get_bvi_map()
    entry = bvi_map.get(team_abbrev.upper())
    return float(entry["bvi"]) if entry else None


def get_team_bvi_components(team_abbrev: str, bvi_map: dict | None = None) -> dict:
    """Return full BVI breakdown for a team (impact, inherited, fatigue norms)."""
    if bvi_map is None:
        bvi_map = get_bvi_map()
    return bvi_map.get(team_abbrev.upper(), {})
