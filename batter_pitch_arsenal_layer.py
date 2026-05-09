"""
batter_pitch_arsenal_layer.py
=============================
PropIQ — Batter vs. pitch-type vulnerability layer.

Fetches how each batter performs against each pitch type (FF/SL/CH/CU/SI/KC/FS):
  - whiff_percent: swing-and-miss rate vs that pitch type
  - k_percent:     K rate when that pitch is thrown
  - woba:          wOBA when that pitch is thrown

Data source: pybaseball.statcast_batter_pitch_arsenal(year, minPA=50)
Cached in Redis for 12 hours.  Falls back to 0.0 (neutral) on any failure.

Public API
----------
get_batter_pitch_vulnerability(batter_id, pitcher_id) -> float
    Returns logit-space adjustment for K probability vs this pitcher's arsenal.
    Positive = batter more likely to K. Clamped [-0.25, +0.25].

prefetch() -> None
    Call at 8:15 AM PT to warm cache before dispatch window.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from typing import Any

logger = logging.getLogger("propiq.batter_pitch_arsenal")

# ── Constants ────────────────────────────────────────────────────────────────
_CACHE_KEY = "batter_pitch_arsenal_2026"
_CACHE_TTL  = 43200   # 12 hours
_MIN_PA     = 50      # minimum PAs to include a batter/pitch-type row

_LG_WHIFF = 0.245    # league avg whiff% vs any pitch type (2026 ABS era)
_LG_K_PCT = 0.235    # league avg K rate

# ── Module-level state ───────────────────────────────────────────────────────
# _data: { batter_mlbam_id → { pitch_type → { whiff_pct, k_pct, woba, pa } } }
_data: dict[int, dict[str, dict]] = {}
_loaded      = False
_load_lock   = threading.Lock()


# ── Redis helper ─────────────────────────────────────────────────────────────

def _get_redis():
    try:
        import redis as _r
        url = os.environ.get("REDIS_URL") or os.environ.get("REDIS_PUBLIC_URL")
        if not url:
            return None
        return _r.from_url(url, decode_responses=True, socket_connect_timeout=3)
    except Exception:
        return None


# ── pybaseball fetch ─────────────────────────────────────────────────────────

def _fetch_from_live_db() -> dict[int, dict[str, dict]]:
    """
    Read batter_pitch_whiff_live table (written by pitch_whiff_refresh.py nightly).
    Returns same shape as _fetch_from_pybaseball: {player_id: {pitch_type: {whiff_pct, k_pct, woba, pa}}}.
    Returns {} if table doesn't exist or has no rows for current season.
    """
    try:
        import os, psycopg2  # noqa: PLC0415, E401
        url = os.environ.get("DATABASE_URL")
        if not url:
            return {}
        conn = psycopg2.connect(url)
        season = __import__("datetime").date.today().year
        result: dict[int, dict[str, dict]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT batter_id, pitch_type, pa, whiff_pct, k_pct
                FROM batter_pitch_whiff_live
                WHERE season = %s AND swings >= 15
                """,
                (season,),
            )
            for row in cur.fetchall():
                bid, pt, pa, whiff, k_pct = row
                if bid not in result:
                    result[int(bid)] = {}
                result[int(bid)][str(pt).strip()] = {
                    "whiff_pct": float(whiff),
                    "k_pct":     float(k_pct),
                    "woba":      0.0,   # not stored in live table yet
                    "pa":        int(pa),
                }
        conn.close()
        if result:
            logger.info("[BatterPitchArsenal] Live DB: %d batters loaded (season=%d)", len(result), season)
        return result
    except Exception as exc:
        logger.debug("[BatterPitchArsenal] Live DB read failed: %s", exc)
        return {}


def _fetch_from_pybaseball() -> dict[int, dict[str, dict]]:
    """Call pybaseball.statcast_batter_pitch_arsenal(2026, minPA=50).
    Returns {player_id: {pitch_type: {whiff_pct, k_pct, woba, pa}}}.
    """
    try:
        import pybaseball  # noqa: PLC0415
        pybaseball.cache.enable()
        df = pybaseball.statcast_batter_pitch_arsenal(2026, minPA=_MIN_PA)
        if df is None or df.empty:
            return {}
        result: dict[int, dict[str, dict]] = {}
        for _, row in df.iterrows():
            try:
                pid = int(row.get("player_id", 0) or 0)
                pt  = str(row.get("pitch_type", "") or "").strip()
                pa  = int(row.get("pa", 0) or 0)
                if not pid or not pt or pa < _MIN_PA:
                    continue
                if pid not in result:
                    result[pid] = {}
                result[pid][pt] = {
                    "whiff_pct": round(float(row.get("whiff_percent", 0) or 0) / 100.0, 4),
                    "k_pct":     round(float(row.get("k_percent",     0) or 0) / 100.0, 4),
                    "woba":      round(float(row.get("woba",          0) or 0),          4),
                    "pa":        pa,
                }
            except Exception:
                continue
        logger.info("[BatterPitchArsenal] pybaseball: %d batters loaded", len(result))
        return result
    except Exception as exc:
        logger.warning("[BatterPitchArsenal] pybaseball fetch failed: %s", exc)
        return {}


# ── Load / cache ─────────────────────────────────────────────────────────────

def _load() -> None:
    global _data, _loaded
    if _loaded:
        return
    with _load_lock:
        if _loaded:
            return

        r = _get_redis()

        # Try Redis cache first
        if r:
            try:
                cached = r.get(_CACHE_KEY)
                if cached:
                    raw = json.loads(cached)
                    _data = {int(k): v for k, v in raw.items()}
                    _loaded = True
                    logger.info(
                        "[BatterPitchArsenal] Loaded %d batters from Redis cache", len(_data)
                    )
                    return
            except Exception as exc:
                logger.debug("[BatterPitchArsenal] Redis read failed: %s", exc)

        # Fetch fresh: live DB first, then pybaseball fallback
        logger.info("[BatterPitchArsenal] Cache miss — checking live DB then pybaseball…")
        fetched = _fetch_from_live_db()
        if not fetched:
            logger.info("[BatterPitchArsenal] Live DB empty — falling back to pybaseball…")
            fetched = _fetch_from_pybaseball()
        if fetched:
            _data = fetched
            if r:
                try:
                    r.setex(
                        _CACHE_KEY,
                        _CACHE_TTL,
                        json.dumps({str(k): v for k, v in fetched.items()}),
                    )
                    logger.info("[BatterPitchArsenal] Cached %d batters in Redis (12h TTL)", len(fetched))
                except Exception as exc:
                    logger.debug("[BatterPitchArsenal] Redis write failed: %s", exc)
        else:
            logger.warning(
                "[BatterPitchArsenal] No data loaded — will return 0.0 adjustments (league avg)"
            )
        _loaded = True


def prefetch() -> None:
    """Force-refresh the Redis cache.  Call at 8:15 AM PT."""
    global _loaded, _data
    _loaded = False
    _data   = {}
    _load()


# ── Public API ────────────────────────────────────────────────────────────────

def get_batter_pitch_vulnerability(batter_id: int, pitcher_id: int) -> float:
    """
    Logit-space K probability adjustment based on batter vs. this pitcher's arsenal.

    Algorithm:
    1. Get pitcher's pitch arsenal (pitch_type → usage%) from statcast_static_layer
    2. For each pitch type in arsenal, look up batter's whiff% against that pitch type
    3. Weighted-average batter whiff% across pitcher's arsenal (weighted by pitch usage)
    4. Compare weighted avg to league average → logit delta

    Args:
        batter_id  — MLBAM ID of the batter
        pitcher_id — MLBAM ID of the opposing starting pitcher

    Returns:
        Logit delta (positive = more likely K). Clamped [-0.25, +0.25].
        Returns 0.0 when data unavailable (graceful degradation).
    """
    _load()

    # Get pitcher's arsenal (already loaded from statcast_static_layer CSVs)
    arsenal: dict[str, dict] = {}
    try:
        from statcast_static_layer import get_pitcher_arsenal  # noqa: PLC0415
        arsenal = get_pitcher_arsenal(int(pitcher_id)) or {}
    except Exception:
        pass

    if not arsenal:
        return 0.0

    batter_data = _data.get(int(batter_id), {})

    # Weighted average batter whiff% across this pitcher's arsenal
    total_usage   = 0.0
    weighted_whiff = 0.0

    for pitch_type, metrics in arsenal.items():
        # arsenal usage may be stored as raw % (45.0) or decimal (0.45) — normalize
        raw_usage = metrics.get("usage", 0.0) or 0.0
        usage = raw_usage / 100.0 if raw_usage > 1.0 else raw_usage
        if usage <= 0.001:
            continue

        # Batter's whiff% vs this pitch type — fallback to league average
        pt_data       = batter_data.get(pitch_type, {})
        batter_whiff  = pt_data.get("whiff_pct", _LG_WHIFF)
        if batter_whiff <= 0:
            batter_whiff = _LG_WHIFF

        weighted_whiff += usage * batter_whiff
        total_usage    += usage

    if total_usage <= 0.001:
        return 0.0

    avg_batter_whiff = weighted_whiff / total_usage

    def _logit(p: float) -> float:
        p = max(0.01, min(0.99, p))
        return math.log(p / (1.0 - p))

    # Logit delta vs league average whiff rate
    delta = _logit(avg_batter_whiff) - _logit(_LG_WHIFF)
    # Scale: whiff% doesn't map 1:1 to K% — apply 0.65 dampener
    delta *= 0.65

    result = max(-0.25, min(0.25, delta))
    logger.debug(
        "[BPV] batter=%d pitcher=%d avg_whiff=%.3f lg_whiff=%.3f delta=%.4f",
        batter_id, pitcher_id, avg_batter_whiff, _LG_WHIFF, result,
    )
    return round(result, 4)
