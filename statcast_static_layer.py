"""statcast_static_layer.py — 2026 Statcast CSV lookup layer.

Loads pitcher arsenal stats, batter bat-tracking, EV, xStats, and discipline
from CSV files in data/statcast/ relative to this module.

All lookups keyed by MLBAM player_id (int). Returns None / empty dict when
a player is not in the dataset — callers should always provide a fallback.

Public API
----------
# Pitcher
get_pitcher_k_rate(player_id)    -> float | None   (e.g. 0.283 = 28.3% K rate)
get_pitcher_whiff_rate(player_id)-> float | None   (e.g. 0.271 = 27.1% whiff)
get_pitcher_xera(player_id)      -> float | None   (e.g. 3.41)
get_pitcher_arsenal(player_id)   -> dict           pitch_type → metrics

# Batter
get_batter_k_susceptibility(player_id) -> float | None  (whiff_per_swing)
get_batter_ev_profile(player_id)       -> dict          (ev50, brl_percent, avg_hit_speed)
get_batter_xstats(player_id)           -> dict          (xba, xwoba, xslg)
get_batter_discipline(player_id)       -> dict          (runs_chase, runs_heart, runs_waste)
get_batter_batted_ball(player_id)      -> dict          (gb_rate, fb_rate, ld_rate, pull_rate)

# Matchup
get_matchup_k_boost(pitcher_id, batter_id) -> float    (logit adjustment, typically -0.1 to +0.2)
"""

from __future__ import annotations

import csv
import logging
import os
import threading
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "statcast")

# ── Lazy-load state ───────────────────────────────────────────────────────────
_loaded   = False
_load_lock = threading.Lock()

# Internal stores (keyed by MLBAM int)
_pitcher_k_rate:    dict[int, float] = {}
_pitcher_whiff:     dict[int, float] = {}
_pitcher_xera:      dict[int, float] = {}
_pitcher_arsenal:   dict[int, dict]  = {}

_batter_tracking:   dict[int, dict] = {}
_batter_ev:         dict[int, dict] = {}
_batter_xstats:     dict[int, dict] = {}
_batter_discipline: dict[int, dict] = {}
_batter_batted:     dict[int, dict] = {}
_batter_percentiles:dict[int, dict] = {}


def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        x = float(v)
        return default if x == 0.0 else x
    except (TypeError, ValueError):
        return default


def _csv_path(filename: str) -> str:
    return os.path.join(_DATA_DIR, filename)


def _read_csv(filename: str) -> list[dict]:
    path = _csv_path(filename)
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except Exception as exc:
        logger.warning("[StatcastStatic] Failed to read %s: %s", filename, exc)
        return []


def _load() -> None:
    global _loaded

    if _loaded:
        return

    with _load_lock:
        if _loaded:
            return

        logger.info("[StatcastStatic] Loading 2026 Statcast CSV data from %s", _DATA_DIR)

        # ── Pitcher arsenal: pitch-arsenal-stats (1).csv ─────────────────────
        arsenal_rows = _read_csv("pitch-arsenal-stats-pitchers.csv")
        for r in arsenal_rows:
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue

            usage = _safe_float(r.get("pitch_usage"), 0.0) or 0.0
            kpct  = _safe_float(r.get("k_percent"),   0.0) or 0.0
            whiff = _safe_float(r.get("whiff_percent"),0.0) or 0.0
            rv100 = _safe_float(r.get("run_value_per_100"), 0.0) or 0.0
            put_a = _safe_float(r.get("put_away"),    0.0) or 0.0
            hh    = _safe_float(r.get("hard_hit_percent"), 0.0) or 0.0
            pt    = r.get("pitch_type", "").strip()

            if pid not in _pitcher_arsenal:
                _pitcher_arsenal[pid] = {}
            _pitcher_arsenal[pid][pt] = {
                "usage": usage, "k_pct": kpct, "whiff_pct": whiff,
                "rv100": rv100, "put_away": put_a, "hard_hit_pct": hh,
            }

        for pid, pitches in _pitcher_arsenal.items():
            total = sum(p["usage"] for p in pitches.values())
            if total <= 0:
                continue
            wk = sum(p["k_pct"]    * p["usage"] for p in pitches.values()) / total
            ww = sum(p["whiff_pct"]* p["usage"] for p in pitches.values()) / total
            if wk > 0:
                _pitcher_k_rate[pid] = round(wk / 100.0, 4)  # % → decimal
            if ww > 0:
                _pitcher_whiff[pid]  = round(ww / 100.0, 4)

        # ── Pitcher xERA: expected_stats (1).csv ──────────────────────────────
        for r in _read_csv("expected-stats-pitchers.csv"):
            pid_s = r.get("player_id", "").strip()
            xera  = _safe_float(r.get("xera"))
            if pid_s and xera:
                try:
                    _pitcher_xera[int(pid_s)] = round(xera, 3)
                except ValueError:
                    pass

        # ── Batter bat tracking ───────────────────────────────────────────────
        for r in _read_csv("bat-tracking.csv"):
            pid_s = r.get("id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _batter_tracking[pid] = {
                "whiff_per_swing":  _safe_float(r.get("whiff_per_swing")),
                "avg_bat_speed":    _safe_float(r.get("avg_bat_speed")),
                "hard_swing_rate":  _safe_float(r.get("hard_swing_rate")),
                "blast_per_swing":  _safe_float(r.get("blast_per_swing")),
                "swing_length":     _safe_float(r.get("swing_length")),
            }

        # ── Batter EV / barrels ───────────────────────────────────────────────
        for r in _read_csv("exit_velocity.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _batter_ev[pid] = {
                "avg_hit_speed": _safe_float(r.get("avg_hit_speed")),
                "ev50":          _safe_float(r.get("ev50")),
                "brl_percent":   _safe_float(r.get("brl_percent")),
                "max_hit_speed": _safe_float(r.get("max_hit_speed")),
            }

        # ── Batter expected stats ─────────────────────────────────────────────
        for r in _read_csv("expected_stats.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _batter_xstats[pid] = {
                "xba":   _safe_float(r.get("est_ba")),
                "xwoba": _safe_float(r.get("est_woba")),
                "xslg":  _safe_float(r.get("est_slg")),
            }

        # ── Batter discipline (swing-take) ────────────────────────────────────
        for r in _read_csv("swing-take.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _batter_discipline[pid] = {
                "runs_chase": _safe_float(r.get("runs_chase")),
                "runs_heart": _safe_float(r.get("runs_heart")),
                "runs_waste": _safe_float(r.get("runs_waste")),
                "runs_all":   _safe_float(r.get("runs_all")),
            }

        # ── Batter batted ball profile ────────────────────────────────────────
        for r in _read_csv("batted-ball.csv"):
            pid_s = r.get("id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _batter_batted[pid] = {
                "gb_rate":   _safe_float(r.get("gb_rate")),
                "fb_rate":   _safe_float(r.get("fb_rate")),
                "ld_rate":   _safe_float(r.get("ld_rate")),
                "pull_rate": _safe_float(r.get("pull_rate")),
            }

        # ── Batter percentile ranks ───────────────────────────────────────────
        for r in _read_csv("percentile_rankings.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _batter_percentiles[pid] = {
                "xwoba_pct":   _safe_float(r.get("xwoba")),
                "k_pct_rank":  _safe_float(r.get("k_percent")),
                "whiff_rank":  _safe_float(r.get("whiff_percent")),
                "chase_rank":  _safe_float(r.get("chase_percent")),
                "ev_rank":     _safe_float(r.get("exit_velocity")),
                "sprint_rank": _safe_float(r.get("sprint_speed")),
            }

        _loaded = True
        logger.info(
            "[StatcastStatic] Loaded: %d pitcher K rates, %d pitcher xERAs, "
            "%d batter tracking, %d batter EV, %d batter xStats",
            len(_pitcher_k_rate), len(_pitcher_xera),
            len(_batter_tracking), len(_batter_ev), len(_batter_xstats),
        )


# ── Public API ────────────────────────────────────────────────────────────────

def get_pitcher_k_rate(player_id: int) -> float | None:
    """Weighted K% across pitcher's 2026 arsenal. Returns decimal (e.g. 0.283)."""
    _load()
    return _pitcher_k_rate.get(int(player_id))


def get_pitcher_whiff_rate(player_id: int) -> float | None:
    """Weighted whiff% across pitcher's 2026 arsenal. Returns decimal."""
    _load()
    return _pitcher_whiff.get(int(player_id))


def get_pitcher_xera(player_id: int) -> float | None:
    """Pitcher's 2026 xERA."""
    _load()
    return _pitcher_xera.get(int(player_id))


def get_pitcher_arsenal(player_id: int) -> dict:
    """Full arsenal breakdown: {pitch_type: {usage, k_pct, whiff_pct, rv100, put_away, hard_hit_pct}}."""
    _load()
    return _pitcher_arsenal.get(int(player_id), {})


def get_batter_k_susceptibility(player_id: int) -> float | None:
    """Batter's whiff_per_swing from bat tracking. Higher = more K-prone."""
    _load()
    bt = _batter_tracking.get(int(player_id), {})
    return bt.get("whiff_per_swing")


def get_batter_bat_tracking(player_id: int) -> dict:
    """Full bat tracking: {whiff_per_swing, avg_bat_speed, hard_swing_rate, blast_per_swing, swing_length}."""
    _load()
    return _batter_tracking.get(int(player_id), {})


def get_batter_ev_profile(player_id: int) -> dict:
    """Batter EV: {avg_hit_speed, ev50, brl_percent, max_hit_speed}."""
    _load()
    return _batter_ev.get(int(player_id), {})


def get_batter_xstats(player_id: int) -> dict:
    """Batter expected stats: {xba, xwoba, xslg}."""
    _load()
    return _batter_xstats.get(int(player_id), {})


def get_batter_discipline(player_id: int) -> dict:
    """Batter swing/take discipline: {runs_chase, runs_heart, runs_waste, runs_all}."""
    _load()
    return _batter_discipline.get(int(player_id), {})


def get_batter_batted_ball(player_id: int) -> dict:
    """Batter batted ball profile: {gb_rate, fb_rate, ld_rate, pull_rate}."""
    _load()
    return _batter_batted.get(int(player_id), {})


def get_batter_percentiles(player_id: int) -> dict:
    """Batter Statcast percentile ranks (0–100 scale): {xwoba_pct, k_pct_rank, whiff_rank, chase_rank}."""
    _load()
    return _batter_percentiles.get(int(player_id), {})


def get_matchup_k_boost(pitcher_id: int, batter_id: int) -> float:
    """Logit-space K probability adjustment for pitcher vs batter.

    Combines:
    - Pitcher arsenal K rate vs league average (0.235)
    - Batter whiff rate vs league average (0.24)

    Returns logit delta (positive = more likely K).
    Clamped to [-0.30, +0.30] to avoid overclaiming.
    """
    import math
    _load()

    _LG_PITCHER_K = 0.235   # league avg pitcher K rate
    _LG_BATTER_W  = 0.240   # league avg batter whiff_per_swing

    pitcher_k = get_pitcher_k_rate(pitcher_id) or _LG_PITCHER_K
    batter_w  = get_batter_k_susceptibility(batter_id) or _LG_BATTER_W

    def logit(p: float) -> float:
        p = max(0.01, min(0.99, p))
        return math.log(p / (1 - p))

    # Pitcher edge: how much better/worse than league avg
    pitcher_edge = logit(pitcher_k) - logit(_LG_PITCHER_K)
    # Batter susceptibility edge
    batter_edge  = logit(batter_w)  - logit(_LG_BATTER_W)

    # Weight pitcher more heavily (60/40) — pitcher is primary driver
    combined = pitcher_edge * 0.60 + batter_edge * 0.40
    return max(-0.30, min(0.30, combined))
