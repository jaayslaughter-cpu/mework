"""statcast_static_layer.py — 2026 Statcast CSV lookup layer.

Loads pitcher arsenal stats, batter bat-tracking, EV, xStats, discipline,
spin direction, handedness splits, and historical trend data from CSV files
in data/statcast/ relative to this module.

All lookups keyed by MLBAM player_id (int). Returns None / empty dict when
a player is not in the dataset — callers should always provide a fallback.

Public API
----------
# Pitcher
get_pitcher_k_rate(player_id)          -> float | None   (0.283 = 28.3%)
get_pitcher_whiff_rate(player_id)      -> float | None
get_pitcher_xera(player_id)            -> float | None   (e.g. 3.41)
get_pitcher_arsenal(player_id)         -> dict           pitch_type → metrics
get_pitcher_statcast(player_id)        -> dict           2026 combined stats
get_pitcher_active_spin(pid, pt)       -> dict           spin direction
get_pitcher_spin_profile(player_id)    -> dict[str, dict]
get_pitcher_percentiles(player_id)     -> dict           0–100 percentile ranks
get_pitcher_expected_stats(player_id)  -> dict           era, est_woba, xera_diff
get_pitcher_arsenal_vs_hand(pid, hand) -> dict           vs R or L batters

# Batter
get_batter_k_susceptibility(player_id) -> float | None  (whiff_per_swing)
get_batter_bat_tracking(player_id)     -> dict
get_batter_ev_profile(player_id)       -> dict          (ev50, brl_percent, avg_hit_speed)
get_batter_xstats(player_id)           -> dict          (xba, xwoba, xslg)
get_batter_discipline(player_id)       -> dict          (runs_chase, runs_heart, runs_waste)
get_batter_batted_ball(player_id)      -> dict          (gb_rate, fb_rate, ld_rate, pull_rate)
get_batter_percentiles(player_id)      -> dict          batter Statcast percentiles
get_batter_statcast(player_id)         -> dict          2026 combined stats
get_batter_fg_proj(player_name)        -> dict | None   FanGraphs projected stats
get_batter_sprint_speed(player_id)     -> dict | None
get_batter_baserunning(player_id)      -> float | None
get_batter_vs_pitch(batter_id, pt)     -> dict          batter perf vs pitch type
get_batter_lhp_splits(batter_id)       -> dict          vs LHP aggregated
get_batter_pitch_vs_lhp(bid, pt)       -> dict          vs specific pitch from LHP
get_batter_k_trend(player_id)          -> dict          multi-year K% trend

# Matchup
get_matchup_k_boost(pitcher_id, batter_id) -> float    (logit delta, -0.30 to +0.30)
"""

from __future__ import annotations

import csv
import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

import unicodedata as _ud


def _norm_name(s: str) -> str:
    """Normalize player name for fuzzy matching: lowercase, ASCII, strip punctuation."""
    nfkd = _ud.normalize('NFKD', s)
    ascii_s = ''.join(c for c in nfkd if not _ud.combining(c))
    return ascii_s.lower().replace("'", "").replace(".", "").replace("-", " ").strip()


_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "statcast")

# ── Lazy-load state ───────────────────────────────────────────────────────────
_loaded    = False
_load_lock = threading.Lock()

# Internal stores (keyed by MLBAM int)
_pitcher_k_rate:    dict[int, float] = {}
_pitcher_whiff:     dict[int, float] = {}
_pitcher_xera:      dict[int, float] = {}
_pitcher_arsenal:   dict[int, dict]  = {}
_pitcher_statcast:  dict[int, dict]  = {}
_pitcher_percentiles: dict[int, dict] = {}  # percentile_rankings-pitchers.csv
_pitcher_expected:  dict[int, dict]  = {}   # expected-stats-pitchers.csv (era, est_woba, diff)

_batter_tracking:   dict[int, dict] = {}
_batter_ev:         dict[int, dict] = {}
_batter_xstats:     dict[int, dict] = {}
_batter_discipline: dict[int, dict] = {}
_batter_batted:     dict[int, dict] = {}
_batter_percentiles: dict[int, dict] = {}
_batter_statcast:   dict[int, dict] = {}
_batter_k_history:  dict[int, list] = {}   # statcast_batters_historical.csv — list of {year, k_pct, ...}

_batter_fg_proj:    dict[str, dict] = {}   # keyed by normalized name
_sprint_speed_data: dict[int, dict] = {}
_baserunning_data:  dict[int, float] = {}

# Matchup / handedness splits
_batter_vs_pitch:      dict[tuple, dict] = {}   # (batter_id, pitch_type) → stats
_BATTER_VS_LHP:        dict[str, dict]   = {}   # str(player_id) → {woba_vs_lhp, k_pct_vs_lhp, whiff_pct_vs_lhp}
_BATTER_PITCH_VS_LHP:  dict[tuple, dict] = {}   # (str(player_id), pitch_type) → {woba_vs_pitch, whiff_pct_vs_pitch}
_PITCHER_ARSENAL_RHP:  dict[str, dict]   = {}   # str(player_id) → opponent-batter stats vs RHP
_PITCHER_ARSENAL_LHP:  dict[str, dict]   = {}   # str(player_id) → opponent-batter stats vs LHP

# Spin direction (2026)
_spin_direction: dict[tuple, dict] = {}    # (player_id_int, api_pitch_type) → spin stats
_pitcher_arm_angles: dict[int, dict] = {}  # pitcher_arm_angles.csv
_swing_take: dict[int, dict] = {}            # swing-take.csv — chase/shadow/waste zone runs


# ── Helpers ───────────────────────────────────────────────────────────────────

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


# ── Loader ────────────────────────────────────────────────────────────────────

def _load() -> None:
    global _loaded

    if _loaded:
        return

    with _load_lock:
        if _loaded:
            return

        logger.info("[StatcastStatic] Loading 2026 Statcast CSV data from %s", _DATA_DIR)

        # ── Pitcher arsenal: pitch-arsenal-stats-pitchers.csv ─────────────────
        arsenal_rows = _read_csv("pitch-arsenal-stats-pitchers.csv")
        _pit_k_total: dict[int, float] = {}
        _pit_k_usage: dict[int, float] = {}
        _pit_whiff_total: dict[int, float] = {}
        for _ar in arsenal_rows:
            try:
                _pid   = int(_ar.get("player_id", 0) or 0)
                _usage = float(_ar.get("pitch_usage", 0) or 0)
                _kpct  = float(_ar.get("k_percent",   0) or 0) / 100.0
                _whiff = float(_ar.get("whiff_percent", 0) or 0) / 100.0
                if _pid and _usage > 0:
                    _pit_k_total[_pid]     = _pit_k_total.get(_pid, 0) + _usage * _kpct
                    _pit_k_usage[_pid]     = _pit_k_usage.get(_pid, 0) + _usage
                    _pit_whiff_total[_pid] = _pit_whiff_total.get(_pid, 0) + _usage * _whiff
            except (ValueError, TypeError):
                pass
        for _pid in _pit_k_usage:
            if _pit_k_usage[_pid] > 0:
                _pitcher_k_rate[_pid] = round(_pit_k_total[_pid] / _pit_k_usage[_pid], 4)
                _pitcher_whiff[_pid]  = round(_pit_whiff_total[_pid] / _pit_k_usage[_pid], 4)

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
            whiff = _safe_float(r.get("whiff_percent"), 0.0) or 0.0
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

        # Re-derive weighted K rate from full arsenal dict
        for pid, pitches in _pitcher_arsenal.items():
            total = sum(p["usage"] for p in pitches.values())
            if total <= 0:
                continue
            wk = sum(p["k_pct"]    * p["usage"] for p in pitches.values()) / total
            ww = sum(p["whiff_pct"] * p["usage"] for p in pitches.values()) / total
            if wk > 0:
                _pitcher_k_rate[pid] = round(wk / 100.0, 4)  # % → decimal
            if ww > 0:
                _pitcher_whiff[pid]  = round(ww / 100.0, 4)

        # ── Pitcher xERA + extended expected stats (expected-stats-pitchers.csv) ─
        for r in _read_csv("expected-stats-pitchers.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            xera = _safe_float(r.get("xera"))
            if xera:
                _pitcher_xera[pid] = round(xera, 3)
            era_val   = _safe_float(r.get("era"))
            est_woba  = _safe_float(r.get("est_woba"))
            woba_allowed = _safe_float(r.get("woba"))
            xera_diff = _safe_float(r.get("era_minus_xera_diff"))
            _pitcher_expected[pid] = {
                "era":              era_val,
                "xera":             xera if xera else None,
                "est_woba_allowed": est_woba,
                "woba_allowed":     woba_allowed,
                "era_minus_xera":   xera_diff,   # negative = pitching better than ERA shows
                "est_ba_allowed":   _safe_float(r.get("est_ba")),
            }

        # ── Pitcher percentile ranks (percentile_rankings-pitchers.csv) ────────
        for r in _read_csv("percentile_rankings-pitchers.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _pitcher_percentiles[pid] = {
                "xwoba_pct":      _safe_float(r.get("xwoba")),
                "brl_pct_rank":   _safe_float(r.get("brl_percent")),
                "ev_rank":        _safe_float(r.get("exit_velocity")),
                "hard_hit_rank":  _safe_float(r.get("hard_hit_percent")),
                "k_pct_rank":     _safe_float(r.get("k_percent")),
                "bb_pct_rank":    _safe_float(r.get("bb_percent")),
                "whiff_rank":     _safe_float(r.get("whiff_percent")),
                "chase_rank":     _safe_float(r.get("chase_percent")),
                "xera_pct":       _safe_float(r.get("xera")),
                "fb_velo_rank":   _safe_float(r.get("fb_velocity")),
                "fb_spin_rank":   _safe_float(r.get("fb_spin")),
            }

        # ── Pitcher combined Statcast (statcast_pitchers_2026.csv) ────────────
        for r in _read_csv("statcast_pitchers_2026.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _pitcher_statcast[pid] = {
                "k_pct":              _safe_float(r.get("k_percent")),
                "bb_pct":             _safe_float(r.get("bb_percent")),
                "woba_against":       _safe_float(r.get("woba")),
                "xwoba_against":      _safe_float(r.get("xwoba")),
                "barrel_against_pct": _safe_float(r.get("barrel_batted_rate")),
                "hard_hit_against":   _safe_float(r.get("hard_hit_percent")),
                "whiff_pct":          _safe_float(r.get("whiff_percent")),
                "swing_pct":          _safe_float(r.get("swing_percent")),
                "sweet_spot_against": _safe_float(r.get("sweet_spot_percent")),
            }

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

        # ── Batter expected stats (batter xwOBA, xBA, xSLG) ──────────────────
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

        # ── Batter percentile ranks (batter leaderboard) ─────────────────────
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

        # ── FanGraphs batter projections (name-based) ────────────────────────
        for r in _read_csv("fg_batter_proj.csv"):
            name = r.get("name", "").strip()
            if not name:
                continue
            key   = _norm_name(name)
            k_pct = _safe_float(r.get("k_pct"))
            bb_pct = _safe_float(r.get("bb_pct"))
            woba  = _safe_float(r.get("woba"))
            wrc   = _safe_float(r.get("wrc_plus"))
            iso   = _safe_float(r.get("iso"))
            if k_pct:
                _batter_fg_proj[key] = {
                    "k_pct":    round(k_pct / 100.0, 4),
                    "bb_pct":   round((bb_pct or 0.0) / 100.0, 4),
                    "woba":     woba or 0.0,
                    "wrc_plus": int(wrc) if wrc else 100,
                    "iso":      iso or 0.0,
                }

        # ── Sprint speed ──────────────────────────────────────────────────────
        for r in _read_csv("sprint_speed.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            spd = _safe_float(r.get("sprint_speed"))
            if spd:
                _sprint_speed_data[pid] = {
                    "sprint_speed": spd,
                    "bolts":        int(_safe_float(r.get("bolts")) or 0),
                    "hp_to_1b":     _safe_float(r.get("hp_to_1b")) or 0.0,
                }

        # ── Baserunning run value ─────────────────────────────────────────────
        for r in _read_csv("baserunning_run_value.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            rv = _safe_float(r.get("runner_runs_tot"))
            if rv is not None:
                _baserunning_data[pid] = rv

        # ── Batter combined Statcast (statcast_batters_2026.csv) ──────────────
        for r in _read_csv("statcast_batters_2026.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _batter_statcast[pid] = {
                "k_pct":           _safe_float(r.get("k_percent")),
                "bb_pct":          _safe_float(r.get("bb_percent")),
                "woba":            _safe_float(r.get("woba")),
                "xwoba":           _safe_float(r.get("xwoba")),
                "sweet_spot_pct":  _safe_float(r.get("sweet_spot_percent")),
                "barrel_pct":      _safe_float(r.get("barrel_batted_rate")),
                "hard_hit_pct":    _safe_float(r.get("hard_hit_percent")),
                "bat_speed_best":  _safe_float(r.get("avg_best_speed")),
                "bat_speed_hyper": _safe_float(r.get("avg_hyper_speed")),
                "whiff_pct":       _safe_float(r.get("whiff_percent")),
                "swing_pct":       _safe_float(r.get("swing_percent")),
            }

        # ── Batter vs pitch type (primary: batter_pitch_arsenal_2026.csv) ─────
        for r in _read_csv("batter_pitch_arsenal_2026.csv"):
            try:
                pid = int(r["player_id"])
                pt  = r["pitch_type"].strip().upper()
                _batter_vs_pitch[(pid, pt)] = {
                    "woba":             float(r.get("woba") or 0),
                    "xwoba":            float(r.get("est_woba") or 0),
                    "whiff_pct":        float(r.get("whiff_percent") or 0),
                    "k_pct":            float(r.get("k_percent") or 0),
                    "hard_hit_pct":     float(r.get("hard_hit_percent") or 0),
                    "put_away":         float(r.get("put_away") or 0),
                    "run_value_per100": float(r.get("run_value_per_100") or 0),
                }
            except (ValueError, KeyError):
                continue

        # ── Batter vs pitch type (supplement: pitch-arsenal-stats-batters.csv) ─
        # Same structure — only fills gaps not covered by the larger CSV above.
        for r in _read_csv("pitch-arsenal-stats-batters.csv"):
            try:
                pid = int(r["player_id"])
                pt  = r["pitch_type"].strip().upper()
                key = (pid, pt)
                if key in _batter_vs_pitch:
                    continue  # don't overwrite larger dataset
                _batter_vs_pitch[key] = {
                    "woba":             float(r.get("woba") or 0),
                    "xwoba":            float(r.get("est_woba") or 0),
                    "whiff_pct":        float(r.get("whiff_percent") or 0),
                    "k_pct":            float(r.get("k_percent") or 0),
                    "hard_hit_pct":     float(r.get("hard_hit_percent") or 0),
                    "put_away":         float(r.get("put_away") or 0),
                    "run_value_per100": float(r.get("run_value_per_100") or 0),
                }
            except (ValueError, KeyError):
                continue

        # ── Batter vs LHP (batter_vs_lhp_2026.csv) ───────────────────────────
        for r in _read_csv("batter_vs_lhp_2026.csv"):
            pid_s = r.get("player_id", "").strip()
            if not pid_s:
                continue
            _BATTER_VS_LHP[pid_s] = {
                "woba_vs_lhp":   _safe_float(r.get("woba_vs_lhp")),
                "k_pct_vs_lhp":  _safe_float(r.get("k_pct_vs_lhp")),
                "whiff_pct_vs_lhp": _safe_float(r.get("whiff_pct_vs_lhp")),
                "pa":            _safe_float(r.get("pa")),
            }

        # ── Batter vs pitch type from LHP (batter_pitch_vs_lhp_2026.csv) ─────
        for r in _read_csv("batter_pitch_vs_lhp_2026.csv"):
            pid_s = r.get("player_id", "").strip()
            pt    = r.get("pitch_type", "").strip().upper()
            if not pid_s or not pt:
                continue
            _BATTER_PITCH_VS_LHP[(pid_s, pt)] = {
                "woba_vs_pitch":    _safe_float(r.get("woba_vs_pitch")),
                "whiff_pct_vs_pitch": _safe_float(r.get("whiff_pct_vs_pitch")),
                "pa_vs_pitch":      _safe_float(r.get("pa_vs_pitch")),
            }

        # ── Spin direction / active spin (spin_direction_pitches_2026.csv) ────
        for r in _read_csv("spin_direction_pitches_2026.csv"):
            pid_s = r.get("player_id", "").strip()
            pt    = r.get("api_pitch_type", "").strip()
            if not pid_s or not pt:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _spin_direction[(pid, pt)] = {
                "active_spin_pct": _safe_float(r.get("alan_active_spin_pct")),
                "clock_label":     r.get("hawkeye_measured_clock_label", "").strip(),
                "movement_inches": _safe_float(r.get("movement_inches")),
                "spin_rate":       _safe_float(r.get("spin_rate")),
                "release_speed":   _safe_float(r.get("release_speed")),
                "n_pitches":       _safe_float(r.get("n_pitches")),
            }

        # ── Pitcher arm angles (pitcher_arm_angles.csv) ──────────────────────────
        for r in _read_csv("pitcher_arm_angles.csv"):
            pid_s = r.get("pitcher", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            _pitcher_arm_angles[pid] = {
                "ball_angle":  _safe_float(r.get("ball_angle")),   # degrees: 0=overhead, 90=sidearm
                "pitch_hand":  r.get("pitch_hand", "").strip(),
                "release_z":   _safe_float(r.get("release_ball_z")),
                "shoulder_z":  _safe_float(r.get("shoulder_z")),
                "n_pitches":   _safe_float(r.get("n_pitches")),
            }
        logger.info("[StatcastStatic] pitcher_arm_angles: %d pitchers loaded", len(_pitcher_arm_angles))

        # ── Batter swing-take discipline (swing-take.csv) ────────────────────────
        # runs_heart/shadow/chase/waste: run value gained/lost per zone decision
        # Negative runs_chase = losing runs from chasing = more Ks expected
        for r in _read_csv("swing-take.csv"):
            try:
                pid = int(r.get("player_id", 0) or 0)
                if not pid:
                    continue
                pa  = max(1, int(r.get("pa", 1) or 1))
                _swing_take[pid] = {
                    "runs_chase_pa":  round(_safe_float(r.get("runs_chase"))  / pa, 5) if r.get("runs_chase")  else None,
                    "runs_shadow_pa": round(_safe_float(r.get("runs_shadow")) / pa, 5) if r.get("runs_shadow") else None,
                    "runs_heart_pa":  round(_safe_float(r.get("runs_heart"))  / pa, 5) if r.get("runs_heart")  else None,
                    "runs_waste_pa":  round(_safe_float(r.get("runs_waste"))  / pa, 5) if r.get("runs_waste")  else None,
                    "pa": pa,
                }
            except (ValueError, TypeError, ZeroDivisionError):
                continue
        logger.info("[StatcastStatic] swing_take: %d batters loaded", len(_swing_take))

        # ── Bat tracking swing path (bat-tracking-swing-path.csv) ────────────
        # Supplements _batter_tracking with attack_angle, swing_tilt, ideal_attack_rate
        _swing_path_count = 0
        for r in _read_csv("bat-tracking-swing-path.csv"):
            pid_s = r.get("id", "").strip()
            if not pid_s:
                continue
            try:
                pid = int(pid_s)
            except ValueError:
                continue
            entry = _batter_tracking.get(pid, {})
            entry["swing_tilt"]        = _safe_float(r.get("swing_tilt"))        # degrees: higher = more uppercut
            entry["attack_angle"]      = _safe_float(r.get("attack_angle"))      # degrees: optimal 8–12
            entry["ideal_attack_rate"] = _safe_float(r.get("ideal_attack_angle_rate"))  # fraction 0–1
            entry["attack_direction"]  = _safe_float(r.get("attack_direction"))  # + = pull-biased, - = oppo
            _batter_tracking[pid] = entry
            _swing_path_count += 1
        logger.info("[StatcastStatic] bat-tracking-swing-path: %d batters supplemented", _swing_path_count)

        # ── Historical batter K% trend (statcast_batters_historical.csv) ──────
        # Store per-player list of {year, k_pct, woba, xwoba, whiff_pct, barrel_pct, hard_hit_pct}
        # Keep 2023 onward (earlier years less predictive)
        for r in _read_csv("statcast_batters_historical.csv"):
            pid_s = r.get("player_id", "").strip()
            yr_s  = r.get("year", "").strip()
            if not pid_s or not yr_s:
                continue
            try:
                pid = int(pid_s)
                yr  = int(yr_s)
            except ValueError:
                continue
            if yr < 2023:
                continue
            entry = {
                "year":        yr,
                "k_pct":       _safe_float(r.get("k_percent")),
                "bb_pct":      _safe_float(r.get("bb_percent")),
                "woba":        _safe_float(r.get("woba")),
                "xwoba":       _safe_float(r.get("xwoba")),
                "whiff_pct":   _safe_float(r.get("whiff_percent")),
                "barrel_pct":  _safe_float(r.get("barrel_batted_rate")),
                "hard_hit_pct": _safe_float(r.get("hard_hit_percent")),
                "pa":          _safe_float(r.get("pa")),
            }
            if pid not in _batter_k_history:
                _batter_k_history[pid] = []
            _batter_k_history[pid].append(entry)

        # Sort each player's history by year ascending
        for pid in _batter_k_history:
            _batter_k_history[pid].sort(key=lambda x: x["year"])

        # ── Pitcher arsenal by handedness (RHP/LHP) ───────────────────────────
        for hand, fname, target in [
            ("RHP", "pitcher_arsenal_rhp_2026.csv", _PITCHER_ARSENAL_RHP),
            ("LHP", "pitcher_arsenal_lhp_2026.csv", _PITCHER_ARSENAL_LHP),
        ]:
            for r in _read_csv(fname):
                pid = str(r.get("player_id", "")).strip()
                if not pid:
                    continue
                target[pid] = {
                    "k_pct":              _safe_float(r.get("k_percent")),
                    "woba_against":       _safe_float(r.get("woba")),
                    "hard_hit_against":   _safe_float(r.get("hardhit_percent")),
                    "barrel_pct_against": _safe_float(r.get("barrels_per_bbe_percent")),
                    "whiff_pct":          _safe_float(r.get("swing_miss_percent")),
                    "arm_angle":          _safe_float(r.get("arm_angle")),
                    "pa":                 _safe_float(r.get("pa")),
                    "name":               r.get("player_name", ""),
                }
            logger.info("pitcher_arsenal_%s: %d pitchers loaded", hand, len(target))

        _loaded = True
        logger.info(
            "[StatcastStatic] Loaded: %d pitcher K rates, %d pitcher xERAs, %d pitcher percentiles, "
            "%d batter tracking, %d batter EV, %d batter xStats, "
            "%d FG proj, %d sprint, %d baserunning, %d spin-dir entries, "
            "%d batter-vs-pitch, %d vs-LHP, %d batter-k-history",
            len(_pitcher_k_rate), len(_pitcher_xera), len(_pitcher_percentiles),
            len(_pitcher_arm_angles),
            len(_batter_tracking), len(_batter_ev), len(_batter_xstats),
            len(_batter_fg_proj), len(_sprint_speed_data), len(_baserunning_data),
            len(_spin_direction), len(_batter_vs_pitch),
            len(_BATTER_VS_LHP), len(_batter_k_history),
        )


# ── Public API ────────────────────────────────────────────────────────────────

# -- Pitcher --

def get_pitcher_k_rate(player_id: int) -> float | None:
    """Pitcher K% for 2026.

    Tier 1: statcast_pitchers_2026.csv actual K% (direct measurement).
    Tier 2: arsenal-weighted K% from pitch-arsenal-stats-pitchers.csv.
    Returns decimal (e.g. 0.283 = 28.3% K rate).
    """
    _load()
    pid = int(player_id)
    sc  = _pitcher_statcast.get(pid, {})
    if sc.get("k_pct") is not None:
        return round(sc["k_pct"] / 100.0, 4)
    return _pitcher_k_rate.get(pid)


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


def get_pitcher_statcast(player_id: int) -> dict:
    """Full 2026 Statcast combined stats for a pitcher.

    Keys: k_pct, bb_pct, woba_against, xwoba_against, barrel_against_pct,
          hard_hit_against, whiff_pct, swing_pct, sweet_spot_against.
    k_pct is raw % (e.g. 28.3 = 28.3% K rate). Returns {} if not found.
    """
    _load()
    return _pitcher_statcast.get(int(player_id), {})


def get_pitcher_percentiles(player_id: int) -> dict:
    """Pitcher 2026 Statcast percentile ranks (0–100 scale).

    Keys: xwoba_pct, brl_pct_rank, ev_rank, hard_hit_rank,
          k_pct_rank, bb_pct_rank, whiff_rank, chase_rank,
          xera_pct, fb_velo_rank, fb_spin_rank.
    High k_pct_rank (e.g. 90) = elite strikeout pitcher.
    Returns {} if not found.
    """
    _load()
    return _pitcher_percentiles.get(int(player_id), {})


def get_pitcher_expected_stats(player_id: int) -> dict:
    """Pitcher 2026 expected vs actual stats.

    Keys: era, xera, est_woba_allowed, woba_allowed, era_minus_xera, est_ba_allowed.
    era_minus_xera < 0 means pitcher is outperforming (ERA < xERA → lucky).
    era_minus_xera > 0 means pitcher due for regression (ERA > xERA → unlucky).
    Returns {} if not found.
    """
    _load()
    return _pitcher_expected.get(int(player_id), {})


def get_pitcher_active_spin(player_id: int, pitch_type: str) -> dict:
    """Active spin / spin direction for a pitcher's specific pitch type.

    Args:
        player_id: MLBAM player ID
        pitch_type: Savant pitch type code (e.g. 'FF', 'SL', 'CH')

    Returns dict with:
        active_spin_pct  – fraction of spin that is 'active' (0.0–1.0)
        clock_label      – spin axis as clock position (e.g. '12:00', '1:30')
        movement_inches  – total movement in inches
        spin_rate        – average spin rate (RPM)
        release_speed    – average velocity (mph)
    Returns {} if not found.
    """
    _load()
    return _spin_direction.get((int(player_id), pitch_type.upper()), {})


def get_pitcher_spin_profile(player_id: int) -> dict[str, dict]:
    """All spin direction entries for a pitcher, keyed by pitch_type code.

    Returns {} if not found.
    """
    _load()
    pid = int(player_id)
    return {pt: data for (p, pt), data in _spin_direction.items() if p == pid}


def get_pitcher_arsenal_vs_hand(pitcher_id: int | str, batter_hand: str) -> dict:
    """Return pitcher stats vs given batter handedness (R/L).

    Keys: k_pct, woba_against, hard_hit_against, barrel_pct_against, whiff_pct, arm_angle, pa
    Returns empty dict if pitcher not found.
    """
    _load()
    pid    = str(pitcher_id)
    target = _PITCHER_ARSENAL_RHP if batter_hand.upper() == "R" else _PITCHER_ARSENAL_LHP
    return target.get(pid, {})


# -- Batter --

def get_batter_k_susceptibility(player_id: int) -> float | None:
    """Batter K-susceptibility. Higher = more K-prone.

    Tier 1: statcast_batters_2026.csv whiff% (overall swing whiff rate).
    Tier 2: bat-tracking.csv whiff_per_swing.
    Returns decimal (e.g. 0.26 = 26% whiff rate).
    """
    _load()
    pid = int(player_id)
    sc  = _batter_statcast.get(pid, {})
    if sc.get("whiff_pct") is not None:
        return round(sc["whiff_pct"] / 100.0, 4)
    bt = _batter_tracking.get(pid, {})
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


def get_batter_statcast(player_id: int) -> dict:
    """Full 2026 Statcast combined stats for a batter.

    Keys: k_pct, bb_pct, woba, xwoba, sweet_spot_pct, barrel_pct,
          hard_hit_pct, bat_speed_best, bat_speed_hyper, whiff_pct, swing_pct.
    Percentages are raw % (e.g. 21.4 means 21.4%). Returns {} if not found.
    """
    _load()
    return _batter_statcast.get(int(player_id), {})


def get_batter_fg_proj(player_name: str) -> dict | None:
    """Return FanGraphs 2026 projected stats keyed by player name.

    Returns dict with k_pct (decimal), bb_pct, woba, wrc_plus, iso.
    k_pct=0.189 means 18.9% projected strikeout rate.
    """
    _load()
    key  = _norm_name(player_name)
    proj = _batter_fg_proj.get(key)
    if proj:
        return proj
    # Fuzzy: try last name only match
    last = key.split()[-1] if key.split() else key
    for k, v in _batter_fg_proj.items():
        if k.endswith(last) and len(last) > 4:
            return v
    return None


def get_batter_sprint_speed(player_id: int) -> dict | None:
    """Return sprint speed metrics for a batter.

    Keys: sprint_speed (ft/s), bolts (runs ≥30 ft/s), hp_to_1b (seconds).
    League avg sprint speed ~27 ft/s; elite ≥30.
    """
    _load()
    return _sprint_speed_data.get(int(player_id))


def get_batter_baserunning(player_id: int) -> float | None:
    """Return FanGraphs baserunning run value for the season.

    Positive = above-average baserunner. League avg ≈ 0.
    """
    _load()
    return _baserunning_data.get(int(player_id))


def get_batter_vs_pitch(batter_id: int, pitch_type: str) -> dict:
    """Return 2026 batter performance against a specific pitch type.

    pitch_type: Savant abbreviation — FF, SL, CH, CU, ST, FC, SI, FS, SV, KN.
    Returns dict with keys: woba, xwoba, whiff_pct, k_pct, hard_hit_pct,
                            put_away, run_value_per100.
    Returns {} if batter or pitch type not found.
    """
    _load()
    return _batter_vs_pitch.get((int(batter_id), pitch_type.upper()), {})


def get_batter_lhp_splits(batter_id: int | str) -> dict:
    """Return 2026 season-to-date splits for this batter vs LHP.

    Returns dict with woba_vs_lhp, k_pct_vs_lhp, whiff_pct_vs_lhp, pa
    or {} if unknown.
    """
    _load()
    return _BATTER_VS_LHP.get(str(batter_id), {})


def get_batter_pitch_vs_lhp(batter_id: int | str, pitch_type: str) -> dict:
    """Return 2026 stats for batter vs specific pitch type thrown by LHP.

    Returns dict with woba_vs_pitch, whiff_pct_vs_pitch, pa_vs_pitch or {} if unknown.
    """
    _load()
    return _BATTER_PITCH_VS_LHP.get((str(batter_id), pitch_type.upper()), {})


def get_batter_k_trend(player_id: int) -> dict:
    """Multi-year K% trend for a batter (2023 onward).

    Returns dict with:
        k_pct_2023, k_pct_2024, k_pct_2025   — historical K% (raw %)
        trend_delta                            — 2025 K% minus 2023 K%
                                                  (positive = K% rising = regression warning)
        most_recent_year, most_recent_k_pct
        records                               — full list sorted by year

    Returns {} if player not found in historical data.

    Usage:
        trend = get_batter_k_trend(player_id)
        if trend.get('trend_delta', 0) > 3:
            # K% rising 3pp → flag as regression candidate
    """
    _load()
    records = _batter_k_history.get(int(player_id))
    if not records:
        return {}

    result: dict = {"records": records}
    for entry in records:
        yr = entry.get("year")
        if yr:
            result[f"k_pct_{yr}"] = entry.get("k_pct")
            result[f"woba_{yr}"]  = entry.get("woba")

    # Most recent year
    last = records[-1]
    result["most_recent_year"]  = last.get("year")
    result["most_recent_k_pct"] = last.get("k_pct")

    # Trend: latest minus earliest (within 2023+)
    if len(records) >= 2:
        earliest_k = records[0].get("k_pct")
        latest_k   = records[-1].get("k_pct")
        if earliest_k is not None and latest_k is not None:
            result["trend_delta"] = round(latest_k - earliest_k, 2)
    return result


def get_pitcher_arm_angle(player_id: int) -> dict:
    """Return pitcher arm angle data from pitcher_arm_angles.csv.

    Keys:
        ball_angle    – degrees from vertical (0 = straight overhead, 90 = pure sidearm)
        pitch_hand    – 'R' or 'L'
        release_z     – release height in feet
        shoulder_z    – shoulder height in feet
        n_pitches     – sample size

    Deception guide:
        ball_angle < 20   → nearly overhand — conventional, fewer K via deception
        ball_angle 20–35  → three-quarter — most common; baseline
        ball_angle 35–55  → low three-quarter / sidearm — deceptive for same-side batters
        ball_angle > 55   → submarine — extreme deception vs same-side, easier vs opposite

    Returns {} if not found.
    """
    _load()
    return _pitcher_arm_angles.get(int(player_id), {})


def get_batter_chase_discipline(player_id: int) -> dict:
    """Return batter swing-take zone discipline data.

    Keys: runs_chase_pa, runs_shadow_pa, runs_heart_pa, runs_waste_pa, pa
      runs_chase_pa < 0  → losing runs on chases → high K susceptibility
      runs_chase_pa > 0  → disciplined or good contact on chase pitches

    Falls back to empty dict if player not in dataset.
    """
    if not _LOADED:
        _load()
    try:
        return _swing_take.get(int(player_id), {})
    except (ValueError, TypeError):
        return {}


# -- Matchup --

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

    pitcher_edge = logit(pitcher_k) - logit(_LG_PITCHER_K)
    batter_edge  = logit(batter_w)  - logit(_LG_BATTER_W)

    # Weight pitcher more heavily (60/40) — pitcher is primary driver
    combined = pitcher_edge * 0.60 + batter_edge * 0.40
    return max(-0.30, min(0.30, combined))
