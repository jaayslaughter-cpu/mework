"""
fg_pitcher_quality_layer.py
============================
SIERA-proxy + quality-start signal layer for pitcher props.

Loaded once at import from data/fg/fg_pitcher_stats_2026.csv (computed
from 2026 game logs: K/9, BB/9, WHIP, ERA, SIERA-proxy, QS rate).

Wire-in:  prop_enrichment_layer.py → pitcher prop block
          from fg_pitcher_quality_layer import get_pitcher_quality_adj

Returns a dict with:
    siera_adj         float  ±0.040  overall SIERA quality adjustment
    qs_prob           float  0–1     quality-start probability
    qs_adj            float  ±0.030  QS vs league avg (0.45) nudge
    k9                float  raw K/9 rate
    bb9               float  raw BB/9 rate
    model_prob_adj    float  ±0.040  prop-type-specific nudge to model_prob
    _source           str    "fg_pitcher_quality_2026"

Prop-type adjustments (all capped ±4pp):
    pitching_outs   → SIERA adj + QS adj (deeper starts → more outs)
    strikeouts      → K9 quality adj (K/9 vs 8.5 avg)
    walks_allowed   → BB9 quality adj (BB/9 vs 3.2 avg, reversed)
    hits_allowed    → H9 quality adj (H/9 vs 8.8 avg, reversed)
    earned_runs     → ERA quality adj (ERA vs 4.25 avg, reversed)
    hitter_strikeouts → K9 quality adj (facing elite K pitcher = more Ks)
"""

from __future__ import annotations

import csv
import logging
import os
import re
from typing import Optional

logger = logging.getLogger("propiq.fg_pitcher_quality")

# ---------------------------------------------------------------------------
# League-average baselines (2026 MLB)
# ---------------------------------------------------------------------------
_LG_SIERA   = 3.75
_LG_ERA     = 4.18   # FG 2026: through game 44 (was 4.25)
_LG_K9      = 8.50
_LG_BB9     = 3.20
_LG_H9      = 8.80
_LG_QS_RATE = 0.45

# CSV path — relative to this file (repo root)
_CSV_RELATIVE = os.path.join("data", "fg", "fg_pitcher_stats_2026.csv")

# ---------------------------------------------------------------------------
# Module-level data store
# ---------------------------------------------------------------------------
_BY_MLBAM: dict[int, dict]   = {}   # mlbam_id → row
_BY_NAME:  dict[str, dict]   = {}   # normalised name → row
_LOADED    = False


def _norm(name: str) -> str:
    s = str(name or "").lower()
    for old, new in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),
                     ("ñ","n"),("ü","u"),("ö","o"),("ä","a")]:
        s = s.replace(old, new)
    return re.sub(r"[^a-z ]", "", s).strip()


def _load() -> None:
    global _LOADED
    if _LOADED:
        return

    # Find CSV relative to this file
    this_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(this_dir, _CSV_RELATIVE)

    if not os.path.isfile(csv_path):
        logger.warning("[PitcherQuality] CSV not found at %s — layer inactive", csv_path)
        _LOADED = True
        return

    loaded = 0
    with open(csv_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                mlbam = int(row["mlbam_id"])
                rec = {
                    "mlbam_id":    mlbam,
                    "player":      row["player"],
                    "num_starts":  int(row.get("num_starts", 0) or 0),
                    "ip":          float(row.get("ip", 0) or 0),
                    "era":         float(row.get("era", _LG_ERA) or _LG_ERA),
                    "k9":          float(row.get("k9",  _LG_K9)  or _LG_K9),
                    "bb9":         float(row.get("bb9", _LG_BB9) or _LG_BB9),
                    "h9":          float(row.get("h9",  _LG_H9)  or _LG_H9),
                    "whip":        float(row.get("whip", 1.28)   or 1.28),
                    "k_pct":       float(row.get("k_pct", 0.223) or 0.223),
                    "bb_pct":      float(row.get("bb_pct", 0.087) or 0.087),
                    "fip":         float(row.get("fip",  _LG_ERA) or _LG_ERA),
                    "siera_proxy": float(row.get("siera_proxy", _LG_SIERA) or _LG_SIERA),
                    "qs_rate":     float(row.get("qs_rate", _LG_QS_RATE) or _LG_QS_RATE),
                    "qs_prob":     float(row.get("qs_prob", _LG_QS_RATE) or _LG_QS_RATE),
                }
                _BY_MLBAM[mlbam] = rec
                _BY_NAME[_norm(row["player"])] = rec
                # Also store last-name key for fuzzy fallback
                parts = _norm(row["player"]).split()
                if parts:
                    _BY_NAME.setdefault(parts[-1], rec)
                loaded += 1
            except (KeyError, ValueError):
                continue

    logger.info("[PitcherQuality] Loaded %d pitchers from %s", loaded, csv_path)
    _LOADED = True


def _lookup(prop: dict) -> Optional[dict]:
    """Return CSV row for this prop's pitcher, or None."""
    _load()
    # Try MLBAM ID first (exact)
    for key in ("mlbam_id", "player_id"):
        raw = prop.get(key)
        if raw:
            try:
                rec = _BY_MLBAM.get(int(raw))
                if rec:
                    return rec
            except (TypeError, ValueError):
                pass

    # Try full name
    player = prop.get("player", "")
    norm_name = _norm(player)
    if norm_name in _BY_NAME:
        return _BY_NAME[norm_name]

    # Try last name only
    parts = norm_name.split()
    if parts and parts[-1] in _BY_NAME:
        return _BY_NAME[parts[-1]]

    return None


# ---------------------------------------------------------------------------
# Per-prop-type adjustment
# ---------------------------------------------------------------------------

def _siera_nudge(siera: float) -> float:
    """
    Translate SIERA relative to league average into a probability nudge.
    Lower SIERA = better pitcher = positive nudge for OVER props
    (more Ks, deeper into game, fewer walks).

    Scale: ±1.0 SIERA from avg (3.75) → ±0.033 pp, capped ±0.040.
    """
    delta = _LG_SIERA - siera        # positive when pitcher is elite
    nudge = delta * 0.033
    return max(-0.040, min(0.040, nudge))


def _k9_nudge(k9: float) -> float:
    """K/9 vs league avg 8.5 → nudge. Each +1 K/9 above avg → +0.015."""
    delta = k9 - _LG_K9
    nudge = delta * 0.015
    return max(-0.040, min(0.040, nudge))


def _bb9_nudge(bb9: float) -> float:
    """BB/9 vs league avg 3.2. LOWER is better for pitcher K/outs props.
    Each −1 BB/9 below avg → +0.010 nudge (reversed for walks_allowed prop)."""
    delta = _LG_BB9 - bb9            # positive when pitcher walks fewer
    nudge = delta * 0.010
    return max(-0.030, min(0.030, nudge))


def _h9_nudge(h9: float) -> float:
    """H/9 vs league avg 8.8 — lower is better. Reversed for hits_allowed."""
    delta = _LG_H9 - h9
    nudge = delta * 0.012
    return max(-0.035, min(0.035, nudge))


def _qs_adj(qs_prob: float) -> float:
    """QS probability vs league avg 0.45 → nudge (deeper starts = more outs)."""
    delta = qs_prob - _LG_QS_RATE
    nudge = delta * 0.067            # ±15pp QS rate → ±1pp
    return max(-0.030, min(0.030, nudge))


def get_pitcher_quality_adj(prop: dict) -> dict:
    """
    Main entry point. Called from prop_enrichment_layer for pitcher props.

    Returns dict with siera_adj, qs_prob, qs_adj, k9, bb9, model_prob_adj.
    Returns empty dict if pitcher not in CSV.
    """
    rec = _lookup(prop)
    if not rec:
        return {}

    # Require at least 3 starts for reliable signal
    if rec["num_starts"] < 3:
        return {}

    prop_type  = (prop.get("prop_type") or "").lower()
    siera      = rec["siera_proxy"]
    k9         = rec["k9"]
    bb9        = rec["bb9"]
    h9         = rec["h9"]
    era        = rec["era"]
    qs_p       = rec["qs_prob"]

    s_nudge  = _siera_nudge(siera)
    k_nudge  = _k9_nudge(k9)
    bb_nudge = _bb9_nudge(bb9)
    h_nudge  = _h9_nudge(h9)
    q_nudge  = _qs_adj(qs_p)

    # ERA nudge for earned_runs
    era_delta  = _LG_ERA - era          # positive when pitcher gives up fewer ER
    era_nudge  = max(-0.040, min(0.040, era_delta * 0.020))

    # ── Prop-type-specific model_prob adjustment ───────────────────────────
    if prop_type in ("pitching_outs",):
        # SIERA (quality/depth) + QS probability both matter
        mp_adj = max(-0.040, min(0.040, s_nudge * 0.60 + q_nudge * 0.40))

    elif prop_type in ("strikeouts",):
        # K/9 is primary; SIERA secondary (command/contact vs power)
        mp_adj = max(-0.040, min(0.040, k_nudge * 0.70 + s_nudge * 0.30))

    elif prop_type in ("walks_allowed",):
        # Lower BB/9 = lower walk rate = UNDER walks_allowed favored
        # For OVER side: reversed (high BB pitcher → more walks)
        # nudge is from pitcher perspective; caller uses as-is, agents handle side
        mp_adj = max(-0.030, min(0.030, -bb_nudge))  # flip: good control → UNDER

    elif prop_type in ("hits_allowed",):
        # Lower H/9 = fewer hits allowed = UNDER hits_allowed favored
        mp_adj = max(-0.035, min(0.035, -h_nudge))   # flip

    elif prop_type in ("earned_runs",):
        # Lower ERA = fewer ER = UNDER earned_runs favored
        mp_adj = max(-0.040, min(0.040, -era_nudge))  # flip

    elif prop_type in ("hitter_strikeouts",):
        # Batter faces elite K pitcher → batter more likely to strike out
        # OVER hitter_strikeouts is favored against high-K9 pitchers
        mp_adj = max(-0.040, min(0.040, k_nudge * 0.80 + s_nudge * 0.20))

    else:
        # Generic: use SIERA as catch-all quality signal
        mp_adj = s_nudge

    # Convert to percentage points (0–100 scale used in model_prob)
    mp_adj_pp = round(mp_adj * 100.0, 2)

    result = {
        "siera_adj":       round(s_nudge, 4),
        "siera_proxy":     siera,
        "qs_prob":         qs_p,
        "qs_adj":          round(q_nudge, 4),
        "k9":              k9,
        "bb9":             bb9,
        "h9":              h9,
        "era":             era,
        "fip":             rec["fip"],
        "whip":            rec["whip"],
        "num_starts":      rec["num_starts"],
        "model_prob_adj":  mp_adj_pp,   # ±4pp, in model_prob units (0–100)
        "_source":         "fg_pitcher_quality_2026",
    }

    logger.debug(
        "[PitcherQuality] %s  prop=%s  siera=%.3f  k9=%.1f  bb9=%.1f  "
        "qs=%.2f  mp_adj=%+.2fpp",
        prop.get("player", "?"), prop_type, siera, k9, bb9, qs_p, mp_adj_pp,
    )
    return result


# ---------------------------------------------------------------------------
# Convenience functions for other layers
# ---------------------------------------------------------------------------

def get_siera_proxy(player: str, mlbam_id: int | None = None) -> float:
    """Return SIERA proxy for a pitcher, or league average if unknown."""
    _load()
    if mlbam_id:
        rec = _BY_MLBAM.get(int(mlbam_id))
        if rec:
            return rec["siera_proxy"]
    rec = _BY_NAME.get(_norm(player))
    return rec["siera_proxy"] if rec else _LG_SIERA


def get_qs_probability(player: str, mlbam_id: int | None = None) -> float:
    """Return QS probability (0–1) for a pitcher, or league average."""
    _load()
    if mlbam_id:
        rec = _BY_MLBAM.get(int(mlbam_id))
        if rec:
            return rec["qs_prob"]
    rec = _BY_NAME.get(_norm(player))
    return rec["qs_prob"] if rec else _LG_QS_RATE


def get_k9(player: str, mlbam_id: int | None = None) -> float:
    """Return 2026 K/9 for a pitcher, or league average."""
    _load()
    if mlbam_id:
        rec = _BY_MLBAM.get(int(mlbam_id))
        if rec:
            return rec["k9"]
    rec = _BY_NAME.get(_norm(player))
    return rec["k9"] if rec else _LG_K9


# ---------------------------------------------------------------------------
# PR #530 — PAR Score (Pitcher Appearance Rating)
# Source: sequencebaseball via propiq_signal_upgrades.compute_par_score()
# Use par_avg_5 / par_avg_3 as XGBoost K-model context features.
# ---------------------------------------------------------------------------

def get_pitcher_par_score(recent_game_logs: list) -> dict:
    """Compute PAR score context features for K prop evaluation.

    Args:
        recent_game_logs: list of game dicts with keys:
            outs_recorded, strikeouts, walks, earned_runs

    Returns dict with:
        par_avg_5   — rolling average PAR over last 5 starts (0–100)
        par_avg_3   — rolling average PAR over last 3 starts (more recent)
        par_last    — PAR score object for most recent start (None if no logs)
        par_grade   — letter grade of most recent start
    """
    try:
        from propiq_signal_upgrades import compute_par_score, rolling_par_avg

        par_avg_5 = rolling_par_avg(recent_game_logs, n_starts=5)
        par_avg_3 = rolling_par_avg(recent_game_logs, n_starts=3)

        par_last = None
        par_grade = None
        if recent_game_logs:
            last = recent_game_logs[-1]
            par_last = compute_par_score(
                pitcher_name="",
                outs_recorded=int(last.get("outs_recorded", 0)),
                strikeouts=int(last.get("strikeouts", 0)),
                walks=int(last.get("walks", 0)),
                earned_runs=int(last.get("earned_runs", 0)),
            )
            par_grade = par_last.grade

        return {
            "par_avg_5":  par_avg_5,
            "par_avg_3":  par_avg_3,
            "par_last":   par_last,
            "par_grade":  par_grade,
        }
    except Exception as exc:
        logger.debug("[FGPitcherQuality] PAR score unavailable: %s", exc)
        return {"par_avg_5": None, "par_avg_3": None, "par_last": None, "par_grade": None}
