"""
reliability_weights.py
======================
Per-feature reliability weights derived from MLB stabilization research.

Based on:
  - Fangraphs stabilization studies (Judge 2019, Sherfy 2021)
  - baseball_simulator_v2 WeightedRBFSimilarity (gmelick/baseball_simulator_v2)
  - Statcast sample size guidance

Used by prop_enrichment_layer and nudge stacks to dampen low-sample signals.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("propiq.reliability")

# Stabilization thresholds (PA for batters, BF for pitchers)
# n = threshold → alpha = 0.50 (50% weight on current data, 50% on prior)
_STABILIZATION_THRESHOLDS: dict[str, int] = {
    # Batter stats
    "o_swing":       80,    # Chase rate — fast (ABS makes it even faster)
    "whiff_rate":    80,    # Swinging strike rate
    "hard_hit_rate": 60,    # HH% — stabilizes quickly
    "contact_pct":   100,   # Zone contact
    "k_pct":         150,   # K rate
    "barrel_rate":   150,   # In BBE not PA
    "xwoba":         100,
    "xba":           100,
    "xslg":          120,
    "iso":           200,   # Isolated power
    "bb_pct":        200,   # Walk rate — slower
    "babip":         800,   # Extremely noisy — shrink hard
    "wrc_plus":      600,   # Park/context adjusted — slow
    "woba":          400,
    # Pitcher stats
    "csw_pct":       50,    # CSW% stabilizes very fast
    "swstr_pct":     80,
    "k_bb_pct":      200,
    "xfip":          400,
    "siera":         500,
    "era":           800,   # Highly variable
    "lob_pct":       1000,  # Almost pure noise early season
    "pitcher_babip": 1000,
}

# Research-backed per-feature weights for the similarity kernel
# Source: baseball_simulator_v2 WeightedRBFSimilarity normalization
# Higher = more reliable signal per unit deviation from mean
FEATURE_RELIABILITY_WEIGHTS: dict[str, float] = {
    "o_swing":       0.776,   # Contact%, Whiff% — from Statcast stabilization study
    "whiff_rate":    0.776,
    "hard_hit_rate": 0.720,
    "contact_pct":   0.689,   # O-Swing proxy
    "k_pct":         0.701,
    "barrel_rate":   0.333,   # Fewer BBE → lower reliability
    "xwoba":         0.650,
    "bb_pct":        0.558,
    "iso":           0.520,
    "babip":         0.150,   # Very low — mostly noise
    "wrc_plus":      0.400,
    "woba":          0.450,
    "csw_pct":       0.820,   # Fastest-stabilizing pitcher metric
    "swstr_pct":     0.776,
    "k_bb_pct":      0.650,
    "xfip":          0.520,
    "siera":         0.480,
    "era":           0.150,
    "lob_pct":       0.080,
}


def reliability_alpha(stat: str, n_sample: int) -> float:
    """
    Empirical Bayes reliability: alpha = n / (n + k)
    where k is the stabilization threshold for that stat.

    Returns 0.0–1.0:
      0.0 = no sample (pure prior)
      0.5 = at stabilization threshold (equal weight current/prior)
      1.0 = large sample (mostly current data)
    """
    k = _STABILIZATION_THRESHOLDS.get(stat, 300)
    return n_sample / (n_sample + k)


def dampen_nudge(nudge: float, stat: str, n_sample: int) -> float:
    """
    Apply reliability dampening to a probability nudge.

    Low sample → nudge shrinks toward 0 (use prior).
    Large sample → nudge passes through nearly unchanged.

    Example:
        nudge=+0.03, stat="babip", n=50 → 0.03 * (50/850) ≈ +0.002
        nudge=+0.03, stat="csw_pct", n=50 → 0.03 * (50/100) ≈ +0.015
    """
    alpha = reliability_alpha(stat, n_sample)
    return nudge * alpha


def get_feature_weights(prop: dict) -> dict[str, float]:
    """
    Return per-feature reliability weights for a prop based on available
    signals. Downstream nudge stacks multiply their raw adjustments
    by these weights to avoid overconfidence in low-sample stats.

    Returns dict mapping feature name → weight [0.0, 1.0].
    """
    weights = dict(FEATURE_RELIABILITY_WEIGHTS)

    # Early-season PA proxy — scale down all batter weights if <30 PA
    n_pa = int(prop.get("_n_pa_season", 0) or 0)
    if 0 < n_pa < 50:
        scale = n_pa / 50.0
        for k in ("babip", "wrc_plus", "woba", "iso", "barrel_rate", "bb_pct"):
            weights[k] = weights.get(k, 0.5) * scale
        logger.debug("[Reliability] Early season PA=%d → low-sample dampening active", n_pa)

    # Pitcher IP proxy
    n_ip = float(prop.get("season_ip", 0) or 0)
    if 0 < n_ip < 15:
        scale = n_ip / 15.0
        for k in ("era", "xfip", "siera", "lob_pct", "pitcher_babip"):
            weights[k] = weights.get(k, 0.5) * scale
        logger.debug("[Reliability] Small sample IP=%.1f → pitcher dampening active", n_ip)

    return weights
