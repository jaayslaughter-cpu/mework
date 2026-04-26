"""
pa_model.py — Plate-appearance probability model.
===================================================
Ported from baseball-sims (github.com/thomasosbot/baseball-sims)
  src/simulation/pa_model.py   — odds_ratio_blend, compute_pa_probabilities
  src/simulation/constants.py  — LEAGUE_RATES (updated to 2025 FG)

How it fits into PropIQ
-----------------------
predict_plus_layer.py and nsfi_layer.py currently use flat base-rate lookups
or simple linear adjustments for batter-pitcher matchups. This module replaces
those with the Bill James odds-ratio method, which correctly handles both
elite matchups (ace vs weak lineup) and average matchups (league-average
players) without over-compressing the probability spread.

Usage:
    from pa_model import compute_pa_probabilities, prop_matchup_prob

    # Full PA outcome distribution
    probs = compute_pa_probabilities(batter_profile, pitcher_profile, park_factors)
    # probs = {"K": 0.22, "BB": 0.085, "HR": 0.033, "1B": 0.145, ...}

    # Single-stat matchup probability (e.g. K-rate for a strikeout prop)
    p_k = prop_matchup_prob("strikeouts", batter_profile, pitcher_profile)
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger("propiq.pa_model")

# ── 2025 MLB league-average PA outcome rates ──────────────────────────────
# Source: FanGraphs 2025 season (confirmed via VSiN Feb 2026)
LEAGUE_RATES: dict[str, float] = {
    "K":   0.223,   # strikeout
    "BB":  0.087,   # walk (non-IBB)
    "HBP": 0.011,   # hit by pitch
    "HR":  0.033,   # home run
    "3B":  0.004,   # triple
    "2B":  0.047,   # double
    "1B":  0.143,   # single
    "OUT": 0.452,   # field out (all other)
}
assert abs(sum(LEAGUE_RATES.values()) - 1.0) < 0.01, "LEAGUE_RATES must sum to 1"

OUTCOMES = ["K", "BB", "HBP", "HR", "3B", "2B", "1B", "OUT"]

# ── Prop-type to PA outcome key mapping ──────────────────────────────────
_PROP_TO_OUTCOME: dict[str, list[str]] = {
    "strikeouts":        ["K"],
    "walks_allowed":     ["BB"],
    "hits":              ["1B", "2B", "3B", "HR"],
    "hits_allowed":      ["1B", "2B", "3B", "HR"],
    "total_bases":       ["1B", "2B", "3B", "HR"],   # used with weights below
    "earned_runs":       ["HR", "2B", "1B"],           # proxy: extra-base hits drive ER
    "hitter_strikeouts": ["K"],
    "home_runs":         ["HR"],
}

# wOBA linear weights for total-bases proxy
_WOBA_WEIGHTS = {"1B": 0.883, "2B": 1.244, "3B": 1.569, "HR": 2.004}


def odds_ratio_blend(batter_rate: float, pitcher_rate: float,
                     league_rate: float) -> float:
    """
    Multiplicative odds-ratio method (Bill James numerator-only form):
        P = batter_rate × pitcher_rate / league_rate

    More accurate than additive blending for rates far from 0.5.
    Does not introduce log5 denominator compression (~7% on OUT rate)
    which compounds across 30+ PA per team into meaningful win-probability
    compression. See baseball-sims backtest_results.md v0.7 for full analysis.

    Source: baseball-sims/src/simulation/pa_model.py
    """
    eps = 1e-9
    b = max(eps, min(1.0, batter_rate))
    p = max(eps, min(1.0, pitcher_rate))
    l = max(eps, min(1.0, league_rate))
    return max(eps, min(1.0, (b * p) / l))


def compute_pa_probabilities(
    batter_profile:  dict[str, float],
    pitcher_profile: dict[str, float],
    league_rates:    dict[str, float] | None = None,
    park_factors:    dict[str, float] | None = None,
) -> dict[str, float]:
    """
    Produce a normalised PA outcome distribution for one batter-pitcher matchup.

    Parameters
    ----------
    batter_profile  : dict mapping outcome → batter's per-PA rate
                      Keys: "K", "BB", "HBP", "HR", "3B", "2B", "1B", "OUT"
                      Missing keys default to league average.
    pitcher_profile : dict mapping outcome → pitcher's allowed per-PA rate
                      Same keys. Missing keys default to league average.
    park_factors    : optional multiplicative adjustments per outcome
                      {"HR": 1.12, "1B": 0.97, "bb": 1.05, "k": 0.98, ...}

    Returns
    -------
    Normalised dict {"K": p, "BB": p, ...} summing to 1.0.
    """
    lg = league_rates or LEAGUE_RATES
    raw: dict[str, float] = {}

    for outcome in OUTCOMES:
        b = batter_profile.get(outcome,  lg[outcome])
        p = pitcher_profile.get(outcome, lg[outcome])
        raw[outcome] = odds_ratio_blend(b, p, lg[outcome])

    # Park factor adjustments (pre-normalisation)
    if park_factors:
        for outcome in ("HR", "3B", "2B", "1B"):
            if outcome in park_factors:
                raw[outcome] *= park_factors[outcome]
        if "bb" in park_factors:
            raw["BB"] *= park_factors["bb"]
        if "k" in park_factors:
            raw["K"] *= park_factors["k"]

    total = sum(raw.values())
    return {k: v / total for k, v in raw.items()}


def prop_matchup_prob(
    prop_type:       str,
    batter_profile:  dict[str, float],
    pitcher_profile: dict[str, float],
    line:            float = 0.5,
    side:            str   = "Over",
    park_factors:    Optional[dict[str, float]] = None,
) -> float | None:
    """
    Single-number P(Over/Under | line) for a specific prop type using the
    odds-ratio PA model.

    Returns None if prop_type is not recognised (caller falls back to base rates).

    Examples
    --------
    # Pitcher strikeout prop
    p = prop_matchup_prob("strikeouts", {}, pitcher_profile, line=5.5, side="Over")

    # Batter hits prop
    p = prop_matchup_prob("hits", batter_profile, {}, line=0.5, side="Over")
    """
    probs = compute_pa_probabilities(batter_profile, pitcher_profile,
                                     park_factors=park_factors)
    outcomes = _PROP_TO_OUTCOME.get(prop_type)
    if not outcomes:
        return None

    if prop_type == "total_bases":
        # Expected total bases per PA using wOBA weights
        exp_tb = sum(probs.get(o, 0.0) * _WOBA_WEIGHTS.get(o, 0.0) for o in outcomes)
        # Approximate P(>= line TB) using Poisson with lambda = 9 * exp_tb_per_pa
        # (9-batter lineup, one PA each — simplified; real PA count varies)
        import math
        lam = max(0.01, 9 * exp_tb)  # expected TB for one time through the order
        # P(X >= line) = 1 - P(X < line) via Poisson CDF
        k = int(line)
        cdf = sum(math.exp(-lam) * lam**i / math.factorial(i) for i in range(k + 1))
        p_over = max(0.01, min(0.99, 1.0 - cdf))
    else:
        # Combined per-PA rate for the relevant outcomes
        p_per_pa = sum(probs.get(o, 0.0) for o in outcomes)
        # Approximate P(>= 1 event in N PA) via Poisson or Bernoulli
        # For line=0.5: P(at least 1) = 1 - P(0) = 1 - (1-p)^N
        # For line>1: use Poisson with lambda = N * p_per_pa
        import math
        N = 4 if prop_type in ("strikeouts", "walks_allowed",
                               "hitter_strikeouts") else 4
        # SP faces ~27 BF; batter gets ~4 PA
        if prop_type in ("strikeouts", "walks_allowed", "hits_allowed", "earned_runs"):
            N = 27  # pitcher faces full lineup
        lam = max(0.01, N * p_per_pa)
        k   = int(line)
        # P(X > line) = P(X >= line+1) since line is .5 → k=0 → P(X>=1)
        cdf = sum(math.exp(-lam) * lam**i / math.factorial(i)
                  for i in range(k + 1))
        p_over = max(0.01, min(0.99, 1.0 - cdf))

    return p_over if side.upper() in ("OVER", "HIGHER") else 1.0 - p_over


def build_batter_profile(mlb_stats: dict) -> dict[str, float]:
    """
    Convert mlb_stats_layer output to a pa_model batter profile.
    All rates are per-PA fractions.
    """
    pa   = max(1.0, float(mlb_stats.get("pa", 1) or 1))
    ab   = max(1.0, float(mlb_stats.get("atBats", pa * 0.85) or pa * 0.85))
    hits = float(mlb_stats.get("hits_total", mlb_stats.get("h", 0)) or 0)
    hr   = float(mlb_stats.get("hr_total",   mlb_stats.get("homeRuns", 0)) or 0)
    d2   = float(mlb_stats.get("doubles",    0) or 0)
    d3   = float(mlb_stats.get("triples",    0) or 0)
    s1b  = max(0.0, hits - hr - d2 - d3)
    bb   = float(mlb_stats.get("bb_total",   mlb_stats.get("baseOnBalls", 0)) or 0)
    k    = float(mlb_stats.get("k_total",    mlb_stats.get("strikeOuts", 0)) or 0)
    hbp  = float(mlb_stats.get("hbp", 0) or 0)
    lg   = LEAGUE_RATES
    return {
        "K":   k   / pa if pa > 0 else lg["K"],
        "BB":  bb  / pa if pa > 0 else lg["BB"],
        "HBP": hbp / pa if pa > 0 else lg["HBP"],
        "HR":  hr  / pa if pa > 0 else lg["HR"],
        "3B":  d3  / pa if pa > 0 else lg["3B"],
        "2B":  d2  / pa if pa > 0 else lg["2B"],
        "1B":  s1b / pa if pa > 0 else lg["1B"],
        "OUT": max(0.0, 1.0 - (k + bb + hbp + hr + d3 + d2 + s1b) / pa)
               if pa > 0 else lg["OUT"],
    }


def build_pitcher_profile(mlb_stats: dict) -> dict[str, float]:
    """
    Convert mlb_stats_layer / fangraphs_layer pitcher output to a pa_model
    pitcher profile (rates-allowed per BF).
    """
    lg  = LEAGUE_RATES
    k   = float(mlb_stats.get("k_pct",  lg["K"])  or lg["K"])
    bb  = float(mlb_stats.get("bb_pct", lg["BB"]) or lg["BB"])
    hr  = float(mlb_stats.get("hr_per_bf", lg["HR"]) or lg["HR"])
    # Derive hit types from whip / babip / k_pct approximation
    whip    = float(mlb_stats.get("whip", 1.30) or 1.30)
    babip   = float(mlb_stats.get("babip", 0.300) or 0.300)
    # h_allowed ≈ WHIP * IP / 9 * 9 / BF ≈ whip * 9/27 = whip/3 per PA
    h_per_pa = min(0.35, whip / 3.0 * (1 - bb))
    s1b = max(0.0, h_per_pa - hr - lg["2B"] - lg["3B"])
    return {
        "K":   min(0.40, k),
        "BB":  min(0.20, bb),
        "HBP": lg["HBP"],
        "HR":  min(0.08, hr),
        "3B":  lg["3B"],
        "2B":  lg["2B"],
        "1B":  max(0.0, s1b),
        "OUT": max(0.0, 1.0 - k - bb - lg["HBP"] - hr - lg["3B"] - lg["2B"] - max(0.0, s1b)),
    }
