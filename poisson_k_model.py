"""
poisson_k_model.py
==================
Poisson-based pitcher strikeout probability model.

Strikeouts per start follow a Poisson distribution — the standard model
for count events in a fixed interval. This gives more accurate probabilities
than simulation for discrete K counts, especially at the tails.

Formula:
    expected_ks = k9 * (ip_per_start / 9) * matchup_factor * park_mult * umpire_mod
    P(K > line)  = 1 - Poisson_CDF(floor(line), expected_ks)
    P(K < line)  = 1 - P(K > line)

Reliability score (0–100):
    Weights how much to trust the 2026 stats vs fall back to career average.
    Based on: number of 2026 starts, total IP, K/9 consistency (σ), xFIP σ.

Source: playbook/models/ev_calculator.py + player_baseline.py
"""
from __future__ import annotations

import math
import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from scipy.stats import poisson as _poisson
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False
    logger.warning("[PoissonK] scipy not available — using normal approximation")


# ---------------------------------------------------------------------------
# Core probability functions
# ---------------------------------------------------------------------------

def prob_k_over(expected_ks: float, line: float) -> float:
    """
    P(strikeouts > line) using Poisson distribution.

    Lines are typically X.5 so P(K > 5.5) = P(K >= 6) = 1 - CDF(5).
    Works correctly for both integer and half-integer lines.
    """
    if expected_ks <= 0:
        return 0.0

    k_floor = math.floor(line)

    if _SCIPY_AVAILABLE:
        return round(float(1.0 - _poisson.cdf(k_floor, expected_ks)), 4)

    # Normal approximation fallback (σ = sqrt(λ) for Poisson)
    mu = expected_ks
    sigma = max(math.sqrt(mu), 0.5)
    z = (line - mu) / sigma
    # 1 - Φ(z) approximation
    p = 0.5 * math.erfc(z / math.sqrt(2))
    return round(min(max(p, 0.0), 1.0), 4)


def prob_k_under(expected_ks: float, line: float) -> float:
    """P(strikeouts < line) — complement of over."""
    return round(1.0 - prob_k_over(expected_ks, line), 4)


def estimate_expected_ks(
    k9:            float,
    ip_per_start:  float,
    matchup_factor: float = 1.0,
    park_mult:      float = 1.0,
    umpire_mod:     float = 1.0,
    velo_trend:     float = 0.0,    # mph delta last 7d vs 30d avg; ±1 mph → ±1.5%
) -> float:
    """
    Compute expected strikeouts for today's start.

    k9             — pitcher's K/9 (blended current + career)
    ip_per_start   — expected innings today (from historical avg, capped for managed use)
    matchup_factor — opposing lineup K-rate relative to league avg (default 1.0)
    park_mult      — from park_k_factors.get_park_k_mult()
    umpire_mod     — from umpire_rates (k_mod field)
    velo_trend     — fastball velocity delta; ±1 mph ≈ ±1.5% K rate (capped ±6%)
    """
    velo_factor = 1.0 + max(min(velo_trend * 0.015, 0.06), -0.06)
    adjusted_k9 = k9 * matchup_factor * park_mult * umpire_mod * velo_factor
    expected    = adjusted_k9 * (ip_per_start / 9.0)
    return round(max(expected, 0.0), 3)


# ---------------------------------------------------------------------------
# Low-line confidence adjustment
# ── Very low K lines are highly sensitive to early hooks/rain delays.
# ── We discount the model probability to avoid over-betting these.
# ---------------------------------------------------------------------------
_LOW_LINE_DISCOUNTS = {2.5: 0.80, 3.5: 0.88}   # line → probability multiplier

def apply_low_line_discount(prob: float, line: float, side: str) -> tuple[float, str | None]:
    """
    Apply a confidence discount for very low K lines (≤3.5).
    Returns (adjusted_prob, note_or_None).
    Only applies to OVER side — under bets on low lines are inherently safer.
    """
    if side.upper() != "OVER":
        return prob, None
    for threshold, discount in sorted(_LOW_LINE_DISCOUNTS.items(), reverse=True):
        if line <= threshold:
            note = f"low-line discount applied (×{discount:.2f} for line≤{threshold})"
            return round(prob * discount, 4), note
    return prob, None


# ---------------------------------------------------------------------------
# Reliability score (0–100)
# How much to trust the 2026 current stats vs historical baseline.
# ---------------------------------------------------------------------------

def reliability_score(
    starts_2026:   int,
    total_ip:      float,
    k9_std:        Optional[float] = None,   # σ of K/9 across seasons
    xfip_std:      Optional[float] = None,   # σ of xFIP across seasons
) -> int:
    """
    Compute a reliability score (0–100) for how much to trust current stats.

    Component weights:
      seasons / data coverage  → 25 pts
      total IP                 → 35 pts  (caps at 200 IP)
      K/9 consistency (σ)      → 25 pts  (σ=0 → 25, σ=3 → 0)
      xFIP consistency (σ)     → 15 pts  (σ=0 → 15, σ=1.5 → 0)

    Score ≥ 70: trust current 2026 stats heavily
    Score 40-69: blend 2026 + career (60/40)
    Score < 40: lean heavily on career/historical (30/70)
    """
    # Data coverage
    season_pts = 25 if starts_2026 >= 8 else (15 if starts_2026 >= 4 else (8 if starts_2026 >= 2 else 0))
    ip_pts     = min(total_ip / 200.0, 1.0) * 35

    # K/9 consistency
    if k9_std is not None:
        k9_pts = max(0.0, 1.0 - k9_std / 3.0) * 25
    else:
        k9_pts = 12.5  # neutral when no multi-season data

    # xFIP consistency
    if xfip_std is not None:
        xfip_pts = max(0.0, 1.0 - xfip_std / 1.5) * 15
    else:
        xfip_pts = 7.5  # neutral

    total = season_pts + ip_pts + k9_pts + xfip_pts
    return int(round(min(max(total, 0), 100)))


def blend_k9(
    k9_current:  float,
    k9_career:   float,
    score:       int,
) -> float:
    """
    Blend current and career K/9 weighted by reliability score.

    score ≥ 70 → 80% current, 20% career
    score 40-69 → 60% current, 40% career
    score < 40  → 30% current, 70% career
    """
    if score >= 70:
        w = 0.80
    elif score >= 40:
        w = 0.60
    else:
        w = 0.30
    blended = w * k9_current + (1.0 - w) * k9_career
    return round(blended, 3)


# ---------------------------------------------------------------------------
# Trend label
# ---------------------------------------------------------------------------

TREND_UP_THRESHOLD   =  0.08
TREND_DOWN_THRESHOLD = -0.08

def trend_label(current: float | None, hist_avg: float | None,
                higher_is_better: bool = True) -> str:
    """UP / DOWN / STABLE / NEW compared to historical average."""
    if current is None or hist_avg is None or hist_avg == 0:
        return "NEW"
    rel = (current - hist_avg) / abs(hist_avg)
    if higher_is_better:
        if rel >=  TREND_UP_THRESHOLD:   return "UP"
        if rel <= TREND_DOWN_THRESHOLD:  return "DOWN"
    else:
        if rel <= -TREND_UP_THRESHOLD:   return "UP"
        if rel >= -TREND_DOWN_THRESHOLD: return "DOWN"
    return "STABLE"


# ---------------------------------------------------------------------------
# Main entry point used by prop_enrichment / agents
# ---------------------------------------------------------------------------

def get_k_probability(
    prop:           dict,
    k9_current:     float,
    k9_career:      float  = 0.0,
    starts_2026:    int    = 0,
    total_ip:       float  = 0.0,
    k9_std:         Optional[float] = None,
    xfip_std:       Optional[float] = None,
    ip_per_start:   float  = 5.5,
    matchup_factor: float  = 1.0,
    park_mult:      float  = 1.0,
    umpire_mod:     float  = 1.0,
    velo_trend:     float  = 0.0,
) -> dict:
    """
    Compute P(K over line) and P(K under line) for a strikeout prop.

    Returns:
        {
          "prob_over":       float,  # 0-1
          "prob_under":      float,  # 0-1
          "expected_ks":     float,
          "reliability":     int,    # 0-100
          "blended_k9":      float,
          "low_line_note":   str | None,
          "trend":           str,    # UP/DOWN/STABLE/NEW
        }
    """
    line = float(prop.get("line", 4.5) or 4.5)
    side = str(prop.get("side", "OVER")).upper()

    score = reliability_score(starts_2026, total_ip, k9_std, xfip_std)

    if k9_career > 0:
        blended = blend_k9(k9_current, k9_career, score)
    else:
        blended = k9_current

    expected = estimate_expected_ks(
        blended, ip_per_start, matchup_factor, park_mult, umpire_mod, velo_trend
    )

    p_over  = prob_k_over(expected, line)
    p_under = prob_k_under(expected, line)

    p_over_adj, low_note = apply_low_line_discount(p_over, line, "OVER")
    if low_note:
        p_under_adj = round(1.0 - p_over_adj, 4)
    else:
        p_under_adj = p_under

    trend = trend_label(k9_current, k9_career if k9_career > 0 else None,
                        higher_is_better=True)

    logger.debug(
        "[PoissonK] k9=%.2f→%.2f (rel=%d) ip=%.1f mup=%.3f park=%.3f ump=%.3f "
        "→ λ=%.2f P(over %.1f)=%.4f",
        k9_current, blended, score, ip_per_start,
        matchup_factor, park_mult, umpire_mod, expected, line, p_over_adj,
    )

    return {
        "prob_over":     p_over_adj,
        "prob_under":    p_under_adj,
        "expected_ks":   expected,
        "reliability":   score,
        "blended_k9":    blended,
        "low_line_note": low_note,
        "trend":         trend,
    }
