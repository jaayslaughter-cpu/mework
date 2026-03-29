"""
adjustment_dampener.py — Phase 91 Step 4

Prevents correlated-signal stacking in post-model probability adjustments.

Problem:
    Multiple signals that share an underlying cause (pitcher quality, lineup
    weakness) can all fire in the same direction simultaneously and push the
    final probability far outside what the base model intended.

    Example (K-prop, base = 65%):
        shadow_whiff_boost  +3.0pp → 68.0%
        zone_integrity×1.10 +6.8pp → 74.8%
        chase_difficulty    +5.0pp → 79.8%
        Net swing: +14.8pp — clearly over-inflated.

Solution:
    Collect all post-model adjustments as (name, effective_delta_pct) pairs.
    Apply them in logit space with a diminishing-returns weight when signals
    agree in direction:
        1st same-direction signal: 100% weight
        2nd same-direction signal:  70% weight  (0.70^1)
        3rd same-direction signal:  49% weight  (0.70^2)
        ...

    Signals pointing in the OPPOSITE direction (diversifying) are always
    applied at full weight, because opposing signals are genuine uncertainty.

    Using logit space ensures adjustments naturally flatten near the extremes
    (0% / 100%), so large adjustments on already-extreme probabilities are
    automatically moderated.

    The minimum net adjustment floor is ±1pp — signals that agree on a
    direction always move the needle at least a little.

Usage:
    from adjustment_dampener import dampen_adjustments

    adjustments = [
        ("shadow_whiff",   +3.0),   # effective delta in percentage points
        ("zone_integrity", +6.8),
        ("chase_difficulty", +5.0),
    ]
    new_prob = dampen_adjustments(base_prob_pct=65.0, adjustments=adjustments)
    # Returns ~72.0 instead of 79.8 — still meaningful, but not runaway

Public API:
    dampen_adjustments(base_prob_pct, adjustments, *, decay=0.70, min_floor=1.0)
        → float (adjusted probability in percentage points, clamped 3–97)
"""

from __future__ import annotations

import logging
import math
from typing import Sequence

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_DECAY   = 0.70   # each additional same-direction signal gets 70% weight of its predecessor
_MIN_FLOOR_PCT   = 1.0    # guaranteed minimum net adjustment when at least one signal fires
_CLAMP_LO        = 3.0    # never return below 3%
_CLAMP_HI        = 97.0   # never return above 97%


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _logit(p: float) -> float:
    """Log-odds of probability p (clamped to avoid ±inf)."""
    p = max(1e-6, min(1 - 1e-6, p))
    return math.log(p / (1.0 - p))


def _sigmoid(x: float) -> float:
    """Inverse logit — maps ℝ → (0, 1)."""
    return 1.0 / (1.0 + math.exp(-x))


def _delta_to_logit_shift(base_p: float, delta_p: float) -> float:
    """
    Convert a probability delta (in [0,1] space) to a logit-space shift
    computed at the current base probability.

    This lets us apply corrections in logit space while keeping the
    magnitude intuitive (specified in probability-point terms).
    """
    shifted_p = max(1e-6, min(1 - 1e-6, base_p + delta_p))
    return _logit(shifted_p) - _logit(base_p)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def dampen_adjustments(
    base_prob_pct: float,
    adjustments: Sequence[tuple[str, float]],
    *,
    decay: float = _DEFAULT_DECAY,
    min_floor: float = _MIN_FLOOR_PCT,
    log_tag: str = "",
) -> float:
    """
    Apply a list of post-model adjustments to *base_prob_pct* using
    correlation dampening.

    Parameters
    ----------
    base_prob_pct : float
        Starting probability in percentage points (e.g. 65.0 means 65%).
    adjustments : list of (signal_name: str, delta_pct: float)
        Each delta is in percentage points (e.g. +3.0 means "this signal
        wants to push the probability up by 3pp").
    decay : float
        Weight multiplier applied to each additional same-direction signal.
        Default 0.70 means 2nd gets 70%, 3rd gets 49%, etc.
    min_floor : float
        If any signals fired, the net adjustment is guaranteed to be at
        least this many percentage points in the dominant direction.
    log_tag : str
        Optional context string for debug logging (e.g. player name).

    Returns
    -------
    float
        Adjusted probability in percentage points, clamped to [3, 97].
    """
    # Filter out noise-level adjustments
    active = [(n, d) for n, d in adjustments if abs(d) >= 0.10]
    if not active:
        return float(max(_CLAMP_LO, min(_CLAMP_HI, base_prob_pct)))

    base_p = max(1e-6, min(1 - 1e-6, base_prob_pct / 100.0))

    # ── Sort by absolute magnitude (largest first for consistent ordering) ──
    sorted_adj = sorted(active, key=lambda x: abs(x[1]), reverse=True)

    # ── Count direction agreement ────────────────────────────────────────────
    n_pos = sum(1 for _, d in sorted_adj if d > 0)
    n_neg = sum(1 for _, d in sorted_adj if d < 0)
    dominant_dir = 1 if n_pos >= n_neg else -1

    # ── Apply signals in logit space with dampening ──────────────────────────
    base_logit = _logit(base_p)
    total_logit_shift = 0.0
    same_dir_count = 0

    details: list[str] = []
    for name, delta_pct in sorted_adj:
        delta_p    = delta_pct / 100.0
        signal_dir = 1 if delta_p > 0 else -1
        logit_shift = _delta_to_logit_shift(base_p, delta_p)

        if signal_dir == dominant_dir:
            same_dir_count += 1
            weight = decay ** (same_dir_count - 1)   # 1.0, 0.70, 0.49, ...
        else:
            weight = 1.0   # opposing signals always full weight

        dampened_shift = logit_shift * weight
        total_logit_shift += dampened_shift

        details.append(
            f"{name}={delta_pct:+.2f}pp w={weight:.2f} ls={logit_shift:+.3f}"
        )

    # ── Enforce minimum floor in dominant direction ───────────────────────────
    final_p     = _sigmoid(base_logit + total_logit_shift)
    net_delta_p = final_p - base_p
    floor_p     = min_floor / 100.0

    if abs(net_delta_p) < floor_p and len(active) > 0:
        # At least one signal fired — guarantee minimum net movement
        direction = 1 if net_delta_p >= 0 else -1
        net_delta_p = direction * floor_p
        final_p = max(1e-6, min(1 - 1e-6, base_p + net_delta_p))

    final_pct = round(float(max(_CLAMP_LO, min(_CLAMP_HI, final_p * 100.0))), 4)

    # ── Debug log (only when adjustment changed the probability meaningfully) ──
    if abs(final_pct - base_prob_pct) >= 0.5:
        logger.debug(
            "[AdjDampener]%s base=%.2f%% → %.2f%% (net %+.2f%%) | %s",
            f" {log_tag}" if log_tag else "",
            base_prob_pct,
            final_pct,
            final_pct - base_prob_pct,
            " | ".join(details),
        )

    return final_pct


# ---------------------------------------------------------------------------
# Convenience: compute undampened total for comparison / logging
# ---------------------------------------------------------------------------

def undampened_total(base_prob_pct: float, adjustments: Sequence[tuple[str, float]]) -> float:
    """
    Return what the probability *would* be if all adjustments stacked
    naively (no dampening). Useful for audit logging.
    """
    total = base_prob_pct
    for _, d in adjustments:
        total += d
    return round(float(max(_CLAMP_LO, min(_CLAMP_HI, total))), 4)
