"""
kelly_sizing.py
===============
Kelly Criterion fractional bet sizing for PropIQ parlays.

Kelly fraction:  f* = (b*p - q) / b
  where  b = decimal_odds - 1   (net profit per unit staked)
         p = model win probability
         q = 1 - p

We use QUARTER-Kelly (fraction=0.25) — the sports-betting industry standard
for variance reduction.  Negative Kelly (EV < 0) → 0.0 (no bet signal).

Multipliers (confirmed PR #332 / PR #458):
  Underdog PowerPlay: 2-leg=3.5x, 3-leg=6x, 5-leg=10x
  PrizePicks Power:   2→3x, 3→5x, 4→10x, 5→20x

Entry size is hard-coded at $10 (DEFAULT_ENTRY directive).  Kelly sizing
is therefore used as a diagnostic / Discord annotation rather than a
live stake adjustment.  unit_multiplier is stored in decision_log for
future adaptive sizing analysis.

Public API
----------
get_kelly_fraction(model_prob, decimal_odds, kelly_fraction=0.25) → float
get_unit_multiplier(model_prob, decimal_odds) → float (0.5–2.0)
get_parlay_kelly(legs, platform, entry_type) → dict
kelly_summary_line(parlay_kelly) → str  (Discord embed line)
"""
from __future__ import annotations

import logging
import math

logger = logging.getLogger(__name__)

_DEFAULT_KELLY_FRACTION = 0.25

_MAX_MULTIPLIER = 2.0
_MIN_MULTIPLIER = 0.5

# Reference point so 1.0x ≈ a 60%-prob 2-leg UD pick (f*≈0.10)
_KELLY_UNIT_PIVOT = 0.10

_UD_MULTIPLIERS: dict[int, float] = {2: 3.5, 3: 6.0, 5: 10.0}
_PP_MULTIPLIERS: dict[int, float] = {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0}


def get_kelly_fraction(
    model_prob: float,
    decimal_odds: float,
    kelly_fraction: float = _DEFAULT_KELLY_FRACTION,
) -> float:
    """Return fractional Kelly bet size as a proportion of bankroll (0–1).

    Args:
        model_prob:    Win probability in [0, 1].  Values >1 treated as 0–100 scale.
        decimal_odds:  Full decimal payout (e.g. 3.5 for 3.5x PowerPlay).
        kelly_fraction: Fraction of full Kelly (default 0.25 = quarter-Kelly).

    Returns:
        Fractional Kelly ∈ [0.0, 1.0].  Returns 0.0 when EV ≤ 0.
    """
    p = model_prob / 100.0 if model_prob > 1.0 else float(model_prob)
    p = max(1e-6, min(1 - 1e-6, p))
    q = 1.0 - p

    if decimal_odds <= 1.0:
        return 0.0

    b = decimal_odds - 1.0           # net profit per unit staked
    full_kelly = (b * p - q) / b
    full_kelly = max(0.0, full_kelly)

    return round(kelly_fraction * full_kelly, 5)


def get_unit_multiplier(
    model_prob: float,
    decimal_odds: float,
    kelly_fraction: float = _DEFAULT_KELLY_FRACTION,
) -> float:
    """Convert Kelly fraction to a unit multiplier (0.5 – 2.0 hard cap).

    Scale: f* = _KELLY_UNIT_PIVOT → 1.0x base unit.
    """
    f = get_kelly_fraction(model_prob, decimal_odds, kelly_fraction)
    if f <= 0:
        return _MIN_MULTIPLIER
    multiplier = f / _KELLY_UNIT_PIVOT
    return round(max(_MIN_MULTIPLIER, min(_MAX_MULTIPLIER, multiplier)), 2)


def get_parlay_kelly(
    legs: list[dict],
    platform: str = "Underdog",
    entry_type: str = "PowerPlay",
) -> dict:
    """Compute Kelly sizing for a full parlay.

    Parlay probability = product of individual leg model_probs.
    Decimal odds = platform multiplier for leg count.

    Args:
        legs:        List of leg dicts with 'model_prob' key (0-100 scale).
        platform:    "Underdog" or "PrizePicks".
        entry_type:  "PowerPlay"/"STANDARD" or "Flex"/"FlexPlay".

    Returns:
        dict: parlay_prob, decimal_odds, kelly_fraction, unit_multiplier,
              ev_pct, recommended_stake, n_legs.
    """
    if not legs:
        return {
            "parlay_prob": 0.0, "decimal_odds": 1.0,
            "kelly_fraction": 0.0, "unit_multiplier": _MIN_MULTIPLIER,
            "ev_pct": -100.0, "recommended_stake": 10.0, "n_legs": 0,
        }

    n = len(legs)

    parlay_prob = 1.0
    for leg in legs:
        raw = leg.get("model_prob", 55.0)
        p = raw / 100.0 if raw > 1.0 else float(raw)
        p = max(0.01, min(0.99, p))
        parlay_prob *= p

    is_pp = platform.lower().startswith("prize")
    mult_table = _PP_MULTIPLIERS if is_pp else _UD_MULTIPLIERS
    # fallback: use nearest available leg count
    decimal_odds = float(mult_table.get(n, sorted(mult_table.items())[-1][1]))

    kf  = get_kelly_fraction(parlay_prob, decimal_odds)
    um  = get_unit_multiplier(parlay_prob, decimal_odds)
    ev  = round((parlay_prob * decimal_odds - 1.0) * 100, 2)
    stake = round(10.0 * um, 2)

    return {
        "parlay_prob":       round(parlay_prob, 4),
        "decimal_odds":      decimal_odds,
        "kelly_fraction":    kf,
        "unit_multiplier":   um,
        "ev_pct":            ev,
        "recommended_stake": stake,
        "n_legs":            n,
    }


def kelly_summary_line(pk: dict) -> str:
    """One-line Kelly summary for Discord parlay embeds."""
    return (
        f"Kelly: {pk.get('kelly_fraction', 0):.4f} "
        f"({pk.get('unit_multiplier', 1):.1f}× unit) | "
        f"Parlay P: {pk.get('parlay_prob', 0)*100:.1f}% | "
        f"EV: {pk.get('ev_pct', 0):+.1f}%"
    )
