"""
api/services/abs_challenge.py
==============================
2026 MLB Automated Ball-Strike (ABS) Challenge System.

Models the impact of the ABS challenge rule on K/BB props.

Key facts modelled:
  1. Strike zone = 27%–53.5% of batter height
  2. Judged at horizontal centre of home plate
  3. Grazing edge counts as a strike
  4. Battery (pitcher+catcher): 53% of challenges, 60% success rate
  5. Hitters: 47% of challenges, 46% success rate
  6. Tall batters (Judge ~6'7") have ~20% larger zones than short batters (Altuve ~5'6")

FIX BUG 4: apply_abs_to_probability() now clamps the MULTIPLIER to [0.88, 1.12]
  — not the final probability to [0.40, 0.80].
  The old 0.40 floor could artificially inflate props already below 0.40 (fabricating
  edge that doesn't exist). The 0.80 ceiling conflicted with predictor's own 0.95 cap.
  The multiplier cap is the correct guardrail: ±12% max ABS influence per prop.

FIX BUG C: This file belongs in api/services/ so predictor.py can import it as
  `from services.abs_challenge import ...` when running from within api/.
  The root-level abs_challenge.py (used by live_dispatcher.py) is a separate copy
  for the dispatcher pipeline.

PEP 8 compliant.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Zone height as fraction of batter height
_ZONE_BOTTOM_FRAC = 0.27
_ZONE_TOP_FRAC    = 0.535

# Reference heights (inches)
_ALTUVE_HEIGHT_IN = 66.0   # 5'6"  — smallest zone
_JUDGE_HEIGHT_IN  = 79.0   # 6'7"  — largest zone (~20% bigger)
_LEAGUE_AVG_IN    = 72.0   # 6'0"  — baseline

# Challenge rates and success rates
_BATTERY_CHALLENGE_RATE   = 0.53   # Battery initiates 53% of challenges
_BATTERY_SUCCESS_RATE     = 0.60   # Battery wins 60% of challenges
_HITTER_CHALLENGE_RATE    = 0.47   # Hitters initiate 47% of challenges
_HITTER_SUCCESS_RATE      = 0.46   # Hitters win 46% of challenges

# Prop categories affected by ABS
_ABS_AFFECTED_PROPS = frozenset({
    "pitcher_strikeouts",
    "batter_strikeouts",
    "batter_walks",
    "pitcher_walks",
})

# FIX BUG 4: Clamp MULTIPLIER, not probability
_MULTIPLIER_MIN = 0.88
_MULTIPLIER_MAX = 1.12


# ---------------------------------------------------------------------------
# ABSContext dataclass
# ---------------------------------------------------------------------------

@dataclass
class ABSContext:
    """
    Context for ABS challenge modelling.

    Attributes:
        batter_height_in:  Batter height in inches. Defaults to league average (72").
        is_2026_season:    True when the 2026 ABS rule is active. When False the
                           multiplier is always 1.0 (no-op).
    """
    batter_height_in: float = _LEAGUE_AVG_IN
    is_2026_season: bool = True


# ---------------------------------------------------------------------------
# Zone helpers
# ---------------------------------------------------------------------------

def _zone_height_inches(height_in: float) -> float:
    """Return the height of the ABS strike zone in inches for a given batter."""
    return ((_ZONE_TOP_FRAC - _ZONE_BOTTOM_FRAC) * height_in)


def _zone_size_ratio(height_in: float) -> float:
    """
    Return this batter's zone size relative to league average (1.0 = average).
    Values > 1.0 → larger zone → more likely strikes → more Ks, fewer BBs.
    """
    zone = _zone_height_inches(height_in)
    avg_zone = _zone_height_inches(_LEAGUE_AVG_IN)
    return zone / avg_zone if avg_zone > 0 else 1.0


# ---------------------------------------------------------------------------
# Core multiplier
# ---------------------------------------------------------------------------

def _compute_abs_multiplier(
    prop_type: str,
    batter_height_in: float,
) -> float:
    """
    Compute the ABS challenge multiplier for a single prop.

    Logic:
      - Zone size ratio drives the direction of adjustment.
        * Taller batter → bigger zone → more K friendly, fewer BB.
        * Shorter batter → smaller zone → fewer Ks, more BB.
      - The challenge system correction partially offsets zone bias
        because hitters can challenge balls that clip a very large zone.
      - Net effect is dampened by the challenge success/failure rates.

    Returns a multiplier in the uncapped range; caller must clamp to
    [_MULTIPLIER_MIN, _MULTIPLIER_MAX].
    """
    prop_lower = prop_type.lower()
    if prop_lower not in _ABS_AFFECTED_PROPS:
        return 1.0

    zone_ratio = _zone_size_ratio(batter_height_in)
    zone_delta = zone_ratio - 1.0   # positive = larger than avg, negative = smaller

    # Battery net benefit: larger zone → battery challenges more borderline calls
    battery_net = (
        _BATTERY_CHALLENGE_RATE
        * _BATTERY_SUCCESS_RATE
        * zone_delta
    )

    # Hitter net benefit: larger zone → hitter challenges called strikes more often
    # This REDUCES the zone advantage for the battery
    hitter_correction = (
        _HITTER_CHALLENGE_RATE
        * _HITTER_SUCCESS_RATE
        * zone_delta
    )

    # Net zone influence (battery advantage minus hitter correction)
    net_zone_effect = battery_net - hitter_correction

    # Map to prop-type direction
    if prop_lower in ("pitcher_strikeouts", "batter_strikeouts"):
        # Larger zone → more Ks for pitcher, more Ks for batter (from their POV)
        raw_multiplier = 1.0 + net_zone_effect
    else:
        # "batter_walks", "pitcher_walks"
        # Larger zone → fewer walks (zone is harder to avoid)
        raw_multiplier = 1.0 - net_zone_effect

    # FIX BUG 4: Clamp the MULTIPLIER to [0.88, 1.12]
    # Do NOT clamp the final probability here — predictor.py applies its own cap.
    clamped = max(_MULTIPLIER_MIN, min(_MULTIPLIER_MAX, raw_multiplier))

    logger.debug(
        "[ABS] prop=%s height=%.1fin zone_ratio=%.3f net_effect=%.4f "
        "raw_mult=%.4f clamped_mult=%.4f",
        prop_lower, batter_height_in, zone_ratio, net_zone_effect,
        raw_multiplier, clamped,
    )
    return clamped


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_abs_to_probability(
    base_probability: float,
    prop_type: str,
    abs_context: ABSContext,
) -> tuple[float, float]:
    """
    Apply the ABS challenge multiplier to a base probability.

    Args:
        base_probability:  Probability before ABS adjustment (0–1).
        prop_type:         Prop category string.
        abs_context:       ABSContext with batter height and season flag.

    Returns:
        Tuple of (adjusted_probability, multiplier_used).
        If prop is not ABS-affected or is_2026_season is False,
        returns (base_probability, 1.0) — a clean no-op.
    """
    if not abs_context.is_2026_season:
        return base_probability, 1.0

    multiplier = _compute_abs_multiplier(prop_type, abs_context.batter_height_in)
    if multiplier == 1.0:
        return base_probability, 1.0

    # Apply multiplier to probability — predictor.py will apply its own 0.95 ceiling
    adjusted = base_probability * multiplier
    return adjusted, multiplier


def judge_vs_altuve_zone_check() -> dict:
    """
    Validation helper — confirms ~20% zone difference between Judge and Altuve.
    Used in tests and startup health checks.
    """
    judge_zone  = _zone_height_inches(_JUDGE_HEIGHT_IN)
    altuve_zone = _zone_height_inches(_ALTUVE_HEIGHT_IN)
    diff_pct    = (judge_zone - altuve_zone) / altuve_zone * 100
    return {
        "judge_zone_in":   round(judge_zone,  2),
        "altuve_zone_in":  round(altuve_zone, 2),
        "difference_pct":  round(diff_pct,    2),
        "expected_pct":    20.0,
        "pass":            abs(diff_pct - 20.0) < 5.0,
    }
