"""
abs_challenge.py
----------------
Automated Ball-Strike (ABS) challenge system modifier for PropIQ.

Models the 2026 MLB rule change where batters and pitchers/catchers can
challenge ball/strike calls, with the ABS camera adjudicating challenges.

Six core facts modeled:
  1. Strike zone = 27% to 53.5% of batter height (vertical)
  2. Zone judged at the CENTER of home plate (front edge)
  3. Grazing the edge = called strike (no benefit-of-the-doubt to batter)
  4. Pitcher/Catcher teams initiate 53% of challenges; succeed 60% of the time
  5. Hitters initiate 47% of challenges; succeed only 46% of the time
  6. Judge's zone is ~20% larger (by area) than Altuve's

Prop multipliers returned for:
  - pitcher_strikeouts
  - batter_strikeouts
  - batter_walks

All other prop types return 1.0 (no ABS effect).
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ── Challenge statistics from 2026 ABS pilot data ──────────────────────────
PITCHER_CATCHER_CHALLENGE_SHARE = 0.53   # 53% of challenges initiated by battery
PITCHER_CATCHER_SUCCESS_RATE    = 0.60   # 60% of those succeed (call overturned to strike)

HITTER_CHALLENGE_SHARE          = 0.47   # 47% of challenges initiated by hitter
HITTER_SUCCESS_RATE             = 0.46   # 46% of those succeed (call overturned to ball)

# Challenges per game (estimated from early 2026 data)
CHALLENGES_PER_GAME_BATTERY     = 1.8    # Battery uses ~1.8 challenges/game
CHALLENGES_PER_GAME_HITTER      = 1.6    # Hitters use ~1.6 challenges/game

# ── Zone height anchors ─────────────────────────────────────────────────────
ZONE_BOTTOM_PCT = 0.270   # 27.0% of height
ZONE_TOP_PCT    = 0.535   # 53.5% of height

# Reference player heights (inches)
# Aaron Judge: 6'7" = 79 inches
# Jose Altuve: 5'6" = 66 inches
HEIGHT_JUDGE_IN  = 79.0
HEIGHT_ALTUVE_IN = 66.0

# K-zone area scales with height squared (both vertical and marginal horizontal)
# Judge zone area / Altuve zone area = (79/66)^2 ≈ 1.43 → ~20% larger by side length
# Using the vertical dimension only: 79/66 = 1.197 ≈ 20% taller
ZONE_SIZE_RATIO_JUDGE_VS_ALTUVE = (HEIGHT_JUDGE_IN / HEIGHT_ALTUVE_IN)  # 1.197


# ── Prop categories this module affects ─────────────────────────────────────
ABS_AFFECTED_PROPS = frozenset({
    "pitcher_strikeouts",
    "batter_strikeouts",
    "batter_walks",
    "pitcher_walks",
})


@dataclass
class ABSContext:
    """
    Context object carrying batter/pitcher ABS-relevant attributes.

    Attributes
    ----------
    batter_height_in : float
        Batter's height in inches. Drives zone sizing. Default 72.0 (6'0").
    pitcher_k_rate : float
        Pitcher's season K% (0.0-1.0). High K pitchers rely more on borderline
        calls, so ABS slightly penalises them. Default 0.22 (league avg).
    batter_k_rate : float
        Batter's season K% (0.0-1.0). High-K batters face more ABS exposure.
        Default 0.23 (league avg).
    batter_bb_rate : float
        Batter's season BB% (0.0-1.0). Short batters earn more walks via
        successful challenges. Default 0.085 (league avg).
    is_high_k_pitcher : bool
        Convenience flag. Set True if pitcher K% > 0.28 (top-tier strikeout arm).
    prop_type : str
        The prop category being evaluated. Used for targeted modifier selection.
    """
    batter_height_in: float = 72.0        # 6'0" league average
    pitcher_k_rate: float   = 0.22        # League-average K%
    batter_k_rate: float    = 0.23        # League-average K%
    batter_bb_rate: float   = 0.085       # League-average BB%
    is_high_k_pitcher: bool = False       # True if pitcher K% > 28%
    prop_type: str          = ""


def calculate_zone_height(batter_height_in: float) -> dict:
    """
    Calculate the ABS strike zone vertical boundaries for a given batter height.

    Zone = 27% to 53.5% of height (measured from ground).
    Grazing the edge = strike (no benefit-of-the-doubt to batter).

    Returns
    -------
    dict with keys: bottom_in, top_in, height_in, size_vs_league_avg
    """
    bottom = batter_height_in * ZONE_BOTTOM_PCT
    top    = batter_height_in * ZONE_TOP_PCT
    zone_height = top - bottom

    # Compare to league-average zone (72-inch batter)
    league_avg_zone = 72.0 * (ZONE_TOP_PCT - ZONE_BOTTOM_PCT)
    size_ratio = zone_height / league_avg_zone

    return {
        "bottom_in":         round(bottom, 2),
        "top_in":            round(top, 2),
        "height_in":         round(zone_height, 2),
        "size_vs_league_avg": round(size_ratio, 4),
    }


def _expected_overturned_to_strikes_per_pa(challenges_per_game: float = CHALLENGES_PER_GAME_BATTERY) -> float:
    """
    Expected calls overturned TO STRIKES per plate appearance via battery challenges.
    Assumes roughly 4 PAs per game per batter.
    """
    overturned_per_game = challenges_per_game * PITCHER_CATCHER_SUCCESS_RATE
    return overturned_per_game / 4.0  # per PA


def _expected_overturned_to_balls_per_pa(challenges_per_game: float = CHALLENGES_PER_GAME_HITTER) -> float:
    """
    Expected calls overturned TO BALLS per plate appearance via hitter challenges.
    """
    overturned_per_game = challenges_per_game * HITTER_SUCCESS_RATE
    return overturned_per_game / 4.0  # per PA


def _zone_size_modifier(batter_height_in: float) -> float:
    """
    Returns a multiplier (>1.0 means bigger zone, <1.0 means smaller zone)
    relative to the 72-inch league-average batter.

    Taller batters: bigger zone = more K risk, fewer BB
    Shorter batters: smaller zone = less K risk, more BB
    """
    league_avg_height = 72.0
    return batter_height_in / league_avg_height


def get_abs_multiplier(prop_type: str, abs_context: "ABSContext") -> float:
    """
    Return the ABS probability multiplier for a given prop type and batter context.

    Multiplier semantics: multiply the base prop probability by this value.
    1.0 = no change | >1.0 = prop more likely | <1.0 = prop less likely

    Affected props:
      - pitcher_strikeouts
      - batter_strikeouts
      - batter_walks / pitcher_walks

    All others return exactly 1.0.

    Parameters
    ----------
    prop_type : str
        Prop category string (e.g., "pitcher_strikeouts").
    abs_context : ABSContext
        Batter/pitcher attributes for this matchup.

    Returns
    -------
    float multiplier clamped to [0.88, 1.12]
    """
    pt = prop_type.lower().strip()

    if pt not in ABS_AFFECTED_PROPS:
        return 1.0

    height = abs_context.batter_height_in
    zone   = calculate_zone_height(height)
    zone_ratio = zone["size_vs_league_avg"]  # e.g., 1.097 for Judge, 0.917 for Altuve

    # Extra strikes added per PA from successful battery challenges
    extra_strikes_pa = _expected_overturned_to_strikes_per_pa()
    # Extra balls added per PA from successful hitter challenges
    extra_balls_pa = _expected_overturned_to_balls_per_pa()

    # ── PITCHER STRIKEOUTS ─────────────────────────────────────────────────
    if pt == "pitcher_strikeouts":
        # Bigger zone (taller batter) → more K opportunities for pitcher
        zone_k_boost = (zone_ratio - 1.0) * 0.30  # 30% sensitivity

        # Battery challenges recover borderline strikes → small K boost
        challenge_k_boost = extra_strikes_pa * 0.40

        # High-K pitchers depend more on borderline calls (which ABS now
        # adjudicates more strictly) → slight penalty for elite K arms
        elite_k_penalty = -0.015 if abs_context.is_high_k_pitcher else 0.0

        multiplier = 1.0 + zone_k_boost + challenge_k_boost + elite_k_penalty

        logger.debug(
            "[ABS] pitcher_strikeouts | height=%.1f zone_ratio=%.4f "
            "zone_k_boost=%.4f challenge_k_boost=%.4f elite_penalty=%.4f "
            "=> multiplier=%.4f",
            height, zone_ratio, zone_k_boost, challenge_k_boost, elite_k_penalty, multiplier,
        )

    # ── BATTER STRIKEOUTS ──────────────────────────────────────────────────
    elif pt == "batter_strikeouts":
        # Bigger zone = batter faces more strike surface = more K risk
        zone_k_boost = (zone_ratio - 1.0) * 0.35

        # Battery challenges add borderline strikes → slightly more K risk for batter
        challenge_k_exposure = extra_strikes_pa * 0.30

        # Hitter challenges recover some bad calls → slight K reduction
        hitter_challenge_save = -extra_balls_pa * 0.25

        # High personal K rate amplifies zone-size sensitivity
        k_rate_amplifier = (abs_context.batter_k_rate - 0.23) * 0.10

        multiplier = 1.0 + zone_k_boost + challenge_k_exposure + hitter_challenge_save + k_rate_amplifier

        logger.debug(
            "[ABS] batter_strikeouts | height=%.1f zone_ratio=%.4f "
            "zone_k_boost=%.4f challenge_exposure=%.4f hitter_save=%.4f k_rate_amp=%.4f "
            "=> multiplier=%.4f",
            height, zone_ratio, zone_k_boost, challenge_k_exposure,
            hitter_challenge_save, k_rate_amplifier, multiplier,
        )

    # ── BATTER / PITCHER WALKS ─────────────────────────────────────────────
    elif pt in ("batter_walks", "pitcher_walks"):
        # Smaller zone (shorter batter) = fewer strike calls = more walk opportunities
        zone_bb_boost = (1.0 - zone_ratio) * 0.35  # inverted: short = positive

        # Hitter challenges that overturn strikes to balls → walk boost
        hitter_challenge_bb_boost = extra_balls_pa * 0.50

        # Battery challenges that recover pitches as strikes → walk reduction
        battery_bb_penalty = -extra_strikes_pa * 0.20

        # Personal BB rate sensitivity
        bb_rate_amplifier = (abs_context.batter_bb_rate - 0.085) * 0.15

        multiplier = 1.0 + zone_bb_boost + hitter_challenge_bb_boost + battery_bb_penalty + bb_rate_amplifier

        logger.debug(
            "[ABS] %s | height=%.1f zone_ratio=%.4f "
            "zone_bb_boost=%.4f hitter_bb_boost=%.4f battery_penalty=%.4f bb_rate_amp=%.4f "
            "=> multiplier=%.4f",
            pt, height, zone_ratio, zone_bb_boost, hitter_challenge_bb_boost,
            battery_bb_penalty, bb_rate_amplifier, multiplier,
        )

    else:
        return 1.0

    # Clamp: ABS effect is real but bounded; don't let it override the model
    return round(max(0.88, min(1.12, multiplier)), 5)


def apply_abs_to_probability(
    base_probability: float,
    prop_type: str,
    abs_context: Optional["ABSContext"] = None,
) -> tuple[float, float]:
    """
    Apply ABS multiplier to a base probability and return (adjusted_prob, multiplier).

    Parameters
    ----------
    base_probability : float
        The probability coming out of the ML pipeline (0.0-1.0).
    prop_type : str
        Prop category string.
    abs_context : ABSContext or None
        If None, returns base_probability unchanged with multiplier 1.0.

    Returns
    -------
    (adjusted_probability: float, multiplier_used: float)
    """
    if abs_context is None:
        return base_probability, 1.0

    multiplier = get_abs_multiplier(prop_type, abs_context)

    if multiplier == 1.0:
        return base_probability, 1.0

    adjusted = base_probability * multiplier
    # Keep probability in sensible bounds
    adjusted = max(0.40, min(0.80, adjusted))

    logger.info(
        "[ABS] %s | base_prob=%.4f multiplier=%.5f adjusted_prob=%.4f",
        prop_type, base_probability, multiplier, adjusted,
    )
    return round(adjusted, 5), multiplier
