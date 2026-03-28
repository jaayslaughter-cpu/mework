"""
dome_adjustment.py
==================
Dome stadium context modifier for PropIQ Analytics Engine.

When a game is played in a controlled-environment stadium (full dome or
closed retractable roof), this module zeroes out weather-based probability
boosts and replaces them with turf/environment-specific modifiers.

Integration point: applied in live_dispatcher._evaluate_props() after
the Predict+ layer (8b) and before the EV/prob gate checks.

Key design decisions:
  - Weather adjustment zeroed for dome games (no wind/temp/humidity effect)
  - Turf modifier only fires at Tropicana Field and Rogers Centre
  - Modifiers are conservative -- dome effect is real but not massive
  - Roof status required for retractable parks; defaults to 'closed' (safe fallback)

2026 ABS note: Dome modifier and ABS challenge modifier are independent --
both can apply to the same K/BB prop without double-counting.
"""

from __future__ import annotations

# Stadiums with full or retractable roof that eliminate weather as a factor
DOME_STADIUMS: set[str] = {
    "Tropicana Field",         # Tampa Bay Rays -- permanent dome
    "loanDepot Park",          # Miami Marlins -- retractable (typically closed)
    "Rogers Centre",           # Toronto Blue Jays -- retractable (typically closed)
    "Minute Maid Park",        # Houston Astros -- retractable
    "T-Mobile Park",           # Seattle Mariners -- retractable
    "American Family Field",   # Milwaukee Brewers -- retractable
    "Chase Field",             # Arizona Diamondbacks -- retractable
}

# Full permanent domes: always controlled-environment, ignore roof_status
_FULL_DOMES: set[str] = {
    "Tropicana Field",
    "loanDepot Park",
    "Rogers Centre",
}

# Artificial turf venues: turf speed modifier (+3%) fires for contact/speed props
_TURF_VENUES: set[str] = {
    "Tropicana Field",
    "Rogers Centre",
}

# Prop-type environment modifiers for dome/closed-roof conditions
# Conservative: dome effect is real but overcorrecting hurts model calibration
_DOME_MODIFIERS: dict[str, float] = {
    "strikeouts":     -0.02,   # K suppression -- cleaner sight lines, no cold stiffness
    "total_bases":     0.01,   # Marginal TB boost -- consistent air, no wind suppression
    "hits":            0.02,   # Turf + consistency = slight hit bump
    "runs":            0.015,  # Moderate run boost -- turf gaps, consistent conditions
    "earned_runs":    -0.01,   # Slight ER suppression -- controlled conditions favor pitching
    "walks":           0.00,   # Neutral -- dome doesn't strongly affect BB rate
    "home_runs":       0.00,   # Neutral -- no wind means no extra carry
    "rbis":            0.01,   # Small positive (run environment slightly elevated)
    "hits_runs_rbis":  0.015,  # Composite: hits + runs both tick up slightly
    "stolen_bases":    0.01,   # Turf assists stolen base success rate
    "fantasy_hitter":  0.01,   # Reflects hits/runs bump
    "fantasy_pitcher": -0.01,  # Reflects K suppression
}

# Turf speed bonus: added to contact/speed props at turf-only venues
_TURF_MODIFIER: float = 0.03

# Turf modifier does NOT apply to pitcher or walk props
_TURF_EXEMPT_PROPS: frozenset[str] = frozenset({
    "strikeouts", "earned_runs", "walks", "fantasy_pitcher",
})

# Enclosed stadium crowd noise -- small home team advantage
_CROWD_MODIFIER: float = 0.005


def is_dome_game(venue: str, roof_status: str = "closed") -> bool:
    """
    Return True if the game is played in a controlled-environment stadium.

    Full domes (Tropicana, loanDepot, Rogers Centre) are always True
    regardless of roof_status.  Retractable-roof parks are True only
    when roof_status is 'closed'.

    Args:
        venue:       Stadium name (matched against DOME_STADIUMS set)
        roof_status: 'open' or 'closed' -- matters for retractable roofs only

    Returns:
        bool -- True if environment is controlled (dome or closed retractable)
    """
    if not venue:
        return False
    if venue in _FULL_DOMES:
        return True
    retractable = DOME_STADIUMS - _FULL_DOMES
    return venue in retractable and roof_status.lower() == "closed"


def apply_dome_adjustment(
    prob: float,
    prop_type: str,
    venue: str,
    roof_status: str = "closed",
    is_home_team: bool = True,
) -> tuple[float, float]:
    """
    Apply dome-specific probability modifier to a prop leg.

    For non-dome games returns (prob, 0.0) unchanged.
    For dome/closed-roof games, applies:
      1. Turf speed modifier (+3%) for contact/speed props at turf venues
      2. Prop-specific environment modifier (see _DOME_MODIFIERS)
      3. Crowd noise modifier (+0.5%) for home team players

    Args:
        prob:         Current implied probability (0.0 to 1.0)
        prop_type:    Prop category key (e.g., 'strikeouts', 'hits')
        venue:        Stadium name (matched against DOME_STADIUMS)
        roof_status:  'open' or 'closed' (retractable roof parks only)
        is_home_team: Whether the player is on the home team

    Returns:
        (adjusted_prob, nudge_delta) tuple
        -- adjusted_prob: clamped to [0.40, 0.80]
        -- nudge_delta: total delta applied (for decision logging)
    """
    if not is_dome_game(venue, roof_status):
        return prob, 0.0

    # Turf speed modifier (contact/speed props only; exempt K/BB/ER/FP props)
    turf_modifier = (
        _TURF_MODIFIER
        if venue in _TURF_VENUES and prop_type not in _TURF_EXEMPT_PROPS
        else 0.0
    )

    # Environment modifier for this prop type
    prop_modifier = _DOME_MODIFIERS.get(prop_type, 0.0)

    # Crowd noise (home team only; enclosed stadiums amplify home advantage)
    crowd_modifier = _CROWD_MODIFIER if is_home_team else 0.0

    total_nudge = turf_modifier + prop_modifier + crowd_modifier
    adjusted = round(min(0.80, max(0.40, prob + total_nudge)), 4)
    return adjusted, round(total_nudge, 4)


def get_dome_context(venue: str, roof_status: str = "closed") -> dict:
    """
    Return a summary dict describing dome context for decision logging.

    Example output:
        {"is_dome": True, "venue": "Tropicana Field", "roof_status": "closed",
         "has_turf": True, "is_full_dome": True}
    """
    dome = is_dome_game(venue, roof_status)
    return {
        "is_dome": dome,
        "venue": venue,
        "roof_status": roof_status,
        "has_turf": venue in _TURF_VENUES,
        "is_full_dome": venue in _FULL_DOMES,
    }
