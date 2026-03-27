"""
altitude_adjustment.py
======================
Altitude-based park factor modifiers for PropIQ.

Why it matters
--------------
Thinner air at elevation reduces aerodynamic drag, causing the ball to carry
farther (HR/XBH boost) and breaking balls to lose movement (K suppression).
Coors Field (~5,280 ft) is the most extreme park factor in professional sports.

MLB mandated a humidor at Coors Field starting in 2002 and Chase Field in 2018.
The humidor stores balls at consistent humidity (~50%), which partially offsets
altitude inflation but does not eliminate it.

Integration
-----------
    from altitude_adjustment import (
        apply_altitude_adjustments,
        get_humidor_status,
        TEAM_TO_VENUE,
    )

    venue = TEAM_TO_VENUE.get(team_name, "")
    projection = apply_altitude_adjustments(
        base_projection=projection,
        prop_type=prop_type,
        venue=venue,
        humidor_active=get_humidor_status(venue),
    )

Note: Chase Field is a retractable roof AND at altitude AND has a humidor.
Both dome and altitude branches apply when the roof is closed.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Altitude in feet for parks that produce meaningful adjustments.
# Parks at or below SEA_LEVEL_BASELINE_FT receive no modifier.
STADIUM_ALTITUDE_FT: dict[str, int] = {
    "Coors Field":            5280,   # Denver -- extreme
    "Chase Field":            1082,   # Phoenix -- mild, retractable + humidor
    "Kauffman Stadium":        909,   # Kansas City -- mild
    "Globe Life Field":        551,   # Arlington -- low-moderate
    "Truist Park":            1050,   # Atlanta -- mild
    "American Family Field":   635,   # Milwaukee -- negligible but tracked
}

# Parks at or below this elevation receive no adjustment
SEA_LEVEL_BASELINE_FT: int = 400

# Stadiums with MLB-mandated humidors (partially offset altitude boost)
HUMIDOR_STADIUMS: frozenset[str] = frozenset({
    "Coors Field",   # Since 2002
    "Chase Field",   # Since 2018
})

# Humidor dampens raw altitude factor by this fraction
HUMIDOR_DAMPENER: float = 0.35

# Research-backed: ~1% offensive boost per 1,000 ft above baseline
ALTITUDE_BOOST_PER_1000_FT: float = 0.01

# Prop-type multipliers applied to the altitude factor.
# Keys must match prop_type strings used elsewhere in PropIQ.
_PROP_MULTIPLIERS: dict[str, float] = {
    "home_runs":   2.0,    # Strongest effect -- ball carries farther
    "total_bases": 1.5,    # TB boosted by extra extra-base hits
    "hits":        1.2,    # Outfield gaps harder to cover
    "runs":        1.4,    # More baserunners + XBH = more runs
    "strikeouts": -1.0,    # Breaking balls flatten -- fewer Ks
    "walks":       0.3,    # Pitchers lose command in thin air (slight)
}

# ---------------------------------------------------------------------------
# Team → Venue map (all 30 MLB franchises as of 2026)
# Keys match the "name" field from statsapi.mlb.com /sports/1/players
# (e.g., "Colorado Rockies", "Houston Astros")
# ---------------------------------------------------------------------------
TEAM_TO_VENUE: dict[str, str] = {
    # National League West
    "Colorado Rockies":         "Coors Field",
    "Arizona Diamondbacks":     "Chase Field",
    "Los Angeles Dodgers":      "Dodger Stadium",
    "San Francisco Giants":     "Oracle Park",
    "San Diego Padres":         "Petco Park",
    # National League Central
    "Chicago Cubs":             "Wrigley Field",
    "St. Louis Cardinals":      "Busch Stadium",
    "Milwaukee Brewers":        "American Family Field",
    "Cincinnati Reds":          "Great American Ball Park",
    "Pittsburgh Pirates":       "PNC Park",
    # National League East
    "Atlanta Braves":           "Truist Park",
    "New York Mets":            "Citi Field",
    "Philadelphia Phillies":    "Citizens Bank Park",
    "Washington Nationals":     "Nationals Park",
    "Miami Marlins":            "loanDepot Park",
    # American League West
    "Houston Astros":           "Minute Maid Park",
    "Los Angeles Angels":       "Angel Stadium",
    "Seattle Mariners":         "T-Mobile Park",
    "Texas Rangers":            "Globe Life Field",
    "Oakland Athletics":        "Sutter Health Park",   # 2025 Sacramento
    # American League Central
    "Minnesota Twins":          "Target Field",
    "Chicago White Sox":        "Guaranteed Rate Field",
    "Cleveland Guardians":      "Progressive Field",
    "Kansas City Royals":       "Kauffman Stadium",
    "Detroit Tigers":           "Comerica Park",
    # American League East
    "New York Yankees":         "Yankee Stadium",
    "Boston Red Sox":           "Fenway Park",
    "Toronto Blue Jays":        "Rogers Centre",
    "Tampa Bay Rays":           "Tropicana Field",
    "Baltimore Orioles":        "Oriole Park at Camden Yards",
}


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_humidor_status(venue: str) -> bool:
    """Return True if the stadium uses an MLB-mandated humidor."""
    return venue in HUMIDOR_STADIUMS


def get_altitude_ft(venue: str) -> int:
    """Return the altitude in feet for a given venue (0 if not tracked)."""
    return STADIUM_ALTITUDE_FT.get(venue, 0)


def get_venue_for_team(team_name: str) -> str:
    """Return venue name for a team name, empty string if not found."""
    return TEAM_TO_VENUE.get(team_name, "")


def apply_altitude_adjustments(
    base_projection: float,
    prop_type: str,
    venue: str,
    humidor_active: bool = False,
) -> float:
    """
    Apply altitude-based modifiers to a raw prop projection.

    Args:
        base_projection:  Raw model output (e.g., expected K, TB, HR).
        prop_type:        Prop category string: 'strikeouts', 'total_bases',
                          'hits', 'home_runs', 'runs', 'walks'.
                          Unknown prop types are returned unchanged.
        venue:            Stadium name matching keys in STADIUM_ALTITUDE_FT.
        humidor_active:   True if the ballpark stores balls in a humidor.
                          Use get_humidor_status(venue) for convenience.

    Returns:
        Adjusted projection (float), rounded to 4 decimal places.
        Returns base_projection unchanged if altitude <= SEA_LEVEL_BASELINE_FT.
    """
    altitude_ft = STADIUM_ALTITUDE_FT.get(venue, 0)

    if altitude_ft <= SEA_LEVEL_BASELINE_FT:
        return base_projection  # No meaningful altitude effect

    # Step 1: Base altitude factor (~1% per 1,000 ft above baseline)
    altitude_above_baseline = altitude_ft - SEA_LEVEL_BASELINE_FT
    raw_altitude_factor = (altitude_above_baseline / 1_000) * ALTITUDE_BOOST_PER_1000_FT

    # Step 2: Humidor dampening -- reduces raw factor by ~35%
    dampener = HUMIDOR_DAMPENER if humidor_active else 0.0
    altitude_factor = raw_altitude_factor * (1.0 - dampener)

    # Step 3: Prop-specific scaling
    prop_scale = _PROP_MULTIPLIERS.get(prop_type, 0.0)
    prop_modifier = altitude_factor * prop_scale

    adjusted = base_projection * (1.0 + prop_modifier)
    return round(adjusted, 4)


# ---------------------------------------------------------------------------
# Convenience wrapper for full pipeline
# ---------------------------------------------------------------------------

def altitude_context_for_game(venue: str) -> dict:
    """
    Return a dict of altitude metadata for logging / decision_log.

    Example return:
        {
            "venue": "Coors Field",
            "altitude_ft": 5280,
            "humidor_active": True,
            "altitude_above_baseline": 4880,
        }
    """
    altitude_ft = STADIUM_ALTITUDE_FT.get(venue, 0)
    humidor = get_humidor_status(venue)
    above = max(0, altitude_ft - SEA_LEVEL_BASELINE_FT)
    return {
        "venue": venue,
        "altitude_ft": altitude_ft,
        "humidor_active": humidor,
        "altitude_above_baseline": above,
    }
