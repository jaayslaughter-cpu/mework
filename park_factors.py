"""
park_factors.py
===============
MLB park factor adjustments per prop type.

Data: 2022-2025 multi-year park factors from FanGraphs/BaseRuns.
Normalized to 1.0 = neutral park. Values above 1.0 inflate the stat,
below 1.0 suppress it.

Prop-type specific factors account for park dimensions, altitude,
humidor use, and foul territory size.

Usage:
    from park_factors import get_park_factor
    pf = get_park_factor("Coors Field", "total_bases")
    # → 1.22  (22% boost to expected total bases at Coors)
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Park factor table
# Format: "venue_key": {prop_type: factor, ...}
# "batting" = general hitter factor (applied when specific type not found)
# ---------------------------------------------------------------------------
_PARK_FACTORS: dict[str, dict[str, float]] = {
    # ── Extreme hitter parks ──────────────────────────────────────────────
    "coors field": {
        "batting":       1.18,
        "hits":          1.15,
        "home_runs":     1.20,
        "total_bases":   1.22,
        "hits_runs_rbis":1.20,
        "runs":          1.22,
        "rbis":          1.18,
        "strikeouts":    0.93,  # pitchers struggle → fewer K per inning
        "earned_runs":   1.25,
        "pitching_outs": 0.96,
    },
    "great american ball park": {
        "batting":       1.10,
        "hits":          1.06,
        "home_runs":     1.15,
        "total_bases":   1.12,
        "hits_runs_rbis":1.10,
        "runs":          1.12,
        "rbis":          1.10,
        "strikeouts":    0.97,
        "earned_runs":   1.12,
        "pitching_outs": 0.98,
    },
    "yankee stadium": {
        "batting":       1.07,
        "hits":          1.04,
        "home_runs":     1.13,
        "total_bases":   1.10,
        "hits_runs_rbis":1.07,
        "runs":          1.09,
        "rbis":          1.07,
        "strikeouts":    1.00,
        "earned_runs":   1.09,
        "pitching_outs": 0.99,
    },
    "citizens bank park": {
        "batting":       1.08,
        "hits":          1.05,
        "home_runs":     1.10,
        "total_bases":   1.09,
        "hits_runs_rbis":1.08,
        "runs":          1.10,
        "rbis":          1.08,
        "strikeouts":    0.99,
        "earned_runs":   1.10,
        "pitching_outs": 0.99,
    },
    "globe life field": {
        "batting":       1.06,
        "hits":          1.04,
        "home_runs":     1.08,
        "total_bases":   1.07,
        "hits_runs_rbis":1.06,
        "runs":          1.07,
        "rbis":          1.06,
        "strikeouts":    1.01,
        "earned_runs":   1.07,
        "pitching_outs": 0.99,
    },
    # ── Neutral parks ─────────────────────────────────────────────────────
    "wrigley field": {
        "batting":       1.04,
        "hits":          1.03,
        "home_runs":     1.05,
        "total_bases":   1.05,
        "hits_runs_rbis":1.04,
        "runs":          1.05,
        "rbis":          1.04,
        "strikeouts":    0.99,
        "earned_runs":   1.05,
        "pitching_outs": 0.99,
    },
    "fenway park": {
        "batting":       1.04,
        "hits":          1.06,   # green monster inflates hits
        "home_runs":     0.96,   # suppresses HR (deep to right)
        "total_bases":   1.03,
        "hits_runs_rbis":1.05,
        "runs":          1.04,
        "rbis":          1.04,
        "strikeouts":    1.00,
        "earned_runs":   1.04,
        "pitching_outs": 0.99,
    },
    "chase field": {
        "batting":       1.03,
        "hits":          1.02,
        "home_runs":     1.04,
        "total_bases":   1.04,
        "hits_runs_rbis":1.03,
        "runs":          1.04,
        "rbis":          1.03,
        "strikeouts":    1.00,
        "earned_runs":   1.04,
        "pitching_outs": 1.00,
    },
    "truist park": {
        "batting":       1.02,
        "hits":          1.01,
        "home_runs":     1.03,
        "total_bases":   1.03,
        "hits_runs_rbis":1.02,
        "runs":          1.03,
        "rbis":          1.02,
        "strikeouts":    1.01,
        "earned_runs":   1.03,
        "pitching_outs": 1.00,
    },
    "american family field": {
        "batting":       1.02,
        "hits":          1.01,
        "home_runs":     1.04,
        "total_bases":   1.03,
        "hits_runs_rbis":1.02,
        "runs":          1.03,
        "rbis":          1.02,
        "strikeouts":    1.00,
        "earned_runs":   1.03,
        "pitching_outs": 1.00,
    },
    "oracle park": {
        "batting":       0.96,
        "hits":          0.97,
        "home_runs":     0.88,   # heavily suppresses HR (marine layer + deep)
        "total_bases":   0.93,
        "hits_runs_rbis":0.95,
        "runs":          0.94,
        "rbis":          0.95,
        "strikeouts":    1.02,
        "earned_runs":   0.94,
        "pitching_outs": 1.01,
    },
    "loandepot park": {
        "batting":       0.97,
        "hits":          0.97,
        "home_runs":     0.96,
        "total_bases":   0.96,
        "hits_runs_rbis":0.97,
        "runs":          0.96,
        "rbis":          0.97,
        "strikeouts":    1.01,
        "earned_runs":   0.96,
        "pitching_outs": 1.01,
    },
    # ── Pitcher-friendly parks ────────────────────────────────────────────
    "petco park": {
        "batting":       0.93,
        "hits":          0.94,
        "home_runs":     0.88,
        "total_bases":   0.91,
        "hits_runs_rbis":0.92,
        "runs":          0.91,
        "rbis":          0.92,
        "strikeouts":    1.03,
        "earned_runs":   0.91,
        "pitching_outs": 1.02,
    },
    "t-mobile park": {
        "batting":       0.94,
        "hits":          0.95,
        "home_runs":     0.90,
        "total_bases":   0.92,
        "hits_runs_rbis":0.93,
        "runs":          0.92,
        "rbis":          0.93,
        "strikeouts":    1.02,
        "earned_runs":   0.92,
        "pitching_outs": 1.02,
    },
    "marlins park": {
        "batting":       0.94,
        "hits":          0.95,
        "home_runs":     0.91,
        "total_bases":   0.93,
        "hits_runs_rbis":0.94,
        "runs":          0.92,
        "rbis":          0.93,
        "strikeouts":    1.02,
        "earned_runs":   0.92,
        "pitching_outs": 1.01,
    },
    "tropicana field": {
        "batting":       0.96,
        "hits":          0.96,
        "home_runs":     0.94,
        "total_bases":   0.95,
        "hits_runs_rbis":0.96,
        "runs":          0.95,
        "rbis":          0.96,
        "strikeouts":    1.01,
        "earned_runs":   0.95,
        "pitching_outs": 1.01,
    },
    "kauffman stadium": {
        "batting":       0.97,
        "hits":          0.98,
        "home_runs":     0.93,
        "total_bases":   0.95,
        "hits_runs_rbis":0.97,
        "runs":          0.96,
        "rbis":          0.97,
        "strikeouts":    1.01,
        "earned_runs":   0.96,
        "pitching_outs": 1.01,
    },
    "busch stadium": {
        "batting":       0.97,
        "hits":          0.97,
        "home_runs":     0.94,
        "total_bases":   0.96,
        "hits_runs_rbis":0.97,
        "runs":          0.96,
        "rbis":          0.97,
        "strikeouts":    1.01,
        "earned_runs":   0.96,
        "pitching_outs": 1.01,
    },
    # ── Other parks (near neutral) ─────────────────────────────────────────
    "pnc park":             {"batting": 0.98, "home_runs": 0.95, "strikeouts": 1.01, "earned_runs": 0.98},
    "progressive field":    {"batting": 1.00, "home_runs": 1.00, "strikeouts": 1.00, "earned_runs": 1.00},
    "target field":         {"batting": 0.99, "home_runs": 0.97, "strikeouts": 1.01, "earned_runs": 0.99},
    "rogers centre":        {"batting": 1.01, "home_runs": 1.03, "strikeouts": 1.00, "earned_runs": 1.01},
    "camden yards":         {"batting": 1.02, "home_runs": 1.05, "strikeouts": 1.00, "earned_runs": 1.02},
    "guaranteed rate field":{"batting": 1.01, "home_runs": 1.04, "strikeouts": 1.00, "earned_runs": 1.01},
    "minute maid park":     {"batting": 1.01, "home_runs": 1.02, "strikeouts": 1.00, "earned_runs": 1.01},
    "angel stadium":        {"batting": 0.99, "home_runs": 0.98, "strikeouts": 1.01, "earned_runs": 0.99},
    "dodger stadium":       {"batting": 0.97, "home_runs": 0.95, "strikeouts": 1.02, "earned_runs": 0.97},
    "oakland coliseum":     {"batting": 0.95, "home_runs": 0.91, "strikeouts": 1.02, "earned_runs": 0.95},
    "comerica park":        {"batting": 0.96, "home_runs": 0.92, "strikeouts": 1.02, "earned_runs": 0.96},
    "citi field":           {"batting": 0.96, "home_runs": 0.93, "strikeouts": 1.02, "earned_runs": 0.96},
    "nationals park":       {"batting": 0.98, "home_runs": 0.97, "strikeouts": 1.01, "earned_runs": 0.98},
    "suntrust park":        {"batting": 1.02, "home_runs": 1.03, "strikeouts": 1.00, "earned_runs": 1.02},
    "safeco field":         {"batting": 0.94, "home_runs": 0.90, "strikeouts": 1.02, "earned_runs": 0.94},
    "sutter health park":   {"batting": 1.00, "home_runs": 1.00, "strikeouts": 1.00, "earned_runs": 1.00},
    "american family field":{"batting": 1.02, "home_runs": 1.04, "strikeouts": 1.00, "earned_runs": 1.02},
    "premier12":            {"batting": 1.00, "home_runs": 1.00, "strikeouts": 1.00, "earned_runs": 1.00},
}

# Team name → venue fallback (when venue not on prop)
_TEAM_TO_VENUE: dict[str, str] = {
    "colorado rockies":       "coors field",
    "colorado":               "coors field",
    "rockies":                "coors field",
    "cincinnati reds":        "great american ball park",
    "cincinnati":             "great american ball park",
    "reds":                   "great american ball park",
    "new york yankees":       "yankee stadium",
    "yankees":                "yankee stadium",
    "philadelphia phillies":  "citizens bank park",
    "phillies":               "citizens bank park",
    "texas rangers":          "globe life field",
    "rangers":                "globe life field",
    "chicago cubs":           "wrigley field",
    "cubs":                   "wrigley field",
    "boston red sox":         "fenway park",
    "red sox":                "fenway park",
    "boston":                 "fenway park",
    "arizona diamondbacks":   "chase field",
    "diamondbacks":           "chase field",
    "arizona":                "chase field",
    "atlanta braves":         "truist park",
    "braves":                 "truist park",
    "atlanta":                "truist park",
    "milwaukee brewers":      "american family field",
    "brewers":                "american family field",
    "milwaukee":              "american family field",
    "san francisco giants":   "oracle park",
    "giants":                 "oracle park",
    "miami marlins":          "loandepot park",
    "marlins":                "loandepot park",
    "miami":                  "loandepot park",
    "san diego padres":       "petco park",
    "padres":                 "petco park",
    "san diego":              "petco park",
    "seattle mariners":       "t-mobile park",
    "mariners":               "t-mobile park",
    "seattle":                "t-mobile park",
    "tampa bay rays":         "tropicana field",
    "rays":                   "tropicana field",
    "tampa bay":              "tropicana field",
    "kansas city royals":     "kauffman stadium",
    "royals":                 "kauffman stadium",
    "kansas city":            "kauffman stadium",
    "st. louis cardinals":    "busch stadium",
    "cardinals":              "busch stadium",
    "st. louis":              "busch stadium",
    "pittsburgh pirates":     "pnc park",
    "pirates":                "pnc park",
    "pittsburgh":             "pnc park",
    "cleveland guardians":    "progressive field",
    "guardians":              "progressive field",
    "cleveland":              "progressive field",
    "minnesota twins":        "target field",
    "twins":                  "target field",
    "minnesota":              "target field",
    "toronto blue jays":      "rogers centre",
    "blue jays":              "rogers centre",
    "toronto":                "rogers centre",
    "baltimore orioles":      "camden yards",
    "orioles":                "camden yards",
    "baltimore":              "camden yards",
    "chicago white sox":      "guaranteed rate field",
    "white sox":              "guaranteed rate field",
    "houston astros":         "minute maid park",
    "astros":                 "minute maid park",
    "houston":                "minute maid park",
    "los angeles angels":     "angel stadium",
    "angels":                 "angel stadium",
    "anaheim":                "angel stadium",
    "los angeles dodgers":    "dodger stadium",
    "dodgers":                "dodger stadium",
    "los angeles":            "dodger stadium",
    "oakland athletics":      "oakland coliseum",
    "athletics":              "oakland coliseum",
    "oakland":                "oakland coliseum",
    "sacramento":             "sutter health park",
    "detroit tigers":         "comerica park",
    "tigers":                 "comerica park",
    "detroit":                "comerica park",
    "new york mets":          "citi field",
    "mets":                   "citi field",
    "washington nationals":   "nationals park",
    "nationals":              "nationals park",
    "washington":             "nationals park",
}

_NEUTRAL = 1.0


def _norm_venue(v: str) -> str:
    return v.lower().strip()


def get_park_factor(venue: str, prop_type: str, team: str = "") -> float:
    """
    Return park factor multiplier for a given venue and prop type.

    1.0 = neutral park, 1.10 = 10% boost, 0.90 = 10% suppression.

    Falls back to team name if venue not found, then to 1.0 (neutral).
    """
    v = _norm_venue(venue)
    park = _PARK_FACTORS.get(v)

    if park is None and team:
        # Try resolving via team name
        t = team.lower().strip()
        for team_key, venue_key in _TEAM_TO_VENUE.items():
            if team_key in t or t in team_key:
                park = _PARK_FACTORS.get(venue_key)
                break

    if park is None:
        return _NEUTRAL

    pt = prop_type.lower().replace(" ", "_").replace("+", "_")
    # Try exact prop type, then fallback to batting/pitching general factor
    factor = park.get(pt)
    if factor is None:
        _PITCHER_PROPS = {"strikeouts", "earned_runs", "pitching_outs",
                          "pitcher_strikeouts", "outs_recorded", "hits_allowed"}
        if pt in _PITCHER_PROPS:
            factor = park.get("earned_runs", park.get("batting", _NEUTRAL))
        else:
            factor = park.get("batting", _NEUTRAL)

    return float(factor)


def get_park_info(venue: str, team: str = "") -> dict[str, float]:
    """
    Return all park factors for a venue as a dict.
    Useful for logging / debugging.
    """
    v = _norm_venue(venue)
    park = _PARK_FACTORS.get(v)
    if park is None and team:
        t = team.lower().strip()
        for team_key, venue_key in _TEAM_TO_VENUE.items():
            if team_key in t or t in team_key:
                park = _PARK_FACTORS.get(venue_key)
                break
    return dict(park) if park else {}
