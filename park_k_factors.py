"""
park_k_factors.py
=================
Strikeout-specific park factors for all 30 MLB stadiums.

Two values per stadium:
  general_factor — 3-year run factor (2022-2024), centred at 100
  k_factor       — K-specific factor, centred at 100

Source: playbook/scrapers/park_factors.py (Baseball Reference + FanGraphs).
Used by simulation_engine.py and prop_enrichment_layer.py to adjust K
probabilities based on the home ballpark.

Usage:
    from park_k_factors import get_park_k_mult, PARK_K_FACTORS
    mult, label = get_park_k_mult("Colorado Rockies")   # 0.94, "Coors — K suppressor"
    mult, label = get_park_k_mult("Tampa Bay Rays")      # 1.04, "K-boosting park"
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# All 30 stadiums — k_factor and general_factor, centred at 100
# ---------------------------------------------------------------------------
PARK_K_FACTORS: dict[str, dict] = {
    "Arizona Diamondbacks":  {"park": "Chase Field",              "general": 101, "k": 102, "dome": True,  "alt": 1082},
    "Atlanta Braves":        {"park": "Truist Park",              "general": 101, "k": 101, "dome": False, "alt": 1050},
    "Athletics":             {"park": "Sutter Health Park",       "general": 100, "k": 100, "dome": False, "alt": 25},
    "Baltimore Orioles":     {"park": "Camden Yards",             "general": 101, "k": 99,  "dome": False, "alt": 20},
    "Boston Red Sox":        {"park": "Fenway Park",              "general": 104, "k": 95,  "dome": False, "alt": 20},
    "Chicago Cubs":          {"park": "Wrigley Field",            "general": 103, "k": 97,  "dome": False, "alt": 595},
    "Chicago White Sox":     {"park": "Guaranteed Rate Field",    "general": 101, "k": 101, "dome": False, "alt": 595},
    "Cincinnati Reds":       {"park": "Great American Ball Park", "general": 106, "k": 103, "dome": False, "alt": 490},
    "Cleveland Guardians":   {"park": "Progressive Field",        "general": 98,  "k": 99,  "dome": False, "alt": 653},
    "Colorado Rockies":      {"park": "Coors Field",              "general": 115, "k": 88,  "dome": False, "alt": 5280},
    "Detroit Tigers":        {"park": "Comerica Park",            "general": 97,  "k": 98,  "dome": False, "alt": 600},
    "Houston Astros":        {"park": "Minute Maid Park",         "general": 99,  "k": 101, "dome": True,  "alt": 43},
    "Kansas City Royals":    {"park": "Kauffman Stadium",         "general": 99,  "k": 98,  "dome": False, "alt": 909},
    "Los Angeles Angels":    {"park": "Angel Stadium",            "general": 98,  "k": 99,  "dome": False, "alt": 160},
    "Los Angeles Dodgers":   {"park": "Dodger Stadium",           "general": 97,  "k": 100, "dome": False, "alt": 512},
    "Miami Marlins":         {"park": "LoanDepot Park",           "general": 94,  "k": 96,  "dome": True,  "alt": 6},
    "Milwaukee Brewers":     {"park": "American Family Field",    "general": 100, "k": 101, "dome": True,  "alt": 635},
    "Minnesota Twins":       {"park": "Target Field",             "general": 99,  "k": 100, "dome": False, "alt": 830},
    "New York Mets":         {"park": "Citi Field",               "general": 97,  "k": 101, "dome": False, "alt": 20},
    "New York Yankees":      {"park": "Yankee Stadium",           "general": 104, "k": 100, "dome": False, "alt": 55},
    "Philadelphia Phillies": {"park": "Citizens Bank Park",       "general": 105, "k": 102, "dome": False, "alt": 20},
    "Pittsburgh Pirates":    {"park": "PNC Park",                 "general": 97,  "k": 98,  "dome": False, "alt": 730},
    "San Diego Padres":      {"park": "Petco Park",               "general": 93,  "k": 97,  "dome": False, "alt": 20},
    "San Francisco Giants":  {"park": "Oracle Park",              "general": 93,  "k": 96,  "dome": False, "alt": 10},
    "Seattle Mariners":      {"park": "T-Mobile Park",            "general": 96,  "k": 104, "dome": False, "alt": 175},
    "St. Louis Cardinals":   {"park": "Busch Stadium",            "general": 98,  "k": 98,  "dome": False, "alt": 465},
    "Tampa Bay Rays":        {"park": "Tropicana Field",          "general": 95,  "k": 109, "dome": True,  "alt": 15},
    "Texas Rangers":         {"park": "Globe Life Field",         "general": 104, "k": 102, "dome": True,  "alt": 551},
    "Toronto Blue Jays":     {"park": "Rogers Centre",            "general": 100, "k": 101, "dome": True,  "alt": 76},
    "Washington Nationals":  {"park": "Nationals Park",           "general": 100, "k": 100, "dome": False, "alt": 25},
}

# Abbreviation / nickname aliases → full team name
_ALIASES: dict[str, str] = {
    "ari": "Arizona Diamondbacks", "az": "Arizona Diamondbacks",
    "atl": "Atlanta Braves",
    "ath": "Athletics", "oak": "Athletics",
    "bal": "Baltimore Orioles",
    "bos": "Boston Red Sox",
    "chc": "Chicago Cubs",
    "cws": "Chicago White Sox", "chw": "Chicago White Sox",
    "cin": "Cincinnati Reds",
    "cle": "Cleveland Guardians",
    "col": "Colorado Rockies",
    "det": "Detroit Tigers",
    "hou": "Houston Astros",
    "kc":  "Kansas City Royals", "kcr": "Kansas City Royals",
    "laa": "Los Angeles Angels",
    "lad": "Los Angeles Dodgers",
    "mia": "Miami Marlins",
    "mil": "Milwaukee Brewers",
    "min": "Minnesota Twins",
    "nym": "New York Mets",
    "nyy": "New York Yankees",
    "phi": "Philadelphia Phillies",
    "pit": "Pittsburgh Pirates",
    "sdp": "San Diego Padres", "sd": "San Diego Padres",
    "sfg": "San Francisco Giants", "sf": "San Francisco Giants",
    "sea": "Seattle Mariners",
    "stl": "St. Louis Cardinals",
    "tbr": "Tampa Bay Rays", "tb": "Tampa Bay Rays",
    "tex": "Texas Rangers",
    "tor": "Toronto Blue Jays",
    "wsn": "Washington Nationals", "was": "Washington Nationals", "wsh": "Washington Nationals",
}


def _resolve(team: str) -> dict | None:
    """Resolve a team name/abbreviation to its PARK_K_FACTORS entry."""
    if not team:
        return None
    # Direct match
    entry = PARK_K_FACTORS.get(team)
    if entry:
        return entry
    # Alias match
    key = team.lower().strip()
    full = _ALIASES.get(key)
    if full:
        return PARK_K_FACTORS.get(full)
    # Partial match
    for name, val in PARK_K_FACTORS.items():
        if key in name.lower() or name.lower() in key:
            return val
    return None


def get_park_k_mult(home_team: str) -> tuple[float, str]:
    """
    Return (k_multiplier, label) for the home team.

    k_multiplier is applied to the pitcher's expected K rate before the
    Poisson probability calculation.

    Coors Field (altitude > 4000 ft): 0.94  — thin air suppresses break
    k_factor ≥ 109:  1.04
    k_factor ≥ 103:  1.02
    k_factor ≥ 97:   1.00  (neutral)
    k_factor ≥ 92:   0.98
    k_factor < 92:   0.96
    """
    entry = _resolve(home_team)
    if entry is None:
        return 1.0, "unknown park"

    k   = entry["k"]
    alt = entry.get("alt", 0)

    if alt > 4000:
        return 0.94, f"{entry['park']} — Coors altitude K suppressor"
    if k >= 109:
        return 1.04, f"{entry['park']} — strong K booster (k={k})"
    if k >= 103:
        return 1.02, f"{entry['park']} — slight K boost (k={k})"
    if k >= 97:
        return 1.00, f"{entry['park']} — neutral (k={k})"
    if k >= 92:
        return 0.98, f"{entry['park']} — slight K suppressor (k={k})"
    return 0.96, f"{entry['park']} — K suppressor (k={k})"


def get_general_run_factor(home_team: str) -> float:
    """Return the general run factor (1.0 = neutral) for run props."""
    entry = _resolve(home_team)
    if entry is None:
        return 1.0
    return round(entry["general"] / 100.0, 4)
