"""
park_factors.py
===============
FanGraphs 3-year rolling park factors (2024–2026) for all 30 MLB venues.
Source: FanGraphs Park Factors tool, wOBA metric, 3-year rolling, both sides.

Columns per park:
  pf        – overall Park Factor (wOBA composite, 100 = neutral)
  woba_con  – wOBA on contact
  xwoba_con – xwOBA on contact
  bacon     – BACON (batting avg on contact)
  xbacon    – xBACON
  hardhit   – hard-hit rate index
  r         – runs factor
  obp       – OBP factor
  h         – hits factor
  singles   – 1B factor
  doubles   – 2B factor
  triples   – 3B factor  (Coors=191, ARI=207 = extreme outliers)
  hr        – HR factor
  bb        – BB factor
  so        – strikeout factor (below 100 = park suppresses Ks; above = amplifies)

Usage:
  from park_factors import get_park_factor, get_k_factor, get_hr_factor

  pf  = get_park_factor("LAD")          # 102
  k   = get_k_factor("SEA")             # 117  → T-Mobile amplifies Ks
  hr  = get_hr_factor("COL")            # 108
  row = get_park_row("NYY")             # full dict

  # Also accepts venue names, city names, full team names
  pf  = get_park_factor("Dodger Stadium")   # 102
  pf  = get_park_factor("Los Angeles")      # 102 (Dodgers)
"""

from __future__ import annotations
from typing import Optional

# ---------------------------------------------------------------------------
# Raw data – FanGraphs 2024-2026 3-year rolling
# ---------------------------------------------------------------------------
_PARK_TABLE: dict[str, dict] = {
    "COL": {"team": "Rockies",   "venue": "Coors Field",                    "pf": 112, "woba_con": 112, "xwoba_con": 101, "bacon": 113, "xbacon": 102, "hardhit": 101, "r": 125, "obp": 111, "h": 117, "singles": 117, "doubles": 120, "triples": 191, "hr": 108, "bb": 100, "so":  91},
    "ARI": {"team": "D-backs",   "venue": "Chase Field",                    "pf": 105, "woba_con": 103, "xwoba_con": 102, "bacon": 103, "xbacon": 103, "hardhit": 103, "r": 110, "obp": 104, "h": 107, "singles": 103, "doubles": 118, "triples": 207, "hr":  93, "bb":  98, "so":  91},
    "MIN": {"team": "Twins",     "venue": "Target Field",                   "pf": 104, "woba_con": 103, "xwoba_con": 101, "bacon": 104, "xbacon": 100, "hardhit": 100, "r": 108, "obp": 104, "h": 104, "singles": 104, "doubles": 112, "triples":  81, "hr":  97, "bb": 100, "so":  97},
    "BAL": {"team": "Orioles",   "venue": "Oriole Park at Camden Yards",    "pf": 104, "woba_con": 105, "xwoba_con": 103, "bacon": 104, "xbacon": 102, "hardhit": 105, "r": 108, "obp": 102, "h": 106, "singles": 104, "doubles": 102, "triples": 129, "hr": 113, "bb":  91, "so":  99},
    "CIN": {"team": "Reds",      "venue": "Great American Ball Park",       "pf": 103, "woba_con": 104, "xwoba_con": 100, "bacon": 102, "xbacon": 100, "hardhit":  95, "r": 106, "obp": 102, "h": 100, "singles":  96, "doubles": 103, "triples":  68, "hr": 122, "bb": 107, "so": 102},
    "BOS": {"team": "Red Sox",   "venue": "Fenway Park",                    "pf": 102, "woba_con": 102, "xwoba_con":  99, "bacon": 104, "xbacon": 100, "hardhit": 100, "r": 104, "obp": 103, "h": 104, "singles": 105, "doubles": 118, "triples":  84, "hr":  84, "bb":  98, "so":  98},
    "PHI": {"team": "Phillies",  "venue": "Citizens Bank Park",             "pf": 102, "woba_con": 104, "xwoba_con":  98, "bacon": 103, "xbacon":  99, "hardhit":  99, "r": 104, "obp": 100, "h": 102, "singles": 103, "doubles":  94, "triples":  99, "hr": 114, "bb":  95, "so": 103},
    "LAD": {"team": "Dodgers",   "venue": "Dodger Stadium",                 "pf": 102, "woba_con": 102, "xwoba_con": 103, "bacon":  98, "xbacon": 102, "hardhit": 102, "r": 104, "obp":  99, "h":  98, "singles":  93, "doubles":  93, "triples":  70, "hr": 129, "bb": 104, "so": 100},
    "NYY": {"team": "Yankees",   "venue": "Yankee Stadium",                 "pf": 101, "woba_con": 101, "xwoba_con": 103, "bacon":  98, "xbacon": 100, "hardhit": 105, "r": 102, "obp": 101, "h":  95, "singles":  91, "doubles":  91, "triples":  69, "hr": 118, "bb": 118, "so": 102},
    "HOU": {"team": "Astros",    "venue": "Daikin Park",                    "pf": 101, "woba_con": 103, "xwoba_con":  99, "bacon": 102, "xbacon":  99, "hardhit":  99, "r": 102, "obp": 100, "h": 100, "singles":  98, "doubles":  97, "triples":  70, "hr": 116, "bb": 101, "so": 106},
    "DET": {"team": "Tigers",    "venue": "Comerica Park",                  "pf": 101, "woba_con": 100, "xwoba_con":  99, "bacon": 100, "xbacon":  99, "hardhit":  98, "r": 102, "obp": 100, "h": 101, "singles": 101, "doubles":  93, "triples": 152, "hr": 103, "bb":  98, "so":  97},
    "WSH": {"team": "Nationals", "venue": "Nationals Park",                 "pf": 101, "woba_con": 100, "xwoba_con": 102, "bacon": 101, "xbacon": 102, "hardhit": 103, "r": 102, "obp": 101, "h": 103, "singles": 106, "doubles":  97, "triples": 100, "hr":  97, "bb":  95, "so":  94},
    "TOR": {"team": "Blue Jays", "venue": "Rogers Centre",                  "pf": 101, "woba_con": 101, "xwoba_con": 101, "bacon":  99, "xbacon": 100, "hardhit": 100, "r": 102, "obp": 100, "h": 101, "singles": 100, "doubles": 104, "triples":  73, "hr": 110, "bb":  98, "so":  97},
    "LAA": {"team": "Angels",    "venue": "Angel Stadium",                  "pf": 100, "woba_con": 101, "xwoba_con":  98, "bacon": 101, "xbacon":  98, "hardhit":  99, "r": 100, "obp": 100, "h":  98, "singles":  98, "doubles":  92, "triples":  92, "hr": 108, "bb": 101, "so": 105},
    "ATL": {"team": "Braves",    "venue": "Truist Park",                    "pf": 100, "woba_con": 102, "xwoba_con": 102, "bacon": 103, "xbacon": 102, "hardhit": 101, "r": 100, "obp": 101, "h": 102, "singles": 105, "doubles":  95, "triples":  93, "hr":  95, "bb": 100, "so": 105},
    "KCR": {"team": "Royals",    "venue": "Kauffman Stadium",               "pf": 100, "woba_con":  98, "xwoba_con": 102, "bacon":  99, "xbacon": 102, "hardhit": 104, "r": 100, "obp": 101, "h": 103, "singles": 101, "doubles": 118, "triples": 185, "hr":  83, "bb": 100, "so":  91},
    "MIA": {"team": "Marlins",   "venue": "loanDepot park",                 "pf": 100, "woba_con":  99, "xwoba_con": 100, "bacon": 100, "xbacon": 100, "hardhit": 100, "r": 100, "obp": 101, "h": 101, "singles": 102, "doubles": 107, "triples": 132, "hr":  88, "bb":  99, "so":  97},
    "PIT": {"team": "Pirates",   "venue": "PNC Park",                       "pf": 100, "woba_con":  99, "xwoba_con": 101, "bacon": 101, "xbacon": 101, "hardhit": 102, "r": 100, "obp": 102, "h": 102, "singles": 102, "doubles": 118, "triples":  79, "hr":  79, "bb": 100, "so":  97},
    "NYM": {"team": "Mets",      "venue": "Citi Field",                     "pf":  99, "woba_con":  98, "xwoba_con": 101, "bacon":  98, "xbacon": 100, "hardhit": 101, "r":  98, "obp": 100, "h":  95, "singles":  95, "doubles":  93, "triples":  81, "hr": 102, "bb": 107, "so": 103},
    "SFG": {"team": "Giants",    "venue": "Oracle Park",                    "pf":  98, "woba_con":  97, "xwoba_con":  98, "bacon":  99, "xbacon":  98, "hardhit":  98, "r":  96, "obp":  99, "h": 101, "singles": 104, "doubles": 108, "triples": 137, "hr":  77, "bb":  93, "so":  97},
    "CLE": {"team": "Guardians", "venue": "Progressive Field",              "pf":  98, "woba_con":  98, "xwoba_con":  98, "bacon":  99, "xbacon":  99, "hardhit":  96, "r":  96, "obp":  99, "h":  97, "singles":  97, "doubles": 103, "triples":  50, "hr":  95, "bb": 102, "so": 105},
    "CWS": {"team": "White Sox", "venue": "Rate Field",                     "pf":  98, "woba_con":  96, "xwoba_con":  98, "bacon":  97, "xbacon":  98, "hardhit":  96, "r":  96, "obp": 100, "h":  97, "singles":  99, "doubles":  93, "triples":  76, "hr":  94, "bb": 105, "so":  97},
    "STL": {"team": "Cardinals", "venue": "Busch Stadium",                  "pf":  98, "woba_con":  95, "xwoba_con":  99, "bacon":  97, "xbacon": 100, "hardhit": 102, "r":  96, "obp":  99, "h": 102, "singles": 107, "doubles": 106, "triples":  77, "hr":  80, "bb":  93, "so":  90},
    "SDP": {"team": "Padres",    "venue": "Petco Park",                     "pf":  97, "woba_con":  98, "xwoba_con": 101, "bacon":  97, "xbacon":  99, "hardhit":  99, "r":  94, "obp":  97, "h":  96, "singles":  96, "doubles":  89, "triples":  70, "hr": 109, "bb": 100, "so": 102},
    "MIL": {"team": "Brewers",   "venue": "American Family Field",          "pf":  97, "woba_con":  99, "xwoba_con":  98, "bacon":  99, "xbacon":  99, "hardhit":  97, "r":  94, "obp":  97, "h":  95, "singles":  95, "doubles":  86, "triples":  93, "hr": 106, "bb": 104, "so": 109},
    "CHC": {"team": "Cubs",      "venue": "Wrigley Field",                  "pf":  95, "woba_con":  95, "xwoba_con":  99, "bacon":  95, "xbacon":  99, "hardhit": 101, "r":  90, "obp":  96, "h":  94, "singles":  96, "doubles":  81, "triples": 119, "hr":  97, "bb": 101, "so": 103},
    "TBR": {"team": "Rays",      "venue": "Tropicana Field",                "pf":  95, "woba_con":  96, "xwoba_con":  98, "bacon":  96, "xbacon":  98, "hardhit":  91, "r":  90, "obp":  95, "h":  95, "singles":  97, "doubles":  86, "triples": 128, "hr":  97, "bb":  96, "so": 104},
    "SEA": {"team": "Mariners",  "venue": "T-Mobile Park",                  "pf":  92, "woba_con":  95, "xwoba_con": 100, "bacon":  95, "xbacon": 100, "hardhit":  99, "r":  85, "obp":  92, "h":  90, "singles":  90, "doubles":  91, "triples":  42, "hr":  96, "bb":  96, "so": 117},
    "TEX": {"team": "Rangers",   "venue": "Globe Life Field",               "pf":  92, "woba_con":  91, "xwoba_con":  98, "bacon":  92, "xbacon":  98, "hardhit": 101, "r":  85, "obp":  92, "h":  93, "singles":  95, "doubles":  89, "triples":  79, "hr":  89, "bb":  95, "so": 103},
    # Athletics: moved to Sacramento 2025 — use neutral until new factors confirmed
    "OAK": {"team": "Athletics", "venue": "Sutter Health Park",             "pf": 100, "woba_con": 100, "xwoba_con": 100, "bacon": 100, "xbacon": 100, "hardhit": 100, "r": 100, "obp": 100, "h": 100, "singles": 100, "doubles": 100, "triples": 100, "hr": 100, "bb": 100, "so": 100},
}

# 2026-only wOBA park factor (from year-by-year table — most current single-year reading)
_PF_2026: dict[str, int] = {
    "COL": 112, "ARI": 105, "BAL": 104, "MIN": 104,
    "CIN": 103, "BOS": 102, "LAD": 102, "PHI": 102,
    "TOR": 101, "HOU": 101, "DET": 101, "WSH": 101,
    "NYY": 101, "LAA": 100, "KCR": 100, "PIT": 100,
    "MIA": 100, "ATL": 100, "NYM":  99, "CWS":  98,
    "CLE":  98, "SFG":  98, "STL":  98, "MIL":  97,
    "SDP":  97, "TBR":  95, "CHC":  95, "SEA":  92,
    "TEX":  92, "OAK": 100,
}

# ---------------------------------------------------------------------------
# Alias tables — venue names, city names, full team names → abbreviation
# ---------------------------------------------------------------------------
_VENUE_TO_ABBR: dict[str, str] = {
    v["venue"].lower(): k for k, v in _PARK_TABLE.items()
}
# Extra venue aliases (abbreviated / alternate names)
_VENUE_TO_ABBR.update({
    "coors field": "COL",
    "chase field": "ARI",
    "target field": "MIN",
    "camden yards": "BAL",
    "oriole park": "BAL",
    "great american ball park": "CIN",
    "gabp": "CIN",
    "fenway": "BOS",
    "fenway park": "BOS",
    "citizens bank park": "PHI",
    "cbp": "PHI",
    "dodger stadium": "LAD",
    "uniqlo field": "LAD",
    "yankee stadium": "NYY",
    "minute maid park": "HOU",
    "daikin park": "HOU",
    "comerica park": "DET",
    "nationals park": "WSH",
    "rogers centre": "TOR",
    "angel stadium": "LAA",
    "truist park": "ATL",
    "kauffman stadium": "KCR",
    "loandepot park": "MIA",
    "loan depot park": "MIA",
    "pnc park": "PIT",
    "citi field": "NYM",
    "oracle park": "SFG",
    "at&t park": "SFG",
    "progressive field": "CLE",
    "rate field": "CWS",
    "guaranteed rate field": "CWS",
    "busch stadium": "STL",
    "petco park": "SDP",
    "american family field": "MIL",
    "miller park": "MIL",
    "wrigley field": "CHC",
    "tropicana field": "TBR",
    "t-mobile park": "SEA",
    "safeco field": "SEA",
    "globe life field": "TEX",
    "sutter health park": "OAK",
    "coliseum": "OAK",
})

_TEAM_NAME_TO_ABBR: dict[str, str] = {
    v["team"].lower(): k for k, v in _PARK_TABLE.items()
}
_TEAM_NAME_TO_ABBR.update({
    "arizona": "ARI",   "diamondbacks": "ARI",
    "colorado": "COL",
    "minnesota": "MIN",
    "baltimore": "BAL",
    "cincinnati": "CIN",
    "boston": "BOS",
    "philadelphia": "PHI",
    "los angeles dodgers": "LAD",  "la dodgers": "LAD",
    "new york yankees": "NYY",     "ny yankees": "NYY",
    "houston": "HOU",
    "detroit": "DET",
    "washington": "WSH",
    "toronto": "TOR",
    "los angeles angels": "LAA",   "la angels": "LAA", "anaheim": "LAA",
    "atlanta": "ATL",
    "kansas city": "KCR",          "kc": "KCR",
    "miami": "MIA", "florida": "MIA",
    "pittsburgh": "PIT",
    "new york mets": "NYM",        "ny mets": "NYM",
    "san francisco": "SFG",        "sf giants": "SFG",
    "cleveland": "CLE",            "guardians": "CLE",
    "chicago white sox": "CWS",
    "st. louis": "STL",            "st louis": "STL",
    "san diego": "SDP",
    "milwaukee": "MIL",
    "chicago cubs": "CHC",
    "tampa bay": "TBR",            "tampa": "TBR",
    "seattle": "SEA",
    "texas": "TEX",
    "oakland": "OAK", "athletics": "OAK", "sacramento": "OAK",
})


def _resolve(team: str) -> Optional[str]:
    """Resolve team name / abbreviation / venue → 3-letter abbreviation."""
    if not team:
        return None
    t = team.strip()
    # Direct abbreviation match (2–3 letters)
    up = t.upper()
    if up in _PARK_TABLE:
        return up
    # Common short forms
    _short = {
        "KC": "KCR", "SD": "SDP", "SF": "SFG", "TB": "TBR",
        "WAS": "WSH", "WDC": "WSH", "CHW": "CWS", "CHA": "CWS",
    }
    if up in _short:
        return _short[up]
    lo = t.lower()
    if lo in _VENUE_TO_ABBR:
        return _VENUE_TO_ABBR[lo]
    if lo in _TEAM_NAME_TO_ABBR:
        return _TEAM_NAME_TO_ABBR[lo]
    # Fuzzy substring match against venue names
    for venue_key, abbr in _VENUE_TO_ABBR.items():
        if lo in venue_key or venue_key in lo:
            return abbr
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_park_row(team: str) -> Optional[dict]:
    """Return the full park-factor row dict or None."""
    abbr = _resolve(team)
    return _PARK_TABLE.get(abbr) if abbr else None


# ---------------------------------------------------------------------------
# prop_type → park factor stat column mapping
# ---------------------------------------------------------------------------
_PROP_TYPE_TO_STAT: dict[str, str] = {
    # Prop type strings used by prop_enrichment_layer → column in _PARK_TABLE
    "strikeouts":          "so",
    "pitcher_strikeouts":  "so",
    "hitter_strikeouts":   "so",
    "hits":                "h",
    "hits_allowed":        "h",
    "total_bases":         "h",
    "home_runs":           "hr",
    "earned_runs":         "r",
    "runs":                "r",
    "rbis":                "r",
    "rbi":                 "r",
    "hits_runs_rbis":      "r",
    "walks_allowed":       "bb",
    "pitching_outs":       "pf",
    "batting":             "pf",   # generic batting factor
    "fantasy_hitter":      "pf",
    "fantasy_pitcher":     "pf",
}


def _get_park_factor_raw(team: str, stat: str = "pf") -> float:
    """Internal: return the park factor INDEX (100=neutral) for a stat column."""
    row = get_park_row(team)
    if row is None:
        return 100.0
    return float(row.get(stat, row["pf"]))


def get_park_factor(venue: str, prop_type: str = "pf", team: str = "") -> float:
    """
    Return a multiplicative park factor for *prop_type* at *venue* / *team*.

    Signature matches prop_enrichment_layer.py calls:
        get_park_factor(venue or "", "batting", team)
        get_park_factor(venue, prop_type, team)

    Returns a multiplier around 1.0 (e.g. 1.04 for Oriole Park, batting).
    Returns 1.0 (neutral) if park not found.

    Resolution order: venue string first, then team string.
    """
    # Try venue first, then team as fallback
    abbr = _resolve(venue) or _resolve(team)
    if abbr is None:
        return 1.0
    stat = _PROP_TYPE_TO_STAT.get(prop_type, "pf")
    row = _PARK_TABLE.get(abbr)
    if row is None:
        return 1.0
    return round(float(row.get(stat, row["pf"])) / 100.0, 4)


def get_park_info(venue: str, team: str = "") -> dict:
    """
    Return a dict of park characteristics for prop_enrichment_layer.

    Keys: team, venue, pf, is_dome, altitude_ft, humidor, hr_factor,
          k_factor, r_factor, bb_factor, h_factor
    """
    abbr = _resolve(venue) or _resolve(team)
    if abbr is None:
        return {"pf": 1.0, "is_dome": False, "altitude_ft": 0, "humidor": False}
    row = _PARK_TABLE.get(abbr, {})
    _DOMES = {"TOR", "TBR", "MIA", "ARI", "MIN", "HOU", "MIL"}
    _HUMIDORS = {"COL", "ARI", "BOS", "NYM", "MIA"}
    _ALT = {
        "COL": 5280, "ARI": 1100, "MIN": 815, "KCR": 910,
        "STL": 465, "CIN": 550, "CHC": 594, "MIL": 635,
        "TEX": 551, "HOU": 43, "SFG": 11, "LAD": 500,
    }
    return {
        "team":        abbr,
        "venue":       row.get("venue", ""),
        "pf":          round(row.get("pf",  100) / 100.0, 4),
        "hr_factor":   round(row.get("hr",  100) / 100.0, 4),
        "k_factor":    round(row.get("so",  100) / 100.0, 4),
        "r_factor":    round(row.get("r",   100) / 100.0, 4),
        "bb_factor":   round(row.get("bb",  100) / 100.0, 4),
        "h_factor":    round(row.get("h",   100) / 100.0, 4),
        "is_dome":     abbr in _DOMES,
        "altitude_ft": _ALT.get(abbr, 0),
        "humidor":     abbr in _HUMIDORS,
    }


def get_k_factor(team: str) -> float:
    """Strikeout park factor (100 = neutral, >100 = amplifies Ks, <100 = suppresses)."""
    return _get_park_factor_raw(team, "so")


def get_hr_factor(team: str) -> float:
    """Home-run park factor."""
    return _get_park_factor_raw(team, "hr")


def get_r_factor(team: str) -> float:
    """Runs park factor."""
    return _get_park_factor_raw(team, "r")


def get_pf_2026(team: str) -> float:
    """2026-only single-year wOBA park factor (most current)."""
    abbr = _resolve(team)
    if abbr:
        return float(_PF_2026.get(abbr, 100))
    return 100.0


def get_k_mult(team: str) -> float:
    """
    Multiplicative K-rate adjustment factor.
    Returns value around 1.0 (e.g. 1.17 for Seattle = +17% Ks).
    """
    return get_k_factor(team) / 100.0


def get_hr_mult(team: str) -> float:
    """Multiplicative HR-rate adjustment factor."""
    return get_hr_factor(team) / 100.0


def get_r_mult(team: str) -> float:
    """Multiplicative runs adjustment factor."""
    return get_r_factor(team) / 100.0


# ---------------------------------------------------------------------------
# Convenience: given a prop_type, return the right park factor stat column
# ---------------------------------------------------------------------------
_PROP_TO_STAT: dict[str, str] = {
    "pitcher_strikeouts":  "so",
    "hitter_strikeouts":   "so",
    "hits_allowed":        "h",
    "hits_runs_rbis":      "r",
    "earned_runs":         "r",
    "walks_allowed":       "bb",
    "home_runs":           "hr",   # excluded from evaluation but factor available
    "total_bases":         "h",
    "pitching_outs":       "pf",
    "rbis":                "r",
    "runs":                "r",
}


def get_prop_park_factor(team: str, prop_type: str) -> float:
    """Return the most relevant park factor for a given prop type (index, 100=neutral)."""
    stat = (_PROP_TYPE_TO_STAT or _PROP_TO_STAT).get(prop_type, "pf")
    return _get_park_factor_raw(team, stat)


def get_prop_park_mult(team: str, prop_type: str) -> float:
    """Multiplicative form of get_prop_park_factor."""
    return get_prop_park_factor(team, prop_type) / 100.0


if __name__ == "__main__":
    # Self-test
    tests = [
        ("COL", "so", 91),
        ("SEA", "so", 117),
        ("COL", "hr", 108),
        ("LAD", "pf", 102),
        ("Dodger Stadium", "pf", 102),
        ("Los Angeles Dodgers", "pf", 102),
        ("yankee stadium", "hr", 118),
        ("KC", "pf", 100),
        ("SF", "hr", 77),
        ("TBR", "pf", 95),
        ("OAK", "pf", 100),
    ]
    fails = 0
    for team, stat, expected in tests:
        got = _get_park_factor_raw(team, stat)
        status = "✅" if got == expected else f"❌ expected {expected}"
        print(f"  {status}  get_park_factor({team!r}, {stat!r}) = {got}")
        if got != expected:
            fails += 1
    print()
    print(f"{'✅ All tests passed' if fails == 0 else f'❌ {fails} test(s) failed'}")
    print()
    print("Sample 3-arg get_park_factor (multiplier):")
    for venue, pt, tm in [
        ("Dodger Stadium", "strikeouts", "LAD"),
        ("T-Mobile Park",  "strikeouts", "SEA"),
        ("Coors Field",    "home_runs",  "COL"),
        ("",               "batting",    "NYY"),
    ]:
        mult = get_park_factor(venue, pt, tm)
        print(f"  get_park_factor({venue!r}, {pt!r}, {tm!r}) = {mult:.4f}")
    print()
    print("Sample get_park_info:")
    for v, t in [("Dodger Stadium", ""), ("", "SEA"), ("Coors Field", "")]:
        info = get_park_info(v, t)
        print(f"  {v or t}: dome={info['is_dome']} alt={info['altitude_ft']} hr={info['hr_factor']:.3f} k={info['k_factor']:.3f}")
    print()
    print("Sample K multipliers:")
    for abbr in ["SEA", "TEX", "COL", "LAD", "NYY"]:
        print(f"  {abbr}: K-mult = {get_k_mult(abbr):.3f}  HR-mult = {get_hr_mult(abbr):.3f}")
