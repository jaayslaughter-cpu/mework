"""
bayesian_shrinkage.py
=====================
Early-season Bayesian shrinkage for MLB player prop rate estimates.

Formula: proj = (n_current * rate_current + k_prior * rate_prior) / (n_current + k_prior)

Where:
  - k_prior varies by stat stability (higher = more shrinkage toward prior)
  - rate_prior = 3-year weighted average (50% last year, 30% year-2, 20% career)
  - n_current = actual PA/BF for batter/pitcher this season

Stat stability tiers (k_prior values):
  STABLE   (K%, BB%, SwStr%, CSW%):   Week 1-2: 10, Week 3-4: 6
  MID_TIER (xwOBA, FIP, wOBA, wRC+):  Week 1-2: 18, Week 3-4: 12
  VOLATILE (HR, ISO, BABIP, ERA, xSLG): Week 1-2: 30, Week 3-4: 20

Home/Away split shrinkage:
  proj_split = (n_split * rate_split + k_split * rate_overall) / (n_split + k_split)
  k_split = 8 (batters), 15 (pitchers)

Season weeks auto-detected from date (MLB season starts ~April 1 each year).
Current weight cap: max 20% (weeks 1-2), max 40% (weeks 3-4), 100% after week 8.

Used by: prop_enrichment_layer._player_specific_rate()
         marcel_layer (consult only — Marcel already does 3yr prior)
"""

from __future__ import annotations

import logging
from datetime import date
from zoneinfo import ZoneInfo

logger = logging.getLogger("propiq.bayesian_shrinkage")

# ---------------------------------------------------------------------------
# MLB season start date (approx. April 1 each year)
# ---------------------------------------------------------------------------
_MLB_SEASON_STARTS = {
    2024: date(2024, 3, 20),
    2025: date(2025, 3, 27),
    2026: date(2026, 3, 26),  # 2026 Opening Day (approx)
}
_DEFAULT_START_MONTH = 3
_DEFAULT_START_DAY   = 28


def _get_season_start(year: int) -> date:
    return _MLB_SEASON_STARTS.get(year, date(year, _DEFAULT_START_MONTH, _DEFAULT_START_DAY))


def get_season_week() -> int:
    """
    Return current week of MLB season (1-indexed).
    Uses America/Los_Angeles (Pacific Time per system directive).
    Returns 1 if before Opening Day, 99 if offseason.
    """
    today = date.today()  # We use ZoneInfo-aware date via PT
    try:
        from datetime import datetime
        today = datetime.now(tz=ZoneInfo("America/Los_Angeles")).date()
    except Exception:
        pass

    year = today.year
    start = _get_season_start(year)
    if today < start:
        return 1  # pre-season, treat as week 1 (most conservative shrinkage)
    days_in = (today - start).days
    week = (days_in // 7) + 1
    return max(1, week)


# ---------------------------------------------------------------------------
# k_prior values by stat stability tier and season week
# ---------------------------------------------------------------------------

# Map stat names → stability tier
_STABLE_STATS = {
    "k_rate", "k_pct", "bb_rate", "bb_pct", "swstr_pct", "csw_pct",
    "o_swing", "z_swing", "strikeouts",
}
_MID_TIER_STATS = {
    "xwoba", "sc_xwoba", "woba", "wrc_plus", "fip", "xfip",
    "k_rate_career", "bb_rate_career",
}
_VOLATILE_STATS = {
    "hr_fb_pct", "iso", "sc_xslg", "sc_barrel_rate", "era",
    "babip", "sc_hard_hit_rate", "sc_exit_velo",
    "total_bases", "rbis", "earned_runs", "hits_allowed",
    "h_rate", "hits", "tb_rate",
}


def get_k_prior(stat: str, week: int | None = None) -> int:
    """
    Return the k_prior value for Bayesian shrinkage.

    Higher k → stronger pull toward prior (less trust in current season data).
    Lower k  → more trust in current season data.

    Args:
        stat: stat name or prop type (e.g. "k_rate", "xwoba", "iso")
        week: season week (auto-detected if None)
    """
    if week is None:
        week = get_season_week()

    stat_lower = stat.lower()

    if stat_lower in _STABLE_STATS:
        # K%, BB%, CSW% stabilize quickest (~80-120 PA)
        return 10 if week <= 2 else 6 if week <= 4 else 3
    elif stat_lower in _MID_TIER_STATS:
        # xwOBA, wRC+, FIP stabilize mid-range (~150-250 PA/BF)
        return 18 if week <= 2 else 12 if week <= 4 else 6
    elif stat_lower in _VOLATILE_STATS:
        # HR, ISO, ERA stabilize slowly (~300+ PA/BF)
        return 30 if week <= 2 else 20 if week <= 4 else 10
    else:
        # Default: mid-tier
        return 15 if week <= 2 else 10 if week <= 4 else 5


def get_current_weight_cap(week: int | None = None) -> float:
    """
    Return the maximum weight to give current-season data.

    Week 1-2:  max 20% current / 80% prior
    Week 3-4:  max 40% current / 60% prior
    Week 5-8:  max 60% current / 40% prior
    Week 9+:   up to 100% current (full trust earned)
    """
    if week is None:
        week = get_season_week()
    if week <= 2:
        return 0.20
    elif week <= 4:
        return 0.40
    elif week <= 8:
        return 0.60
    else:
        return 1.00


# ---------------------------------------------------------------------------
# Prior computation: 3-year weighted average
# ---------------------------------------------------------------------------

def compute_prior(
    rate_last_year: float,
    rate_year2:     float | None = None,
    rate_career:    float | None = None,
) -> float:
    """
    Compute a 3-year weighted prior.

    Weights: 50% last year, 30% year-2, 20% career (or year-3).
    If year-2 or career are missing, normalize weights among available years.

    Args:
        rate_last_year: rate from most recent completed season (2025)
        rate_year2:     rate from two seasons ago (2024) — optional
        rate_career:    career average or 3-season average — optional

    Returns:
        Weighted prior rate as float.
    """
    weights: list[tuple[float, float]] = [(rate_last_year, 0.50)]

    if rate_year2 is not None and rate_year2 > 0:
        weights.append((rate_year2, 0.30))
    if rate_career is not None and rate_career > 0:
        weights.append((rate_career, 0.20))

    total_weight = sum(w for _, w in weights)
    if total_weight <= 0:
        return rate_last_year

    return sum(r * w for r, w in weights) / total_weight


# ---------------------------------------------------------------------------
# Core shrinkage formula
# ---------------------------------------------------------------------------

def shrink_rate(
    n_current:    float,
    rate_current: float,
    rate_prior:   float,
    k_prior:      int,
) -> float:
    """
    Apply Bayesian shrinkage toward prior.

    Formula: proj = (n_current * rate_current + k_prior * rate_prior)
                    / (n_current + k_prior)

    Args:
        n_current:    current season PA/BF/IP (sample size)
        rate_current: current season rate (e.g. 0.28 for 28% K rate)
        rate_prior:   prior/baseline rate (3-year weighted average)
        k_prior:      shrinkage strength (higher = more regression)

    Returns:
        Shrunk rate blended toward prior.
    """
    if k_prior <= 0:
        return rate_current
    denom = n_current + k_prior
    if denom <= 0:
        return rate_prior
    proj = (n_current * rate_current + k_prior * rate_prior) / denom
    return round(proj, 6)


def shrink_rate_auto(
    n_current:    float,
    rate_current: float,
    rate_prior:   float,
    stat:         str,
    week:         int | None = None,
) -> float:
    """
    Shrink rate using auto-selected k_prior for stat type and season week.

    Convenience wrapper around shrink_rate() + get_k_prior().
    """
    if week is None:
        week = get_season_week()
    k = get_k_prior(stat, week)
    return shrink_rate(n_current, rate_current, rate_prior, k)


# ---------------------------------------------------------------------------
# Home/Away split shrinkage
# ---------------------------------------------------------------------------

def shrink_split(
    n_split:       float,
    rate_split:    float,
    rate_overall:  float,
    is_pitcher:    bool = False,
) -> float:
    """
    Shrink a home/away split toward the player's overall rate.

    Rule (from FanGraphs sabermetrics library):
      <50 PA/BF:  mostly ignore split (k=25 → ~80-95% overall)
      50-150:     mild adjustment (k=15)
      150-300:    moderate trust (k=8)
      300+:       trust split (k=4)

    Pitchers regress more slowly (stricter thresholds).

    Args:
        n_split:      PA (batter) or BF (pitcher) in this split
        rate_split:   rate in this specific venue/side
        rate_overall: player's overall career/season rate
        is_pitcher:   True = use pitcher thresholds (more conservative)

    Returns:
        Shrunk split rate.
    """
    if n_split < 1:
        return rate_overall

    if is_pitcher:
        if n_split < 80:
            k = 30
        elif n_split < 200:
            k = 20
        elif n_split < 400:
            k = 10
        else:
            k = 5
    else:
        if n_split < 50:
            k = 25
        elif n_split < 150:
            k = 15
        elif n_split < 300:
            k = 8
        else:
            k = 4

    return shrink_rate(n_split, rate_split, rate_overall, k)


# ---------------------------------------------------------------------------
# High-level: apply shrinkage to a prop dict's rates
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# PROP_TO_RATE: maps allowed prop types to their enrichment rate field
# Banned props (home_runs, walks, singles, doubles, triples,
# stolen_bases, walks_allowed) are intentionally excluded.
# ---------------------------------------------------------------------------
PROP_TO_RATE: dict[str, str] = {
    # Batter props
    "hits":            "h_rate",
    "total_bases":     "tb_rate",
    "rbis":            "woba",
    "runs":            "wrc_plus",
    "hits_runs_rbis":  "woba",
    "fantasy_hitter":  "woba",
    # Pitcher props
    "strikeouts":      "k_rate",
    "pitching_outs":   "k_rate",
    "earned_runs":     "era",
    "hits_allowed":    "h_rate",
    "fantasy_pitcher": "k_rate",
}


_STAT_FIELDS_PITCHER = [
    ("k_rate",   "k_rate",   "strikeouts"),
    ("k_pct",    "k_pct",    "strikeouts"),
    ("csw_pct",  "csw_pct",  "csw_pct"),
    ("bb_pct",   "bb_pct",   "bb_pct"),
    ("era",      "era",      "era"),
    ("xfip",     "xfip",     "xfip"),
    ("fip",      "fip",      "fip"),
]

_STAT_FIELDS_BATTER = [
    ("wrc_plus", "wrc_plus", "wrc_plus"),
    ("woba",     "woba",     "woba"),
    ("sc_xwoba", "sc_xwoba", "xwoba"),
    ("sc_xslg",  "sc_xslg",  "sc_xslg"),
    ("sc_barrel_rate", "sc_barrel_rate", "sc_barrel_rate"),
    ("iso",      "iso",      "iso"),
    ("hr_fb_pct","hr_fb_pct","hr_fb_pct"),
    ("k_pct",    "k_pct",    "k_rate"),
    ("o_swing",  "o_swing",  "o_swing"),
    ("h_rate",   "h_rate",   "h_rate"),
    ("tb_rate",  "tb_rate",  "tb_rate"),
]


def apply_shrinkage_to_prop(
    prop:        dict,
    is_pitcher:  bool,
    n_current:   float,
    week:        int | None = None,
) -> dict:
    """
    Apply Bayesian shrinkage in-place to all rate fields in a prop dict.

    For each rate field on the prop:
      - Uses the field's *_prior sibling if present (e.g. k_rate_prior)
      - Falls back to league averages from confidence_shrinkage.LEAGUE_RATES
      - Applies shrink_rate_auto() with appropriate k_prior

    Args:
        prop:       enriched prop dict (modified in-place)
        is_pitcher: True for pitcher props
        n_current:  current season sample (PA for batters, BF for pitchers)
        week:       season week (auto-detected if None)

    Returns:
        Modified prop dict (same reference).
    """
    if week is None:
        week = get_season_week()

    fields = _STAT_FIELDS_PITCHER if is_pitcher else _STAT_FIELDS_BATTER

    # Import league averages for fallback priors
    try:
        from confidence_shrinkage import LEAGUE_RATES, LEAGUE_WOBA
        _LEAGUE_FALLBACKS = {
            "k_rate":          LEAGUE_RATES.get("K",  0.222),
            "k_pct":           LEAGUE_RATES.get("K",  0.222),
            "bb_pct":          LEAGUE_RATES.get("BB", 0.084),
            "csw_pct":         0.284,    # 2025 MLB average CSW%
            "era":             4.12,     # 2025 MLB ERA
            "xfip":            4.10,
            "fip":             4.10,
            "woba":            LEAGUE_WOBA,
            "sc_xwoba":        LEAGUE_WOBA,
            "wrc_plus":        100.0,    # league average by definition
            "sc_xslg":         0.401,    # 2025 MLB xSLG approx
            "sc_barrel_rate":  0.077,    # 2025 MLB barrel%
            "iso":             0.159,    # 2025 MLB ISO
            "hr_fb_pct":       0.125,    # 2025 MLB HR/FB%
            "o_swing":         0.316,    # 2025 MLB O-swing%
            "h_rate":          0.204,    # 2025 H/PA (not H/BIP) — 2025 actual
            "hits":            0.204,    # same
            "tb_rate":         0.350,    # 2025 TB/PA approx
        }
    except ImportError:
        _LEAGUE_FALLBACKS = {}

    for (field, prior_field, stat_key) in fields:
        current_val = prop.get(field)
        if current_val is None or (isinstance(current_val, (int, float)) and current_val <= 0):
            continue  # no current data to shrink

        # Get prior: look for _prior sibling first, then league fallback
        prior_key  = f"{prior_field}_prior"
        prior_val  = prop.get(prior_key)
        if prior_val is None or (isinstance(prior_val, (int, float)) and prior_val <= 0):
            prior_val = _LEAGUE_FALLBACKS.get(field)
        if prior_val is None or prior_val <= 0:
            continue  # can't shrink without a prior

        shrunk = shrink_rate_auto(
            n_current=n_current,
            rate_current=float(current_val),
            rate_prior=float(prior_val),
            stat=stat_key,
            week=week,
        )
        prop[field] = shrunk

    prop["_shrinkage_week"]    = week
    prop["_shrinkage_n"]       = n_current
    prop["_shrinkage_applied"] = True
    return prop
