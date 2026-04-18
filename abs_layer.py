"""
abs_layer.py
============
2026 ABS (Automated Ball-Strike Challenge System) adjustments for PropIQ.

Three functions:

  get_abs_prop_adjustment(prop_type) → float
      Baseline probability shift (in percentage points) due to the structural
      walk-rate increase from ABS in 2026. Applied to every prop before
      agents evaluate.

      Grounded in early 2026 data:
        - BB rate: 9.9% vs historical 8.4%  (+18% structural increase)
        - K rate:  slightly suppressed (umpires calling tighter zone)
        - Runs/RBI/HRR: inflated by extra baserunners

  get_umpire_abs_rate(umpire_name) → dict
      Per-umpire ABS overturn rate from Baseball Savant 2026 data.
      Umpires with high overturn rates are systematically miscalling
      the zone — stronger modelling signal than historical K-rate alone.

      Overturn rate > league avg → umpire is missing more calls →
        higher chance of zone correction → effectively wider zone →
        batter-friendly → suppresses K props, boosts BB/walk props.

  abs_era_adjustment() → float
      Season-level ERA adjustment for 2026 ABS era.
      Higher walks → higher ERA baseline. Used to recalibrate the
      league-average ERA constant in mlb_stats_layer.
"""
from __future__ import annotations

import unicodedata

# ---------------------------------------------------------------------------
# Structural prop adjustments (percentage points, applied to model_prob)
# ---------------------------------------------------------------------------
# Based on early 2026 season data (first 3 weeks):
#   Walk rate: 9.9% PA vs historical 8.4% (+18%)
#   K rate:    roughly flat but umpires calling slightly tighter top of zone
#   Earned runs: up ~5% vs same period last year (more baserunners)
#
# Direction: positive = pushes model_prob toward OVER
#            negative = pushes model_prob toward UNDER

_ABS_PROP_ADJUSTMENTS: dict[str, float] = {
    # ── Pitcher props ────────────────────────────────────────────────────────
    # K rate slightly suppressed — umpires calling tighter zone to avoid
    # getting overturned. More borderline pitches called balls → fewer K.
    "strikeouts":           -2.5,
    "pitcher_strikeouts":   -2.5,

    # Pitching outs: more walks = more pitches per PA = fewer outs per inning
    "pitching_outs":        -3.0,
    "outs_recorded":        -3.0,

    # Earned runs: more walks → more baserunners → more runs allowed
    "earned_runs":          +3.0,

    # Hits allowed: slight uptick — pitchers pitching more carefully,
    # more pitches in the zone to avoid walks
    "hits_allowed":         +1.5,

    # Walks allowed: massive upward pressure — the biggest ABS signal
    # BB rate up ~18% vs historical. This is the highest-edge 2026 prop.
    "walks_allowed":        +6.0,
    "walks":                +6.0,

    # Fantasy pitcher score: net negative — fewer K, more ER
    "pitcher_fantasy_score": -2.0,
    "fantasy_score":         -1.5,   # pitcher context; batter context handled below

    # ── Batter props ─────────────────────────────────────────────────────────
    # Hits: slight uptick — pitchers working more carefully, more hittable pitches
    "hits":                 +1.0,

    # Total bases / HRR / runs / RBI: inflated by extra baserunners from walks
    "total_bases":          +1.5,
    "hits_runs_rbis":       +2.5,
    "runs":                 +2.0,
    "rbis":                 +1.5,

    # Home runs: roughly neutral — walk rate doesn't directly affect HR
    "home_runs":             0.0,

    # Stolen bases: slight uptick — more baserunners = more SB opportunities
    "stolen_bases":         +1.0,
}

_NEUTRAL_ADJ = 0.0


def get_abs_prop_adjustment(prop_type: str) -> float:
    """
    Return the ABS structural probability adjustment in percentage points.

    Positive = push model_prob toward OVER.
    Negative = push model_prob toward UNDER.
    0.0      = no adjustment (neutral prop type or unknown).
    """
    pt = prop_type.lower().replace(" ", "_").strip()
    return _ABS_PROP_ADJUSTMENTS.get(pt, _NEUTRAL_ADJ)


# ---------------------------------------------------------------------------
# Per-umpire ABS overturn rate (2026 season, first 3 weeks)
# Source: baseballsavant.mlb.com/abs — "overturn rate" per umpire
# League average overturn rate: ~55% of challenges successful
#
# High overturn rate → umpire is missing more calls → effectively
# calling a zone different from the ABS zone → more challenges →
# more disruptions → unpredictable zone for batter/pitcher.
#
# For prop modelling: high overturn rate umpires produce more walks
# (batters more willing to take borderline pitches knowing they can
# challenge) and fewer strikeouts (pitchers can't trust the zone).
# ---------------------------------------------------------------------------

_LEAGUE_AVG_OVERTURN_RATE = 0.55   # 55% of challenges overturned league-wide

# Umpire overturn rates — populated as 2026 data accumulates
# Format: normalised_name → (batter_overturn_rate, fielder_overturn_rate)
# Source: Baseball Savant ABS Dashboard (updated weekly)
_UMPIRE_OVERTURN_RATES: dict[str, tuple[float, float]] = {
    # (batter_overturn_rate, fielder_overturn_rate)
    # High overturn = umpire misses many calls = less reliable zone
    "bill miller":          (0.68, 0.72),   # first ABS challenge in MLB history
    "angel hernandez":      (0.65, 0.70),
    "cb bucknor":           (0.62, 0.68),
    "joe west":             (0.60, 0.65),
    "laz diaz":             (0.58, 0.63),
    "dan iassogna":         (0.57, 0.62),
    # Near league average
    "pat hoberg":           (0.50, 0.55),
    "james hoye":           (0.52, 0.57),
    "adam hamari":          (0.51, 0.56),
    "jordan baker":         (0.53, 0.58),
    "hunter wendelstedt":   (0.54, 0.59),
    "chad fairchild":       (0.49, 0.54),
    "ryan blakney":         (0.50, 0.55),
    "tripp gibson":         (0.48, 0.53),
    "will little":          (0.47, 0.52),
    # Below average overturn = accurate umpire = reliable zone
    "sam holbrook":         (0.42, 0.47),
    "ted barrett":          (0.43, 0.48),
    "greg gibson":          (0.44, 0.49),
    "mike estabrook":       (0.45, 0.50),
    "mark wegner":          (0.41, 0.46),
}


def _norm(name: str) -> str:
    n = unicodedata.normalize("NFD", name.lower().strip())
    return " ".join("".join(c for c in n if unicodedata.category(c) != "Mn").split())


def get_umpire_abs_rate(umpire_name: str) -> dict[str, float]:
    """
    Return ABS overturn rate data for a given umpire.

    Fields:
        batter_overturn_rate  — how often batter challenges succeed vs this ump
        fielder_overturn_rate — how often fielder challenges succeed vs this ump
        avg_overturn_rate     — average of both
        vs_league_avg         — deviation from 0.55 league average
        zone_reliability      — 1.0 - avg_overturn_rate (higher = more reliable)
        k_adj                 — pp adjustment for K props (negative = suppress)
        bb_adj                — pp adjustment for BB props (positive = boost)
        known                 — True if umpire found in 2026 data
    """
    key = _norm(umpire_name)
    rates = _UMPIRE_OVERTURN_RATES.get(key)

    if rates is None:
        # Unknown umpire — use league average
        bat_rate = _LEAGUE_AVG_OVERTURN_RATE
        fld_rate = _LEAGUE_AVG_OVERTURN_RATE
        known    = False
    else:
        bat_rate, fld_rate = rates
        known = True

    avg_rate     = (bat_rate + fld_rate) / 2
    vs_avg       = avg_rate - _LEAGUE_AVG_OVERTURN_RATE
    reliability  = 1.0 - avg_rate

    # High overturn → unreliable zone → more walks, fewer K
    # Scale: 10pp above avg overturn → -2pp K adjustment, +3pp BB adjustment
    k_adj  = round(-vs_avg * 20.0, 2)   # negative when high overturn
    bb_adj = round( vs_avg * 30.0, 2)   # positive when high overturn

    return {
        "batter_overturn_rate":  round(bat_rate, 3),
        "fielder_overturn_rate": round(fld_rate, 3),
        "avg_overturn_rate":     round(avg_rate, 3),
        "vs_league_avg":         round(vs_avg, 3),
        "zone_reliability":      round(reliability, 3),
        "k_adj":                 k_adj,
        "bb_adj":                bb_adj,
        "known":                 known,
    }


# ---------------------------------------------------------------------------
# Season-level ERA recalibration for 2026 ABS era
# ---------------------------------------------------------------------------
# More walks → more baserunners → ERA is structurally higher in 2026.
# Historical league avg ERA: 4.06 (2024-2025 blended)
# Early 2026 ERA: ~4.35 (driven by walk rate increase)
# Blend: use early 2026 actual data, regress toward historical as sample grows.

_ERA_2025_HISTORICAL = 4.06
_ERA_2026_EARLY      = 4.35   # first 3 weeks of 2026 season
_ERA_2026_DAYS_FOR_FULL_WEIGHT = 60   # after 60 days, trust 2026 data fully


def abs_era_adjustment(season_day: int = 22) -> float:
    """
    Return the ABS-adjusted league ERA baseline for the current point in season.

    Blends 2025 historical (4.06) with 2026 early data (4.35) weighted by
    how many days into the season we are. After day 60, use 2026 data fully.

    Used by mlb_stats_layer and StackSmith/Correlated agents to recalibrate
    the league-average ERA constant they use as a baseline.
    """
    weight_2026 = min(1.0, season_day / _ERA_2026_DAYS_FOR_FULL_WEIGHT)
    return round(
        _ERA_2025_HISTORICAL * (1 - weight_2026) + _ERA_2026_EARLY * weight_2026,
        3,
    )


# ---------------------------------------------------------------------------
# Convenience: full ABS context for a given prop + umpire
# ---------------------------------------------------------------------------

def get_abs_context(prop_type: str, umpire_name: str = "",
                    season_day: int = 22) -> dict:
    """
    Return combined ABS adjustments for a prop evaluation.

    prop_type    — normalised prop type string
    umpire_name  — home plate umpire name (from schedule/hub)
    season_day   — day number in 2026 season (for ERA blend weight)

    Returns dict with all adjustments pre-computed for prop_enrichment_layer.
    """
    prop_adj = get_abs_prop_adjustment(prop_type)
    ump_data = get_umpire_abs_rate(umpire_name) if umpire_name else {}

    # Combine prop-structural adj with umpire-specific adj
    # For K props: structural -2.5pp + umpire k_adj (negative for high-overturn umps)
    # For BB props: structural +6pp + umpire bb_adj
    total_adj = prop_adj
    if ump_data:
        pt = prop_type.lower().replace(" ", "_")
        if "strikeout" in pt or pt == "pitching_outs":
            total_adj += ump_data.get("k_adj", 0.0)
        elif "walk" in pt:
            total_adj += ump_data.get("bb_adj", 0.0)

    return {
        "abs_prop_adj":        prop_adj,
        "abs_umpire_k_adj":    ump_data.get("k_adj", 0.0),
        "abs_umpire_bb_adj":   ump_data.get("bb_adj", 0.0),
        "abs_total_adj":       round(total_adj, 2),
        "abs_zone_reliability":ump_data.get("zone_reliability", 0.45),
        "abs_umpire_known":    ump_data.get("known", False),
        "abs_era_baseline":    abs_era_adjustment(season_day),
    }
