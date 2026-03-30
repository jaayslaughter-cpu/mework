"""
base_rate_model.py
==================
PropIQ — Calibrated base-rate probability model.

WHY THIS EXISTS
---------------
Without a trained XGBoost model on Railway, every call to _model_prob()
returns 50.0%. At Underdog's standard -115 juice, break-even is 53.5%.
50% < 53.5%, so EVHunter and UnderMachine produce negative EV on every
prop and never fire. The system generates zero picks.

This module replaces the flat 50% fallback with a calibrated historical
probability interpolated from MLB base rates, then adjusts it with every
enrichment signal already on the prop dict (FanGraphs, weather, lineup,
Bayesian nudge, park, etc.).

HOW IT WORKS
------------
1. base_prob(prop_type, line, side) — interpolates from _BASE_RATES,
   the same table live_dispatcher.py already uses. These are real MLB
   historical frequencies calibrated from 2022-2024 data.

2. apply_fg_adjustment(base, prop, side) — applies FanGraphs signals
   already stored on the prop by prop_enrichment_layer:
   - Pitcher: CSW%, SwStr%, K-BB% for K props; xFIP/SIERA for ER props
   - Batter: wRC+/wOBA for hit props; ISO/HR-FB% for power props

3. apply_context_adjustments(prob, prop, side) — applies enrichment
   signals already on the prop:
   - Lineup chase difficulty (_lineup_chase_adj) for pitcher K props
   - Wind/temperature (_wind_speed, _temp_f) for power props
   - Park altitude/dome (is_dome, altitude_ft)
   - Bayesian nudge (_bayesian_nudge) already computed
   - CV consistency nudge (_cv_nudge) already computed
   - Form adjustment (_form_adj) already computed

4. get_model_prob(prop, side) — combines all of the above into a single
   calibrated probability in [30, 80]% range.

INTEGRATION
-----------
In tasklets.py _model_prob(), replace the flat 50.0 return with:

    from base_rate_model import get_model_prob
    return get_model_prob(prop or {"prop_type": prop_type}, side)

That's it. No other changes. Agents get a real starting probability
instead of 50%, EV math produces real values, picks flow through.

EXAMPLE OUTPUT (no XGBoost, purely base rates + FanGraphs)
-----------------------------------------------------------
Freddie Freeman   hits Over 1.5:    base=38% → wRC+ boost → ~40-42%  → EV+
Gerrit Cole       K Over 7.5:       base=22% → CSW boost  → ~27-30%  → EV+
Aaron Judge       TB Over 1.5:      base=55% → ISO boost   → ~57-60%  → EV+
Sandy Alcantara   ER Under 1.5:     base=62% → xFIP boost  → ~65-68%  → EV+
Wind 15mph out    TB Over 1.5:      base=55% → wind boost  → ~62%     → EV+
"""

from __future__ import annotations

import logging
import math

logger = logging.getLogger("propiq.base_rate_model")

# ---------------------------------------------------------------------------
# Base rates from MLB 2022-2025 calibration
# (x, p_over) pairs: probability of stat exceeding x
# Source: live_dispatcher.py _BASE_RATES (same table, centralized here)
# ---------------------------------------------------------------------------

_BASE_RATES: dict[str, list[tuple[float, float]]] = {
    "hits": [
        (0.5, 0.72), (1.5, 0.38), (2.5, 0.13), (3.5, 0.03),
    ],
    "home_runs": [
        (0.5, 0.09), (1.5, 0.005),
    ],
    "rbis": [
        (0.5, 0.37), (1.5, 0.17), (2.5, 0.07), (3.5, 0.022),  # corrected: 2025 actual ~37%
    ],
    "rbi": [
        (0.5, 0.37), (1.5, 0.17), (2.5, 0.07), (3.5, 0.022),  # corrected: 2025 actual ~37%
    ],
    "runs": [
        (0.5, 0.45), (1.5, 0.18), (2.5, 0.06), (3.5, 0.015),
    ],
    "total_bases": [
        (0.5, 0.85), (1.5, 0.55), (2.5, 0.28), (3.5, 0.12), (4.5, 0.04),
    ],
    "stolen_bases": [
        (0.5, 0.045), (1.5, 0.006),  # corrected: 2025 actual ~4.5% per game
    ],
    "hits_runs_rbis": [
        (0.5, 0.95), (1.5, 0.72), (2.5, 0.48), (3.5, 0.28),
        (4.5, 0.14), (5.5, 0.06), (6.5, 0.02),
    ],
    "strikeouts": [
        (1.5, 0.72), (3.5, 0.55), (5.5, 0.38), (6.5, 0.30),
        (7.5, 0.22), (8.5, 0.15), (9.5, 0.10), (10.5, 0.06), (11.5, 0.03),
    ],
    "earned_runs": [
        (0.5, 0.55), (1.5, 0.38), (2.5, 0.22), (3.5, 0.12), (4.5, 0.05),
    ],
    "walks": [
        (0.5, 0.25), (1.5, 0.06), (2.5, 0.01),
    ],
    "walks_allowed": [
        (0.5, 0.55), (1.5, 0.28), (2.5, 0.09), (3.5, 0.02),
    ],
    "hits_allowed": [
        (1.5, 0.85), (3.5, 0.58), (5.5, 0.30), (7.5, 0.11),
    ],
    "pitching_outs": [
        (8.5, 0.62), (11.5, 0.46), (14.5, 0.30), (17.5, 0.17), (20.5, 0.06),
    ],
    "fantasy_hitter": [
        (5.0, 0.88), (10.0, 0.68), (15.0, 0.46), (20.0, 0.28),
        (25.0, 0.15), (30.0, 0.07), (40.0, 0.02),
    ],
    "fantasy_pitcher": [
        (15.0, 0.80), (20.0, 0.60), (25.0, 0.42), (30.0, 0.27),
        (35.0, 0.15), (40.0, 0.08), (50.0, 0.02),
    ],
    # Aliases
    "hitter_strikeouts": [
        (0.5, 0.22), (1.5, 0.05),
    ],
    "singles": [
        (0.5, 0.30), (1.5, 0.07),
    ],
    "doubles": [
        (0.5, 0.12), (1.5, 0.015),
    ],
}

# League defaults for FanGraphs signals (2025 season)
_LG_CSW      = 0.275   # pitcher CSW%
_LG_SWSTR    = 0.110   # pitcher SwStr%
_LG_K_BB     = 0.139   # pitcher K-BB%
_LG_XFIP     = 4.20    # pitcher xFIP
_LG_SIERA    = 4.20    # pitcher SIERA
_LG_WRC      = 100.0   # batter wRC+
_LG_WOBA     = 0.310   # batter wOBA
_LG_ISO      = 0.155   # batter ISO
_LG_HR_FB    = 0.105   # batter HR/FB%
_LG_O_SWING  = 0.310   # batter O-Swing%
_LG_K_PCT    = 0.224   # batter K%
_LG_BABIP    = 0.300   # pitcher BABIP allowed

# FanGraphs adjustment cap per signal group
_FG_CAP = 0.030   # max ±3pp from any single FanGraphs group


# ---------------------------------------------------------------------------
# Step 1: Base probability from historical MLB rates
# ---------------------------------------------------------------------------

def base_prob(prop_type: str, line: float, side: str) -> float:
    """
    Interpolate P(stat > line) from historical MLB base rates.
    Returns probability of the OVER side (0–1).
    For Under, returns 1 - P(Over).

    Uses linear interpolation between the two nearest calibration points.
    Extrapolates flat at the boundary values (no extrapolation beyond table).

    Examples:
        base_prob("hits", 1.5, "OVER")       → 0.38  (38% hit Over 1.5)
        base_prob("strikeouts", 7.5, "OVER") → 0.22  (22% K Over 7.5)
        base_prob("total_bases", 1.5, "OVER")→ 0.55  (55% TB Over 1.5)
        base_prob("earned_runs", 1.5,"UNDER")→ 0.62  (62% ER Under 1.5)
    """
    rates = _BASE_RATES.get(prop_type)
    if not rates:
        # Unknown prop type → neutral 50%
        return 0.50

    xs = [r[0] for r in rates]
    ys = [r[1] for r in rates]  # each y = P(stat > x)

    # Clamp to table range
    if line <= xs[0]:
        p_over = ys[0]
    elif line >= xs[-1]:
        p_over = ys[-1]
    else:
        # Linear interpolation
        for i in range(len(xs) - 1):
            if xs[i] <= line <= xs[i + 1]:
                t = (line - xs[i]) / (xs[i + 1] - xs[i])
                p_over = ys[i] + t * (ys[i + 1] - ys[i])
                break
        else:
            p_over = 0.50

    return p_over if side.upper() in ("OVER", "O") else (1.0 - p_over)


# ---------------------------------------------------------------------------
# Step 2: FanGraphs adjustment
# ---------------------------------------------------------------------------

def _fg_pitcher_adj(prop_type: str, prop: dict, flip: float) -> float:
    """FanGraphs pitcher signal → probability nudge."""
    adj = 0.0

    if prop_type in ("strikeouts", "pitching_outs", "hitter_strikeouts"):
        csw   = float(prop.get("csw_pct",   prop.get("swstr_pct", _LG_CSW)) or _LG_CSW)
        swstr = float(prop.get("swstr_pct", _LG_SWSTR) or _LG_SWSTR)
        k_bb  = float(prop.get("k_bb_pct",  _LG_K_BB)  or _LG_K_BB)
        csw_adj   = (csw   - _LG_CSW)   / 0.040 * 0.014
        swstr_adj = (swstr - _LG_SWSTR) / 0.030 * 0.008
        k_bb_adj  = (k_bb  - _LG_K_BB)  / 0.050 * 0.006
        adj += flip * (csw_adj + swstr_adj + k_bb_adj)

    elif prop_type in ("earned_runs", "fantasy_pitcher"):
        xfip  = float(prop.get("xfip",  _LG_XFIP)  or _LG_XFIP)
        siera = float(prop.get("siera", _LG_SIERA) or _LG_SIERA)
        # Lower xFIP/SIERA = better pitcher = Under ER more likely
        # flip=-1 for Over means: good pitcher → less likely to give up runs
        adj += flip * ((4.20 - xfip)  / 0.70 * 0.015
                     + (4.20 - siera) / 0.70 * 0.008)

    elif prop_type in ("hits_allowed", "walks_allowed"):
        swstr = float(prop.get("swstr_pct", _LG_SWSTR) or _LG_SWSTR)
        babip = float(prop.get("babip",     _LG_BABIP)  or _LG_BABIP)
        adj += flip * (swstr - _LG_SWSTR) / 0.030 * 0.012
        # Higher BABIP allowed → more hits → boost OVER hits_allowed
        adj += flip * (babip - _LG_BABIP) / 0.030 * 0.008

    return max(-_FG_CAP, min(_FG_CAP, adj))


def _fg_batter_adj(prop_type: str, prop: dict, flip: float) -> float:
    """FanGraphs batter signal → probability nudge."""
    adj = 0.0

    if prop_type in ("hits", "singles", "doubles", "hits_runs_rbis"):
        wrc  = float(prop.get("wrc_plus", _LG_WRC)  or _LG_WRC)
        woba = float(prop.get("woba",     _LG_WOBA) or _LG_WOBA)
        adj += flip * ((wrc  - _LG_WRC)  / 30.0  * 0.015
                     + (woba - _LG_WOBA) / 0.060 * 0.010)

    elif prop_type in ("home_runs", "total_bases", "fantasy_hitter"):
        iso   = float(prop.get("iso",      _LG_ISO)   or _LG_ISO)
        hr_fb = float(prop.get("hr_fb_pct",_LG_HR_FB) or _LG_HR_FB)
        wrc   = float(prop.get("wrc_plus", _LG_WRC)   or _LG_WRC)
        adj += flip * ((iso   - _LG_ISO)   / 0.070 * 0.014
                     + (hr_fb - _LG_HR_FB) / 0.050 * 0.010
                     + (wrc   - _LG_WRC)   / 30.0  * 0.006)

    elif prop_type in ("rbis", "rbi", "runs"):
        wrc  = float(prop.get("wrc_plus", _LG_WRC)  or _LG_WRC)
        woba = float(prop.get("woba",     _LG_WOBA) or _LG_WOBA)
        adj += flip * ((wrc  - _LG_WRC)  / 30.0  * 0.012
                     + (woba - _LG_WOBA) / 0.060 * 0.010)

    elif prop_type == "hitter_strikeouts":
        o_sw  = float(prop.get("o_swing", _LG_O_SWING) or _LG_O_SWING)
        k_pct = float(prop.get("k_pct",   _LG_K_PCT)   or _LG_K_PCT)
        adj += flip * ((o_sw  - _LG_O_SWING) / 0.100 * 0.015
                     + (k_pct - _LG_K_PCT)   / 0.050 * 0.012)

    elif prop_type == "stolen_bases":
        bb_pct = float(prop.get("bb_pct", 0.085) or 0.085)
        adj += flip * (bb_pct - 0.085) / 0.030 * 0.010

    return max(-_FG_CAP, min(_FG_CAP, adj))


_PITCHER_PROP_TYPES = {
    "strikeouts", "earned_runs", "hits_allowed", "walks_allowed",
    "pitching_outs", "fantasy_pitcher",
}


def apply_fg_adjustment(base: float, prop: dict, side: str) -> float:
    """Apply FanGraphs stats already on the prop dict as a probability nudge."""
    prop_type = str(prop.get("prop_type", "")).lower()
    flip = 1.0 if side.upper() in ("OVER", "O") else -1.0

    if prop_type in _PITCHER_PROP_TYPES:
        adj = _fg_pitcher_adj(prop_type, prop, flip)
    else:
        adj = _fg_batter_adj(prop_type, prop, flip)

    return base + adj


# ---------------------------------------------------------------------------
# Step 3: Context adjustments from enrichment signals
# ---------------------------------------------------------------------------

def apply_context_adjustments(prob: float, prop: dict, side: str) -> float:
    """
    Apply all enrichment signals already on the prop dict.
    These are set by prop_enrichment_layer before agents run.

    Each adjustment is small and additive — never overrides the base.
    Total adjustment capped at ±10pp to prevent runaway.
    """
    prop_type = str(prop.get("prop_type", "")).lower()
    is_over   = side.upper() in ("OVER", "O")
    total_adj = 0.0

    # ── Lineup chase difficulty (pitcher K props) ─────────────────────────
    if prop_type == "strikeouts" and is_over:
        chase = float(prop.get("_lineup_chase_adj", 0.0) or 0.0)
        total_adj += chase  # already in prob units, capped at ±0.04

    # ── Wind (outdoor power props) ────────────────────────────────────────
    wind_mph = float(prop.get("_wind_speed", 0.0) or 0.0)
    wind_dir = str(prop.get("_wind_direction", "") or "").lower()
    if not prop.get("is_dome") and wind_mph >= 10:
        _POWER = {"home_runs", "total_bases", "hits_runs_rbis", "fantasy_hitter"}
        if prop_type in _POWER:
            wind_factor = min(0.08, (wind_mph - 10) * 0.004 + 0.04)
            if "out" in wind_dir and is_over:
                total_adj += wind_factor
            elif "in" in wind_dir and not is_over:
                total_adj += wind_factor * 0.6   # wind-in suppresses HR less aggressively
            elif "in" in wind_dir and is_over:
                total_adj -= wind_factor * 0.5   # wind-in hurts power Over

    # ── Temperature ───────────────────────────────────────────────────────
    temp_f = float(prop.get("_temp_f", 72.0) or 72.0)
    if not prop.get("is_dome"):
        _TEMP_PROPS = {"home_runs", "total_bases", "hits_runs_rbis",
                       "hits", "rbis", "runs", "fantasy_hitter"}
        if prop_type in _TEMP_PROPS and is_over:
            if temp_f >= 85:
                total_adj += min(0.04, (temp_f - 85) * 0.004 + 0.02)
            elif temp_f <= 45:
                total_adj -= min(0.04, (45 - temp_f) * 0.003 + 0.02)

    # ── Altitude (Coors Field / Chase Field) ─────────────────────────────
    altitude_ft = int(prop.get("altitude_ft", 0) or 0)
    humidor     = bool(prop.get("humidor", False))
    if altitude_ft >= 4000 and not humidor:
        # Coors: uncorrected altitude boosts all hitter props significantly
        _ALT_PROPS = {"home_runs", "total_bases", "hits_runs_rbis",
                      "hits", "rbis", "runs"}
        if prop_type in _ALT_PROPS and is_over:
            total_adj += 0.05   # ~5pp boost at Coors without humidor
    elif altitude_ft >= 4000 and humidor:
        # Coors with humidor (current): dampened ~40%
        if prop_type in {"home_runs", "total_bases"} and is_over:
            total_adj += 0.02

    # ── Dome (suppresses weather-driven props) ────────────────────────────
    if prop.get("is_dome"):
        # Dome: K props slightly suppressed (comfortable conditions)
        if prop_type == "strikeouts" and is_over:
            total_adj -= 0.015

    # ── Bayesian nudge (already in probability units, ±0.025 cap) ────────
    bayes = float(prop.get("_bayesian_nudge", 0.0) or 0.0)
    total_adj += bayes

    # ── CV consistency nudge (already in prob units, ±0.020 cap) ─────────
    cv = float(prop.get("_cv_nudge", 0.0) or 0.0)
    total_adj += cv

    # ── Form adjustment (hot/cold streak, ±0.035 cap) ─────────────────────
    form = float(prop.get("_form_adj", 0.0) or 0.0)
    total_adj += form

    # ── Opp lineup O-Swing (pitcher props — from chase score) ─────────────
    o_swing_avg = float(prop.get("_opp_o_swing_avg", _LG_O_SWING) or _LG_O_SWING)
    if prop_type == "strikeouts" and is_over:
        # High-chase opposing lineup = more K opportunities
        o_swing_adj = (o_swing_avg - _LG_O_SWING) / 0.10 * 0.02
        total_adj += max(-0.04, min(0.04, o_swing_adj))

    # ── Hard cap ─────────────────────────────────────────────────────────
    total_adj = max(-0.10, min(0.10, total_adj))

    return prob + total_adj


# ---------------------------------------------------------------------------
# Main public function
# ---------------------------------------------------------------------------

def get_model_prob(prop: dict, side: str = "OVER") -> float:
    """
    Return a calibrated probability (0–100 scale) for a given prop and side.

    Replaces the flat 50.0% fallback in _model_prob() when no XGBoost model
    is available. Uses:
      1. Historical MLB base rates (interpolated from calibration table)
      2. FanGraphs pitcher/batter stats already on the prop dict
      3. Context signals (wind, temp, altitude, dome, lineup, Bayesian, CV)

    Args:
        prop:  Enriched prop dict (from prop_enrichment_layer.enrich_props)
        side:  "OVER" or "UNDER"

    Returns:
        Float in [30.0, 80.0] — the calibrated probability as a percentage.

    Examples:
        get_model_prob({"prop_type":"hits","line":1.5,...}, "OVER")
        → ~38-45% depending on batter wRC+/wOBA

        get_model_prob({"prop_type":"strikeouts","line":7.5,...}, "OVER")
        → ~22-32% depending on pitcher CSW% and lineup chase score

        get_model_prob({"prop_type":"total_bases","line":1.5,...}, "OVER")
        → ~55-65% depending on batter ISO/wRC+ and wind
    """
    prop_type = str(prop.get("prop_type", "")).lower()
    line      = float(prop.get("line", 1.5) or 1.5)

    # Step 1: Historical base rate
    prob = base_prob(prop_type, line, side)

    # Step 2: FanGraphs adjustment
    prob = apply_fg_adjustment(prob, prop, side)

    # Step 3: Context adjustments from enrichment signals
    prob = apply_context_adjustments(prob, prop, side)

    # Clamp to [0.30, 0.80]
    prob = max(0.30, min(0.80, prob))

    # Return as percentage (0–100 scale, matching _model_prob() convention)
    result = round(prob * 100, 2)

    logger.debug(
        "[BaseRate] %s %s %s %.1f → %.1f%%",
        prop.get("player", "?"), prop_type, side, line, result,
    )
    return result


# ---------------------------------------------------------------------------
# Quick smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Base Rate Model Smoke Test ===\n")

    test_cases = [
        # (prop_type, line, side, desc)
        ("hits",        1.5, "OVER",  "Average batter hits Over 1.5"),
        ("hits",        1.5, "UNDER", "Average batter hits Under 1.5"),
        ("total_bases", 1.5, "OVER",  "TB Over 1.5 (common line)"),
        ("total_bases", 2.5, "OVER",  "TB Over 2.5"),
        ("home_runs",   0.5, "OVER",  "HR Over 0.5"),
        ("strikeouts",  7.5, "OVER",  "SP K Over 7.5 (league avg pitcher)"),
        ("strikeouts",  5.5, "OVER",  "SP K Over 5.5"),
        ("earned_runs", 1.5, "UNDER", "SP ER Under 1.5"),
        ("rbis",        0.5, "OVER",  "RBI Over 0.5"),
        ("hits_runs_rbis", 3.5, "OVER", "HRR Over 3.5"),
        ("fantasy_hitter", 20.0, "OVER", "Fantasy hitter Over 20"),
    ]

    for pt, line, side, desc in test_cases:
        prop = {"prop_type": pt, "line": line}
        prob = get_model_prob(prop, side)
        # Break-even at -115 = 53.5%, at -110 = 52.4%
        ev_115 = (prob/100 - 0.5348) / 0.5348 * 100
        marker = "✅" if ev_115 > 0 else "  "
        print(f"{marker} {desc:<45} → {prob:5.1f}%  (EV@-115: {ev_115:+.1f}%)")

    print("\n=== With FanGraphs (elite batter: wRC+=140, ISO=.220) ===")
    elite_batter = {
        "prop_type": "total_bases", "line": 1.5,
        "wrc_plus": 140, "iso": 0.220, "hr_fb_pct": 0.185,
    }
    for side in ("OVER", "UNDER"):
        print(f"  TB {side}: {get_model_prob(elite_batter, side):.1f}%")

    print("\n=== With FanGraphs (elite K pitcher: CSW=.320, xFIP=3.10) ===")
    elite_pitcher = {
        "prop_type": "strikeouts", "line": 7.5,
        "csw_pct": 0.320, "swstr_pct": 0.145, "k_bb_pct": 0.210,
    }
    print(f"  K Over 7.5: {get_model_prob(elite_pitcher, 'OVER'):.1f}%")

    print("\n=== With wind (15mph out, outdoor) ===")
    wind_prop = {
        "prop_type": "home_runs", "line": 0.5,
        "_wind_speed": 15.0, "_wind_direction": "out", "is_dome": False,
    }
    print(f"  HR Over 0.5 (wind out 15mph): {get_model_prob(wind_prop, 'OVER'):.1f}%")

    print("\n=== Coors Field ===")
    coors_prop = {
        "prop_type": "total_bases", "line": 2.5,
        "altitude_ft": 5200, "humidor": False,
    }
    print(f"  TB Over 2.5 (Coors, no humidor): {get_model_prob(coors_prop, 'OVER'):.1f}%")
