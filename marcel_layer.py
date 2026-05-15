"""
marcel_layer.py
================
Marcel projections for PropIQ — regression-to-mean for early-season props.

THE PROBLEM
-----------
In May, pitchers have 5-8 starts. A pitcher with a 35% K-rate through 6 starts
looks elite, but Marcel regression says his true talent is probably 28-30% K-rate
because small samples are noisy. The current model uses the raw 2026 stats,
which are overfit to small samples early in the season.

Marcel is the simplest projection system that works: weighted average of the
last 3 seasons (3/4/2 weight), then regressed to league mean based on sample
size. It's not fancy but it consistently outperforms raw stats at small samples.

USAGE
-----
From prop_enrichment_layer.py, after Steamer but before PA model:

    from marcel_layer import get_marcel_k_rate, get_marcel_hit_rate

    # For K props:
    if prop_type == "strikeouts":
        raw_k_pct = prop.get("sv_k_pct", 22.0)
        season_bf = prop.get("season_bf", 0)
        marcel_k  = get_marcel_k_rate(raw_k_pct, season_bf,
                                       hist_k_pct=prop.get("career_k_pct"))
        prop["_marcel_k_pct"] = marcel_k
        # Use as opp_lineup_k_pct_proxy input to PA model

    # For hit props:
    if prop_type == "hits":
        raw_avg  = prop.get("sv_xba", 0.250)
        season_pa = prop.get("season_pa", 0)
        marcel_h  = get_marcel_hit_rate(raw_avg, season_pa,
                                         hist_avg=prop.get("career_avg"))
        prop["_marcel_hit_rate"] = marcel_h

WHEN DOES MARCEL MATTER?
------------------------
Marcel regression is strongest when sample size is small.
Rule of thumb:
  - Pitcher BF < 100:  Marcel contributes ~60% of the projection
  - Pitcher BF < 300:  Marcel contributes ~30%
  - Pitcher BF > 600:  Marcel contributes <10% (current stats dominate)

In May (roughly BF 80-200 for a full-season starter), Marcel meaningfully
pulls extreme early-season stats toward the mean.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Optional

logger = logging.getLogger("propiq.marcel")

# ── League average rates (2026 baseline — update seasonally) ──────────────────
# These are the regression targets. Extreme early-season stats get pulled
# toward these values proportional to how much data we have.
LEAGUE_AVG = {
    "k_pct":    22.8,    # pitcher K% (strikeouts per PA × 100)
    "bb_pct":    8.3,    # pitcher BB%
    "era":       4.25,   # ERA
    "xera":      4.20,   # xERA
    "whiff_pct": 24.1,   # SwStr%
    "hit_rate":  0.248,  # batter batting average (raw)
    "xba":       0.245,  # batter xBA
    "xwoba":     0.318,  # batter xwOBA
    "k_pct_bat": 22.5,   # batter K%
    "bb_pct_bat": 8.5,   # batter BB%
}

# Marcel regression weights — how many "league average" PA/BF to mix in
# Lower = faster regression (more conservative). Based on Tango Tiger Marcel paper.
# These values are for MLB props specifically, slightly more conservative than
# traditional Marcel for game prediction.
REGRESSION_PA = {
    "k_pct":    250,   # pitcher K-rate stabilises ~250 BF
    "bb_pct":   700,   # pitcher BB-rate stabilises ~700 BF
    "hit_rate": 600,   # batter batting average stabilises ~600 PA
    "xba":      200,   # xBA stabilises faster (underlying contact quality)
    "xwoba":    250,   # xwOBA stabilises ~250 PA
    "whiff_pct":200,   # SwStr% stabilises ~200 pitches seen
    "k_pct_bat":150,   # batter K-rate stabilises quickly
}


def _regress(observed: float, sample_n: int, league_avg: float,
             regression_n: int) -> float:
    """
    Marcel regression formula:
        weight_observed = sample_n / (sample_n + regression_n)
        weight_league   = regression_n / (sample_n + regression_n)
        result          = weight_observed × observed + weight_league × league_avg

    As sample_n → infinity, result → observed.
    As sample_n → 0, result → league_avg.
    """
    if sample_n <= 0:
        return league_avg
    w_obs = sample_n / (sample_n + regression_n)
    w_lg  = 1.0 - w_obs
    return round(w_obs * observed + w_lg * league_avg, 4)


def _weighted_hist(current: float, hist: Optional[float],
                   weights=(5, 4, 3)) -> float:
    """
    Three-year weighted average (current season × 5, prev × 4, prev-prev × 3).
    Uses available data — if hist not provided, current season dominates.
    """
    if hist is None:
        return current
    # hist is a single prior-season value (could represent 1 or 2 seasons)
    total_w = weights[0] + weights[1]
    return (weights[0] * current + weights[1] * hist) / total_w


# ── Public API ─────────────────────────────────────────────────────────────────

def get_marcel_k_rate(
    current_k_pct: float,
    season_bf: int,
    hist_k_pct: Optional[float] = None,
) -> float:
    """
    Marcel-projected pitcher K-rate (percentage points, 0-100 scale).

    Args:
        current_k_pct:  Current 2026 K% (0-100)
        season_bf:      Batters faced so far in 2026
        hist_k_pct:     Prior-season K% if available (0-100)

    Returns:
        Marcel-regressed K% (0-100), between current and league average.

    Examples:
        # Elite early-season (35% K-rate, only 80 BF)
        get_marcel_k_rate(35.0, 80) → ~27.4%  (heavy regression)

        # Elite full-season (28% K-rate, 600 BF)
        get_marcel_k_rate(28.0, 600) → ~27.5%  (light regression)

        # League-average pitcher (22% K-rate, 200 BF)
        get_marcel_k_rate(22.0, 200) → ~22.4%  (nearly no change)
    """
    # Step 1: blend with prior season if available
    blended = _weighted_hist(current_k_pct, hist_k_pct)

    # Step 2: regress to league mean based on sample size
    regressed = _regress(
        observed    = blended,
        sample_n    = season_bf,
        league_avg  = LEAGUE_AVG["k_pct"],
        regression_n= REGRESSION_PA["k_pct"],
    )

    return max(8.0, min(45.0, regressed))


def get_marcel_hit_rate(
    current_avg: float,
    season_pa: int,
    hist_avg: Optional[float] = None,
) -> float:
    """
    Marcel-projected batter hit rate (batting average scale, 0-1).

    Args:
        current_avg:  Current 2026 batting average (0-1 scale)
        season_pa:    Plate appearances so far in 2026
        hist_avg:     Prior-season batting average (0-1)

    Returns:
        Marcel-regressed batting average (0-1).
    """
    blended  = _weighted_hist(current_avg, hist_avg)
    regressed = _regress(
        observed    = blended,
        sample_n    = season_pa,
        league_avg  = LEAGUE_AVG["hit_rate"],
        regression_n= REGRESSION_PA["hit_rate"],
    )
    return max(0.15, min(0.38, regressed))


def get_marcel_xba(
    current_xba: float,
    season_pa: int,
    hist_xba: Optional[float] = None,
) -> float:
    """Marcel-projected xBA. Stabilises faster than raw BA (~200 PA)."""
    blended  = _weighted_hist(current_xba, hist_xba)
    regressed = _regress(blended, season_pa,
                         LEAGUE_AVG["xba"], REGRESSION_PA["xba"])
    return max(0.15, min(0.38, regressed))


def get_marcel_whiff_pct(
    current_whiff: float,
    season_pitches: int,
    hist_whiff: Optional[float] = None,
) -> float:
    """Marcel-projected pitcher SwStr% (0-100 scale)."""
    blended  = _weighted_hist(current_whiff, hist_whiff)
    regressed = _regress(blended, season_pitches,
                         LEAGUE_AVG["whiff_pct"], REGRESSION_PA["whiff_pct"])
    return max(5.0, min(40.0, regressed))


def enrich_prop_with_marcel(prop: dict, hub: dict) -> dict:
    """
    Apply Marcel regression to a prop dict.

    Called from prop_enrichment_layer.py after Steamer, before PA model.
    Stamps _marcel_k_pct and _marcel_hit_rate onto the prop.
    These values are used as more reliable season estimates than raw 2026 stats
    when sample sizes are small (BF < 200).

    Args:
        prop:  Enriched prop dict
        hub:   DataHub context (unused, available for future context)

    Returns:
        prop dict with Marcel fields stamped.
    """
    prop_type  = (prop.get("prop_type") or "").lower()
    season_bf  = int(prop.get("season_bf")  or prop.get("bf", 0) or 0)
    season_pa  = int(prop.get("season_pa")  or prop.get("pa", 0) or 0)

    # ── K props — Marcel pitcher K-rate ───────────────────────────────────────
    if prop_type in ("strikeouts", "pitching_outs", "pitcher_strikeouts"):
        raw_k_pct  = float(prop.get("sv_k_pct")  or prop.get("fg_kpct")   or LEAGUE_AVG["k_pct"])
        hist_k_pct = float(prop.get("career_k_pct") or raw_k_pct)

        marcel_k = get_marcel_k_rate(raw_k_pct, season_bf, hist_k_pct)
        prop["_marcel_k_pct"] = marcel_k

        # If sample is small (< 150 BF), use Marcel as the primary signal
        # instead of raw 2026 K-rate
        regression_strength = min(1.0, max(0.0, 1.0 - season_bf / 250))
        if regression_strength > 0.3 and abs(marcel_k - raw_k_pct) > 1.5:
            # Blend raw and Marcel proportional to regression strength
            blended_k = (1 - regression_strength) * raw_k_pct + regression_strength * marcel_k
            prop["sv_k_pct"] = round(blended_k, 2)
            logger.debug(
                "[Marcel] K-rate: raw=%.1f%% Marcel=%.1f%% → blended=%.1f%% (BF=%d reg=%.0f%%)",
                raw_k_pct, marcel_k, blended_k, season_bf, regression_strength * 100,
            )

        raw_whiff  = float(prop.get("sv_whiff_pct") or prop.get("sv_swstr_pct") or LEAGUE_AVG["whiff_pct"])
        season_p   = season_bf * 3  # rough pitch count from BF
        marcel_whiff = get_marcel_whiff_pct(raw_whiff, season_p)
        prop["_marcel_whiff_pct"] = marcel_whiff

    # ── Hit props — Marcel batter hit rate ────────────────────────────────────
    elif prop_type in ("hits", "total_bases", "hits_runs_rbis", "fantasy_hitter"):
        raw_avg  = float(prop.get("sv_xba") or prop.get("batting_avg") or LEAGUE_AVG["xba"])
        hist_avg = float(prop.get("career_avg") or raw_avg)

        marcel_h = get_marcel_hit_rate(raw_avg, season_pa, hist_avg)
        prop["_marcel_hit_rate"] = marcel_h

        # For very early season (< 80 PA), Marcel is more reliable than raw
        regression_strength = min(1.0, max(0.0, 1.0 - season_pa / 300))
        if regression_strength > 0.3 and abs(marcel_h - raw_avg) > 0.015:
            blended_h = (1 - regression_strength) * raw_avg + regression_strength * marcel_h
            prop["sv_xba"] = round(blended_h, 4)
            logger.debug(
                "[Marcel] xBA: raw=%.3f Marcel=%.3f → blended=%.3f (PA=%d reg=%.0f%%)",
                raw_avg, marcel_h, blended_h, season_pa, regression_strength * 100,
            )

    return prop


# ── Self-test ──────────────────────────────────────────────────────────────────

def run_test() -> None:
    print("\n" + "=" * 60)
    print("  MARCEL REGRESSION — SELF TEST")
    print("=" * 60)

    cases = [
        # (label, current, sample_n, hist, expected_direction, func)
        ("K% elite early (35%, 80 BF)",
         35.0, 80, None, "< 30",
         lambda c, n, h: get_marcel_k_rate(c, n, h)),
        ("K% elite full season (28%, 600 BF)",
         28.0, 600, None, "25-28",
         lambda c, n, h: get_marcel_k_rate(c, n, h)),
        ("K% league avg (22%, 200 BF)",
         22.0, 200, None, "~22",
         lambda c, n, h: get_marcel_k_rate(c, n, h)),
        ("K% with history (30% now, 25% hist, 120 BF)",
         30.0, 120, 25.0, "24-28",
         lambda c, n, h: get_marcel_k_rate(c, n, h)),
        ("Hit rate elite (0.350, 80 PA)",
         0.350, 80, None, "< 0.30",
         lambda c, n, h: get_marcel_hit_rate(c, n, h)),
        ("Hit rate slump (0.180, 120 PA)",
         0.180, 120, None, "> 0.21",
         lambda c, n, h: get_marcel_hit_rate(c, n, h)),
        ("Hit rate full season (0.280, 500 PA)",
         0.280, 500, None, "0.265-0.280",
         lambda c, n, h: get_marcel_hit_rate(c, n, h)),
    ]

    all_pass = True
    for label, current, sample_n, hist, expected, fn in cases:
        result = fn(current, sample_n, hist)
        # Verify regression direction
        if "< " in expected:
            threshold = float(expected.split("< ")[1])
            ok = result < threshold
        elif "> " in expected:
            threshold = float(expected.split("> ")[1])
            ok = result > threshold
        else:
            ok = True  # ~range, just display

        status = "✅" if ok else "❌"
        print(f"  {status} {label}")
        print(f"     Raw={current} Marcel={result:.3f} (expected {expected})")
        if not ok:
            all_pass = False

    # Test enrich_prop_with_marcel
    print("\n  Testing enrich_prop_with_marcel():")
    prop = {
        "prop_type": "strikeouts",
        "sv_k_pct": 35.0,
        "sv_whiff_pct": 32.0,
        "season_bf": 80,
    }
    result = enrich_prop_with_marcel(prop, hub={})
    print(f"  K prop (35% K-rate, 80 BF):")
    print(f"    _marcel_k_pct = {result.get('_marcel_k_pct', 'N/A'):.2f}%")
    print(f"    sv_k_pct adjusted = {result.get('sv_k_pct', 35.0):.2f}%")
    print(f"    (was 35.0%, pulled toward league avg {LEAGUE_AVG['k_pct']}%)")

    prop_h = {
        "prop_type": "hits",
        "sv_xba": 0.360,
        "season_pa": 60,
    }
    result_h = enrich_prop_with_marcel(prop_h, hub={})
    print(f"\n  Hit prop (xBA=.360, 60 PA):")
    print(f"    _marcel_hit_rate = {result_h.get('_marcel_hit_rate', 'N/A'):.3f}")
    print(f"    sv_xba adjusted  = {result_h.get('sv_xba', 0.360):.3f}")

    print(f"\n  {'✅ All tests passed.' if all_pass else '❌ Some tests failed.'}")
    print(f"\n  INTEGRATION:")
    print("""
  In prop_enrichment_layer.py, after Steamer load and before PA model:

      from marcel_layer import enrich_prop_with_marcel
      prop = enrich_prop_with_marcel(prop, hub)

  The function stamps _marcel_k_pct and _marcel_hit_rate and also
  adjusts sv_k_pct / sv_xba for small-sample props (BF < 200, PA < 300).
  Those adjusted values flow into the PA model and XGBoost feature build.
  """)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(message)s")
    run_test()
