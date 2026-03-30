"""
market_validator.py — Phase 91 Step 6
======================================
Validates model_prob against market implied probability after shrinkage.

Sharp books price props tightly. When our model diverges >12pp from market
the cause is almost always one of three things:
  1. Enrichment data error (FanGraphs fetch returned bad stats, lineup miss, etc.)
  2. Model overfit on a thin feature cluster
  3. A genuine massive edge (rare — true edges are usually 3-8pp, rarely >12pp)

Strategy:
  - ≤12pp divergence  → CLEAN  — pass through unchanged
  - 12-20pp divergence → WIDE   — flag + warn, no adjustment (monitor)
  - >20pp divergence   → EXTREME — hard soft-cap to 20pp above market;
                                   stamp _market_capped = True on prop
  - Wrong-side signal  → FLIP   — model and market disagree on which side is
                                   favored; flag regardless of magnitude

Flags are stamped on the prop dict for audit:
  prop["_market_divergence_pp"]   — signed float: model_prob - market_implied
  prop["_market_flag"]            — "CLEAN" | "WIDE" | "EXTREME" | "FLIP+WIDE" etc.
  prop["_market_capped"]          — True if soft-cap was applied
  prop["_market_capped_delta"]    — how many pp were trimmed by the cap

All values are in percentage-point space (model_prob in %, market_implied in %).
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
_WIDE_THRESHOLD    = 12.0   # pp — begin flagging
_EXTREME_THRESHOLD = 20.0   # pp — apply soft-cap
_NEUTRAL_CENTER    = 50.0   # pp — midpoint between OVER/UNDER


def compute_divergence(model_prob_pct: float, market_implied_pct: float) -> float:
    """
    Signed divergence in percentage points.
    Positive  → model more confident than market (model says more probable).
    Negative  → model less confident than market (model says less probable).
    """
    return round(model_prob_pct - market_implied_pct, 3)


def validate_market_alignment(
    model_prob_pct: float,
    market_implied_pct: float,
    prop_type: str = "",
    player: str = "",
) -> tuple[float, list[str], bool]:
    """
    Compare model probability to market implied probability.

    Parameters
    ----------
    model_prob_pct    : float  — model output after shrinkage, 0-100 scale
    market_implied_pct: float  — sportsbook implied probability, 0-100 scale
    prop_type         : str    — for logging only
    player            : str    — for logging only

    Returns
    -------
    (adjusted_prob_pct, flags, is_valid)
      adjusted_prob_pct : float — model_prob after any soft-cap applied (%)
      flags             : list[str] — zero or more flag strings
      is_valid          : bool  — False only if data is clearly corrupted
                                  (model_prob outside 1-99 range after all steps)
    """
    flags: list[str] = []
    adjusted = float(model_prob_pct)
    capped_delta = 0.0

    divergence = compute_divergence(model_prob_pct, market_implied_pct)

    # ── Wrong-side check ─────────────────────────────────────────────────────
    # Model favors OVER (>50%) but market has OVER as underdog (<50%), or vice versa.
    model_over  = model_prob_pct  > _NEUTRAL_CENTER
    market_over = market_implied_pct > _NEUTRAL_CENTER
    if model_over != market_over:
        flags.append("FLIP")
        logger.warning(
            "[MarketValidator] FLIP %s %s | model=%.1f%% market=%.1f%% "
            "(model and market disagree on favored side)",
            player, prop_type, model_prob_pct, market_implied_pct
        )

    abs_div = abs(divergence)

    # ── EXTREME divergence: soft-cap ─────────────────────────────────────────
    if abs_div > _EXTREME_THRESHOLD:
        flags.append("EXTREME")
        # Cap divergence at _EXTREME_THRESHOLD pp in the same direction
        capped_target = market_implied_pct + (
            _EXTREME_THRESHOLD if divergence > 0 else -_EXTREME_THRESHOLD
        )
        capped_delta  = round(capped_target - adjusted, 3)
        adjusted      = round(capped_target, 3)
        logger.warning(
            "[MarketValidator] EXTREME cap %s %s | raw=%.1f%% → capped=%.1f%% "
            "(market=%.1f%%, div=%.1fpp trimmed to %.0fpp)",
            player, prop_type, model_prob_pct, adjusted,
            market_implied_pct, abs_div, _EXTREME_THRESHOLD
        )

    # ── WIDE divergence: flag only ────────────────────────────────────────────
    elif abs_div > _WIDE_THRESHOLD:
        flags.append("WIDE")
        logger.info(
            "[MarketValidator] WIDE divergence %s %s | model=%.1f%% market=%.1f%% "
            "div=%.1fpp — flagged, not capped",
            player, prop_type, model_prob_pct, market_implied_pct, abs_div
        )

    # ── CLEAN ─────────────────────────────────────────────────────────────────
    else:
        flags.append("CLEAN")

    # Compose combined flag string (e.g. "FLIP+EXTREME")
    flag_str = "+".join(f for f in flags if f != "CLEAN") or "CLEAN"

    # Validity check — anything outside 1-99 after all adjustments is corrupted
    is_valid = 1.0 <= adjusted <= 99.0

    return adjusted, [flag_str], is_valid, divergence, capped_delta


def stamp_market_validation(
    prop: dict,
    model_prob_pct: float,
    market_implied_pct: float,
) -> tuple[float, bool]:
    """
    Convenience wrapper: runs validate_market_alignment(), stamps audit fields
    onto prop dict, and returns (adjusted_prob_pct, is_valid).
    """
    adjusted, flags, is_valid, divergence, capped_delta = validate_market_alignment(
        model_prob_pct=model_prob_pct,
        market_implied_pct=market_implied_pct,
        prop_type=prop.get("prop_type", ""),
        player=prop.get("player", ""),
    )
    prop["_market_divergence_pp"] = divergence
    prop["_market_flag"]          = flags[0]
    prop["_market_capped"]        = capped_delta != 0.0
    prop["_market_capped_delta"]  = capped_delta
    return adjusted, is_valid
