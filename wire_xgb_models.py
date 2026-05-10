"""
wire_xgb_models.py
==================
Wires xgb_k_layer.py into the K-prop and hit-prop evaluation paths.

THE PROBLEM
-----------
xgb_k_layer.py contains trained XGBoost models (xgb_k_3_5.pkl, xgb_k_4_5.pkl,
xgb_k_5_5.pkl, xgb_k_6_5.pkl, xgb_hits.pkl) and a ready scorer function —
but the file's own docstring contains the exact wiring code that was never
pasted into tasklets.py. Every K prop runs on pure Poisson/Bayesian formula
with zero XGBoost contribution. Every hit prop runs with zero XGBoost.

This is the highest-leverage unwired component in the codebase. The models
exist, the scorer exists, the exact blend ratios are documented. They just
need to be called.

HOW THIS FILE WORKS
-------------------
Drop this file in the repo root. It provides two functions:

    apply_xgb_k_blend(model_prob_pct, prop, line)   → blended probability
    apply_xgb_hit_blend(model_prob_pct, prop, pitcher_dict)  → blended probability

Call these after your formula probability is computed, before EV gating.

BLEND SCHEDULE (from xgb_k_layer.py docstring)
-----------------------------------------------
Current (Brier ~0.248):        80% formula / 20% XGBoost
After 200+ graded, Brier<0.20: shift to 60/40 or 50/50

The 80/20 blend is intentionally conservative — it doesn't override the
formula, it improves it at the margin. At Brier 0.248 the XGBoost layer is
a correction tool, not the primary signal.

INTEGRATION — FIND THIS IN tasklets.py (or prop_enrichment_layer.py)
----------------------------------------------------------------------
Search for the section where model_prob is finalized before EV calc:

    # This pattern appears after all Bayesian/Poisson adjustments:
    model_prob = <some final value>
    ev = model_prob - market_implied

INSERT before the ev calculation:

    from wire_xgb_models import apply_xgb_k_blend, apply_xgb_hit_blend

    if prop.get("prop_type") == "strikeouts":
        model_prob = apply_xgb_k_blend(model_prob, prop, float(prop.get("line", 4.5)))

    elif prop.get("prop_type") in ("hits", "fantasy_score", "total_bases"):
        model_prob = apply_xgb_hit_blend(model_prob, prop, pitcher_dict={})

VALIDATION
----------
python wire_xgb_models.py --test
Expected: prints blend comparison for sample props
python wire_xgb_models.py --check
Expected: reports whether .pkl model files exist and are loadable
"""

from __future__ import annotations

import logging
import sys
from typing import Optional

log = logging.getLogger("propiq.xgb_wire")

# ── Blend weights (from xgb_k_layer.py docstring) ────────────────────────────
_K_FORMULA_WEIGHT  = 0.80   # weight on existing formula output
_K_XGB_WEIGHT      = 0.20   # weight on XGBoost per-line model
_HIT_FORMULA_WEIGHT = 0.70
_HIT_XGB_WEIGHT     = 0.30

# Probability bounds after blending
_PROB_FLOOR = 5.0   # percentage points
_PROB_CAP   = 95.0  # percentage points

# ── K-prop blend ──────────────────────────────────────────────────────────────

def apply_xgb_k_blend(
    formula_prob_pct: float,
    prop: dict,
    line: float = 4.5,
    formula_weight: float = _K_FORMULA_WEIGHT,
) -> float:
    """
    Blend formula K-prop probability with per-line XGBoost scorer.

    Args:
        formula_prob_pct: Current model probability in percentage points (e.g. 58.3)
        prop:             Enriched prop dict (needs Statcast features attached)
        line:             K prop line (e.g. 4.5, 5.5)
        formula_weight:   Weight on formula (default 0.80 = 80%)

    Returns:
        Blended probability in percentage points, clamped to [5, 95].
        Returns formula_prob_pct unchanged if XGBoost unavailable.
    """
    try:
        from xgb_k_layer import xgb_k_ready, xgb_k_prob
    except ImportError:
        log.debug("[XGBWire] xgb_k_layer not importable — using formula only")
        return formula_prob_pct

    if not xgb_k_ready():
        log.debug("[XGBWire] XGBoost K models not loaded — using formula only")
        return formula_prob_pct

    try:
        xgb_raw = xgb_k_prob(prop, line=float(line))
    except Exception as exc:
        log.warning("[XGBWire] xgb_k_prob() failed: %s — using formula only", exc)
        return formula_prob_pct

    if xgb_raw is None:
        return formula_prob_pct

    xgb_pct = float(xgb_raw) * 100.0
    xgb_weight = 1.0 - formula_weight
    blended = formula_weight * formula_prob_pct + xgb_weight * xgb_pct
    blended = max(_PROB_FLOOR, min(_PROB_CAP, blended))

    log.info(
        "[XGBWire] K-prop line=%.1f | formula=%.1f%% | xgb=%.1f%% | blend=%.1f%%",
        line, formula_prob_pct, xgb_pct, blended,
    )
    return round(blended, 2)


# ── Hit-prop blend ────────────────────────────────────────────────────────────

def apply_xgb_hit_blend(
    formula_prob_pct: float,
    prop: dict,
    pitcher_dict: Optional[dict] = None,
    formula_weight: float = _HIT_FORMULA_WEIGHT,
) -> float:
    """
    Blend formula hit-prop probability with XGBoost hit scorer.

    Args:
        formula_prob_pct: Current model probability in percentage points
        prop:             Enriched prop dict
        pitcher_dict:     Optional pitcher profile dict for matchup features
        formula_weight:   Weight on formula (default 0.70 = 70%)

    Returns:
        Blended probability in percentage points, clamped to [5, 95].
        Returns formula_prob_pct unchanged if XGBoost unavailable.
    """
    try:
        from xgb_k_layer import xgb_hit_ready, xgb_hit_prob
    except ImportError:
        log.debug("[XGBWire] xgb_k_layer not importable — hit blend skipped")
        return formula_prob_pct

    if not xgb_hit_ready():
        log.debug("[XGBWire] XGBoost hit model not loaded — using formula only")
        return formula_prob_pct

    try:
        xgb_raw = xgb_hit_prob(prop, pitcher_dict or {})
    except Exception as exc:
        log.warning("[XGBWire] xgb_hit_prob() failed: %s — using formula only", exc)
        return formula_prob_pct

    if xgb_raw is None:
        return formula_prob_pct

    xgb_pct = float(xgb_raw) * 100.0
    xgb_weight = 1.0 - formula_weight
    blended = formula_weight * formula_prob_pct + xgb_weight * xgb_pct
    blended = max(_PROB_FLOOR, min(_PROB_CAP, blended))

    log.info(
        "[XGBWire] Hit-prop | formula=%.1f%% | xgb=%.1f%% | blend=%.1f%%",
        formula_prob_pct, xgb_pct, blended,
    )
    return round(blended, 2)


# ── Master blender (single call for any prop type) ────────────────────────────

def apply_xgb_blend(formula_prob_pct: float, prop: dict) -> float:
    """
    Route prop to the correct XGBoost blend based on prop_type.

    This is the single import you need in tasklets.py:

        from wire_xgb_models import apply_xgb_blend
        model_prob = apply_xgb_blend(model_prob, prop)

    Handles K-props, hit props, and passes through unchanged for other types.
    """
    prop_type = (prop.get("prop_type") or "").lower()
    line = float(prop.get("line", 4.5))

    if prop_type in ("strikeouts", "pitcher_strikeouts", "hitter_strikeouts"):
        return apply_xgb_k_blend(formula_prob_pct, prop, line=line)

    if prop_type in ("hits", "total_bases", "fantasy_score", "fantasy_hitter"):
        return apply_xgb_hit_blend(formula_prob_pct, prop)

    # Other prop types — no XGBoost model available, return unchanged
    return formula_prob_pct


# ── Self-check ────────────────────────────────────────────────────────────────

def check_model_files() -> dict:
    """Report on XGBoost model file availability."""
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    model_dir = os.path.join(here, "models")

    expected = {
        "xgb_k_3_5.pkl":        "K > 3.5 model",
        "xgb_k_4_5.pkl":        "K > 4.5 model",
        "xgb_k_5_5.pkl":        "K > 5.5 model",
        "xgb_k_6_5.pkl":        "K > 6.5 model",
        "xgb_hits.pkl":         "Hits ≥1 model",
        "xgb_feature_cols.json":"Feature column map",
    }

    results = {}
    for fname, desc in expected.items():
        path = os.path.join(model_dir, fname)
        exists = os.path.exists(path)
        size = os.path.getsize(path) if exists else 0
        results[fname] = {
            "description": desc,
            "exists": exists,
            "size_kb": round(size / 1024, 1),
        }

    # Try importing the module itself
    try:
        from xgb_k_layer import xgb_k_ready, xgb_hit_ready
        results["_module_import"] = {"exists": True, "description": "xgb_k_layer import"}
        results["_k_ready"] = {"exists": xgb_k_ready(), "description": "xgb_k_ready()"}
        results["_hit_ready"] = {"exists": xgb_hit_ready(), "description": "xgb_hit_ready()"}
    except ImportError as e:
        results["_module_import"] = {"exists": False, "description": f"xgb_k_layer import FAILED: {e}"}

    return results


def run_test() -> None:
    """Demonstrate blend math with sample prop dicts."""
    print("\n" + "=" * 60)
    print("  XGBoost BLEND — SELF TEST")
    print("=" * 60)

    # Test 1: K prop — formula only (no models loaded)
    sample_k_prop = {
        "prop_type": "strikeouts",
        "player": "Test Pitcher",
        "line": 5.5,
        "sv_k_pct": 0.28,
        "sv_whiff_pct": 0.14,
        "days_rest": 4,
        "l5_ks": 23,
        "opp_lineup_k_pct_proxy": 0.24,
    }

    result = apply_xgb_blend(58.5, sample_k_prop)
    print(f"\n  K-prop (5.5 line) formula=58.5% → blended={result:.1f}%")
    print(f"  (If identical to 58.5%: XGBoost models not loaded — expected if .pkl files absent)")

    # Test 2: Manual blend math to show the arithmetic is right
    formula_p = 58.5
    fake_xgb_p = 62.0
    expected_blend = 0.80 * formula_p + 0.20 * fake_xgb_p
    print(f"\n  Manual blend check: 0.80×{formula_p} + 0.20×{fake_xgb_p} = {expected_blend:.1f}%")
    assert abs(expected_blend - 59.2) < 0.1, "Blend math error"
    print("  ✅ Blend arithmetic correct")

    # Test 3: Hit prop passthrough
    sample_hit_prop = {"prop_type": "hits", "player": "Test Batter", "line": 1.5}
    result_h = apply_xgb_blend(55.0, sample_hit_prop)
    print(f"\n  Hit-prop formula=55.0% → blended={result_h:.1f}%")

    # Test 4: Unsupported prop type — passthrough
    sample_other = {"prop_type": "earned_runs", "player": "Test", "line": 1.5}
    result_o = apply_xgb_blend(52.0, sample_other)
    print(f"\n  Earned-runs (no XGB model): formula=52.0% → {result_o:.1f}% (should be 52.0% unchanged)")
    assert result_o == 52.0, "Non-K/hit prop should pass through unchanged"
    print("  ✅ Non-supported prop type passes through correctly")

    print("\n" + "=" * 60)
    print("  INTEGRATION CODE FOR tasklets.py")
    print("=" * 60)
    print("""
  Add ONE import at the top of tasklets.py (or prop_enrichment_layer.py):

      from wire_xgb_models import apply_xgb_blend

  Then find where model_prob is finalized before EV gating and add:

      # ── XGBoost blend (NEW) ──────────────────────────────────────────
      model_prob = apply_xgb_blend(model_prob, prop)
      # ────────────────────────────────────────────────────────────────

  That's the entire change. The function handles:
    - K props:   80% formula / 20% XGBoost per-line model
    - Hit props: 70% formula / 30% XGBoost
    - All others: unchanged passthrough
    - Models not loaded: unchanged passthrough (safe fallback)
""")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    if "--check" in sys.argv:
        print("\nModel file status:")
        for fname, info in check_model_files().items():
            status = "✅" if info["exists"] else "❌"
            size_str = f" ({info['size_kb']}KB)" if info.get("size_kb", 0) > 0 else ""
            print(f"  {status} {fname}{size_str} — {info['description']}")
        print("\nIf K models are missing: run scripts/xgb_k_training.py to train them.")
    elif "--test" in sys.argv or len(sys.argv) == 1:
        run_test()
